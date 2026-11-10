use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::compute::{cast, partition as equal_value_runs};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, exec_datafusion_err, exec_err};
use datafusion::execution::TaskContext;

use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{ParquetReadOptions, ident};
use datafusion_distributed_benchmarks::datasets::output::DatasetOutput;
use datafusion_distributed_benchmarks::datasets::{dataset_suite, table_columns};
use datafusion_distributed_iceberg::IcebergIntegrationOptions;
use datafusion_distributed_iceberg::iceberg;
use futures::StreamExt;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::DataFile;
use iceberg::spec::{DataFileFormat, Transform, UnboundPartitionField, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::clustered_writer::ClusteredWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use parquet::file::properties::WriterProperties;

#[expect(
    clippy::disallowed_types,
    reason = "the iceberg catalog API requires std::collections::HashMap"
)]
type CatalogProperties = std::collections::HashMap<String, String>;

/// The dataset directory used for the Iceberg representation of `input`:
/// `<root>/tpch_parquet/<key>=<value>/...` becomes
/// `<root>/tpch_iceberg/<key>=<value>/.../target_file_size=<bytes>`, so the path records every
/// parameter of both the Parquet generation and the conversion. With `partitioned`, the path gets
/// `partitioned=true/` just before `target_file_size`. That puts the partitioned and unpartitioned
/// datasets in separate folders, side by side.
pub(super) fn output_path(
    input: &Path,
    target_file_size: usize,
    partitioned: bool,
) -> Result<PathBuf, DataFusionError> {
    let input = input.to_string_lossy();
    let Some((root, parameters)) = input.rsplit_once("_parquet/") else {
        return exec_err!("{input} is not a <benchmark>_parquet dataset; pass --output");
    };
    let parameters = parameters.trim_end_matches('/');
    let partitioning = if partitioned { "partitioned=true/" } else { "" };
    Ok(PathBuf::from(format!(
        "{root}_iceberg/{parameters}/{partitioning}target_file_size={target_file_size}"
    )))
}

/// How much memory the sort for a partitioned table can use. Past that, the sort moves rows into
/// temporary files on disk. They go in the system temp folder, which `TMPDIR` sets.
const SORT_MEMORY_LIMIT: usize = 4 * 1024 * 1024 * 1024;

/// Converts the local Parquet tables made by `dfbench prepare` into Iceberg tables. By default it
/// reads and writes one input file at a time. With `partitioned`, a table that has a partition
/// column is sorted by that column, then written in one pass. The dataset is marked complete only
/// after every table is written.
pub(super) async fn convert_parquet_to_iceberg(
    source_dir: &Path,
    output: &DatasetOutput,
    target_file_size: usize,
    partitioned: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if target_file_size == 0 {
        return Err("target file size must be greater than zero".into());
    }
    if !source_dir.is_dir() {
        return Err(format!("source dataset does not exist: {}", source_dir.display()).into());
    }
    if output.is_complete().await? {
        return Ok(());
    }

    let table_dirs = fs::read_dir(source_dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|path| {
            path.is_dir()
                && !path
                    .file_name()
                    .unwrap()
                    .as_encoded_bytes()
                    .starts_with(b".")
        })
        .collect::<Vec<_>>();
    if table_dirs.is_empty() {
        return Err("conversion requires a non-empty Parquet dataset".into());
    }
    let source = source_dir.to_string_lossy();
    let suite = dataset_suite(&source);
    let mut tables = Vec::with_capacity(table_dirs.len());
    for table_path in table_dirs {
        let table_name = table_path
            .file_name()
            .unwrap()
            .to_str()
            .ok_or("Table name is not valid UTF-8")?
            .to_string();
        let partition_column = if partitioned {
            table_columns(suite, &table_name)?.partition_column
        } else {
            None
        };
        tables.push((table_path, table_name, partition_column));
    }
    if partitioned && tables.iter().all(|(_, _, column)| column.is_none()) {
        return Err(exec_datafusion_err!(
            "--partitioned found no table to partition: {suite} declares no partition column for \
             the tables in {source}"
        )
        .into());
    }
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(IcebergIntegrationOptions::default().storage_factory)
        .load(
            "benchmark",
            CatalogProperties::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), output.location())]),
        )
        .await?;
    let namespace = NamespaceIdent::new("benchmark".to_string());
    catalog
        .create_namespace(&namespace, CatalogProperties::new())
        .await?;

    // One stream per input file preserves the source file boundaries unless rolling
    // is requested. Execution-time partition counts are unrelated to conversion.
    let config = datafusion::prelude::SessionConfig::new().with_target_partitions(1);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(SORT_MEMORY_LIMIT, 1.0)
        .build_arc()?;
    let ctx = SessionContext::new_with_config_rt(config, runtime);

    for (table_path, table_name, partition_column) in tables {
        let files = parquet_files(&table_path)?;
        let plans = match partition_column {
            Some(column) if !files.is_empty() => {
                vec![sorted_parquet_plan(&ctx, files, column).await?]
            }
            _ => parquet_file_plans(&ctx, files).await?,
        };
        write_table(
            &catalog,
            &namespace,
            &table_name,
            plans,
            ctx.task_ctx(),
            output,
            TableLayout {
                target_file_size,
                partition_column,
            },
        )
        .await?;
    }
    output.mark_complete().await?;
    Ok(())
}

/// How to split one table into data files.
struct TableLayout {
    target_file_size: usize,
    /// The column to partition by. `None` writes the table unpartitioned.
    partition_column: Option<&'static str>,
}

async fn write_table(
    catalog: &impl Catalog,
    namespace: &NamespaceIdent,
    table_name: &str,
    plans: Vec<Arc<dyn ExecutionPlan>>,
    task_ctx: Arc<TaskContext>,
    output: &DatasetOutput,
    layout: TableLayout,
) -> Result<(), Box<dyn std::error::Error>> {
    let TableLayout {
        target_file_size,
        partition_column,
    } = layout;
    let Some(first_plan) = plans.first() else {
        return Err(exec_datafusion_err!("Parquet table {table_name} contains no files").into());
    };
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(first_plan.schema().as_ref())?;
    let mut partition_spec = UnboundPartitionSpec::builder();
    if let Some(column) = partition_column {
        let Some(source) = iceberg_schema.field_by_name(column) else {
            return Err(exec_datafusion_err!(
                "cannot partition {table_name} by {column}: no such column"
            )
            .into());
        };
        partition_spec = partition_spec.add_partition_field(
            UnboundPartitionField::builder()
                .source_ids(vec![source.id])
                .name(column.to_string())
                .transform(Transform::Identity)
                .build()?,
        )?;
    }
    let table_location = format!("{}/{table_name}", output.location());
    let table = catalog
        .create_table(
            namespace,
            TableCreation::builder()
                .name(table_name.to_string())
                .location(table_location)
                .schema(iceberg_schema)
                .partition_spec(partition_spec.build())
                .build(),
        )
        .await?;
    let arrow_schema_with_ids = Arc::new(schema_to_arrow_schema(
        table.metadata().current_schema().as_ref(),
    )?);

    let data_files = if let Some(column) = partition_column {
        write_partitioned(
            &table,
            table_name,
            &plans,
            &task_ctx,
            &arrow_schema_with_ids,
            target_file_size,
            column,
        )
        .await?
    } else {
        write_unpartitioned(
            &table,
            table_name,
            &plans,
            &task_ctx,
            &arrow_schema_with_ids,
            target_file_size,
        )
        .await?
    };

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let table = action.apply(tx)?.commit(catalog).await?;
    // These benchmark tables are immutable: keep the committed metadata at a fixed path.
    output
        .write(
            &format!("{table_name}/metadata.json"),
            serde_json::to_vec(table.metadata())?,
        )
        .await?;
    Ok(())
}

fn rolling_writer_builder(
    table: &Table,
    file_name_prefix: String,
    target_file_size: usize,
) -> Result<
    RollingFileWriterBuilder<
        ParquetWriterBuilder,
        DefaultLocationGenerator,
        DefaultFileNameGenerator,
    >,
    Box<dyn std::error::Error>,
> {
    Ok(RollingFileWriterBuilder::new(
        ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            table.metadata().current_schema().clone(),
        ),
        target_file_size,
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata())?,
        DefaultFileNameGenerator::new(file_name_prefix, None, DataFileFormat::Parquet),
    ))
}

/// Writes each input file with its own writer, so no output file mixes rows from two input files.
async fn write_unpartitioned(
    table: &Table,
    table_name: &str,
    plans: &[Arc<dyn ExecutionPlan>],
    task_ctx: &Arc<TaskContext>,
    arrow_schema_with_ids: &SchemaRef,
    target_file_size: usize,
) -> Result<Vec<DataFile>, Box<dyn std::error::Error>> {
    let mut data_files = Vec::new();
    let mut file_index = 0;
    for plan in plans {
        for partition in 0..plan.output_partitioning().partition_count() {
            let rolling_writer = rolling_writer_builder(
                table,
                format!("{table_name}-{file_index}"),
                target_file_size,
            )?;
            file_index += 1;
            let mut writer = DataFileWriterBuilder::new(rolling_writer)
                .build(None)
                .await?;
            let mut stream = plan.execute(partition, Arc::clone(task_ctx))?;
            while let Some(batch) = stream.next().await {
                writer
                    .write(align_batch_schema(
                        batch?,
                        Arc::clone(arrow_schema_with_ids),
                    )?)
                    .await?;
            }
            data_files.extend(writer.close().await?);
        }
    }
    Ok(data_files)
}

/// Writes rows that `plans` return sorted by `partition_column`. Because of the sort, all rows with
/// the same value arrive together. The writer keeps one file open at a time. It starts a new file
/// when the value changes or the file reaches `target_file_size`. So the table gets about one file
/// per value, however many input files it had. Rows where the column is null get their own
/// partition.
async fn write_partitioned(
    table: &Table,
    table_name: &str,
    plans: &[Arc<dyn ExecutionPlan>],
    task_ctx: &Arc<TaskContext>,
    arrow_schema_with_ids: &SchemaRef,
    target_file_size: usize,
    partition_column: &str,
) -> Result<Vec<DataFile>, Box<dyn std::error::Error>> {
    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
    )?;
    let mut writer = ClusteredWriter::new(DataFileWriterBuilder::new(rolling_writer_builder(
        table,
        table_name.to_string(),
        target_file_size,
    )?));
    for plan in plans {
        for partition in 0..plan.output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, Arc::clone(task_ctx))?;
            while let Some(batch) = stream.next().await {
                let batch = align_batch_schema(batch?, Arc::clone(arrow_schema_with_ids))?;
                let Some(values) = batch.column_by_name(partition_column) else {
                    return Err(exec_datafusion_err!(
                        "{table_name} has no partition column {partition_column}"
                    )
                    .into());
                };
                // `split` can return a batch's partitions in any order. The writer fails if a
                // partition it already closed comes back. So first cut the sorted batch into runs
                // that share one value. Then split each run, in order.
                for run in equal_value_runs(&[Arc::clone(values)])?.ranges() {
                    let run = batch.slice(run.start, run.end - run.start);
                    for (partition_key, run) in splitter.split(&run)? {
                        writer.write(partition_key, run).await?;
                    }
                }
            }
        }
    }
    Ok(writer.close().await?)
}

/// Register immutable local benchmark tables written by `dfbench-iceberg prepare`.
pub async fn register_tables(
    ctx: &SessionContext,
    data_path: &Path,
) -> Result<(), DataFusionError> {
    if !data_path.join("_SUCCESS").is_file() {
        return exec_err!(
            "Iceberg dataset is missing or incomplete: {}. Run dfbench-iceberg prepare first.",
            data_path.display()
        );
    }
    for entry in fs::read_dir(data_path)? {
        let path = entry?.path();
        let name = path.file_name().unwrap().to_string_lossy();
        if !path.is_dir() || name.starts_with('.') {
            continue;
        }
        let name = name.replace('"', "\"\"");
        let metadata = path.join("metadata.json").canonicalize()?;
        let location = url::Url::from_file_path(&metadata)
            .map_err(|()| exec_datafusion_err!("Invalid metadata path: {}", metadata.display()))?;
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE \"{name}\" STORED AS ICEBERG LOCATION '{}'",
            location.as_str().replace('\'', "''")
        ))
        .await?
        .collect()
        .await?;
    }
    Ok(())
}

/// Lists the table's Parquet files, sorted by name.
fn parquet_files(table_dir: &Path) -> Result<Vec<String>, DataFusionError> {
    let mut files = Vec::new();
    for entry in fs::read_dir(table_dir)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            let Some(file) = path.to_str() else {
                return exec_err!("Parquet path is not valid UTF-8: {}", path.display());
            };
            files.push(file.to_string());
        }
    }
    files.sort_unstable();
    Ok(files)
}

/// Builds one plan per Parquet file. Each plan reads just that file.
async fn parquet_file_plans(
    ctx: &SessionContext,
    files: Vec<String>,
) -> Result<Vec<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let mut plans = Vec::with_capacity(files.len());
    for file in files {
        plans.push(
            ctx.read_parquet(file, ParquetReadOptions::default())
                .await?
                .create_physical_plan()
                .await?,
        );
    }
    Ok(plans)
}

/// Builds one plan that reads all `files` and sorts the rows by `column`, nulls first. If the rows
/// don't fit in the context's memory limit, the sort moves them into temporary files on disk.
async fn sorted_parquet_plan(
    ctx: &SessionContext,
    files: Vec<String>,
    column: &str,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    ctx.read_parquet(files, ParquetReadOptions::default())
        .await?
        .sort(vec![ident(column).sort(true, true)])?
        .create_physical_plan()
        .await
}

fn align_batch_schema(
    batch: RecordBatch,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column, field.data_type()).map_err(DataFusionError::from)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}
