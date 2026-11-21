use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::compute::{cast, partition as equal_value_runs};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, exec_datafusion_err, exec_err};

use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{ParquetReadOptions, ident};
use datafusion_datasets::output::DatasetOutput;
use datafusion_datasets::table_columns;
use datafusion_distributed_benchmarks::run::dataset_suite;
use datafusion_distributed_iceberg::IcebergIntegrationOptions;
use datafusion_distributed_iceberg::iceberg;
use futures::StreamExt;
use iceberg::arrow::{
    RecordBatchPartitionSplitter, arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema,
};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{DataFile, DataFileFormat, PartitionSpec, Transform};
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
/// `<root>/tpch_iceberg/<key>=<value>/.../partitioned=<bool>/target_file_size=<bytes>`, so the
/// path records every parameter of both the Parquet generation and the conversion.
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
    Ok(PathBuf::from(format!(
        "{root}_iceberg/{parameters}/partitioned={partitioned}/target_file_size={target_file_size}"
    )))
}

/// How much memory the sort for a partitioned table can use. Past that, the sort moves rows into
/// temporary files on disk. They go in the system temp folder, which `TMPDIR` sets.
const SORT_MEMORY_LIMIT: usize = 4 * 1024 * 1024 * 1024;

/// Converts prepared local Parquet tables. By default, data is streamed one input file at a time.
/// With `partitioned`, we sort each table by the partition columns its query suite declares and
/// write it in one pass. Tables without partition columns keep the default. Completion is marked
/// last.
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

    let tables = fs::read_dir(source_dir)?
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
    if tables.is_empty() {
        return Err("conversion requires a non-empty Parquet dataset".into());
    }
    let source = source_dir.to_string_lossy();
    let suite = dataset_suite(&source);
    // We stop before writing anything when no table would get partitions.
    if partitioned
        && tables.iter().all(|table| {
            let table = table.file_name().unwrap().to_string_lossy();
            table_columns(suite, &table).partition_columns.is_empty()
        })
    {
        return Err(exec_datafusion_err!(
            "--partitioned found no table to partition: {suite} declares no partition columns \
             for the tables in {source}"
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

    for table_path in tables {
        let table_name = table_path
            .file_name()
            .unwrap()
            .to_str()
            .ok_or("Table name is not valid UTF-8")?;
        let partition_columns = if partitioned {
            table_columns(suite, table_name).partition_columns
        } else {
            &[]
        };
        let plans = if partition_columns.is_empty() {
            parquet_file_plans(&ctx, &table_path).await?
        } else {
            vec![sorted_parquet_plan(&ctx, &table_path, partition_columns).await?]
        };
        write_table(
            &catalog,
            &namespace,
            table_name,
            plans,
            ctx.task_ctx(),
            output,
            TableLayout {
                target_file_size,
                partition_columns,
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
    /// The columns to partition by. With none, we write the table unpartitioned.
    partition_columns: &'static [&'static str],
}

async fn write_table(
    catalog: &impl Catalog,
    namespace: &NamespaceIdent,
    table_name: &str,
    plans: Vec<Arc<dyn ExecutionPlan>>,
    task_ctx: Arc<datafusion::execution::TaskContext>,
    output: &DatasetOutput,
    layout: TableLayout,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(first_plan) = plans.first() else {
        return Err(format!("Parquet table {table_name} contains no files").into());
    };
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(first_plan.schema().as_ref())?;
    let mut partition_spec = PartitionSpec::builder(iceberg_schema.clone());
    for column in layout.partition_columns {
        partition_spec =
            partition_spec.add_partition_field(column, *column, Transform::Identity)?;
    }
    let table_location = format!("{}/{table_name}", output.location());
    let table = catalog
        .create_table(
            namespace,
            TableCreation::builder()
                .name(table_name.to_string())
                .location(table_location)
                .schema(iceberg_schema)
                .partition_spec(partition_spec.build()?)
                .build(),
        )
        .await?;
    let arrow_schema_with_ids = Arc::new(schema_to_arrow_schema(
        table.metadata().current_schema().as_ref(),
    )?);
    let mut data_files = Vec::new();

    let mut file_index = 0;
    for plan in plans {
        for partition in 0..plan.output_partitioning().partition_count() {
            let parquet_writer = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                table.metadata().current_schema().clone(),
            );
            let rolling_writer = RollingFileWriterBuilder::new(
                parquet_writer,
                layout.target_file_size,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata())?,
                DefaultFileNameGenerator::new(
                    format!("{table_name}-{file_index}"),
                    None,
                    DataFileFormat::Parquet,
                ),
            );
            file_index += 1;
            if !layout.partition_columns.is_empty() {
                let writer = ClusteredWriter::new(DataFileWriterBuilder::new(rolling_writer));
                let stream = plan.execute(partition, Arc::clone(&task_ctx))?;
                let columns = layout.partition_columns;
                data_files.extend(
                    write_partitioned(&table, writer, stream, &arrow_schema_with_ids, columns)
                        .await?,
                );
                continue;
            }
            let mut writer = DataFileWriterBuilder::new(rolling_writer)
                .build(None)
                .await?;
            let mut stream = plan.execute(partition, Arc::clone(&task_ctx))?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                writer
                    .write(align_batch_schema(
                        batch,
                        Arc::clone(&arrow_schema_with_ids),
                    )?)
                    .await?;
            }
            data_files.extend(writer.close().await?);
        }
    }

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

/// Writes the rows of `stream` through `writer`. The rows arrive sorted by `partition_columns`, so
/// all rows of a partition arrive together and `writer` keeps one file open at a time. It starts
/// a new file when the partition changes or the file reaches the target size. Rows where a
/// partition column is null get their own partition.
async fn write_partitioned(
    table: &Table,
    mut writer: impl PartitioningWriter,
    mut stream: SendableRecordBatchStream,
    arrow_schema_with_ids: &SchemaRef,
    partition_columns: &[&str],
) -> Result<Vec<DataFile>, Box<dyn std::error::Error>> {
    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
    )?;
    let mut columns = Vec::with_capacity(partition_columns.len());
    for column in partition_columns {
        columns.push(arrow_schema_with_ids.index_of(column)?);
    }
    while let Some(batch) = stream.next().await {
        let batch = align_batch_schema(batch?, Arc::clone(arrow_schema_with_ids))?;
        // `split` can return a batch's partitions in any order. The writer fails if a partition
        // it already closed comes back. So first we cut the sorted batch into runs that share
        // the same partition values. Then we split each run, in order.
        for run in equal_value_runs(batch.project(&columns)?.columns())?.ranges() {
            let run = batch.slice(run.start, run.end - run.start);
            for (partition_key, run) in splitter.split(&run)? {
                writer.write(partition_key, run).await?;
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

async fn parquet_file_plans(
    ctx: &SessionContext,
    table_dir: &Path,
) -> Result<Vec<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let mut files = Vec::new();
    for entry in fs::read_dir(table_dir)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            files.push(path);
        }
    }
    files.sort_unstable();

    let mut plans = Vec::with_capacity(files.len());
    for file in files {
        let Some(file) = file.to_str() else {
            return exec_err!("Parquet path is not valid UTF-8: {}", file.display());
        };
        plans.push(
            ctx.read_parquet(file, ParquetReadOptions::default())
                .await?
                .create_physical_plan()
                .await?,
        );
    }
    Ok(plans)
}

/// Builds one plan that reads all of the table's Parquet files and sorts the rows by `columns`,
/// nulls first.
///
/// We sort because the source files can spread the rows of one partition across the whole table.
/// A writer that takes the rows in that order has to keep a file open for every partition until
/// the last row. Each open file holds rows in memory.
///
/// After the sort, the rows of a partition arrive together. We finish one partition's file before
/// we start the next, so we keep one file open at a time.
///
/// If the rows don't fit in the context's memory limit, the sort moves them into temporary files
/// on disk.
async fn sorted_parquet_plan(
    ctx: &SessionContext,
    table_dir: &Path,
    columns: &[&str],
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let Some(table_dir) = table_dir.to_str() else {
        return exec_err!("Parquet path is not valid UTF-8: {}", table_dir.display());
    };
    let sort_keys = columns
        .iter()
        .map(|column| ident(*column).sort(true, true))
        .collect();
    ctx.read_parquet(table_dir, ParquetReadOptions::default())
        .await?
        .sort(sort_keys)?
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
