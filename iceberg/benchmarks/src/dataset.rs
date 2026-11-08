use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, exec_datafusion_err, exec_err};
use datafusion::execution::TaskContext;

use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::ParquetReadOptions;
use datafusion_distributed_benchmarks::datasets::output::DatasetOutput;
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
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
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
/// parameter of both the Parquet generation and the conversion. A partitioned conversion adds
/// `partitioned=true` before `target_file_size`, so that it sits beside the unpartitioned dataset
/// and not inside it. Unpartitioned paths are unchanged.
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

/// The TPC-DS fact tables that the standard partitioned layout (spark-sql-perf, Trino) splits by
/// their date key, with an identity transform. Every other table stays unpartitioned.
fn tpcds_partition_column(table_name: &str) -> Option<&'static str> {
    match table_name {
        "store_sales" => Some("ss_sold_date_sk"),
        "store_returns" => Some("sr_returned_date_sk"),
        "catalog_sales" => Some("cs_sold_date_sk"),
        "catalog_returns" => Some("cr_returned_date_sk"),
        "web_sales" => Some("ws_sold_date_sk"),
        "web_returns" => Some("wr_returned_date_sk"),
        "inventory" => Some("inv_date_sk"),
        _ => None,
    }
}

/// Converts prepared local Parquet tables, independently of their query suite.
/// Unpartitioned tables are streamed one input file at a time. With `partitioned`, the TPC-DS
/// fact tables are split by date key, streaming each table's input files through one writer.
/// Completion is marked last.
pub(super) async fn convert_parquet_to_iceberg(
    source_dir: &Path,
    output: &DatasetOutput,
    target_file_size: usize,
    partitioned: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if target_file_size == 0 {
        return Err("target file size must be greater than zero".into());
    }
    if partitioned
        && !source_dir
            .components()
            .any(|component| component.as_os_str() == "tpcds_parquet")
    {
        return Err(exec_datafusion_err!(
            "--partitioned only supports TPC-DS datasets, but {} is not under tpcds_parquet",
            source_dir.display()
        )
        .into());
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
    let ctx = SessionContext::new_with_config(config);

    for table_path in tables {
        let table_name = table_path
            .file_name()
            .unwrap()
            .to_str()
            .ok_or("Table name is not valid UTF-8")?;
        let plans = parquet_file_plans(&ctx, &table_path).await?;
        let partition_column = partitioned
            .then(|| tpcds_partition_column(table_name))
            .flatten();
        write_table(
            &catalog,
            &namespace,
            table_name,
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

/// How one table's data files are laid out.
struct TableLayout<'a> {
    target_file_size: usize,
    /// The identity partition column, or none for an unpartitioned table.
    partition_column: Option<&'a str>,
}

async fn write_table(
    catalog: &impl Catalog,
    namespace: &NamespaceIdent,
    table_name: &str,
    plans: Vec<Arc<dyn ExecutionPlan>>,
    task_ctx: Arc<TaskContext>,
    output: &DatasetOutput,
    layout: TableLayout<'_>,
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

    let data_files = if partition_column.is_some() {
        write_partitioned(
            &table,
            table_name,
            &plans,
            &task_ctx,
            &arrow_schema_with_ids,
            target_file_size,
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

/// One writer per input file keeps the source file boundaries.
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

/// All input files of the table flow through one fanout writer, so each partition gets one open
/// file at a time and rolls to a new file only at `target_file_size`. The table ends up with about
/// one file per partition value, however many source files it has. Rows with a null partition
/// value go to their own partition.
async fn write_partitioned(
    table: &Table,
    table_name: &str,
    plans: &[Arc<dyn ExecutionPlan>],
    task_ctx: &Arc<TaskContext>,
    arrow_schema_with_ids: &SchemaRef,
    target_file_size: usize,
) -> Result<Vec<DataFile>, Box<dyn std::error::Error>> {
    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
    )?;
    let mut writer = FanoutWriter::new(DataFileWriterBuilder::new(rolling_writer_builder(
        table,
        table_name.to_string(),
        target_file_size,
    )?));
    for plan in plans {
        for partition in 0..plan.output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, Arc::clone(task_ctx))?;
            while let Some(batch) = stream.next().await {
                let batch = align_batch_schema(batch?, Arc::clone(arrow_schema_with_ids))?;
                for (partition_key, batch) in splitter.split(&batch)? {
                    writer.write(partition_key, batch).await?;
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
