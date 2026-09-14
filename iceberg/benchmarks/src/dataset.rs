use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::compute::cast;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, exec_err};

use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::ParquetReadOptions;
use datafusion_distributed_iceberg::iceberg;
use futures::StreamExt;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::DataFileFormat;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use parquet::file::properties::WriterProperties;

#[expect(
    clippy::disallowed_types,
    reason = "the iceberg catalog API requires std::collections::HashMap"
)]
type CatalogProperties = std::collections::HashMap<String, String>;

/// The sibling dataset directory used for the Iceberg representation.
pub(super) fn output_path(input: &Path) -> PathBuf {
    let mut name = input.file_name().unwrap_or_default().to_os_string();
    name.push("-iceberg");
    input.with_file_name(name)
}

/// Converts prepared local Parquet tables, independently of their query suite.
/// Data is streamed one input file at a time; completion is marked last.
pub(super) async fn convert_parquet_to_iceberg(
    source_dir: &Path,
    output_dir: &Path,
    target_file_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if target_file_size == 0 {
        return Err("target file size must be greater than zero".into());
    }
    if !source_dir.is_dir() {
        return Err(format!("source dataset does not exist: {}", source_dir.display()).into());
    }
    if output_dir.exists() && fs::read_dir(output_dir)?.next().is_some() {
        return Err(format!("output dataset is not empty: {}", output_dir.display()).into());
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
    fs::create_dir_all(output_dir)?;
    let output_dir = output_dir.canonicalize()?;
    let warehouse = file_uri(&output_dir)?;
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "benchmark",
            CatalogProperties::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
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
        write_table(
            &catalog,
            &namespace,
            table_name,
            plans,
            ctx.task_ctx(),
            &output_dir,
            target_file_size,
        )
        .await?;
    }
    fs::write(output_dir.join("_SUCCESS"), b"")?;
    Ok(())
}

async fn write_table(
    catalog: &impl Catalog,
    namespace: &NamespaceIdent,
    table_name: &str,
    plans: Vec<Arc<dyn ExecutionPlan>>,
    task_ctx: Arc<datafusion::execution::TaskContext>,
    output_dir: &Path,
    target_file_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(first_plan) = plans.first() else {
        return Err(format!("Parquet table {table_name} contains no files").into());
    };
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(first_plan.schema().as_ref())?;
    let table_location = file_uri(&output_dir.join(table_name))?;
    let table = catalog
        .create_table(
            namespace,
            TableCreation::builder()
                .name(table_name.to_string())
                .location(table_location)
                .schema(iceberg_schema)
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
                target_file_size,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata())?,
                DefaultFileNameGenerator::new(
                    format!("{table_name}-{file_index}"),
                    None,
                    DataFileFormat::Parquet,
                ),
            );
            file_index += 1;
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
    fs::write(
        output_dir.join(table_name).join("metadata.json"),
        serde_json::to_vec(table.metadata())?,
    )?;
    Ok(())
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
        let location = url::Url::from_file_path(&metadata).map_err(|()| {
            datafusion::common::exec_datafusion_err!(
                "Invalid metadata path: {}",
                metadata.display()
            )
        })?;
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

fn file_uri(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    url::Url::from_directory_path(path)
        .map(|url| url.to_string())
        .map_err(|()| {
            format!(
                "Path cannot be represented as a file URI: {}",
                path.display()
            )
            .into()
        })
}
