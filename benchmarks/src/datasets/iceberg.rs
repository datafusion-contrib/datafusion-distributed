use std::fs;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::compute::cast;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, exec_err};

use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::ParquetReadOptions;
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

pub const ICEBERG_DIR: &str = ".iceberg";

/// Converts prepared local Parquet tables, independently of their query suite.
/// Data is streamed one input file at a time; completion is marked last.
pub async fn convert_parquet_to_iceberg(
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

pub async fn register_tables(
    ctx: &SessionContext,
    data_path: &Path,
) -> Result<(), DataFusionError> {
    if !data_path.join("_SUCCESS").is_file() {
        return exec_err!(
            "Iceberg dataset is missing or incomplete: {}. Run prepare-iceberg first.",
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

#[cfg(test)]
mod tests {
    use std::fs::File;

    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::execution::SessionStateBuilder;
    use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn generates_snapshot_statistics() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let output = temp.path().join("output");
        write_source_table(&source);

        convert_parquet_to_iceberg(&source, &output, 1024 * 1024)
            .await
            .unwrap();

        let location = output.join("example/metadata.json");
        let metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(location).unwrap()).unwrap();
        let summary = &metadata["snapshots"][0]["summary"];
        assert_eq!(summary["total-records"], "3");
        assert!(
            summary["total-files-size"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 0
        );

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_iceberg_integration(IcebergIntegrationOptions::default())
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_tables(&ctx, &output).await.unwrap();
        let batches = ctx
            .sql("SELECT COUNT(*) FROM example WHERE id > 0")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count = ScalarValue::try_from_array(batches[0].column(0), 0).unwrap();
        assert_eq!(count.to_string(), "3");
    }

    fn write_source_table(source: &Path) {
        let table = source.join("example");
        fs::create_dir_all(&table).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(table.join("part.parquet")).unwrap(),
            schema,
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
}
