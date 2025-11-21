use arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::ParquetReadOptions;
use parquet::arrow::ArrowWriter;
use std::path::PathBuf;
use uuid::Uuid;

/// Creates a temporary Parquet file from RecordBatches and registers it with the SessionContext
/// under the provided table name. Returns the file name.
///
/// TODO: consider expanding this to support partitioned data
pub async fn register_temp_parquet_table(
    table_name: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    ctx: &SessionContext,
) -> Result<PathBuf> {
    if batches.is_empty() {
        return Err(datafusion::error::DataFusionError::Execution(
            "cannot create parquet file from empty batch list".to_string(),
        ));
    }
    for batch in &batches {
        if batch.schema() != schema {
            return Err(datafusion::error::DataFusionError::Execution(
                "all batches must have the same schema".to_string(),
            ));
        }
    }

    let temp_dir = std::env::temp_dir();
    let file_id = Uuid::new_v4();
    let temp_file_path = temp_dir.join(format!("{table_name}_{file_id}.parquet",));

    let file = std::fs::File::create(&temp_file_path)?;
    let schema = batches[0].schema();
    let mut writer = ArrowWriter::try_new(file, schema, None)?;

    for batch in batches {
        writer.write(&batch)?;
    }
    writer.close()?;

    ctx.register_parquet(
        table_name,
        temp_file_path.to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;

    Ok(temp_file_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tokio::fs::remove_file;

    use std::sync::Arc;

    #[tokio::test]
    async fn test_register_temp_parquet_table() {
        let ctx = SessionContext::new();

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // Register temp table
        let temp_file =
            register_temp_parquet_table("test_table", schema.clone(), vec![batch], &ctx)
                .await
                .unwrap();

        let df = ctx.sql("SELECT * FROM test_table").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);

        let _ = remove_file(temp_file).await;
    }
}
