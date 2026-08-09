use super::common;
use arrow::array::Array;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DataFusionError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::JoinSet;

const URL: &str =
    "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet";

pub fn get_queries() -> Vec<String> {
    common::get_queries("testdata/clickbench/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    common::get_query("testdata/clickbench/queries", id)
}

/// Downloads the datafusion-benchmarks repository as a zip file
async fn download_benchmark(
    dest_path: PathBuf,
    i: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if dest_path.exists() {
        return Ok(());
    }

    // Create directory if it doesn't exist
    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Download the file
    let response = reqwest::get(URL.replace("{}", &i.to_string())).await?;
    let bytes = response.bytes().await?;

    // Write to file
    let mut file = fs::File::create(&dest_path)?;
    file.write_all(&bytes)?;

    println!("Downloaded to {}", dest_path.display());

    Ok(())
}

async fn download_partitioned(
    dest_path: PathBuf,
    range: Range<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut join_set = JoinSet::new();
    for i in range {
        let dest_path = dest_path.clone();
        join_set.spawn(async move {
            download_benchmark(dest_path.join("hits").join(format!("{i}.parquet")), i).await
        });
    }
    join_set.join_all().await;
    Ok(())
}

/// Rewrites the `EventDate` column in every parquet file under `path/hits/` from its raw
/// integer type (UInt16, days since epoch) to `Date32`, so that comparisons with date string
/// literals like `'2013-07-01'` work correctly at planning time.
///
/// The function is idempotent: files whose `EventDate` column is already `Date32` are skipped.
async fn fix_event_dates(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let hits_dir = path.join("hits");

    for entry in fs::read_dir(&hits_dir)? {
        let file_path = entry?.path();
        if file_path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }

        let file = fs::File::open(&file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();

        let Ok(event_date_idx) = schema.index_of("EventDate") else {
            continue;
        };
        if schema.field(event_date_idx).data_type() == &DataType::Date32 {
            continue; // already fixed
        }

        let batches: Vec<_> = builder.build()?.collect::<Result<_, _>>()?;

        // Build the new schema with EventDate → Date32.
        let new_fields: Vec<Field> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == event_date_idx {
                    Field::new(f.name(), DataType::Date32, f.is_nullable())
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let new_schema = Arc::new(Schema::new(new_fields));

        // Write to a temp file, then atomically replace the original.
        let temp_path = file_path.with_extension("tmp");
        {
            let temp_file = fs::File::create(&temp_path)?;
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(temp_file, new_schema.clone(), Some(props))?;

            for batch in batches {
                let new_columns: Vec<Arc<dyn Array>> = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(i, col)| {
                        if i == event_date_idx {
                            // Arrow does not support UInt16 → Date32 directly;
                            // go through Int32 (same underlying representation).
                            cast(col.as_ref(), &DataType::Int32)
                                .and_then(|c| cast(c.as_ref(), &DataType::Date32))
                        } else {
                            Ok(col.clone())
                        }
                    })
                    .collect::<Result<_, _>>()?;

                writer.write(&arrow::record_batch::RecordBatch::try_new(
                    new_schema.clone(),
                    new_columns,
                )?)?;
            }

            writer.close()?;
        }

        fs::rename(&temp_path, &file_path)?;
        println!("Fixed EventDate in {}", file_path.display());
    }

    Ok(())
}

pub async fn generate_clickbench_data(
    dest_path: &Path,
    range: Range<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    download_partitioned(dest_path.to_path_buf(), range).await?;
    fix_event_dates(dest_path.to_path_buf()).await?;
    Ok(())
}
