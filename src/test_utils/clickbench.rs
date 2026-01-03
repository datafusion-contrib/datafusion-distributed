use datafusion::common::{DataFusionError, internal_datafusion_err, internal_err};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::fs;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use tokio::task::JoinSet;

const URL: &str =
    "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet";

/// Load a single ClickBench query by ID (0-42).
pub fn get_test_clickbench_query(id: usize) -> Result<String, DataFusionError> {
    let queries_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/clickbench/queries");

    if !queries_dir.exists() {
        return internal_err!(
            "TPC-DS queries directory not found: {}",
            queries_dir.display()
        );
    }

    let query_file = queries_dir.join(format!("q{id}.sql"));

    if !query_file.exists() {
        return internal_err!("Query file not found: {}", query_file.display());
    }

    let query_sql = fs::read_to_string(&query_file)
        .map_err(|e| {
            internal_datafusion_err!("Failed to read query file {}: {e}", query_file.display())
        })?
        .trim()
        .to_string();

    Ok(query_sql)
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

pub async fn generate_clickbench_data(
    dest_path: &Path,
    range: Range<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    download_partitioned(dest_path.to_path_buf(), range).await?;
    Ok(())
}

pub async fn register_tables(
    ctx: &SessionContext,
    data_path: &Path,
) -> Result<(), DataFusionError> {
    for entry in fs::read_dir(data_path)? {
        let path = entry?.path();
        if path.is_dir() {
            let table_name = path.file_name().unwrap().to_str().unwrap();
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        }
    }
    Ok(())
}
