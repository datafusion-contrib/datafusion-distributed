use crate::test_utils::benchmarks_common;
use datafusion::common::DataFusionError;
use std::fs;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use tokio::task::JoinSet;

const URL: &str =
    "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet";

pub fn get_queries() -> Vec<String> {
    benchmarks_common::get_queries("testdata/clickbench/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    benchmarks_common::get_query("testdata/clickbench/queries", id)
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
