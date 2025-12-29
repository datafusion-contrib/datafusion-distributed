use datafusion::error::DataFusionError;
use datafusion_distributed::test_utils::clickbench;
use std::path::{Path, PathBuf};
use structopt::StructOpt;

/// Prepare ClickBench parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct PrepareClickBenchOpt {
    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Clickbench dataset is partitioned in 100 files. You may not want to use all the files for
    /// the benchmark, so this allows setting from which file partition to start.
    #[structopt(long, default_value = "0")]
    partition_start: usize,

    /// Clickbench dataset is partitioned in 100 files. You may not want to use all the files for
    /// the benchmark, so this allows setting a maximum in the file partition index.
    #[structopt(long, default_value = "100")]
    partition_end: usize,
}

impl PrepareClickBenchOpt {
    pub async fn run(self) -> datafusion::common::Result<()> {
        clickbench::generate_clickbench_data(
            Path::new(&self.output_path),
            self.partition_start..self.partition_end,
        )
        .await
        .map_err(|e| DataFusionError::Internal(format!("{e:?}")))
    }
}
