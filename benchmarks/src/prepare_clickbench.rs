use datafusion::common::exec_datafusion_err;
use datafusion_distributed_benchmarks::datasets::{clickbench, output::DatasetOutput};
use structopt::StructOpt;

/// Prepare ClickBench parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct PrepareClickBenchOpt {
    /// Empty local directory or s3://bucket/prefix
    #[structopt(required = true, short = "o", long = "output")]
    output_path: String,

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
        clickbench::generate_data(
            &DatasetOutput::new(&self.output_path).await?,
            self.partition_start..self.partition_end,
        )
        .await
        .map_err(|e| exec_datafusion_err!("{e}"))
    }
}
