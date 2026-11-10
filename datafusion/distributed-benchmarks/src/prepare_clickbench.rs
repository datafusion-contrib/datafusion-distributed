use datafusion::common::{Result, exec_datafusion_err};

use datafusion_distributed_benchmarks::datasets::clickbench::{ClickBenchSpec, generate_data};
use datafusion_distributed_benchmarks::datasets::output::TESTDATA_ROOT;
use structopt::StructOpt;

/// Prepare ClickBench parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct PrepareClickBenchOpt {
    /// Local directory or s3://bucket/prefix under which the dataset path is created
    #[structopt(long, default_value = TESTDATA_ROOT.as_str())]
    root: String,

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
    pub async fn run(self) -> Result<()> {
        let spec = ClickBenchSpec {
            location: self.root,
            partition_start: self.partition_start,
            partition_end: self.partition_end,
        };
        generate_data(&spec)
            .await
            .map_err(|e| exec_datafusion_err!("{e}"))?;
        // The only output on stdout, so that `DATASET=$(dfbench prepare-...)` captures it.
        println!("{}", spec.dataset());
        Ok(())
    }
}
