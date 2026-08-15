use datafusion::error::DataFusionError;
use datafusion_distributed_benchmarks::datasets::clickbench_sorted;
use std::path::{Path, PathBuf};
use structopt::StructOpt;

/// Prepare a globally sorted ClickBench parquet dataset
#[derive(Debug, StructOpt)]
pub struct PrepareClickBenchSortedOpt {
    /// Output path. Hits parquet files are written under `<output>/hits/`.
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// ClickBench is partitioned in 100 files. Start of the inclusive-exclusive
    /// partition range to download and sort.
    #[structopt(long, default_value = "0")]
    partition_start: usize,

    /// Exclusive end of the partition range to download and sort.
    #[structopt(long, default_value = "100")]
    partition_end: usize,
}

impl PrepareClickBenchSortedOpt {
    pub async fn run(self) -> datafusion::common::Result<()> {
        println!(
            "Generating sorted ClickBench data from partition {} to {} in '{}' (ORDER BY {})",
            self.partition_start,
            self.partition_end,
            self.output_path.display(),
            clickbench_sorted::CLICKBENCH_SORT_KEY.join(", ")
        );
        clickbench_sorted::generate_clickbench_sorted_data(
            Path::new(&self.output_path),
            self.partition_start..self.partition_end,
        )
        .await
        .map_err(|e| DataFusionError::Internal(format!("{e:?}")))?;
        println!("Sorted ClickBench data generation complete.");
        Ok(())
    }
}
