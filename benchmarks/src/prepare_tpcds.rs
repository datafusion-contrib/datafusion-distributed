use datafusion::error::DataFusionError;
use datafusion_distributed_benchmarks::datasets::tpcds;
use std::path::{Path, PathBuf};
use structopt::StructOpt;

/// Prepare TPC-DS parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct PrepareTpcdsOpt {
    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Number of partitions to produce. By default, uses only 1 partition.
    #[structopt(short = "n", long = "partitions", default_value = "1")]
    partitions: usize,

    /// Scale factor (e.g. 1.0, 10.0, 100.0)
    #[structopt(short = "s", long = "scale-factor", default_value = "1")]
    scale_factor: f64,
}

impl PrepareTpcdsOpt {
    pub async fn run(self) -> datafusion::common::Result<()> {
        println!(
            "Generating TPC-DS data at scale factor {} with {} partitions in '{}'",
            self.scale_factor,
            self.partitions,
            self.output_path.display()
        );
        tpcds::generate_data(
            Path::new(&self.output_path),
            self.scale_factor,
            self.partitions,
        )
        .await
        .map_err(|e| DataFusionError::Internal(format!("{e:?}")))?;
        println!("TPC-DS data generation complete.");
        Ok(())
    }
}
