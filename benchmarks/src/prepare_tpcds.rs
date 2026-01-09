use datafusion::error::DataFusionError;
use datafusion_distributed::test_utils::tpcds;
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

    /// Scale factor of the TPC-DS data
    #[structopt(long, default_value = "1")]
    sf: f64,
}

impl PrepareTpcdsOpt {
    pub async fn run(self) -> datafusion::common::Result<()> {
        tpcds::generate_data(Path::new(&self.output_path), self.sf, self.partitions)
            .await
            .map_err(|e| DataFusionError::Internal(format!("{e:?}")))
    }
}
