use datafusion::common::exec_datafusion_err;
use datafusion_distributed_benchmarks::datasets::{output::DatasetOutput, tpcds};
use structopt::StructOpt;

/// Prepare TPC-DS parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct PrepareTpcdsOpt {
    /// Empty local directory or s3://bucket/prefix
    #[structopt(required = true, short = "o", long = "output")]
    output_path: String,

    /// Number of partitions to produce. By default, uses only 1 partition.
    #[structopt(short = "n", long = "partitions", default_value = "1")]
    partitions: usize,

    /// Scale factor of the TPC-DS data
    #[structopt(long, default_value = "1")]
    sf: f64,
}

impl PrepareTpcdsOpt {
    pub async fn run(self) -> datafusion::common::Result<()> {
        tpcds::generate_data(
            &DatasetOutput::new(&self.output_path).await?,
            self.sf,
            self.partitions,
        )
        .await
        .map_err(|e| exec_datafusion_err!("{e}"))
    }
}
