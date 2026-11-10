use datafusion::common::{Result, exec_datafusion_err};
use datafusion_distributed_benchmarks::datasets::output::TESTDATA_ROOT;
use datafusion_distributed_benchmarks::datasets::parquet_layout::ParquetLayout;
use datafusion_distributed_benchmarks::datasets::tpcds::{TpcdsSpec, generate_data};
use structopt::StructOpt;

/// Prepare TPC-DS parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct PrepareTpcdsOpt {
    /// Local directory or s3://bucket/prefix under which the dataset path is created
    #[structopt(long, default_value = TESTDATA_ROOT.as_str())]
    root: String,

    /// Scale factor of the TPC-DS data
    #[structopt(short = "s", long = "scale-factor", alias = "sf", default_value = "1")]
    scale_factor: f64,

    #[structopt(flatten)]
    layout: ParquetLayout,
}

impl PrepareTpcdsOpt {
    pub async fn run(self) -> Result<()> {
        let spec = TpcdsSpec {
            location: self.root,
            scale_factor: self.scale_factor,
            layout: self.layout,
        };
        generate_data(&spec)
            .await
            .map_err(|e| exec_datafusion_err!("{e}"))?;
        // The only output on stdout, so that `DATASET=$(dfbench prepare-...)` captures it.
        println!("{}", spec.dataset());
        Ok(())
    }
}
