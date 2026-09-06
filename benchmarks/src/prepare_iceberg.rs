use std::path::PathBuf;

use datafusion::error::{DataFusionError, Result};
use datafusion_distributed_benchmarks::datasets::iceberg::{
    ICEBERG_DIR, convert_parquet_to_iceberg,
};
use structopt::StructOpt;

/// Convert prepared local Parquet tables into Iceberg tables.
#[derive(Debug, StructOpt)]
pub struct PrepareIcebergOpt {
    /// Existing Parquet dataset directory.
    #[structopt(parse(from_os_str), long = "input")]
    input_path: PathBuf,

    /// Rolling threshold in bytes. The default preserves source file boundaries.
    #[structopt(long, default_value = "1099511627776")]
    target_file_size: usize,
}

impl PrepareIcebergOpt {
    pub async fn run(self) -> Result<()> {
        let output_path = self.input_path.join(ICEBERG_DIR);
        convert_parquet_to_iceberg(&self.input_path, &output_path, self.target_file_size)
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        println!("Iceberg dataset prepared in {}", output_path.display());
        Ok(())
    }
}
