use std::path::PathBuf;

use super::dataset::{convert_parquet_to_iceberg, output_location};
use datafusion::error::{DataFusionError, Result};
use structopt::StructOpt;

/// Convert local Parquet tables into a sibling <input>_iceberg dataset.
#[derive(Debug, StructOpt)]
pub struct PrepareIcebergOpt {
    /// Existing Parquet dataset directory.
    #[structopt(parse(from_os_str), long = "input")]
    input_path: PathBuf,

    /// Destination path or object-store URI. Defaults to the local sibling <input>_iceberg.
    #[structopt(long)]
    output: Option<String>,

    /// Rolling threshold in bytes. The default preserves source file boundaries.
    #[structopt(long, default_value = "1099511627776")]
    target_file_size: usize,
}

impl PrepareIcebergOpt {
    pub async fn run(self) -> Result<()> {
        let input_path = std::path::absolute(&self.input_path)?;
        let output_location = output_location(&input_path, self.output.as_deref())
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        convert_parquet_to_iceberg(&input_path, &output_location, self.target_file_size)
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        println!("Iceberg dataset prepared in {output_location}");
        Ok(())
    }
}
