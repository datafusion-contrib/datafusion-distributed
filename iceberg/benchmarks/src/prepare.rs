use std::path::PathBuf;

use super::dataset::{convert_parquet_to_iceberg, output_path};
use datafusion::common::{Result, exec_datafusion_err};
use datafusion_distributed_benchmarks::datasets::output::DatasetOutput;
use structopt::StructOpt;

/// Convert local Parquet tables into an Iceberg dataset.
#[derive(Debug, StructOpt)]
pub struct PrepareIcebergOpt {
    /// Existing Parquet dataset directory.
    #[structopt(parse(from_os_str), long = "input")]
    input_path: PathBuf,

    /// Local directory or s3://bucket/prefix. Defaults to the `<benchmark>_iceberg` dataset
    /// mirroring the input's parameters, with `target_file_size` appended.
    #[structopt(short = "o", long = "output")]
    output: Option<String>,

    /// Rolling threshold in bytes. The default preserves source file boundaries.
    #[structopt(long, default_value = "1099511627776")]
    target_file_size: usize,
}

impl PrepareIcebergOpt {
    pub async fn run(self) -> Result<()> {
        let input_path = std::path::absolute(&self.input_path)?;
        let location = match self.output {
            Some(location) => location,
            None => output_path(&input_path, self.target_file_size)?
                .to_string_lossy()
                .into_owned(),
        };
        let output = DatasetOutput::new(&location).await?;
        convert_parquet_to_iceberg(&input_path, &output, self.target_file_size)
            .await
            .map_err(|error| exec_datafusion_err!("{error}"))?;
        // The only output on stdout, so that `ICEBERG=$(dfbench-iceberg prepare ...)` captures it.
        println!("{location}");
        Ok(())
    }
}
