//! DataFusion Distributed benchmark runner
mod compare;
mod prepare_clickbench;
mod prepare_tpcds;
mod prepare_tpch;
mod results;
mod run;

use datafusion::error::Result;
use std::path::PathBuf;
use structopt::StructOpt;

pub(crate) const RESULTS_DIR: &str = ".results";

pub(crate) fn dataset_path(dataset: &str) -> PathBuf {
    let (suite, variant) = dataset.split_once('_').unwrap_or((dataset, ""));
    let directory = match (suite, variant) {
        ("clickbench", range) if !range.is_empty() => format!("benchmark_range{range}"),
        (_, "") => "benchmark".to_string(),
        (_, variant) => format!("benchmark_{variant}"),
    };

    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("testdata")
        .join(suite)
        .join(directory)
}

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    Run(run::RunOpt),
    Compare(compare::CompareOpt),
    PrepareTpch(prepare_tpch::PrepareTpchOpt),
    PrepareTpcds(prepare_tpcds::PrepareTpcdsOpt),
    PrepareClickbench(prepare_clickbench::PrepareClickBenchOpt),
}

// Main benchmark runner entrypoint
pub fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Run(opt) => opt.run(),
        Options::Compare(opt) => opt.run(),
        Options::PrepareTpch(opt) => opt.run(),
        Options::PrepareTpcds(opt) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async { opt.run().await })
        }
        Options::PrepareClickbench(opt) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async { opt.run().await })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::dataset_path;

    #[test]
    fn resolves_datasets_under_testdata() {
        assert!(dataset_path("tpch_sf1").ends_with("testdata/tpch/benchmark_sf1"));
        assert!(dataset_path("tpcds_sf1").ends_with("testdata/tpcds/benchmark_sf1"));
        assert!(
            dataset_path("clickbench_0-100").ends_with("testdata/clickbench/benchmark_range0-100")
        );
    }
}
