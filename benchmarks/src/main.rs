//! DataFusion Distributed benchmark runner
mod compare;
mod generate_tpch;
mod prepare_clickbench;
mod prepare_tpcds;
mod results;
mod run;

use datafusion::error::Result;
use structopt::StructOpt;

pub(crate) mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub(crate) const DATA_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data");
pub(crate) const RESULTS_DIR: &str = ".results";

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    Run(run::RunOpt),
    Compare(compare::CompareOpt),
    GenerateTpch(generate_tpch::GenerateTpchOpt),
    PrepareTpcds(prepare_tpcds::PrepareTpcdsOpt),
    PrepareClickbench(prepare_clickbench::PrepareClickBenchOpt),
}

// Main benchmark runner entrypoint
pub fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Run(opt) => opt.run(),
        Options::Compare(opt) => opt.run(),
        Options::GenerateTpch(opt) => opt.run(),
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
