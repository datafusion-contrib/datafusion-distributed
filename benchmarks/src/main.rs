//! DataFusion Distributed benchmark runner
mod prepare_clickbench;
mod prepare_tpcds;
mod prepare_tpch;
mod run;

use datafusion::error::Result;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    Run(run::RunOpt),
    PrepareTpch(prepare_tpch::PrepareTpchOpt),
    PrepareTpcds(prepare_tpcds::PrepareTpcdsOpt),
    PrepareClickbench(prepare_clickbench::PrepareClickBenchOpt),
}

// Main benchmark runner entrypoint
pub fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Run(opt) => opt.run(),
        Options::PrepareTpch(opt) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async { opt.run().await })
        }
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
