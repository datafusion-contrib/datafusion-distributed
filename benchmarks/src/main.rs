//! DataFusion Distributed benchmark runner
mod convert;
mod run;

use datafusion::error::Result;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    Tpch(run::RunOpt),
    TpchConvert(convert::ConvertOpt),
}

// Main benchmark runner entrypoint
pub fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Tpch(opt) => opt.run(),
        Options::TpchConvert(opt) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async { opt.run().await })
        }
    }
}
