//! DataFusion Distributed benchmark runner
mod prepare_clickbench;
mod prepare_tpcds;
mod prepare_tpch;

use datafusion::error::Result;
use datafusion_distributed_benchmarks::{backend::ParquetBenchmarkBackend, compare, run};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    Run(run::RunOpt),
    /// Compare two saved benchmark states.
    Compare {
        /// Two `dataset[@branch]` states; omitted branches default to the current branch.
        /// With --dataset, both arguments are branch names instead.
        #[structopt(name = "STATES")]
        states: Vec<String>,

        /// Shared dataset for the existing two-branch comparison shorthand.
        #[structopt(long)]
        dataset: Option<String>,
    },
    PrepareTpch(prepare_tpch::PrepareTpchOpt),
    PrepareTpcds(prepare_tpcds::PrepareTpcdsOpt),
    PrepareClickbench(prepare_clickbench::PrepareClickBenchOpt),
}

// Main benchmark runner entrypoint
pub fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Run(opt) => opt.run(ParquetBenchmarkBackend),
        Options::Compare { states, dataset } => {
            compare::run(compare::parse_comparison_args(states, dataset)?)
        }
        Options::PrepareTpch(opt) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(opt.run())
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
