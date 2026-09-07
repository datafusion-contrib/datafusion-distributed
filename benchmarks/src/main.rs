//! DataFusion Distributed benchmark runner
mod compare;
mod format;
mod prepare_clickbench;
mod prepare_iceberg;
mod prepare_tpcds;
mod prepare_tpch;
mod results;
mod run;

use datafusion::error::Result;
use structopt::StructOpt;

pub(crate) const RESULTS_DIR: &str = ".results";

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    Run(run::RunOpt),
    /// Compare two saved benchmark states.
    Compare {
        /// Two branches to compare. With --compare-iceberg, accepts at most one branch,
        /// defaulting to the current branch.
        #[structopt(name = "BRANCHES")]
        branches: Vec<String>,

        /// Path to data files
        #[structopt(long)]
        dataset: String,

        /// Use Iceberg results on both branches.
        #[structopt(long, conflicts_with = "compare-iceberg")]
        iceberg: bool,

        /// Compare Parquet [prev] against Iceberg [new].
        #[structopt(long, conflicts_with = "iceberg")]
        compare_iceberg: bool,
    },
    PrepareTpch(prepare_tpch::PrepareTpchOpt),
    PrepareIceberg(prepare_iceberg::PrepareIcebergOpt),
    PrepareTpcds(prepare_tpcds::PrepareTpcdsOpt),
    PrepareClickbench(prepare_clickbench::PrepareClickBenchOpt),
}

fn comparison_states(
    branches: Vec<String>,
    dataset: String,
    iceberg: bool,
    compare_iceberg: bool,
) -> Result<[compare::BenchmarkState; 2]> {
    let [base, new] = match (branches.as_slice(), compare_iceberg) {
        ([], true) => {
            let branch = results::get_current_branch();
            [branch.clone(), branch]
        }
        ([branch], true) => [branch.clone(), branch.clone()],
        ([base, new], false) => [base.clone(), new.clone()],
        (_, true) => {
            return datafusion::common::internal_err!(
                "--compare-iceberg accepts at most one branch; comparing formats across branches is not supported"
            );
        }
        (rest, false) => {
            return datafusion::common::internal_err!(
                "Exactly two branches must be specified, got: {rest:?}"
            );
        }
    };
    let state = |iceberg, branch| compare::BenchmarkState {
        dataset: (format::BenchmarkFormat::new(iceberg).dataset)(&dataset),
        branch,
    };
    Ok([state(iceberg, base), state(iceberg || compare_iceberg, new)])
}

// Main benchmark runner entrypoint
pub fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Run(opt) => opt.run(),
        Options::Compare {
            branches,
            dataset,
            iceberg,
            compare_iceberg,
        } => compare::run(comparison_states(
            branches,
            dataset,
            iceberg,
            compare_iceberg,
        )?),
        Options::PrepareTpch(opt) => opt.run(),
        Options::PrepareIceberg(opt) => {
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
