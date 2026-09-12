//! DataFusion Distributed benchmark runner
mod backend;
mod compare;
mod prepare_clickbench;
mod prepare_tpcds;
mod prepare_tpch;
mod results;
mod run;

use backend::BenchmarkBackend;
use datafusion::error::Result;
use datafusion_distributed_iceberg::benchmarks::PrepareIcebergOpt;
use structopt::{StructOpt, clap::arg_enum};

arg_enum! {
    #[derive(Debug)]
    enum Format { Parquet, Iceberg }
}

pub(crate) const RESULTS_DIR: &str = ".results";

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    /// Run benchmarks using the selected table backend.
    Run {
        #[structopt(flatten)]
        options: run::RunOpt,
        /// Table format of the dataset directory.
        #[structopt(long, default_value = "parquet", possible_values = &Format::variants(), case_insensitive = true)]
        format: Format,
    },
    /// Compare two saved benchmark states.
    Compare {
        /// Two dataset[@branch] states; omitted branches default to the current branch.
        /// With --dataset, both arguments are branch names instead.
        #[structopt(name = "STATES")]
        states: Vec<String>,

        /// Shared dataset for the existing two-branch comparison shorthand.
        #[structopt(long)]
        dataset: Option<String>,
    },
    PrepareTpch(prepare_tpch::PrepareTpchOpt),
    PrepareIceberg(PrepareIcebergOpt),
    PrepareTpcds(prepare_tpcds::PrepareTpcdsOpt),
    PrepareClickbench(prepare_clickbench::PrepareClickBenchOpt),
}

fn comparison_states(
    states: Vec<String>,
    dataset: Option<String>,
) -> Result<[compare::BenchmarkState; 2]> {
    let [base, new]: [String; 2] = states.try_into().map_err(|states| {
        datafusion::common::internal_datafusion_err!(
            "Exactly two states must be specified, got: {states:?}"
        )
    })?;
    let state = |value: String| {
        let (dataset, branch) = match &dataset {
            Some(dataset) => (dataset.clone(), value),
            None => match value.rsplit_once('@') {
                Some((dataset, branch)) => (dataset.to_owned(), branch.to_owned()),
                None => (value, results::get_current_branch()),
            },
        };
        if dataset.is_empty() || branch.is_empty() {
            return datafusion::common::internal_err!("Dataset and branch must not be empty");
        }
        Ok(compare::BenchmarkState { dataset, branch })
    };
    Ok([state(base)?, state(new)?])
}

// Main benchmark runner entrypoint
pub fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Run { options, format } => options.run(match format {
            Format::Parquet => BenchmarkBackend::parquet(),
            Format::Iceberg => BenchmarkBackend::iceberg(),
        }),
        Options::Compare { states, dataset } => compare::run(comparison_states(states, dataset)?),
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
