use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion_distributed::{DisplayPlanDumpOptions, display_plan_dump};
use std::env;
use std::io::{self, IsTerminal};

#[derive(Debug, clap::Args, PartialEq)]
pub(super) struct Args {
    /// The plan dump, as produced by `datafusion_distributed::generate_plan_dump`. If not given, it
    /// is read from stdin.
    dump: Option<String>,

    /// Display the nodes of this stage with their metrics instead of the tree of stages. Stage 0 is
    /// the head of the plan, run by the coordinator.
    #[clap(short, long)]
    stage: Option<usize>,
}

/// Displays a plan dump.
pub(super) fn run(args: Args) -> Result<()> {
    let dump = match args.dump {
        Some(dump) => dump,
        None if io::stdin().is_terminal() => {
            return exec_err!("Pass a plan dump as an argument, or pipe it through stdin");
        }
        None => io::read_to_string(io::stdin())?,
    };
    let mut opts = DisplayPlanDumpOptions::default().with_color(color_enabled());
    if let Some(stage) = args.stage {
        opts = opts.with_stage_id(stage);
    }
    print!("{}", display_plan_dump(dump, opts)?);
    Ok(())
}

/// Output is colored when printed to a terminal, unless `NO_COLOR` is set. `FORCE_COLOR` keeps
/// colors when piping the output, e.g. into `less -R`.
fn color_enabled() -> bool {
    let force = env::var("FORCE_COLOR").is_ok_and(|value| !value.is_empty() && value != "0");
    let disable = env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty());
    force || (!disable && io::stdout().is_terminal())
}
