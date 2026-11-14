use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion_distributed::{DisplayPlanDumpOptions, DistributedMetricsFormat, display_plan_dump};
use std::io::{self, IsTerminal};
use std::path::Path;
use std::{env, fs};

#[derive(Debug, clap::Args, PartialEq)]
pub(super) struct Args {
    /// The plan dump, as produced by `datafusion_distributed::generate_plan_dump`, or the path of a
    /// file containing it. If not given, it is read from stdin.
    dump: Option<String>,

    /// Display the nodes of this stage with their metrics instead of the tree of stages. Stage 0 is
    /// the head of the plan, run by the coordinator.
    #[clap(short, long)]
    stage: Option<usize>,

    /// Enable verbose display mode. In this mode, metrics are displayed per-task, rather than
    /// globally.
    #[clap(short, long)]
    verbose: bool,

    /// Display only the metrics whose name contains this text. Repeat it to display the metrics
    /// matching any of them, like `-m bytes -m rows`.
    #[clap(short, long = "metric", value_name = "TEXT")]
    metrics: Vec<String>,
}

/// Displays a plan dump.
pub(super) fn run(args: Args) -> Result<()> {
    let dump = match args.dump {
        // Plan dumps are base64, so arguments with other characters, like `.`, are paths too.
        Some(path) if Path::new(&path).is_file() || !is_base64(&path) => fs::read_to_string(&path)
            .map_err(|err| exec_datafusion_err!("Failed to read the plan dump in {path}: {err}"))?,
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

    opts = opts.with_metrics_format(match args.verbose {
        true => DistributedMetricsFormat::PerTask,
        false => DistributedMetricsFormat::Aggregated,
    });
    for filter in args.metrics {
        opts = opts.with_metric_filter(filter);
    }
    print!("{}", display_plan_dump(dump, opts)?);
    Ok(())
}

fn is_base64(text: &str) -> bool {
    (text.trim().bytes()).all(|byte| byte.is_ascii_alphanumeric() || b"+/=".contains(&byte))
}

/// Output is colored when printed to a terminal, unless `NO_COLOR` is set. `FORCE_COLOR` keeps
/// colors when piping the output, e.g. into `less -R`.
fn color_enabled() -> bool {
    let force = env::var("FORCE_COLOR").is_ok_and(|value| !value.is_empty() && value != "0");
    let disable = env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty());
    force || (!disable && io::stdout().is_terminal())
}
