use crate::results::{BenchResult, get_current_branch, print_comparison_total};
use datafusion::common::{Result, internal_err};

/// One saved benchmark state, independent of how the CLI selected it.
pub struct BenchmarkState {
    pub dataset: String,
    pub branch: String,
}

pub fn parse_comparison_args(
    states: Vec<String>,
    dataset: Option<String>,
) -> Result<[BenchmarkState; 2]> {
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
                None => (value, get_current_branch()),
            },
        };
        if dataset.is_empty() || branch.is_empty() {
            return datafusion::common::internal_err!("Dataset and branch must not be empty");
        }
        Ok(BenchmarkState { dataset, branch })
    };
    Ok([state(base)?, state(new)?])
}

pub fn run([base, new]: [BenchmarkState; 2]) -> Result<()> {
    println!(
        "=== Comparing {} results from branch '{}' [prev] with {} results from branch '{}' [new] ===",
        base.dataset, base.branch, new.dataset, new.branch
    );
    let base_results = BenchResult::load_many(&base.dataset, &base.branch);
    let new_results = BenchResult::load_many(&new.dataset, &new.branch);
    // Preserve the existing empty-result behavior for same-dataset branch comparisons.
    if base.dataset != new.dataset && (base_results.is_empty() || new_results.is_empty()) {
        return internal_err!("Missing saved benchmark results; run both sides before comparing");
    }
    for query in new_results.iter() {
        let Some(prev) = base_results.iter().find(|v| v.id == query.id) else {
            continue;
        };
        query.compare(prev)
    }
    print_comparison_total(&base_results, &new_results);
    Ok(())
}
