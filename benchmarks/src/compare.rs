use crate::format::BenchmarkFormat;
use crate::results::{
    BenchResult, branch_results_path, dataset_path, get_current_branch, print_comparison_total,
};
use datafusion::common::{Result, internal_err};
use structopt::StructOpt;

/// Compare different runs of the tpch benchmarks.
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct CompareOpt {
    /// Two branches to compare. With --compare-iceberg, accepts at most one branch,
    /// defaulting to the current branch.
    #[structopt(name = "BRANCHES")]
    pub branches: Vec<String>,

    /// Path to data files
    #[structopt(long)]
    dataset: String,

    /// Use Iceberg results on both branches.
    #[structopt(long, conflicts_with = "compare-iceberg")]
    iceberg: bool,

    /// Compare Parquet [prev] against Iceberg [new].
    #[structopt(long, conflicts_with = "iceberg")]
    compare_iceberg: bool,
}

impl CompareOpt {
    pub fn run(&self) -> Result<()> {
        let branches = match (self.branches.as_slice(), self.compare_iceberg) {
            ([], true) => vec![get_current_branch(); 2],
            ([branch], true) => vec![branch.clone(); 2],
            (_, true) => {
                return internal_err!(
                    "--compare-iceberg accepts at most one branch; comparing formats across branches is not supported"
                );
            }
            _ => self.branches.clone(),
        };
        let (base, new) = match branches.as_slice() {
            [one, two] => (one, two),
            rest => {
                return internal_err!("Exactly two branches must be specified, got: {rest:?}");
            }
        };
        if self.compare_iceberg {
            println!(
                "=== Comparing {} results from branch '{}' (parquet) [prev] with '{}' (iceberg) [new] ===",
                self.dataset, base, new
            );
        } else {
            println!(
                "=== Comparing {} results from branch '{}' [prev] with '{}' [new] ===",
                self.dataset, base, new
            );
        }
        let base_format = BenchmarkFormat::new(self.iceberg);
        let new_format = BenchmarkFormat::new(self.iceberg || self.compare_iceberg);
        let data_dir = dataset_path(&self.dataset);
        let base_dir = (base_format.directory)(&data_dir);
        let new_dir = (new_format.directory)(&data_dir);
        let base = BenchResult::load_many(&branch_results_path(&base_dir, base));
        let new = BenchResult::load_many(&branch_results_path(&new_dir, new));
        if self.compare_iceberg && (base.is_empty() || new.is_empty()) {
            return internal_err!(
                "Missing saved benchmark results; run both sides before comparing"
            );
        }
        for query in new.iter() {
            let Some(prev) = base.iter().find(|v| v.id == query.id) else {
                continue;
            };
            query.compare(prev)
        }
        print_comparison_total(&base, &new);
        Ok(())
    }
}
