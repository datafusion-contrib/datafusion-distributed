use crate::results::BenchResult;
use datafusion::common::{Result, internal_err};
use structopt::StructOpt;

/// Compare different runs of the tpch benchmarks.
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct CompareOpt {
    /// Query number. If not specified, runs all queries
    #[structopt(name = "BRANCHES", required = true)]
    pub branches: Vec<String>,

    /// Path to data files
    #[structopt(long)]
    dataset: String,
}

impl CompareOpt {
    pub fn run(&self) -> Result<()> {
        let (base, new) = match self.branches.as_slice() {
            [one, two] => (one, two),
            rest => {
                return internal_err!("Exactly two branches must be specified, got: {rest:?}");
            }
        };
        println!(
            "=== Comparing {} results from branch '{}' [prev] with '{}' [new] ===",
            self.dataset, base, new
        );
        let base = BenchResult::load_many(&self.dataset, base);
        let new = BenchResult::load_many(&self.dataset, new);
        for query in new {
            let Some(prev) = base.iter().find(|v| v.id == query.id) else {
                continue;
            };
            query.compare(prev)
        }
        Ok(())
    }
}
