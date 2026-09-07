use std::path::Path;

use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_distributed_benchmarks::datasets::{iceberg, register_tables};
use futures::future::BoxFuture;

type RegisterTables = for<'a> fn(&'a SessionContext, &'a Path) -> BoxFuture<'a, Result<()>>;

/// Resolve the dataset name and registration once, before entering the benchmark runner.
pub(crate) struct BenchmarkFormat {
    pub dataset: fn(&str) -> String,
    pub register: RegisterTables,
}

impl BenchmarkFormat {
    pub fn new(iceberg: bool) -> Self {
        if iceberg {
            Self {
                dataset: |name| {
                    iceberg::output_path(Path::new(name))
                        .to_string_lossy()
                        .into_owned()
                },
                register: |ctx, path| Box::pin(iceberg::register_tables(ctx, path)),
            }
        } else {
            Self {
                dataset: str::to_owned,
                register: |ctx, path| Box::pin(register_tables(ctx, path)),
            }
        }
    }
}
