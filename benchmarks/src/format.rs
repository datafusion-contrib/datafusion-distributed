use std::path::{Path, PathBuf};

use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_distributed_benchmarks::datasets::{iceberg, register_tables};
use futures::future::BoxFuture;

type RegisterTables = for<'a> fn(&'a SessionContext, &'a Path) -> BoxFuture<'a, Result<()>>;

/// Select format-specific behavior once; execution and results share the same directory.
pub(crate) struct BenchmarkFormat {
    pub directory: fn(&Path) -> PathBuf,
    pub register: RegisterTables,
}

impl BenchmarkFormat {
    pub fn new(iceberg: bool) -> Self {
        if iceberg {
            Self {
                directory: |path| path.join(iceberg::ICEBERG_DIR),
                register: |ctx, path| Box::pin(iceberg::register_tables(ctx, path)),
            }
        } else {
            Self {
                directory: Path::to_path_buf,
                register: |ctx, path| Box::pin(register_tables(ctx, path)),
            }
        }
    }
}
