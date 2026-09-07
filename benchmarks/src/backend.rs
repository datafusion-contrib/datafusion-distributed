use std::path::Path;

use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed_benchmarks::datasets::register_tables;
use datafusion_distributed_iceberg::benchmarks::{self as iceberg, IcebergBenchmarkOptions};
use futures::future::BoxFuture;

type RegisterTables = for<'a> fn(&'a SessionContext, &'a Path) -> BoxFuture<'a, Result<()>>;

/// Backend callbacks shared by the coordinator and localhost worker runner.
pub struct BenchmarkBackend {
    pub register: RegisterTables,
    pub configure: Box<dyn Fn(SessionStateBuilder) -> SessionStateBuilder + Send + Sync>,
}

impl BenchmarkBackend {
    pub fn parquet() -> Self {
        Self {
            register: |ctx, path| Box::pin(register_tables(ctx, path)),
            configure: Box::new(std::convert::identity),
        }
    }

    pub fn iceberg(options: IcebergBenchmarkOptions) -> Self {
        Self {
            register: |ctx, path| Box::pin(iceberg::register_tables(ctx, path)),
            configure: Box::new(move |builder| options.configure_session(builder)),
        }
    }
}
