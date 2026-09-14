use std::path::Path;

use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use futures::future::BoxFuture;

use crate::datasets::register_tables;

pub type RegisterTables = for<'a> fn(&'a SessionContext, &'a Path) -> BoxFuture<'a, Result<()>>;
pub type ConfigureSession = fn(SessionStateBuilder) -> SessionStateBuilder;

/// Backend callbacks shared by the coordinator and localhost worker runner.
#[derive(Clone, Copy)]
pub struct BenchmarkBackend {
    pub(crate) register: RegisterTables,
    pub(crate) configure: ConfigureSession,
}

impl BenchmarkBackend {
    pub fn new(register: RegisterTables, configure: ConfigureSession) -> Self {
        Self {
            register,
            configure,
        }
    }

    pub fn parquet() -> Self {
        Self::new(
            |ctx, path| Box::pin(register_tables(ctx, path)),
            std::convert::identity,
        )
    }
}
