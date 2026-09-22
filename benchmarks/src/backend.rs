use std::path::Path;

use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;

use crate::datasets::register_tables;

#[async_trait]
pub trait BenchmarkBackend: Send + Sync + 'static {
    async fn register_tables(&mut self, ctx: &SessionContext, path: &Path) -> Result<()>;

    fn configure_session(&self, builder: SessionStateBuilder) -> SessionStateBuilder {
        builder
    }
}

pub struct ParquetBenchmarkBackend;

#[async_trait]
impl BenchmarkBackend for ParquetBenchmarkBackend {
    async fn register_tables(&mut self, ctx: &SessionContext, path: &Path) -> Result<()> {
        register_tables(ctx, path).await
    }
}
