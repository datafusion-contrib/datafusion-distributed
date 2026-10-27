use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed_iceberg_test_tools::{FixtureCatalog, create_taxi_table};
use iceberg::TableIdent;
use iceberg::io::LocalFsStorageFactory;

use crate::{IcebergExt, IcebergIntegrationOptions};

/// Registers a generated taxi table as `taxi` with the given SQL table options, which may
/// be empty. The table's files live as long as `fixtures`.
pub(crate) async fn taxi_context(
    fixtures: &FixtureCatalog,
    options: &str,
) -> Result<SessionContext> {
    let ident = TableIdent::from_strs(["default", "taxi"]).map_err(external)?;
    let taxi = create_taxi_table(fixtures, ident).await.map_err(external)?;
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_iceberg_integration(IcebergIntegrationOptions {
            storage_factory: Arc::new(LocalFsStorageFactory),
            iceberg_runtime: iceberg::Runtime::current(),
        })
        .build();
    let ctx = SessionContext::new_with_state(state);
    let mut statement = format!(
        "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG LOCATION '{}'",
        taxi.metadata_location().map_err(external)?
    );
    if !options.is_empty() {
        statement.push_str(&format!(" OPTIONS ({options})"));
    }
    ctx.sql(&statement).await?.collect().await?;
    Ok(ctx)
}

pub(crate) fn external(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
