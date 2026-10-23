use std::sync::Arc;

mod catalog;
mod factory;
mod r#static;

pub use catalog::IcebergCatalogTableProvider;
pub use factory::IcebergTableProviderFactory;
pub use r#static::IcebergStaticTableProvider;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Result, config_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use iceberg::table::Table;

use crate::file_source;
use crate::{IcebergConfig, IcebergDataSource};

/// Options shared by eager and lazy scan planning.
#[derive(Default, Clone)]
pub(crate) struct ScanOptions<'a> {
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) fetch: Option<usize>,
    pub(crate) filters: &'a [Expr],
    pub(crate) iceberg_runtime: Option<iceberg::Runtime>,
}

/// Choose the execution source before constructing either representation.
async fn plan_scan(
    table: Table,
    schema: SchemaRef,
    state: &dyn Session,
    options: ScanOptions<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let context = state.task_ctx();
    let config = IcebergConfig::from_task_context(&context);
    if config.eager_file_planning {
        return file_source::plan_scan(table, schema, options, context, config).await;
    }
    if config.greedy_file_assignment {
        return config_err!(
            "Iceberg greedy_file_assignment requires iceberg.eager_file_planning=true"
        );
    }
    let projection = options.projection.map(Vec::as_slice);
    let mut source = IcebergDataSource::new(
        table.clone(),
        schema,
        Partitioning::UnknownPartitioning(state.config().target_partitions()),
        options,
    );
    if config.column_stats_enabled {
        source = source.with_column_statistics(table, projection).await?;
    }
    Ok(DataSourceExec::from_data_source(source))
}
