use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, plan_datafusion_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;

use crate::common::df_err;
use crate::data_source::IcebergDataSourceOptions;
use crate::{IcebergConfig, IcebergDataSource};

/// Static, read-only provider for a table or a specific snapshot.
#[derive(Debug, Clone)]
pub struct IcebergStaticTableProvider {
    table: iceberg::table::Table,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
    iceberg_runtime: iceberg::Runtime,
}

impl IcebergStaticTableProvider {
    /// Creates a provider that reads the provided table snapshot, or the current snapshot
    /// if none provided.
    pub fn try_new(
        table: iceberg::table::Table,
        snapshot_id: Option<i64>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Result<Self> {
        let table_schema = if let Some(snapshot_id) = snapshot_id {
            let snapshot = table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "snapshot id {snapshot_id} not found in table {}",
                        table.identifier().name()
                    )
                })?;
            snapshot.schema(table.metadata()).map_err(df_err)?
        } else {
            Arc::clone(table.metadata().current_schema())
        };

        Ok(Self {
            table,
            snapshot_id,
            schema: Arc::new(schema_to_arrow_schema(&table_schema).map_err(df_err)?),
            iceberg_runtime,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergStaticTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut data_source = IcebergDataSource::new(
            self.table.clone(),
            self.schema.clone(),
            Partitioning::UnknownPartitioning(state.config().target_partitions()),
            IcebergDataSourceOptions {
                snapshot_id: self.snapshot_id,
                projection,
                filters,
                fetch: limit,
                iceberg_runtime: Some(self.iceberg_runtime.clone()),
            },
        );
        let iceberg_config = IcebergConfig::from_task_context(&state.task_ctx());
        if iceberg_config.column_stats_enabled {
            data_source = data_source
                .with_column_statistics(self.table.clone())
                .await?;
        }
        Ok(DataSourceExec::from_data_source(data_source))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
