use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use crate::common::df_err;
use crate::data_source::IcebergDataSourceOptions;
use crate::{IcebergConfig, IcebergDataSource};

/// Catalog-backed, read-only table provider with automatic metadata refresh.
///
/// The provider loads fresh table metadata from the catalog on every scan. For
/// a fixed snapshot, use IcebergStaticTableProvider instead.
#[derive(Debug, Clone)]
pub struct IcebergCatalogTableProvider {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema: SchemaRef,
    iceberg_runtime: iceberg::Runtime,
}

impl IcebergCatalogTableProvider {
    /// Creates a read-only catalog-backed provider.
    pub async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        name: impl Into<String>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Result<Self> {
        let table_ident = TableIdent::new(namespace, name.into());
        let table = catalog.load_table(&table_ident).await.map_err(df_err)?;
        let schema = schema_to_arrow_schema(table.metadata().current_schema()).map_err(df_err)?;

        Ok(Self {
            catalog,
            table_ident,
            schema: Arc::new(schema),
            iceberg_runtime,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergCatalogTableProvider {
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
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(df_err)?;

        let mut data_source = IcebergDataSource::new(
            table.clone(),
            self.schema.clone(),
            Partitioning::UnknownPartitioning(state.config().target_partitions()),
            IcebergDataSourceOptions {
                snapshot_id: None,
                projection,
                filters,
                fetch: limit,
                iceberg_runtime: Some(self.iceberg_runtime.clone()),
            },
        );
        let iceberg_config = IcebergConfig::from_task_context(&state.task_ctx());
        if iceberg_config.column_stats_enabled {
            data_source = data_source.with_column_statistics(table).await?;
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
