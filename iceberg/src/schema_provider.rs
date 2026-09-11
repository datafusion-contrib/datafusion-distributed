use std::sync::Arc;

use dashmap::DashMap;
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::Result,
};
use futures::future::try_join_all;
use iceberg::{Catalog, NamespaceIdent, Runtime};

use crate::{IcebergCatalogTableProvider, common::df_err};

#[derive(Debug)]
pub struct IcebergSchemaProvider {
    // Using Arc + DashMap for cheap clones of the tables in this schema
    tables: Arc<DashMap<String, Arc<IcebergCatalogTableProvider>>>,
}

impl IcebergSchemaProvider {
    pub async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        iceberg_runtime: Runtime,
    ) -> Result<Self> {
        let table_names: Vec<_> = catalog
            .list_tables(&namespace)
            .await
            .map_err(df_err)?
            .into_iter()
            .map(|ident| ident.name().to_string())
            .collect();

        let table_providers = try_join_all(
            table_names
                .iter()
                .map(|name| {
                    IcebergCatalogTableProvider::try_new(
                        catalog.clone(),
                        namespace.clone(),
                        name,
                        iceberg_runtime.clone(),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await?;

        let tables = Arc::new(DashMap::new());

        // Getting a map of: <table_name, table_provider>
        for (name, provider) in table_names.into_iter().zip(table_providers) {
            tables.insert(name, Arc::new(provider));
        }

        Ok(IcebergSchemaProvider { tables })
    }
}

#[async_trait::async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .tables
            .get(name)
            .map(|provider| provider.clone() as Arc<dyn TableProvider>))
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.iter().map(|k| k.key().clone()).collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
