// Iceberg's Catalog backed into DataFusion catalog

use std::{collections::HashMap, sync::Arc};

use datafusion::{
    catalog::{CatalogProvider, SchemaProvider},
    error::Result,
};
use futures::future::try_join_all;
use iceberg::{Catalog, Runtime};

use crate::{common::df_err, schema_provider::IcebergSchemaProvider};

#[derive(Debug)]
pub struct IcebergCatalog {
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl IcebergCatalog {
    pub async fn try_new(catalog: Arc<dyn Catalog>, iceberg_runtime: Runtime) -> Result<Self> {
        let namespaces = catalog.list_namespaces(None).await.map_err(df_err)?;

        let schema_providers = try_join_all(namespaces.iter().map(|ns| {
            IcebergSchemaProvider::try_new(catalog.clone(), ns.clone(), iceberg_runtime.clone())
        }))
        .await?;

        let schemas: HashMap<String, Arc<dyn SchemaProvider>> = namespaces
            .into_iter()
            .zip(schema_providers)
            .map(|(name, provider)| {
                let key = name.as_ref().join(".");
                (key, Arc::new(provider) as Arc<dyn SchemaProvider>)
            })
            .collect();

        Ok(Self { schemas })
    }
}

impl CatalogProvider for IcebergCatalog {
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }
}
