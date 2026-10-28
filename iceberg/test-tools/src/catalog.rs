use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::Result;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::Schema;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use tempfile::TempDir;

use crate::TableBuilder;
use crate::error::df_err;

/// An in-memory Iceberg catalog over a temporary directory, holding the
/// tables a test builds. It isn't tied to any dataset.
///
/// Its tables' files are deleted once it and every table created in it have
/// been dropped, so a [`FixtureTable`](crate::FixtureTable) can outlive it.
/// Create tables with
/// [`FixtureCatalog::table`]. Each test should build its own; nothing is
/// shared between them.
pub struct FixtureCatalog {
    catalog: Arc<MemoryCatalog>,
    directory: Arc<TempDir>,
}

impl FixtureCatalog {
    /// Creates an empty catalog in a new temporary directory. Its tables' paths
    /// are `file://` URLs, so any storage that reads local files can read them.
    pub async fn try_new() -> Result<Self> {
        let directory = TempDir::new()?;
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "fixture",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", directory.path().display()),
                )]),
            )
            .await
            .map_err(df_err)?;
        Ok(Self {
            catalog: Arc::new(catalog),
            directory: Arc::new(directory),
        })
    }

    /// The underlying Iceberg catalog, e.g. to pass to
    /// `IcebergCatalogProvider::try_new`.
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }

    /// Starts building table `ident` with `schema`. Its namespace is created
    /// if it doesn't exist.
    pub fn table(&self, ident: TableIdent, schema: Schema) -> TableBuilder {
        TableBuilder::new(
            self.catalog.clone(),
            Arc::clone(&self.directory),
            ident,
            schema,
        )
    }
}
