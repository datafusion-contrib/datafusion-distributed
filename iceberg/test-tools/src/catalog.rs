use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::Schema;
use iceberg::{Catalog, CatalogBuilder, Result, TableIdent};
use tempfile::TempDir;

use crate::TableBuilder;

/// An in-memory Iceberg catalog over a temporary directory, holding the
/// tables a test builds. It isn't tied to any dataset.
///
/// It owns every file its tables write. Dropping it deletes them, so keep it
/// alive while anything reads those tables. Create tables with
/// [`FixtureCatalog::table`]. Each test should build its own; nothing is
/// shared between them.
pub struct FixtureCatalog {
    catalog: Arc<MemoryCatalog>,
    _directory: TempDir,
}

impl FixtureCatalog {
    /// Creates an empty catalog in a new temporary directory.
    pub async fn new() -> Result<Self> {
        let directory = TempDir::new()?;
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "fixture",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    directory.path().display().to_string(),
                )]),
            )
            .await?;
        Ok(Self {
            catalog: Arc::new(catalog),
            _directory: directory,
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
        TableBuilder::new(self.catalog.clone(), ident, schema)
    }
}
