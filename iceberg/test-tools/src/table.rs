use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use iceberg::memory::MemoryCatalog;
use iceberg::spec::{
    Schema, TableMetadata, Transform, UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, MetadataLocation, Result, TableCreation, TableIdent};

use crate::Append;

/// Describes a new table, which [`TableBuilder::create`] then creates.
///
/// It covers what is fixed at creation: schema, partition spec and
/// properties. The table starts with no snapshot; add data with
/// [`FixtureTable::append`].
pub struct TableBuilder {
    catalog: Arc<MemoryCatalog>,
    ident: TableIdent,
    schema: Schema,
    partition_fields: Vec<(String, Transform)>,
    properties: HashMap<String, String>,
}

impl TableBuilder {
    pub(crate) fn new(catalog: Arc<MemoryCatalog>, ident: TableIdent, schema: Schema) -> Self {
        Self {
            catalog,
            ident,
            schema,
            partition_fields: Vec::new(),
            properties: HashMap::new(),
        }
    }

    /// Partitions the table by `transform` of top-level `column`. The
    /// partition field is named after the column, suffixed with the transform
    /// unless it's the identity, as Iceberg Java names them.
    pub fn partition_by(mut self, column: impl Into<String>, transform: Transform) -> Self {
        self.partition_fields.push((column.into(), transform));
        self
    }

    /// Sets table property `key`, e.g. a `write.parquet.*` setting.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Creates the table in the catalog, with format version 2, creating its
    /// namespace too if needed.
    pub async fn create(self) -> Result<FixtureTable> {
        let mut partition_spec = UnboundPartitionSpec::builder();
        for (column, transform) in self.partition_fields {
            let source = self.schema.field_by_name(&column).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("cannot partition by {column}: no such column"),
                )
            })?;
            partition_spec = partition_spec.add_partition_field(
                UnboundPartitionField::builder()
                    .source_ids(vec![source.id])
                    .name(partition_field_name(&column, transform))
                    .transform(transform)
                    .build()?,
            )?;
        }

        let namespace = self.ident.namespace();
        if !self.catalog.namespace_exists(namespace).await? {
            self.catalog
                .create_namespace(namespace, HashMap::new())
                .await?;
        }
        let table = self
            .catalog
            .create_table(
                namespace,
                TableCreation::builder()
                    .name(self.ident.name().to_string())
                    .schema(self.schema)
                    .partition_spec(partition_spec.build())
                    .properties(self.properties)
                    .build(),
            )
            .await?;
        Ok(FixtureTable {
            catalog: self.catalog,
            table,
        })
    }
}

fn partition_field_name(column: &str, transform: Transform) -> String {
    let suffix = match transform {
        Transform::Identity => return column.to_string(),
        Transform::Bucket(_) => "bucket",
        Transform::Truncate(_) => "trunc",
        Transform::Year => "year",
        Transform::Month => "month",
        Transform::Day => "day",
        Transform::Hour => "hour",
        Transform::Void => "null",
        Transform::Unknown => "unknown",
    };
    format!("{column}_{suffix}")
}

/// A table created by a [`TableBuilder`], and the handle for changing it.
///
/// It tracks the table as of its latest commit, so [`FixtureTable::table`]
/// reflects every change made through it. Changes made in other ways, such as
/// SQL `INSERT`s, aren't seen until the table is loaded from the catalog again.
pub struct FixtureTable {
    catalog: Arc<MemoryCatalog>,
    table: Table,
}

impl FixtureTable {
    /// The table as of its latest commit through this handle.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// The table's name in the catalog.
    pub fn ident(&self) -> &TableIdent {
        self.table.identifier()
    }

    /// The table's current metadata file.
    pub fn metadata_location(&self) -> Result<&str> {
        self.table.metadata_location_result()
    }

    /// Starts a commit that appends data files.
    pub fn append(&mut self) -> Append<'_> {
        Append::new(self)
    }

    /// Makes `metadata` the table's current metadata.
    ///
    /// iceberg-rust's catalogs only accept commits built from transaction
    /// actions, and there is none for some of the changes fixtures need, such
    /// as choosing snapshot ids. This writes the next metadata file and then
    /// re-registers the table under it, which the in-memory catalog does
    /// without touching the table's files.
    pub(crate) async fn commit(&mut self, metadata: TableMetadata) -> Result<()> {
        let location = MetadataLocation::from_str(self.metadata_location()?)?.with_next_version();
        metadata.write_to(self.table.file_io(), &location).await?;
        let ident = self.ident().clone();
        self.catalog.drop_table(&ident).await?;
        self.table = self
            .catalog
            .register_table(&ident, location.to_string())
            .await?;
        Ok(())
    }
}
