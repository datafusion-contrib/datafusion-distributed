use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::sql::TableReference;
use iceberg::TableIdent;
use iceberg::io::{FileIOBuilder, StorageFactory};
use iceberg::table::StaticTable;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

use crate::IcebergStaticTableProvider;
use crate::common::df_err;

const SNAPSHOT_ID_OPTION: &str = "iceberg.snapshot_id";

/// Creates table providers for `CREATE EXTERNAL TABLE ... STORED AS ICEBERG`.
///
/// # Example
///
/// ```no_run
/// use datafusion::execution::SessionStateBuilder;
/// use datafusion::prelude::SessionContext;
/// use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};
///
/// # async fn example() -> datafusion::error::Result<()> {
/// let state = SessionStateBuilder::new()
///     .with_default_features()
///     .with_iceberg_integration(IcebergIntegrationOptions::default())
///     .build();
/// let ctx = SessionContext::new_with_state(state);
///
/// ctx.sql(
///     "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
///      LOCATION 's3://warehouse/taxi/metadata/v1.metadata.json'",
/// )
/// .await?
/// .collect()
/// .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct IcebergTableProviderFactory {
    storage_factory: Arc<dyn StorageFactory>,
    iceberg_runtime: iceberg::Runtime,
}

impl IcebergTableProviderFactory {
    pub fn new() -> Self {
        Self {
            storage_factory: Arc::new(OpenDalResolvingStorageFactory::new()),
            iceberg_runtime: iceberg::Runtime::current(),
        }
    }

    /// Create a new factory with a custom storage factory for creating FileIO instances.
    pub fn new_with_storage_factory(storage_factory: Arc<dyn StorageFactory>) -> Self {
        Self::new_with_runtime(storage_factory, iceberg::Runtime::current())
    }

    /// Creates a factory with custom storage and Tokio runtime handles.
    pub fn new_with_runtime(
        storage_factory: Arc<dyn StorageFactory>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Self {
        Self {
            storage_factory,
            iceberg_runtime,
        }
    }
}

impl Default for IcebergTableProviderFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProviderFactory for IcebergTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        check_cmd(cmd)?;

        let table_name = &cmd.name;
        let metadata_file_path = &cmd.location;
        let options = &cmd.options;
        let snapshot_id = parse_snapshot_id(options)?;
        let mut storage_props = options.clone();
        storage_props.remove(SNAPSHOT_ID_OPTION);

        let table_name_with_ns = match table_name {
            TableReference::Bare { table } => {
                Cow::Owned(TableReference::partial("default", table.as_ref()))
            }
            other => Cow::Borrowed(other),
        };

        let table_ident = TableIdent::from_strs(table_name_with_ns.to_vec()).map_err(df_err)?;
        let file_io = FileIOBuilder::new(self.storage_factory.clone())
            .with_props(&storage_props)
            .build();
        let table = StaticTable::from_metadata_file(metadata_file_path, table_ident, file_io)
            .await
            .map_err(df_err)?
            .into_table();

        Ok(Arc::new(IcebergStaticTableProvider::try_new(
            table,
            snapshot_id,
            self.iceberg_runtime.clone(),
        )?))
    }
}

fn parse_snapshot_id(options: &HashMap<String, String>) -> Result<Option<i64>> {
    options
        .get(SNAPSHOT_ID_OPTION)
        .map(|snapshot_id| {
            snapshot_id.parse::<i64>().map_err(|error| {
                plan_datafusion_err!(
                    "{SNAPSHOT_ID_OPTION} must be a valid Iceberg snapshot ID: {error}"
                )
            })
        })
        .transpose()
}

fn check_cmd(cmd: &CreateExternalTable) -> Result<()> {
    let CreateExternalTable {
        schema,
        table_partition_cols,
        order_exprs,
        constraints,
        column_defaults,
        ..
    } = cmd;

    // Check if any of the fields violate the constraints in a single condition
    let is_invalid = !schema.fields().is_empty()
        || !table_partition_cols.is_empty()
        || !order_exprs.is_empty()
        || !constraints.is_empty()
        || !column_defaults.is_empty();

    if is_invalid {
        return plan_err!(
            "Currently we only support reading existing icebergs tables in external table command. To create new table, please use catalog provider."
        );
    }

    Ok(())
}
