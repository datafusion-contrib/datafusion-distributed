use std::collections::HashMap;
use std::fmt::Debug;

use datafusion::common::Result;
use datafusion::execution::TaskContext;
use iceberg::io::FileIO;

/// Customizes how an Iceberg scan's file IO crosses the coordinator/worker boundary.
///
/// Register with [`crate::IcebergFileIOExt`] on the coordinator and workers.
/// The map is carried in the existing storage-properties field of the plan; no
/// storage implementation or factory is serialized. Implementations using a custom
/// representation must agree on that representation on both sides.
pub trait IcebergFileIOCodec: Debug + Send + Sync {
    /// Encodes file IO as storage properties. Override to transport custom metadata.
    fn encode(&self, file_io: &FileIO) -> Result<HashMap<String, String>> {
        Ok(file_io.config().props().clone())
    }

    /// Rebuilds file IO for each decoded scan, within the worker's query context.
    ///
    /// This can bind a storage factory to query-local observability or use
    /// extensions in `ctx` to resolve application-managed storage.
    fn decode(&self, properties: HashMap<String, String>, ctx: &TaskContext) -> Result<FileIO>;
}
