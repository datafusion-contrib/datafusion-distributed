use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::common::{Result, exec_datafusion_err, exec_err};
use datafusion::execution::TaskContext;
use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};

/// Customizes how an Iceberg scan's file IO crosses the coordinator/worker boundary.
///
/// Set [`crate::IcebergIntegrationOptions::file_io_codec`] on coordinators and workers.
/// Both sides must use compatible codecs. The payload is opaque to the plan codec.
pub trait IcebergFileIOCodec: Debug + Send + Sync {
    /// Encodes file IO into an application-defined byte payload.
    fn encode(&self, file_io: &FileIO) -> Result<Vec<u8>>;

    /// Rebuilds file IO within the worker's query context.
    fn decode(&self, bytes: &[u8], ctx: &TaskContext) -> Result<FileIO>;
}

/// Shared file IO handling for eager and lazy plan codecs.
#[derive(Debug, Clone)]
pub(crate) struct FileIOCodec {
    storage_factory: Arc<dyn StorageFactory>,
    pub(crate) custom: Option<Arc<dyn IcebergFileIOCodec>>,
}

impl FileIOCodec {
    pub(crate) fn new(storage_factory: Arc<dyn StorageFactory>) -> Self {
        Self {
            storage_factory,
            custom: None,
        }
    }

    pub(crate) fn encode(&self, file_io: &FileIO) -> Result<EncodedFileIO> {
        match &self.custom {
            Some(codec) => Ok(EncodedFileIO {
                properties: HashMap::new(),
                custom: Some(codec.encode(file_io)?),
            }),
            None => Ok(EncodedFileIO {
                properties: file_io.config().props().clone(),
                custom: None,
            }),
        }
    }

    pub(crate) fn decode(
        &self,
        properties: HashMap<String, String>,
        custom: Option<&[u8]>,
        ctx: &TaskContext,
    ) -> Result<FileIO> {
        if let Some(bytes) = custom {
            if !properties.is_empty() {
                return exec_err!(
                    "Iceberg scan contains both storage properties and custom file IO"
                );
            }
            let codec = self.custom.as_ref().ok_or_else(|| {
                exec_datafusion_err!("Iceberg scan requires a custom file IO codec")
            })?;
            return codec.decode(bytes, ctx);
        }
        Ok(FileIOBuilder::new(Arc::clone(&self.storage_factory))
            .with_props(properties)
            .build())
    }
}

pub(crate) struct EncodedFileIO {
    pub(crate) properties: HashMap<String, String>,
    pub(crate) custom: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::io::LocalFsStorageFactory;

    #[test]
    fn empty_custom_payload_still_requires_a_codec() {
        let codec = FileIOCodec::new(Arc::new(LocalFsStorageFactory));
        let error = codec
            .decode(HashMap::new(), Some(&[]), &TaskContext::default())
            .expect_err("custom payload must not silently use default storage");
        insta::assert_snapshot!(error, @"Execution error: Iceberg scan requires a custom file IO codec");
    }

    #[test]
    fn rejects_conflicting_file_io_representations() {
        let codec = FileIOCodec::new(Arc::new(LocalFsStorageFactory));
        let error = codec
            .decode(
                HashMap::from([("region".into(), "test-region".into())]),
                Some(&[1]),
                &TaskContext::default(),
            )
            .expect_err("only one representation is valid");
        insta::assert_snapshot!(error, @"Execution error: Iceberg scan contains both storage properties and custom file IO");
    }
}
