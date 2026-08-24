use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result as IcebergResult};
use serde::{Deserialize, Serialize};

use crate::{IcebergExt, IcebergIntegrationOptions};

pub const FIXTURE_URI: &str = "s3://iceberg-test/warehouse/taxi";
const WAREHOUSE_URI: &str = "s3://iceberg-test/warehouse/";

pub struct IcebergTestHarness {
    ctx: SessionContext,
}

impl IcebergTestHarness {
    pub async fn new() -> Result<Self> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_iceberg_integration(IcebergIntegrationOptions {
                storage_factory: Arc::new(FixtureStorageFactory::default()),
                iceberg_runtime: iceberg::Runtime::current(),
            })
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
         LOCATION '{FIXTURE_URI}/metadata/v1.metadata.json'"
        ))
        .await?
        .collect()
        .await?;
        Ok(Self { ctx })
    }

    #[cfg(test)]
    pub(crate) fn context(&self) -> &SessionContext {
        &self.ctx
    }

    pub async fn query(&self, sql: &str) -> Result<(String, String)> {
        let dataframe: DataFrame = self.ctx.sql(sql).await?;
        let plan = dataframe.create_physical_plan().await?;
        let batches = dataframe.collect().await?;

        Ok((
            displayable(plan.as_ref()).indent(true).to_string(),
            pretty_format_batches(&batches)?.to_string(),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureStorageFactory {
    root: PathBuf,
}

impl Default for FixtureStorageFactory {
    fn default() -> Self {
        Self {
            root: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../testdata/iceberg"),
        }
    }
}

#[typetag::serde]
impl StorageFactory for FixtureStorageFactory {
    fn build(&self, _config: &StorageConfig) -> IcebergResult<Arc<dyn Storage>> {
        Ok(Arc::new(FixtureStorage {
            root: self.root.clone(),
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureStorage {
    root: PathBuf,
}

impl FixtureStorage {
    fn local_path(&self, path: &str) -> IcebergResult<String> {
        let relative = path.strip_prefix(WAREHOUSE_URI).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("unsupported fixture URI: {path}"),
            )
        })?;
        Ok(self.root.join(relative).display().to_string())
    }

    fn local(&self) -> LocalFsStorage {
        LocalFsStorage::new()
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for FixtureStorage {
    async fn exists(&self, path: &str) -> IcebergResult<bool> {
        self.local().exists(&self.local_path(path)?).await
    }

    async fn metadata(&self, path: &str) -> IcebergResult<FileMetadata> {
        self.local().metadata(&self.local_path(path)?).await
    }

    async fn read(&self, path: &str) -> IcebergResult<Bytes> {
        self.local().read(&self.local_path(path)?).await
    }

    async fn reader(&self, path: &str) -> IcebergResult<Box<dyn FileRead>> {
        self.local().reader(&self.local_path(path)?).await
    }

    async fn write(&self, path: &str, bytes: Bytes) -> IcebergResult<()> {
        self.local().write(&self.local_path(path)?, bytes).await
    }

    async fn writer(&self, path: &str) -> IcebergResult<Box<dyn FileWrite>> {
        self.local().writer(&self.local_path(path)?).await
    }

    async fn delete(&self, path: &str) -> IcebergResult<()> {
        self.local().delete(&self.local_path(path)?).await
    }

    async fn delete_prefix(&self, path: &str) -> IcebergResult<()> {
        self.local().delete_prefix(&self.local_path(path)?).await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> IcebergResult<()> {
        let mut paths = paths;
        while let Some(path) = paths.next().await {
            self.delete(&path).await?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> IcebergResult<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> IcebergResult<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}
