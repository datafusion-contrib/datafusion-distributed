use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::DistributedCodec;
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::spec::TableMetadata;
use iceberg::{Error, ErrorKind, Result as IcebergResult};
use serde::{Deserialize, Serialize};

use super::taxi_metadata;
use crate::{IcebergExt, IcebergIntegrationOptions};

pub const FIXTURE_URI: &str = "s3://iceberg-test/warehouse/taxi";
const WAREHOUSE_URI: &str = "s3://iceberg-test/warehouse/";
const FIXTURE_METADATA_URI: &str = "s3://iceberg-test/warehouse/taxi/metadata/v1.metadata.json";

pub struct IcebergTestHarness {
    pub ctx: SessionContext,
}

impl IcebergTestHarness {
    pub fn builder() -> IcebergTestHarnessBuilder {
        IcebergTestHarnessBuilder::default()
    }

    pub async fn new() -> Result<Self> {
        Self::builder().build().await
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

    pub async fn physical_plan(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        self.ctx.sql(sql).await?.create_physical_plan().await
    }

    pub fn roundtrip_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let task_ctx = self.ctx.task_ctx();
        let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
        let proto =
            datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
        proto.try_into_physical_plan(&task_ctx, &codec)
    }
}

pub struct IcebergTestHarnessBuilder {
    metadata: TableMetadata,
    table_options: BTreeMap<String, String>,
    files: HashMap<String, Vec<u8>>,
}

impl Default for IcebergTestHarnessBuilder {
    fn default() -> Self {
        Self {
            metadata: taxi_metadata(),
            table_options: BTreeMap::new(),
            files: HashMap::new(),
        }
    }
}

impl IcebergTestHarnessBuilder {
    /// Serves the supplied metadata while reading data files from the taxi fixture.
    pub fn with_table_metadata(mut self, metadata: TableMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Sets a table option; later values replace earlier ones for the same key.
    pub fn with_table_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.table_options.insert(key.into(), value.into());
        self
    }

    /// Overrides a fixture URI with in-memory bytes; later files replace earlier ones.
    /// An explicit file also takes precedence over the generated table metadata.
    pub fn with_file(mut self, uri: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        self.files.insert(uri.into(), bytes.into());
        self
    }

    pub async fn build(mut self) -> Result<IcebergTestHarness> {
        let metadata = serde_json::to_vec(&self.metadata)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        self.files
            .entry(FIXTURE_METADATA_URI.to_string())
            .or_insert(metadata);
        let storage_factory = FixtureStorageFactory {
            files: self.files,
            ..FixtureStorageFactory::default()
        };
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_iceberg_integration(IcebergIntegrationOptions {
                storage_factory: Arc::new(storage_factory),
                iceberg_runtime: iceberg::Runtime::current(),
            })
            .build();
        let ctx = SessionContext::new_with_state(state);
        let mut statement = format!(
            "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
             LOCATION '{FIXTURE_METADATA_URI}'"
        );
        if !self.table_options.is_empty() {
            let options = self
                .table_options
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "'{}' '{}'",
                        key.replace('\'', "''"),
                        value.replace('\'', "''")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            statement.push_str(&format!(" OPTIONS ({options})"));
        }
        ctx.sql(&statement).await?.collect().await?;
        Ok(IcebergTestHarness { ctx })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureStorageFactory {
    root: PathBuf,
    files: HashMap<String, Vec<u8>>,
}

impl Default for FixtureStorageFactory {
    fn default() -> Self {
        Self {
            root: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../testdata/iceberg"),
            files: HashMap::new(),
        }
    }
}

#[typetag::serde]
impl StorageFactory for FixtureStorageFactory {
    fn build(&self, _config: &StorageConfig) -> IcebergResult<Arc<dyn Storage>> {
        Ok(Arc::new(FixtureStorage {
            root: self.root.clone(),
            files: self.files.clone(),
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureStorage {
    root: PathBuf,
    files: HashMap<String, Vec<u8>>,
}

impl FixtureStorage {
    fn input(&self, path: &str) -> IcebergResult<FixtureInput<'_>> {
        match self.files.get(path) {
            Some(bytes) => Ok(FixtureInput::Memory(bytes)),
            None => Ok(FixtureInput::Local(
                self.local().new_input(&self.local_path(path)?)?,
            )),
        }
    }

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
        self.input(path)?.exists().await
    }

    async fn metadata(&self, path: &str) -> IcebergResult<FileMetadata> {
        self.input(path)?.metadata().await
    }

    async fn read(&self, path: &str) -> IcebergResult<Bytes> {
        self.input(path)?.read().await
    }

    async fn reader(&self, path: &str) -> IcebergResult<Box<dyn FileRead>> {
        self.input(path)?.reader().await
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

enum FixtureInput<'a> {
    Memory(&'a [u8]),
    Local(InputFile),
}

impl FixtureInput<'_> {
    async fn exists(self) -> IcebergResult<bool> {
        match self {
            Self::Memory(_) => Ok(true),
            Self::Local(input) => input.exists().await,
        }
    }

    async fn metadata(self) -> IcebergResult<FileMetadata> {
        match self {
            Self::Memory(bytes) => Ok(FileMetadata {
                size: bytes.len() as u64,
            }),
            Self::Local(input) => input.metadata().await,
        }
    }

    async fn read(self) -> IcebergResult<Bytes> {
        match self {
            Self::Memory(bytes) => Ok(Bytes::copy_from_slice(bytes)),
            Self::Local(input) => input.read().await,
        }
    }

    async fn reader(self) -> IcebergResult<Box<dyn FileRead>> {
        match self {
            Self::Memory(bytes) => Ok(Box::new(FixtureFileRead(Bytes::copy_from_slice(bytes)))),
            Self::Local(input) => input.reader().await,
        }
    }
}

#[derive(Debug)]
struct FixtureFileRead(Bytes);

#[async_trait]
impl FileRead for FixtureFileRead {
    async fn read(&self, range: Range<u64>) -> IcebergResult<Bytes> {
        let start = range.start as usize;
        let end = range.end as usize;
        if start > end || end > self.0.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "range {start}..{end} is out of bounds for fixture with length {}",
                    self.0.len()
                ),
            ));
        }
        Ok(self.0.slice(start..end))
    }
}
