use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{
    DesiredTaskCountEvent, DistributedCodec, DistributedConfig, DistributedExt, display_plan_ascii,
};
#[cfg(feature = "integration")]
use datafusion_distributed::{
    SessionStateBuilderExt, WorkerQueryContext,
    test_utils::in_memory_channel_resolver::{InMemoryChannelResolver, InMemoryWorkerResolver},
};
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::TableMetadata;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, NamespaceIdent, Result as IcebergResult, TableIdent,
};
use serde::{Deserialize, Serialize};

use super::taxi_metadata;
use crate::common::df_err;
use crate::{IcebergCatalog, IcebergExt, IcebergIntegrationOptions, iceberg_desired_task_count};

pub const FIXTURE_CATALOG: &str = "iceberg";
pub const FIXTURE_NAMESPACE: &str = "nyc";
pub const FIXTURE_URI: &str = "s3://iceberg-test/warehouse/taxi";
const WAREHOUSE_URI: &str = "s3://iceberg-test/warehouse/";
const FIXTURE_METADATA_URI: &str = "s3://iceberg-test/warehouse/taxi/metadata/v1.metadata.json";

pub struct IcebergTestHarness {
    ctx: SessionContext,
    catalog: Option<Arc<dyn Catalog>>,
}

impl IcebergTestHarness {
    pub fn builder() -> IcebergTestHarnessBuilder {
        IcebergTestHarnessBuilder::default()
    }

    pub async fn new() -> Result<Self> {
        Self::builder().build().await
    }

    pub async fn query(&self, sql: &str) -> Result<(String, String)> {
        let plan = self.physical_plan(sql).await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        let batches = collect(plan, self.ctx.task_ctx()).await?;

        Ok((display, pretty_format_batches(&batches)?.to_string()))
    }

    pub async fn physical_plan(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        self.ctx.sql(sql).await?.create_physical_plan().await
    }

    /// Returns the in-memory Iceberg catalog backing a fixture built with
    /// [`IcebergTestHarnessBuilder::with_catalog`].
    pub fn iceberg_catalog(&self) -> Result<Arc<dyn Catalog>> {
        self.catalog.clone().ok_or_else(|| {
            DataFusionError::Plan("the fixture was not built with a catalog".to_string())
        })
    }

    /// Returns the fixture scan without SQL optimization, including for an empty table.
    pub async fn scan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        self.ctx
            .table_provider("taxi")
            .await?
            .scan(&self.ctx.state(), None, &[], None)
            .await
    }

    /// Estimates Iceberg scan tasks using this harness's session configuration.
    pub fn estimate_task_count(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<Option<usize>> {
        iceberg_desired_task_count(DesiredTaskCountEvent {
            plan,
            session_config: self.ctx.state().config(),
        })
        .transpose()
        .map(|response| response.map(|response| response.task_count.as_usize()))
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
    session_builder: SessionStateBuilder,
    metadata: TableMetadata,
    table_options: BTreeMap<String, String>,
    files: HashMap<String, Vec<u8>>,
    catalog: bool,
    #[cfg(feature = "integration")]
    workers: Option<usize>,
}

impl Default for IcebergTestHarnessBuilder {
    fn default() -> Self {
        Self {
            session_builder: SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_distributed_option_extension(DistributedConfig::default()),
            metadata: taxi_metadata(),
            table_options: BTreeMap::new(),
            files: HashMap::new(),
            catalog: false,
            #[cfg(feature = "integration")]
            workers: None,
        }
    }
}

impl IcebergTestHarnessBuilder {
    /// Customizes the existing session builder, retaining defaults unless explicitly replaced.
    /// Fixture integration and optional worker wiring are installed during [`Self::build`].
    pub fn configure_session(
        mut self,
        configure: impl FnOnce(SessionStateBuilder) -> Result<SessionStateBuilder>,
    ) -> Result<Self> {
        self.session_builder = configure(self.session_builder)?;
        Ok(self)
    }

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

    /// Registers the fixture through an in-memory Iceberg catalog exposed as
    /// [`FIXTURE_CATALOG`].[`FIXTURE_NAMESPACE`] instead of `CREATE EXTERNAL TABLE`.
    /// Table options are not applied in this mode.
    pub fn with_catalog(mut self) -> Self {
        self.catalog = true;
        self
    }

    /// Enables distributed planning with logical workers backed by in-memory gRPC.
    #[cfg(feature = "integration")]
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = Some(workers);
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
        let options = IcebergIntegrationOptions {
            storage_factory: Arc::new(storage_factory.clone()),
            iceberg_runtime: iceberg::Runtime::current(),
        };
        let iceberg_runtime = options.iceberg_runtime.clone();
        let mut state = self
            .session_builder
            .with_iceberg_integration(options.clone());
        if self.catalog {
            // Makes `taxi` resolve through the registered Iceberg catalog.
            let config = state.config().get_or_insert_default();
            *config = std::mem::take(config)
                .with_default_catalog_and_schema(FIXTURE_CATALOG, FIXTURE_NAMESPACE);
        }
        #[cfg(feature = "integration")]
        let state = if let Some(workers) = self.workers {
            let resolver =
                InMemoryChannelResolver::from_session_builder(move |ctx: WorkerQueryContext| {
                    let state = ctx
                        .builder
                        .with_iceberg_integration(options.clone())
                        .build();
                    async move { Ok(state) }
                });
            state
                .with_distributed_planner()
                .with_distributed_worker_resolver(InMemoryWorkerResolver::new(workers))
                .with_distributed_channel_resolver(resolver)
        } else {
            state
        };
        let ctx = SessionContext::new_with_state(state.build());
        if self.catalog && !self.table_options.is_empty() {
            return Err(DataFusionError::Plan(
                "table options are not supported with a catalog-backed fixture".to_string(),
            ));
        }
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

        let mut iceberg_catalog = None;
        if self.catalog {
            let catalog = MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(storage_factory))
                .with_runtime(iceberg_runtime.clone())
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        WAREHOUSE_URI.to_string(),
                    )]),
                )
                .await
                .map_err(df_err)?;
            let namespace = NamespaceIdent::new(FIXTURE_NAMESPACE.to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .map_err(df_err)?;
            catalog
                .register_table(
                    &TableIdent::new(namespace, "taxi".to_string()),
                    FIXTURE_METADATA_URI.to_string(),
                )
                .await
                .map_err(df_err)?;
            let catalog: Arc<dyn Catalog> = Arc::new(catalog);
            let provider = IcebergCatalog::try_new(catalog.clone(), iceberg_runtime).await?;
            ctx.register_catalog(FIXTURE_CATALOG, Arc::new(provider));
            iceberg_catalog = Some(catalog);
        } else {
            ctx.sql(&statement).await?.collect().await?;
        }
        Ok(IcebergTestHarness {
            ctx,
            catalog: iceberg_catalog,
        })
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
