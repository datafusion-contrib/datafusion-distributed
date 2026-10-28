// Each test binary compiles this module and uses only part of it.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::mem;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::exec_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::test_utils::in_memory_channel_resolver::{
    InMemoryChannelResolver, InMemoryWorkerResolver,
};
use datafusion_distributed::{
    DesiredTaskCountEvent, DistributedCodec, DistributedConfig, DistributedExt,
    DistributedMetricsFormat, SessionStateBuilderExt, WorkerQueryContext, display_plan_ascii,
    rewrite_distributed_plan_with_metrics,
};
use datafusion_distributed_iceberg::{
    IcebergConfig, IcebergExt, IcebergIntegrationOptions, iceberg_desired_task_count,
};
use datafusion_distributed_iceberg_test_tools::{
    DescribedFile, FixtureCatalog, FixtureTable, create_taxi_table, taxi_schema,
};
use datafusion_proto::physical_plan::AsExecutionPlan;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{MAIN_BRANCH, TableMetadata, TableMetadataBuilder, Transform};
use iceberg::{MetadataLocation, TableIdent};

/// A DataFusion session for integration tests, over the table its builder chose, if any.
///
/// The table is registered under its name, e.g. `taxi` for
/// [`IcebergTestHarnessBuilder::with_taxi_dataset`], with
/// `CREATE EXTERNAL TABLE ... STORED AS ICEBERG`. The harness owns the fixture catalog,
/// so the table's files live as long as the harness.
pub struct IcebergTestHarness {
    ctx: SessionContext,
    table: Option<FixtureTable>,
    _fixtures: FixtureCatalog,
}

impl IcebergTestHarness {
    pub fn builder() -> IcebergTestHarnessBuilder {
        IcebergTestHarnessBuilder::default()
    }

    /// The registered table, e.g. to inspect its files.
    pub fn table(&self) -> Option<&FixtureTable> {
        self.table.as_ref()
    }

    pub async fn query(&self, sql: &str) -> Result<(String, String)> {
        let plan = self.physical_plan(sql).await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        let batches = collect(plan, self.ctx.task_ctx()).await?;

        Ok((display, pretty_format_batches(&batches)?.to_string()))
    }

    /// Executes a query and returns its plan with collected per-task metrics.
    pub async fn query_with_metrics(&self, sql: &str) -> Result<(String, String)> {
        let plan = self.physical_plan(sql).await?;
        let batches = collect(Arc::clone(&plan), self.ctx.task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::PerTask).await?;
        Ok((
            display_plan_ascii(plan.as_ref(), true),
            pretty_format_batches(&batches)?.to_string(),
        ))
    }

    pub async fn physical_plan(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        self.ctx.sql(sql).await?.create_physical_plan().await
    }

    /// Returns the registered table's scan without SQL optimization, including for an
    /// empty table.
    pub async fn scan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(table) = &self.table else {
            return exec_err!("the harness has no table to scan");
        };
        self.ctx
            .table_provider(table.ident().name())
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

/// Derives the metadata to register a table from, given its own.
type MetadataTransform = Box<dyn FnOnce(&TableMetadata) -> TableMetadata>;

pub struct IcebergTestHarnessBuilder {
    session_builder: SessionStateBuilder,
    table_options: BTreeMap<String, String>,
    taxi_dataset: bool,
    described_files: Vec<DescribedFile>,
    metadata: Option<MetadataTransform>,
    workers: Option<usize>,
}

impl Default for IcebergTestHarnessBuilder {
    fn default() -> Self {
        Self {
            session_builder: SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_distributed_option_extension(DistributedConfig::default()),
            table_options: BTreeMap::new(),
            taxi_dataset: false,
            described_files: Vec::new(),
            metadata: None,
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

    /// Customizes Iceberg settings while preserving other session options.
    pub fn configure_iceberg(mut self, configure: impl FnOnce(&mut IcebergConfig)) -> Self {
        let session = self.session_builder.config().get_or_insert_default();
        let mut config = IcebergConfig::from_config_options(session.options());
        configure(&mut config);
        session.set_distributed_option_extension(config);
        self
    }

    /// Discovers files during planning instead of execution.
    pub fn with_eager_file_planning(self) -> Self {
        self.configure_iceberg(|config| config.eager_file_planning = true)
    }

    /// Enables Iceberg column statistics during planning.
    pub fn with_column_stats_enabled(self) -> Self {
        self.configure_iceberg(|config| config.column_stats_enabled = true)
    }

    /// Registers the taxi dataset from [`create_taxi_table`] as `taxi`.
    pub fn with_taxi_dataset(mut self) -> Self {
        self.taxi_dataset = true;
        self
    }

    /// Sets a table option; later values replace earlier ones for the same key.
    pub fn with_table_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.table_options.insert(key.into(), value.into());
        self
    }

    /// Registers as `taxi` a table with the taxi dataset's schema and partitioning whose
    /// only snapshot lists the described files, which are never written. Can't be combined
    /// with [`Self::with_taxi_dataset`].
    pub fn with_described_file(mut self, file: DescribedFile) -> Self {
        self.described_files.push(file);
        self
    }

    /// Registers the chosen table from the metadata `metadata` returns for its own. The
    /// metadata is written next to the table's, so it can refer to the table's manifests
    /// and data files.
    pub fn with_metadata(
        mut self,
        metadata: impl FnOnce(&TableMetadata) -> TableMetadata + 'static,
    ) -> Self {
        self.metadata = Some(Box::new(metadata));
        self
    }

    /// Enables distributed planning with logical workers backed by in-memory gRPC.
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = Some(workers);
        self
    }

    /// Creates the chosen table, if any, and registers it, from customized metadata if
    /// [`Self::with_metadata`] was given.
    pub async fn build(mut self) -> Result<IcebergTestHarness> {
        let fixtures = FixtureCatalog::try_new().await?;
        let table = match (self.create_table(&fixtures).await?, self.metadata.take()) {
            (None, None) => None,
            (None, Some(_)) => {
                return exec_err!("table metadata can only be customized for a chosen table");
            }
            (Some(table), None) => {
                let location = table.metadata_location()?.to_string();
                Some((table, location))
            }
            (Some(table), Some(metadata)) => {
                let metadata = metadata(table.table().metadata());
                let location = MetadataLocation::from_str(table.metadata_location()?)
                    .map_err(external)?
                    .with_next_version();
                metadata
                    .write_to(table.table().file_io(), &location)
                    .await
                    .map_err(external)?;
                Some((table, location.to_string()))
            }
        };
        self.register(fixtures, table).await
    }

    /// Creates the table chosen with [`Self::with_taxi_dataset`] or
    /// [`Self::with_described_file`] in `fixtures`, if any.
    async fn create_table(&mut self, fixtures: &FixtureCatalog) -> Result<Option<FixtureTable>> {
        let described_files = mem::take(&mut self.described_files);
        if self.taxi_dataset && !described_files.is_empty() {
            return exec_err!("the taxi dataset can't be combined with described files");
        }
        let ident = TableIdent::from_strs(["default", "taxi"]).map_err(external)?;
        if self.taxi_dataset {
            return Ok(Some(create_taxi_table(fixtures, ident).await?));
        }
        if described_files.is_empty() {
            return Ok(None);
        }
        let mut table = fixtures
            .table(ident, taxi_schema())
            .partition_by("pickup_date", Transform::Identity)
            .create()
            .await?;
        let mut append = table.append();
        for file in described_files {
            append = append.described_file(file);
        }
        append.commit().await?;
        Ok(Some(table))
    }

    /// Builds the session and registers `table`, given with its metadata location.
    async fn register(
        self,
        fixtures: FixtureCatalog,
        table: Option<(FixtureTable, String)>,
    ) -> Result<IcebergTestHarness> {
        let options = IcebergIntegrationOptions {
            storage_factory: Arc::new(LocalFsStorageFactory),
            iceberg_runtime: iceberg::Runtime::current(),
        };
        let state = self
            .session_builder
            .with_iceberg_integration(options.clone());
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
        let Some((table, metadata_location)) = table else {
            return Ok(IcebergTestHarness {
                ctx,
                table: None,
                _fixtures: fixtures,
            });
        };
        let mut statement = format!(
            "CREATE EXTERNAL TABLE {} STORED AS ICEBERG LOCATION '{metadata_location}'",
            table.ident().name()
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
        Ok(IcebergTestHarness {
            ctx,
            table: Some(table),
            _fixtures: fixtures,
        })
    }
}

/// Returns a data file's path relative to its table's `data/` directory, which,
/// unlike the temp dir the table lives in, is the same on every run.
pub fn data_file_name(path: &str) -> &str {
    path.rsplit_once("/data/").map_or(path, |(_, name)| name)
}

/// Starts from `metadata` with its schema, partition spec and properties, but no snapshots.
pub fn without_snapshots(metadata: &TableMetadata) -> TableMetadataBuilder {
    let snapshot_ids: Vec<_> = metadata.snapshots().map(|s| s.snapshot_id()).collect();
    metadata
        .clone()
        .into_builder(None)
        .remove_ref(MAIN_BRANCH)
        .remove_snapshots(&snapshot_ids)
}

/// Converts an error from iceberg-rust into a DataFusion error.
pub fn external(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
