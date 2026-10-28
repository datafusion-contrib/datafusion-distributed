// Each test binary compiles this module and uses only part of it.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
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
use datafusion_distributed_iceberg_test_tools::{FixtureCatalog, FixtureTable, create_taxi_table};
use datafusion_proto::physical_plan::AsExecutionPlan;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{MAIN_BRANCH, TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{MetadataLocation, TableIdent};

/// A DataFusion session for integration tests, over a freshly generated taxi table.
///
/// The table from [`create_taxi_table`] is registered as `taxi` with
/// `CREATE EXTERNAL TABLE ... STORED AS ICEBERG`. The harness owns the fixture catalog,
/// so the table's files live as long as the harness.
pub struct IcebergTestHarness {
    ctx: SessionContext,
    taxi: FixtureTable,
    _fixtures: FixtureCatalog,
}

impl IcebergTestHarness {
    pub fn builder() -> IcebergTestHarnessBuilder {
        IcebergTestHarnessBuilder::default()
    }

    pub async fn new() -> Result<Self> {
        Self::builder().build().await
    }

    /// The generated taxi table, e.g. to inspect its files.
    pub fn taxi(&self) -> &FixtureTable {
        &self.taxi
    }

    /// The current metadata file of the generated taxi table.
    pub fn taxi_metadata_location(&self) -> Result<&str> {
        self.taxi.metadata_location()
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
    table_options: BTreeMap<String, String>,
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

    /// Sets a table option; later values replace earlier ones for the same key.
    pub fn with_table_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.table_options.insert(key.into(), value.into());
        self
    }

    /// Enables distributed planning with logical workers backed by in-memory gRPC.
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = Some(workers);
        self
    }

    /// Generates the taxi table and registers it as `taxi`.
    pub async fn build(self) -> Result<IcebergTestHarness> {
        let (fixtures, taxi) = generate_taxi_table().await?;
        let location = taxi.metadata_location()?.to_string();
        self.register(fixtures, taxi, location).await
    }

    /// Generates the taxi table and registers `taxi` from the metadata `metadata` returns
    /// for it. The metadata is written next to the generated table's, so it can refer to
    /// the generated manifests and data files, as well as to files written through the
    /// table's `FileIO`.
    pub async fn build_with_table_metadata(
        self,
        metadata: impl AsyncFnOnce(&Table) -> Result<TableMetadata>,
    ) -> Result<IcebergTestHarness> {
        let (fixtures, taxi) = generate_taxi_table().await?;
        let metadata = metadata(taxi.table()).await?;
        let location = MetadataLocation::from_str(taxi.metadata_location()?)
            .map_err(external)?
            .with_next_version();
        metadata
            .write_to(taxi.table().file_io(), &location)
            .await
            .map_err(external)?;
        self.register(fixtures, taxi, location.to_string()).await
    }

    async fn register(
        self,
        fixtures: FixtureCatalog,
        taxi: FixtureTable,
        metadata_location: String,
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
        let mut statement = format!(
            "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
             LOCATION '{metadata_location}'"
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
            taxi,
            _fixtures: fixtures,
        })
    }
}

/// Returns a data file's path relative to the generated table's `data/` directory, which,
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

async fn generate_taxi_table() -> Result<(FixtureCatalog, FixtureTable)> {
    let fixtures = FixtureCatalog::try_new().await?;
    let ident = TableIdent::from_strs(["default", "taxi"]).map_err(external)?;
    let taxi = create_taxi_table(&fixtures, ident).await?;
    Ok((fixtures, taxi))
}

fn external(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
