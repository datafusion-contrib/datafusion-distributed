use crate::worker::{SingleWriteMultiRead, WorkerSessionBuilder};
use crate::{DefaultSessionBuilder, TaskData, TaskKey};
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use moka::future::Cache;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

const TASK_CACHE_TTI: Duration = Duration::from_mins(10);

#[allow(clippy::type_complexity)]
type OnPlanHook = dyn Fn(Arc<dyn ExecutionPlan>, &SessionConfig) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>
    + Sync
    + Send;

#[derive(Clone, Default)]
pub(super) struct WorkerHooks {
    pub(super) on_plan: Vec<Arc<OnPlanHook>>,
}

pub(crate) type ResultTaskData = Result<TaskData, Arc<DataFusionError>>;
pub(crate) type TaskDataEntries = Cache<TaskKey, Arc<SingleWriteMultiRead<ResultTaskData>>>;

#[derive(Clone)]
pub struct Worker {
    pub(super) runtime: Arc<RuntimeEnv>,
    /// TTL-based cache for task execution data. Entries are automatically evicted after
    /// TASK_CACHE_TTI seconds. This prevents memory leaks from abandoned or incomplete queries
    /// while allowing concurrent access to task results across multiple partition requests.
    pub(crate) task_data_entries: Arc<TaskDataEntries>,
    pub(super) session_builder: Arc<dyn WorkerSessionBuilder + Send + Sync>,
    pub(super) hooks: WorkerHooks,
    pub(crate) max_message_size: Option<usize>,
    pub(super) version: Cow<'static, str>,
}

impl Default for Worker {
    fn default() -> Self {
        let cache = Cache::builder().time_to_idle(TASK_CACHE_TTI).build();
        Self {
            runtime: Arc::new(RuntimeEnv::default()),
            task_data_entries: Arc::new(cache),
            session_builder: Arc::new(DefaultSessionBuilder),
            hooks: WorkerHooks::default(),
            max_message_size: Some(usize::MAX),
            version: Cow::Borrowed(""),
        }
    }
}

impl Worker {
    /// Builds a [Worker] with a custom [WorkerSessionBuilder]. Use this
    /// method whenever you need to add custom stuff to the `SessionContext` that executes the query.
    pub fn from_session_builder(
        session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    ) -> Self {
        Self {
            session_builder: Arc::new(session_builder),
            ..Default::default()
        }
    }

    /// Sets a [RuntimeEnv] to be used in all the queries this [Worker] will handle during
    /// its lifetime.
    pub fn with_runtime_env(mut self, runtime_env: Arc<RuntimeEnv>) -> Self {
        self.runtime = runtime_env;
        self
    }

    /// Adds a callback for when an [ExecutionPlan] is received in the `set_plan` call.
    ///
    /// The callback runs after worker session construction and plan decoding, and before task
    /// registration and execution. It receives the per-query [SessionConfig], so it can use
    /// propagated options or config extensions when rewriting the plan.
    ///
    /// The callback is trusted to preserve the worker stage contract already planned by the
    /// coordinator: row semantics, output schema, partitioning, and ordering requirements. It is
    /// intended for transparent wrappers, such as instrumentation, or semantics-preserving physical
    /// rewrites. Do not use it to add or remove rows or columns, repartition the stage, or otherwise
    /// re-plan distributed execution. Returned errors are propagated as worker plan-registration
    /// failures.
    pub fn add_on_plan_hook(
        &mut self,
        hook: impl Fn(
            Arc<dyn ExecutionPlan>,
            &SessionConfig,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>
        + Sync
        + Send
        + 'static,
    ) {
        self.hooks.on_plan.push(Arc::new(hook));
    }

    /// Set the maximum message size for FlightData chunks.
    ///
    /// Defaults to `usize::MAX` to minimize chunking overhead for internal communication.
    /// See [`FlightDataEncoderBuilder::with_max_flight_data_size`] for details.
    ///
    /// If you change this to a lower value, ensure you configure the server's
    /// max_encoding_message_size and max_decoding_message_size to at least 2x this value
    /// to allow for overhead. For most use cases, the default of `usize::MAX` is appropriate.
    ///
    /// [`FlightDataEncoderBuilder::with_max_flight_data_size`]: https://arrow.apache.org/rust/arrow_flight/encode/struct.FlightDataEncoderBuilder.html#structfield.max_flight_data_size
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Sets a version string reported by the `GetWorkerInfo` gRPC endpoint.
    pub fn with_version(mut self, version: impl Into<Cow<'static, str>>) -> Self {
        self.version = version.into();
        self
    }

    /// Returns the version set by [Self::with_version].
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the number of cached task entries currently held by this worker.
    #[cfg(any(test, feature = "integration"))]
    pub async fn tasks_running(&self) -> usize {
        // Use `run_pending_tasks()` to migigate inaccuracy from potential stale
        // `entry_count()` task data.
        self.task_data_entries.run_pending_tasks().await;
        self.task_data_entries.entry_count() as usize
    }
}
