use crate::worker::WorkerSessionBuilder;
use crate::worker::generated::worker::coordinator_to_worker_msg::Inner;
use crate::worker::generated::worker::worker_service_server::{WorkerService, WorkerServiceServer};
use crate::worker::generated::worker::{
    CoordinatorToWorkerMsg, ExecuteTaskRequest, TaskKey, WorkerToCoordinatorMsg,
};
use crate::worker::impl_set_plan::TaskData;
use crate::worker::single_write_multi_read::SingleWriteMultiRead;
use crate::{
    DefaultSessionBuilder, ObservabilityServiceImpl, ObservabilityServiceServer, WorkerResolver,
};
use arrow_flight::FlightData;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status, Streaming};

use super::generated::worker::{GetWorkerInfoRequest, GetWorkerInfoResponse};

#[allow(clippy::type_complexity)]
#[derive(Clone, Default)]
pub(super) struct WorkerHooks {
    pub(super) on_plan:
        Vec<Arc<dyn Fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> + Sync + Send>>,
}

type ResultTaskData = Result<TaskData, Arc<DataFusionError>>;

#[derive(Clone)]
pub struct Worker {
    pub(super) runtime: Arc<RuntimeEnv>,
    /// TTL-based cache for task execution data. Entries are automatically evicted after 60 seconds.
    /// This prevents memory leaks from abandoned or incomplete queries while allowing concurrent
    /// access to task results across multiple partition requests.
    pub(super) task_data_entries: Arc<Cache<TaskKey, Arc<SingleWriteMultiRead<ResultTaskData>>>>,
    pub(super) session_builder: Arc<dyn WorkerSessionBuilder + Send + Sync>,
    pub(super) hooks: WorkerHooks,
    pub(super) max_message_size: Option<usize>,
    pub(super) version: String,
}

impl Default for Worker {
    fn default() -> Self {
        let cache = Cache::builder()
            .time_to_idle(Duration::from_secs(60))
            .build();
        Self {
            runtime: Arc::new(RuntimeEnv::default()),
            task_data_entries: Arc::new(cache),
            session_builder: Arc::new(DefaultSessionBuilder),
            hooks: WorkerHooks::default(),
            max_message_size: Some(usize::MAX),
            version: String::default(),
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
    /// The callback takes the plan and returns another plan that must be either the same,
    /// or equivalent in terms of execution. Mutating the plan by adding nodes or removing them
    /// will make the query blow up in unexpected ways.
    pub fn add_on_plan_hook(
        &mut self,
        hook: impl Fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> + Sync + Send + 'static,
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

    /// Converts this [Worker] into a [`WorkerServiceServer`] with high default message size limits.
    ///
    /// This is a convenience method that wraps the endpoint in a [`WorkerServiceServer`] and
    /// configures it with `max_decoding_message_size(usize::MAX)` and
    /// `max_encoding_message_size(usize::MAX)` to avoid message size limitations for internal
    /// communication.
    ///
    /// You can further customize the returned server by chaining additional tonic methods.
    ///
    /// # Example
    ///
    /// ```
    /// # use datafusion_distributed::Worker;
    /// # use tonic::transport::Server;
    /// # use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    /// # async fn f() {
    ///
    /// let worker = Worker::default();
    /// let server = worker.into_worker_server();
    ///
    /// Server::builder()
    ///     .add_service(Worker::default().into_worker_server())
    ///     .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080))
    ///     .await;
    ///
    /// # }
    /// ```
    pub fn into_worker_server(self) -> WorkerServiceServer<Self> {
        WorkerServiceServer::new(self)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX)
    }

    /// Creates an [`ObservabilityServiceServer`] that exposes task progress and cluster
    /// worker discovery via the provided [`WorkerResolver`].
    ///
    /// The returned server is meant to be added to the same [`tonic::transport::Server`] as the
    /// Flight service — gRPC multiplexes both services on a single port.
    pub fn with_observability_service(
        &self,
        worker_resolver: Arc<dyn WorkerResolver + Send + Sync>,
    ) -> ObservabilityServiceServer<ObservabilityServiceImpl> {
        ObservabilityServiceServer::new(ObservabilityServiceImpl::new(
            self.task_data_entries.clone(),
            worker_resolver,
        ))
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
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

/// Implementation of the `worker.proto` specification based on the generated Rust stubs.
///
/// The methods are delegated to plan `impl Worker` implementations so that they can be implemented
/// in different files.
#[async_trait]
impl WorkerService for Worker {
    type CoordinatorChannelStream = BoxStream<WorkerToCoordinatorMsg>;

    async fn coordinator_channel(
        &self,
        request: Request<Streaming<CoordinatorToWorkerMsg>>,
    ) -> Result<Response<Self::CoordinatorChannelStream>, Status> {
        let (metadata, _ext, mut body) = request.into_parts();
        if let Some(msg) = body.next().await {
            let Some(inner) = msg?.inner else {
                return Err(Status::internal("Empty Coordinator message"));
            };

            match inner {
                Inner::SetPlanRequest(request) => {
                    self.impl_set_plan(request, metadata).await?;
                }
            };
        }
        Ok(Response::new(futures::stream::empty().boxed()))
    }

    type ExecuteTaskStream = BoxStream<FlightData>;

    async fn execute_task(
        &self,
        request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<Self::ExecuteTaskStream>, Status> {
        self.impl_execute_task(request).await
    }

    async fn get_worker_info(
        &self,
        _request: Request<GetWorkerInfoRequest>,
    ) -> Result<Response<GetWorkerInfoResponse>, Status> {
        Ok(Response::new(GetWorkerInfoResponse {
            version_number: self.version.clone(),
        }))
    }
}
