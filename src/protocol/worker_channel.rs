use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use futures::stream::BoxStream;
use http::HeaderMap;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

/// Abstraction over the specific transport protocol implementation.
///
/// WARNING: The API in this trait is unstable, and it's subject to change as more things get properly
///  decoupled from details like protobuf serialization and http headers.
#[async_trait]
pub trait WorkerChannel: Send + Sync {
    /// Establishes a bidirectional message stream between a coordinator and a worker, over which messages
    /// will be exchanged at any time during a query's lifetime. It's expected to be one coordinator channel
    /// per task.
    async fn coordinator_channel(
        &mut self,
        headers: HeaderMap,
        c2w_stream: BoxStream<'static, CoordinatorToWorkerMsg>,
    ) -> Result<BoxStream<'static, Result<WorkerToCoordinatorMsg>>>;

    /// Executes the requested partition range of a subplan previously sent by the coordinator channel.
    async fn execute_task(
        &mut self,
        headers: HeaderMap,
        request: ExecuteTaskRequest,
        metrics: ExecutionPlanMetricsSet,
        task_ctx: &Arc<TaskContext>,
    ) -> Result<Vec<BoxStream<'static, Result<RecordBatch>>>>;

    /// Returns metadata about a worker. Currently only used for worker versioning.
    async fn get_worker_info(
        &mut self,
        request: GetWorkerInfoRequest,
    ) -> Result<GetWorkerInfoResponse>;
}

pub enum CoordinatorToWorkerMsg {
    /// Sends a subplan to a worker so that a future ExecuteTask call can actually execute it.
    /// The plan is identified by a TaskKey.
    SetPlanRequest(SetPlanRequest),
    /// A batch of messages from a work unit feed belonging to different partitions from one node from the plan set in
    /// set_plan_request. A work unit feed is a per-partition stream of information that tells the node what should
    /// be executed within a partition, for example, a stream of file addresses that should be read.
    WorkUnitBatch(WorkUnitBatch),
    /// Signals an EOS for WorkUnits. After this message is received, no more WorkUnits will be sent.
    WorkUnitEos,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TaskKey {
    /// Our query id.
    pub query_id: Uuid,
    /// Our stage id.
    pub stage_id: usize,
    /// The task number within the stage.
    pub task_number: usize,
}

pub struct WorkUnitFeedDeclaration {
    /// Unique identifier of the node to which work unit feeds are expected to be streamed.
    pub id: Uuid,
    /// The amount of partitions expected to be streamed.
    pub partitions: usize,
}

pub struct SetPlanRequest {
    /// The unique identifier of the task to which the subplan belongs to.
    pub task_key: TaskKey,
    /// The amount of tasks that share the same subplan. Necessary for building the DistributedTaskContext during execution.
    pub task_count: usize,
    /// The subplan the worker is expected to execute.
    // TODO: this still forces implementations to pass a serialized plan. In-memory implementations
    //  might want to omit the serde step, so there should be a way to pass here a normal plan, and
    //  pass the serializer/deserialized separately instead of being coupled to protobuf serialization
    pub plan_proto: Vec<u8>,
    /// Information about all the work unit feeds that will be streamed from coordinator to worker.
    /// This information is needed here because at the moment of setting the plan, all the appropriate
    /// channels for the incoming work unit feeds need to be constructed.
    ///
    /// If no WorkUnitFeedExec nodes are present in the plan, this should be empty.
    pub work_unit_feed_declarations: Vec<WorkUnitFeedDeclaration>,
    /// The worker URL to which this message will go. The receiving worker will use this information
    /// to identify itself, and avoid further calls in case it needs to call itself for executing tasks.
    pub target_worker_url: Url,
    /// Unix nanos when the query started as reported by the coordinator. Used for collecting temporal metrics
    /// relative to when the query was fired in the coordinator.
    pub query_start_time_ns: usize,
}

pub struct WorkUnitBatch {
    /// A batch of WorkUnits.
    pub batch: Vec<WorkUnitMsg>,
}

pub struct WorkUnitMsg {
    /// Identifier of the node to which this work unit feed belongs to.
    pub id: Uuid,
    /// The partition index within the node to which the work unit feed belongs to.
    pub partition: usize,
    /// Arbitrary user-defined data (e.g., a file address) necessary during execution.
    pub body: Vec<u8>,
    /// Unix timestamp in nanoseconds at which this message was created in the coordinator.
    pub created_timestamp_unix_nanos: usize,
    /// Unix timestamp in nanoseconds at which this message was sent by the coordinator.
    pub sent_timestamp_unix_nanos: usize,
    /// Unix timestamp in nanoseconds at which this message was received by a worker.
    pub received_timestamp_unix_nanos: usize,
    /// Unix timestamp in nanoseconds at which this message started being processed.
    pub processed_timestamp_unix_nanos: usize,
}

pub enum WorkerToCoordinatorMsg {
    /// Sends the metrics collected during task execution back to the coordinator.
    /// This is sent after all partitions of a task have finished (or been dropped),
    /// ensuring metrics are never lost due to early stream termination.
    /// metrics[i] is the set of metrics for plan node i in pre-order traversal order.
    TaskMetrics(TaskMetrics),
    /// Load information reported by a task. This information is used for dynamically
    /// sizing the number of workers involved in a query.
    LoadInfo(LoadInfo),
    LoadInfoEos,
}

#[derive(Clone, Debug)]
pub struct TaskMetrics {
    /// Metrics for a single task's plan nodes in pre-order traversal order.
    /// The TaskKey is implicit — it is determined by the SetPlanRequest that
    /// opened this coordinator channel connection.
    pub pre_order_plan_metrics: Vec<MetricsSet>,
    /// Metrics related to the execution of a task within a stage. This metrics, instead of being
    /// associated to a specific node, they are global to the task, like the time at which the plan
    /// was fed by the coordinator to the worker.
    pub task_metrics: MetricsSet,
}

#[derive(Default)]
pub struct LoadInfo {
    /// The partition index to which this message belongs to.
    pub partition: usize,
    /// The amount of rows ready to be returned.
    pub rows_ready: usize,
    /// The amount of bytes ready to be returned per column.
    pub per_column_bytes_ready: Vec<usize>,
    /// Approximate ratio of NDV for each column.
    pub per_column_ndv_percentage: Vec<f32>,
    /// Approximate ratio of null count for each column.
    pub per_column_null_percentage: Vec<f32>,
    /// The amount of rows that were pulling from leaf nodes while the partition to which this
    /// LoadInfo belongs to was sampling data. Used for estimating how much data is left by
    /// comparing this value to the estimated total rows pulled from leaf nodes.
    pub rows_pulled_from_leaf: usize,
    /// Whether the sampled partition stream reached end-of-stream (i.e. the partition finished
    /// producing all of its output) by the time this LoadInfo was captured. When true, `rows_ready`
    /// and `per_column_bytes_ready` are final rather than a partial snapshot.
    pub reached_eos: bool,
}

pub struct ExecuteTaskRequest {
    /// The unique identifier of the task that is going to get executed.
    pub task_key: TaskKey,
    /// The start of the partition range of the specified task that is going to be executed.
    pub target_partition_start: usize,
    /// The end of the partition range of the specified task that is going to be executed.
    pub target_partition_end: usize,
    /// The head node the requested task should have. Depending on the network boundary executing
    /// the task, the head node should be prepared differently, for example:
    /// - A RepartitionExecHead implies a RepartitionExec at the head of the task.
    /// - A BroadcastExecHead implies a BroadcastExec at the head of the task.
    /// - A NoneHead does not need any specific head.
    pub producer_head_spec: ProducerHeadSpec,
}

#[derive(Clone)]
pub enum ProducerHeadSpec {
    /// No specific head node is necessary.
    None,
    /// The head node should be a [BroadcastExec].
    BroadcastExec { output_partitions: usize },
    /// The head node should be a [RepartitionExec].
    RepartitionExec { partitioning: Vec<u8> },
}

pub struct GetWorkerInfoRequest {}

pub struct GetWorkerInfoResponse {
    pub version: String,
}
