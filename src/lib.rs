#![deny(clippy::all)]

mod codec;
mod common;
mod config_extension_ext;
mod coordinator;
mod distributed_ext;
mod distributed_planner;
mod execution_plans;
mod explain_analyze;
mod metrics;
mod passthrough_headers;
mod protocol;
mod stage;
mod work_unit_feed;
mod worker;
mod worker_resolver;

#[cfg(feature = "grpc")]
pub use arrow_ipc::CompressionType;
pub use coordinator::DistributedExec;
pub use distributed_ext::{DistributedExt, DistributedGetterExt};
pub use distributed_planner::{
    DistributedConfig, NetworkBoundary, NetworkBoundaryExt, ProducerHead, SessionStateBuilderExt,
};
pub use events::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, DesiredTaskCountHandler, RouteTasksEvent,
    RouteTasksEventResponse, RouteTasksHandler, ScaleUpLeafNodeEvent, ScaleUpLeafNodeEventResponse,
    ScaleUpLeafNodeHandler, TaskCountAnnotation, WorkerPlanRewriteEvent,
    WorkerPlanRewriteEventResponse, WorkerPlanRewriteHandler,
};
pub use execution_plans::{
    BroadcastExec, DistributedLeafExec, NetworkBroadcastExec, NetworkCoalesceExec,
    NetworkShuffleExec,
};
pub use metrics::{
    AvgLatencyMetric, BytesCounterMetric, BytesMetricExt, DistributedMetricsFormat,
    FirstLatencyMetric, GaugeMetricExt, LatencyMetricExt, MaxGaugeMetric, MaxLatencyMetric,
    MinLatencyMetric, P50LatencyMetric, P75LatencyMetric, P95LatencyMetric, P99LatencyMetric,
    rewrite_distributed_plan_with_metrics,
};
pub use metrics::{
    BYTES_READY_METRIC, BYTES_TRANSFERRED_METRIC, CPU_COST_METRIC,
    DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, ESTIMATED_OUTPUT_BYTES_METRIC,
    ESTIMATED_PCT_SAMPLED_METRIC, KICK_OFF_TO_EXECUTION_MAX_METRIC,
    KICK_OFF_TO_EXECUTION_P50_METRIC, KICK_OFF_TO_FIRST_BATCH_MAX_METRIC,
    KICK_OFF_TO_FIRST_BATCH_P50_METRIC, KICK_OFF_TO_LOAD_INFO_SENT_MAX_METRIC,
    KICK_OFF_TO_LOAD_INFO_SENT_P50_METRIC, LOCAL_CONNECTIONS_USED_METRIC,
    LOCAL_COORDINATOR_CHANNELS_METRIC, MAX_BATCHES_PEEKED_METRIC, MAX_MEMORY_USED_METRIC,
    MEMORY_COST_METRIC, MESSAGE_COUNT_METRIC, NETWORK_COST_METRIC, NETWORK_LATENCY_COUNT_METRIC,
    NETWORK_LATENCY_FIRST_METRIC, NETWORK_LATENCY_MAX_METRIC, NETWORK_LATENCY_MIN_METRIC,
    NETWORK_LATENCY_P50_METRIC, NETWORK_LATENCY_P95_METRIC, NETWORK_LATENCY_SUM_METRIC,
    PLAN_ADDED_AT_METRIC, PLAN_BYTES_SENT_METRIC, PLAN_EXECUTED_AT_METRIC, PLAN_FINISHED_AT_METRIC,
    PLAN_SEND_LATENCY_METRIC, REMOTE_COORDINATOR_CHANNELS_METRIC, WORK_UNIT_BYTES_METRIC,
    WORK_UNIT_COUNT_METRIC, WORK_UNIT_IN_MEMORY_COUNT_METRIC,
    WORK_UNIT_PROCESSED_LATENCY_MAX_METRIC, WORK_UNIT_PROCESSED_LATENCY_P50_METRIC,
    WORK_UNIT_RECEIVED_LATENCY_MAX_METRIC, WORK_UNIT_RECEIVED_LATENCY_P50_METRIC,
    WORK_UNIT_SEND_LATENCY_MAX_METRIC, WORK_UNIT_SEND_LATENCY_P50_METRIC,
};
pub use protocol::LocalWorkerContext;

mod events;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;

#[cfg(feature = "grpc")]
pub use protocol::grpc;

pub use codec::DistributedCodec;
pub use common::MaybeEncoded;
pub use worker_resolver::{WorkerResolver, get_distributed_worker_resolver};

pub use protocol::{
    ChannelResolver, CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest,
    GetWorkerInfoResponse, LoadInfo, SetPlanRequest, TaskKey, TaskMetrics, WorkUnitBatch,
    WorkUnitFeedDeclaration, WorkUnitMsg, WorkerChannel, WorkerToCoordinatorMsg,
    get_distributed_channel_resolver,
};
pub use stage::{
    DistributedTaskContext, Stage, display_plan_ascii, display_plan_graphviz, explain_analyze,
};
pub use work_unit_feed::{
    DistributedWorkUnitFeedContext, WorkUnit, WorkUnitFeed, WorkUnitFeedProto, WorkUnitFeedProvider,
};
pub use worker::{
    DefaultSessionBuilder, MappedWorkerSessionBuilder, MappedWorkerSessionBuilderExt, TaskData,
    Worker, WorkerQueryContext, WorkerSessionBuilder,
};

#[cfg(any(feature = "integration", test))]
pub use execution_plans::benchmarks::{
    LocalRepartitionBench, LocalRepartitionFixture, LocalRepartitionMode, ShuffleBench,
    ShuffleFixture, TransportBench, TransportBenchMode, TransportFixture,
};
