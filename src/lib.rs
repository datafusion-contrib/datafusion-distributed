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
    AvgLatencyMetric, BytesCounterMetric, BytesMetricExt, DISTRIBUTED_DATAFUSION_TASK_ID_LABEL,
    DistributedMetricsFormat, FirstLatencyMetric, GaugeMetricExt, LatencyMetricExt, MaxGaugeMetric,
    MaxLatencyMetric, MinLatencyMetric, P50LatencyMetric, P75LatencyMetric, P95LatencyMetric,
    P99LatencyMetric, rewrite_distributed_plan_with_metrics,
};
pub use protocol::LocalWorkerContext;

mod events;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;

#[cfg(feature = "grpc")]
pub use protocol::grpc;

/// The worker-protocol prost message types, independent of any transport. A non-gRPC transport
/// reaches for these to speak the same wire shape the gRPC path serializes. Unstable: this
/// mirrors `worker.proto`, which is regenerated freely.
pub use protocol::generated::worker as proto;

pub use codec::DistributedCodec;
pub use common::MaybeEncoded;
pub use worker_resolver::{WorkerResolver, get_distributed_worker_resolver};

pub use protocol::{
    ChannelResolver, CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest,
    GetWorkerInfoResponse, InProcessChannelResolver, LoadInfo, SetPlanRequest, TaskKey,
    TaskMetrics, WorkUnitBatch, WorkUnitFeedDeclaration, WorkUnitMsg, WorkerChannel,
    WorkerToCoordinatorMsg, get_distributed_channel_resolver,
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

#[cfg(all(feature = "grpc", any(feature = "integration", test)))]
pub use execution_plans::benchmarks::{
    LocalRepartitionBench, LocalRepartitionFixture, LocalRepartitionMode, ShuffleBench,
    ShuffleFixture, TransportBench, TransportBenchMode, TransportFixture,
};
