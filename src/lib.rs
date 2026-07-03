#![deny(clippy::all)]

mod codec;
mod common;
mod config_extension_ext;
mod coordinator;
mod distributed_ext;
mod distributed_planner;
mod execution_plans;
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
pub use distributed_ext::DistributedExt;
pub use distributed_planner::{
    DistributedConfig, NetworkBoundary, NetworkBoundaryExt, SessionStateBuilderExt,
    TaskCountAnnotation, TaskEstimation, TaskEstimator, TaskRoutingContext,
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

#[cfg(any(feature = "integration", test))]
pub mod test_utils;
#[cfg(feature = "grpc")]
pub use protocol::grpc;

pub use codec::DistributedCodec;
pub use worker_resolver::{WorkerResolver, get_distributed_worker_resolver};

pub use protocol::{
    ChannelResolver, CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest,
    GetWorkerInfoResponse, LoadInfo, ProducerHeadSpec, SetPlanRequest, TaskKey, TaskMetrics,
    WorkUnitBatch, WorkUnitFeedDeclaration, WorkUnitMsg, WorkerChannel, WorkerToCoordinatorMsg,
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
