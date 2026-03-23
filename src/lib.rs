#![deny(clippy::all)]

mod common;
mod config_extension_ext;
mod distributed_ext;
mod execution_plans;
mod metrics;
mod passthrough_headers;
mod stage;
mod worker;

mod distributed_planner;
mod networking;
mod observability;
mod protobuf;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use arrow_ipc::CompressionType;
pub use distributed_ext::DistributedExt;
pub use distributed_planner::{
    DistributedConfig, DistributedPhysicalOptimizerRule, NetworkBoundary, NetworkBoundaryExt,
    TaskCountAnnotation, TaskEstimation, TaskEstimator,
};
pub use execution_plans::{
    BroadcastExec, DistributedExec, NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec,
    PartitionIsolatorExec,
};
pub use metrics::{
    AvgLatencyMetric, BytesCounterMetric, BytesMetricExt, DISTRIBUTED_DATAFUSION_TASK_ID_LABEL,
    DistributedMetricsFormat, FirstLatencyMetric, LatencyMetricExt, MaxLatencyMetric,
    MinLatencyMetric, P50LatencyMetric, P75LatencyMetric, P95LatencyMetric, P99LatencyMetric,
    rewrite_distributed_plan_with_metrics,
};
pub use networking::{
    BoxCloneSyncChannel, ChannelResolver, DefaultChannelResolver, WorkerResolver,
    create_worker_client, get_distributed_channel_resolver, get_distributed_worker_resolver,
};
pub use stage::{
    DistributedTaskContext, ExecutionTask, Stage, display_plan_ascii, display_plan_graphviz,
    explain_analyze,
};
pub use worker::{
    DefaultSessionBuilder, MappedWorkerSessionBuilder, MappedWorkerSessionBuilderExt, TaskData,
    Worker, WorkerQueryContext, WorkerSessionBuilder,
    generated::worker::{
        GetWorkerInfoRequest, GetWorkerInfoResponse, worker_service_client::WorkerServiceClient,
    },
};

pub use observability::{
    GetClusterWorkersRequest, GetClusterWorkersResponse, GetTaskProgressRequest,
    GetTaskProgressResponse, ObservabilityService, ObservabilityServiceClient,
    ObservabilityServiceImpl, ObservabilityServiceServer, PingRequest, PingResponse, TaskProgress,
    TaskStatus, WorkerMetrics,
};

#[cfg(any(feature = "integration", test))]
pub use execution_plans::benchmarks::ShuffleBench;
