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
pub use protobuf::DistributedCodec;
mod coordinator;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;
mod work_unit_feed;

pub use arrow_ipc::CompressionType;
pub use common::{
    TreeNodeExt, deserialize_uuid, get_distributed_cancellation_token, serialize_uuid,
};
pub use config_extension_ext::get_config_extension_propagation_headers;
pub use coordinator::{DistributedExec, LatencyMetric, MetricsStore};
pub use distributed_ext::DistributedExt;
pub use distributed_planner::{
    DistributedConfig, NetworkBoundary, NetworkBoundaryExt, PartitionRoute, ProducerHead,
    SessionStateBuilderExt, TaskCountAnnotation, TaskEstimation, TaskEstimator, TaskRoutingContext,
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
pub use networking::{
    BoxCloneSyncChannel, ChannelResolver, DefaultChannelResolver, WorkerResolver,
    create_worker_client, get_distributed_channel_resolver, get_distributed_worker_resolver,
    get_distributed_worker_transport, set_distributed_worker_transport,
};
pub use passthrough_headers::get_passthrough_headers;
pub use stage::{
    DistributedTaskContext, RemoteStage, Stage, display_plan_ascii, display_plan_graphviz,
    explain_analyze,
};
pub use work_unit_feed::{
    DistributedWorkUnitFeedContext, WorkUnit, WorkUnitFeed, WorkUnitFeedProto, WorkUnitFeedProvider,
};
pub use worker::generated::worker::worker_service_client::WorkerServiceClient;
pub use worker::generated::worker::worker_service_server::WorkerServiceServer;
pub use worker::generated::worker::{GetWorkerInfoRequest, GetWorkerInfoResponse, TaskKey};
pub use worker::{
    DefaultSessionBuilder, FlightWorkerTransport, MappedWorkerSessionBuilder,
    MappedWorkerSessionBuilderExt, TaskData, Worker, WorkerConnection, WorkerDispatch,
    WorkerDispatchRequest, WorkerQueryContext, WorkerSessionBuilder, WorkerTransport,
};

pub use observability::{
    GetClusterWorkersRequest, GetClusterWorkersResponse, GetTaskProgressRequest,
    GetTaskProgressResponse, ObservabilityService, ObservabilityServiceClient,
    ObservabilityServiceImpl, ObservabilityServiceServer, PingRequest, PingResponse, TaskProgress,
    TaskStatus, WorkerMetrics,
};

#[cfg(any(feature = "integration", test))]
pub use execution_plans::benchmarks::{
    LocalRepartitionBench, LocalRepartitionFixture, LocalRepartitionMode, ShuffleBench,
    ShuffleFixture, TransportBench, TransportBenchMode, TransportFixture,
};
