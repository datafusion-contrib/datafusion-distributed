//! Asserts the extension surface an out-of-crate WorkerTransport needs stays public.

#![allow(dead_code)]

// A `tests/*.rs` file links the lib as an external crate, so any item that is still `pub(crate)`
// (not truly `pub`) fails to compile here. Each reference below pins one extension point.

// Traits: a generic bound rejects a non-`pub` trait regardless of object safety.
fn _assert_tree_node_ext<T: datafusion_distributed::TreeNodeExt>() {}
fn _assert_worker_transport<T: datafusion_distributed::WorkerTransport>() {}
fn _assert_worker_connection<T: datafusion_distributed::WorkerConnection>() {}
fn _assert_worker_dispatch<T: datafusion_distributed::WorkerDispatch>() {}
fn _assert_worker_resolver<T: datafusion_distributed::WorkerResolver>() {}
fn _assert_network_boundary_ext<T: datafusion_distributed::NetworkBoundaryExt>() {}
fn _assert_distributed_ext<T: datafusion_distributed::DistributedExt>() {}
fn _assert_session_state_builder_ext<T: datafusion_distributed::SessionStateBuilderExt>() {}

#[test]
fn extension_points_are_public() {
    // Free functions referenced as values.
    let _ = datafusion_distributed::execute_local_task;
    let _ = datafusion_distributed::collect_plan_metrics_protos;
    let _ = datafusion_distributed::encode_task_plan;
    let _ = datafusion_distributed::collect_task_work_unit_feeds;
    let _ = datafusion_distributed::set_sent_time;
    let _ = datafusion_distributed::set_received_time;
    let _ = datafusion_distributed::serialize_uuid;
    let _ = datafusion_distributed::deserialize_uuid;
    let _ = datafusion_distributed::get_config_extension_propagation_headers;
    let _ = datafusion_distributed::get_passthrough_headers;
    let _: fn(_, datafusion_distributed::InMemoryWorkerTransport) =
        datafusion_distributed::set_distributed_worker_transport;
    let _ = datafusion_distributed::get_distributed_cancellation_token;
    let _ = datafusion_distributed::get_distributed_worker_transport;
    let _ = datafusion_distributed::display_plan_ascii;

    // Types referenced in type position.
    let _: Option<datafusion_distributed::TaskDataEntries> = None;
    let _: Option<datafusion_distributed::ResultTaskData> = None;
    let _: Option<datafusion_distributed::SingleWriteMultiRead<u8>> = None;
    let _: Option<datafusion_distributed::EncodedTaskPlan> = None;
    let _: Option<datafusion_distributed::CoordinatorToWorkerMetrics> = None;
    let _: Option<datafusion_distributed::MetricsStore> = None;
    let _: Option<datafusion_distributed::LatencyMetric> = None;
    let _: Option<datafusion_distributed::ProducerHead> = None;
    let _: Option<datafusion_distributed::RemoteStage> = None;
    let _: Option<datafusion_distributed::RemoteWorkUnitFeedRegistry> = None;
    let _: Option<datafusion_distributed::RemoteWorkUnitFeedTxs> = None;
    let _: Option<datafusion_distributed::RemoteWorkUnitFeedRxs> = None;
    let _: Option<datafusion_distributed::WorkUnitTx> = None;
    let _: Option<datafusion_distributed::WorkUnitRx> = None;
    let _: Option<datafusion_distributed::DistributedTaskContext> = None;
    let _: Option<datafusion_distributed::WorkerDispatchRequest<'static>> = None;
    let _: Option<datafusion_distributed::DistributedConfig> = None;
    let _: Option<datafusion_distributed::NetworkCoalesceExec> = None;
    let _: Option<datafusion_distributed::BroadcastExec> = None;

    // The generated proto messages, re-exported as `proto` because `protobuf` already names a
    // private module.
    datafusion_distributed::proto::TaskKey::default();
    let _: Option<datafusion_distributed::proto::TaskMetrics> = None;
    let _: Option<datafusion_distributed::proto::SetPlanRequest> = None;
    let _: Option<datafusion_distributed::proto::ExecuteTaskRequest> = None;
    let _: Option<datafusion_distributed::proto::WorkUnit> = None;
    let _: Option<datafusion_distributed::proto::MetricsSet> = None;

    // `Worker::task_data_entries` is public, referenced as a method value.
    let _ = datafusion_distributed::Worker::task_data_entries;

    // An embedder files worker `TaskMetrics` it collected out-of-band through these.
    let _ = datafusion_distributed::DistributedExec::metrics_store;
    let _ = datafusion_distributed::MetricsStore::insert;
}
