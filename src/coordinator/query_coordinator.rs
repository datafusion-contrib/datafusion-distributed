use crate::common::{TreeNodeExt, now_ns, serialize_uuid, task_ctx_with_extension};
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::coordinator::MetricsStore;
use crate::coordinator::latency_metric::LatencyMetric;
use crate::execution_plans::{ChildrenIsolatorUnionExec, DistributedLeafExec};
use crate::passthrough_headers::get_passthrough_headers;
use crate::protobuf::tonic_status_to_datafusion_error;
use crate::work_unit_feed::{build_work_unit_batch_msg, set_work_unit_send_time};
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::coordinator_to_worker_msg::Inner;
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::worker::{WorkerDispatch, WorkerDispatchRequest};
use crate::{
    BytesCounterMetric, BytesMetricExt, DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, DistributedCodec,
    DistributedConfig, DistributedTaskContext, DistributedWorkUnitFeedContext, TaskKey,
    get_distributed_channel_resolver,
};
use datafusion::common::Result;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::common::{DataFusionError, exec_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, Label, MetricBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::{Stream, StreamExt};
use http::Extensions;
use prost::Message;
use std::sync::{Arc, OnceLock};
use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataMap;
use url::Url;
use uuid::Uuid;

/// How many [crate::WorkUnit] messages are allowed to be chunked synchronously together in order to
/// send fewer bigger [crate::WorkUnit] batches over the wire, reducing the overhead of sending many
/// small batches. See [StreamExt::ready_chunks] docs for more details about how chunking works.
const WORK_UNIT_FEED_CHUNK_SIZE: usize = 256;

/// The Arrow-Flight plan-delivery side: a [WorkerDispatch] that ships each task's plan over the
/// bidirectional coordinator-to-worker gRPC stream and wires up the work-unit feed and metrics
/// back-channel. Per-query state (plan-send metrics, the keep-alive notifier) is created lazily on
/// the first stage's dispatch and shared across stages. Dropping the dispatcher fires the notifier,
/// which closes the coordinator->worker streams and propagates EOS to the workers so they can clean
/// up; the coordinator holds it until the query's result stream is drained.
#[derive(Default)]
pub(crate) struct FlightWorkerDispatch {
    state: OnceLock<FlightDispatchState>,
}

struct FlightDispatchState {
    coordinator_to_worker_metrics: CoordinatorToWorkerMetrics,
    end_stream_notifier: Arc<Notify>,
}

impl WorkerDispatch for FlightWorkerDispatch {
    fn dispatch(&self, request: WorkerDispatchRequest<'_>) -> Result<()> {
        let state = self.state.get_or_init(|| FlightDispatchState {
            coordinator_to_worker_metrics: CoordinatorToWorkerMetrics::new(request.metrics),
            end_stream_notifier: Arc::new(Notify::new()),
        });
        let stage = request.stage;
        let mut stage_coordinator = StageCoordinator {
            plan: &stage.plan,
            query_id: stage.query_id,
            stage_id: stage.num,
            task_count: stage.tasks,
            task_ctx: request.task_ctx,
            metrics: &state.coordinator_to_worker_metrics,
            metrics_store: request.metrics_store,
            end_stream_notifier: &state.end_stream_notifier,
            join_set: request.join_set,
        };
        for (task_i, url) in request.routed_urls.iter().enumerate() {
            let (worker_tx, worker_rx) = stage_coordinator.send_plan_task(task_i, url.clone())?;
            stage_coordinator.worker_to_coordinator_task(task_i, worker_rx);
            stage_coordinator.coordinator_to_worker_task(task_i, worker_tx)?;
        }
        Ok(())
    }
}

impl Drop for FlightWorkerDispatch {
    fn drop(&mut self) {
        if let Some(state) = self.state.get() {
            // Signal the coordinator->worker streams that the query is finished, ending them and
            // propagating the EOS so workers can clean up any remaining state.
            state.end_stream_notifier.notify_waiters();
        }
    }
}

/// Manages all the coordinator->worker and worker->coordinator comms that happen during the
/// execution of an individual Stage. As this struct is scoped per Stage, it will handle the
/// connection to N workers, where N is the number of tasks of the managed Stage.
///
/// This struct is responsible for:
/// - Building tasks that communicate a serialized plan to multiple workers for further execution.
/// - Building tasks that stream partition feeds from local [WorkUnitFeedExec] nodes to their
///   remote counterparts.
pub(super) struct StageCoordinator<'a> {
    plan: &'a Arc<dyn ExecutionPlan>,
    query_id: Uuid,
    stage_id: usize,
    task_count: usize,
    task_ctx: &'a Arc<TaskContext>,
    metrics: &'a CoordinatorToWorkerMetrics,
    metrics_store: Option<&'a Arc<MetricsStore>>,
    end_stream_notifier: &'a Arc<Notify>,
    join_set: &'a mut JoinSet<Result<()>>,
}

impl<'a> StageCoordinator<'a> {
    /// Sends a serialized plan to a specific worker and sets up the bidirectional gRPC stream.
    /// Returns the sender for outbound coordinator-to-worker messages and the receiver for
    /// inbound worker-to-coordinator messages.
    pub(super) fn send_plan_task(
        &mut self,
        task_i: usize,
        url: Url,
    ) -> Result<(
        UnboundedSender<pb::CoordinatorToWorkerMsg>,
        UnboundedReceiver<pb::WorkerToCoordinatorMsg>,
    )> {
        let session_config = self.task_ctx.session_config();
        let codec = DistributedCodec::new_combined_with_user(session_config);

        let (specialized, work_unit_feed_declarations) = self.task_specialized_plan(task_i)?;

        let plan_proto =
            PhysicalPlanNode::try_from_physical_plan(specialized, &codec)?.encode_to_vec();
        let plan_size = plan_proto.len();

        let task_key = TaskKey {
            query_id: serialize_uuid(&self.query_id),
            stage_id: self.stage_id as u64,
            task_number: task_i as u64,
        };
        let msg = pb::CoordinatorToWorkerMsg {
            inner: Some(Inner::SetPlanRequest(pb::SetPlanRequest {
                plan_proto,
                task_count: self.task_count as u64,
                task_key: Some(task_key.clone()),
                work_unit_feed_declarations,
                target_worker_url: url.to_string(),
                query_start_time_ns: self.metrics.instantiation_time,
            })),
        };

        let (coordinator_to_worker_tx, coordinator_to_worker_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (worker_to_coordinator_tx, worker_to_coordinator_rx) =
            tokio::sync::mpsc::unbounded_channel();

        let channel_resolver = get_distributed_channel_resolver(self.task_ctx.as_ref());

        let mut headers = get_config_extension_propagation_headers(session_config)?;
        headers.extend(get_passthrough_headers(session_config));

        let request = Request::from_parts(
            MetadataMap::from_headers(headers),
            Extensions::default(),
            futures::stream::once(async { msg })
                .chain(UnboundedReceiverStream::new(coordinator_to_worker_rx))
                .map(set_work_unit_send_time)
                // Keep the request side of the channel open until the query ends: this tail emits
                // no messages and only completes, once the `Notify` fires. Workers interpret this
                // EOS of this stream as a query finished/aborted signal.
                .chain(keep_stream_alive(Arc::clone(self.end_stream_notifier))),
        );

        let metrics = self.metrics.clone();

        self.join_set.spawn(async move {
            let start = Instant::now();
            let mut client = channel_resolver.get_worker_client_for_url(&url).await?;
            let response = client.coordinator_channel(request).await.map_err(|e| {
                tonic_status_to_datafusion_error(&e).unwrap_or_else(|| {
                    exec_datafusion_err!("Error sending plan to worker {url}: {e}")
                })
            })?;
            metrics.plan_send_latency.record(&start);
            metrics.plan_bytes_sent.add_bytes(plan_size);
            let mut worker_to_coordinator_stream = response.into_inner();
            while let Some(msg_or_err) = worker_to_coordinator_stream.next().await {
                let msg = msg_or_err.map_err(|err| {
                    tonic_status_to_datafusion_error(err).unwrap_or_else(|| {
                        exec_datafusion_err!("Unknown error on worker to coordinator stream")
                    })
                })?;
                if worker_to_coordinator_tx.send(msg).is_err() {
                    break; // receiver dropped
                }
            }
            Ok::<_, DataFusionError>(())
        });

        Ok((coordinator_to_worker_tx, worker_to_coordinator_rx))
    }

    /// Spawns a background task in charge of collecting messages sent by a worker. Some things that
    /// are collected from workers are:
    /// - Execution metrics information, sent once the worker has finished executing the task.
    pub(super) fn worker_to_coordinator_task(
        &mut self,
        task_i: usize,
        mut worker_to_coordinator_rx: UnboundedReceiver<pb::WorkerToCoordinatorMsg>,
    ) {
        let task_key = TaskKey {
            query_id: serialize_uuid(&self.query_id),
            stage_id: self.stage_id as u64,
            task_number: task_i as u64,
        };
        let task_metrics = self.metrics_store.cloned();

        // Cannot use self.join_set because that's tied to the lifetime of the query, and the
        // metrics collection process might outlive the query's lifetime.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            while let Some(msg) = worker_to_coordinator_rx.recv().await {
                let Some(inner) = msg.inner else { continue };

                match inner {
                    pb::worker_to_coordinator_msg::Inner::TaskMetrics(pre_order_metrics) => {
                        if let Some(task_metrics) = &task_metrics {
                            task_metrics.insert(task_key.clone(), pre_order_metrics);
                        }
                    }
                }
            }
        });
    }

    /// Spawns a background task in charge of sending messages to workers. Some things that are sent
    /// to workers here are:
    /// - WorkUnits collected from [WorkUnitFeeds] present in the plan.
    pub(super) fn coordinator_to_worker_task(
        &mut self,
        task_i: usize,
        tx: UnboundedSender<pb::CoordinatorToWorkerMsg>,
    ) -> Result<()> {
        let session_config = self.task_ctx.session_config();
        let d_cfg = DistributedConfig::from_config_options(session_config.options())?;
        let wuf_registry = &d_cfg.__private_work_unit_feed_registry;

        let d_ctx = DistributedTaskContext {
            task_index: task_i,
            task_count: self.task_count,
        };
        let mut futures = vec![];
        self.plan.apply_with_dt_ctx(d_ctx, |plan, d_ctx| {
            let Some(wuf) = wuf_registry.get_work_unit_feed(plan) else {
                return Ok(TreeNodeRecursion::Continue);
            };

            let partitions = plan.properties().partitioning.partition_count();
            let start_partition = partitions * d_ctx.task_index;
            let end_partition = start_partition + partitions;

            let dist_feed_ctx = DistributedWorkUnitFeedContext {
                fan_out_tasks: d_ctx.task_count,
            };
            let t_ctx = Arc::new(task_ctx_with_extension(self.task_ctx, dist_feed_ctx));

            let mut feeds = Vec::with_capacity(end_partition - start_partition);
            for (partition, feed_idx) in (start_partition..end_partition).enumerate() {
                let feed = wuf
                    .feed(feed_idx, Arc::clone(&t_ctx))?
                    .map(move |el| (partition, el));
                feeds.push(feed);
            }
            let interleaved_feed = futures::stream::select_all(feeds);
            let mut chunked_interleaved_feed =
                interleaved_feed.ready_chunks(WORK_UNIT_FEED_CHUNK_SIZE);

            let id = wuf.id();
            let tx = tx.clone();
            futures.push(Box::pin(async move {
                // At this point, the partition feed contains a stream of decoded messages,
                // so they must be encoded in order to send them over the wire.
                while let Some(chunk) = chunked_interleaved_feed.next().await {
                    if tx.send(build_work_unit_batch_msg(&id, chunk)?).is_err() {
                        break; // channel closed.
                    };
                }
                Ok::<_, DataFusionError>(())
            }));
            Ok(TreeNodeRecursion::Continue)
        })?;

        struct WorkUnitEosOnDrop(UnboundedSender<pb::CoordinatorToWorkerMsg>);
        impl Drop for WorkUnitEosOnDrop {
            fn drop(&mut self) {
                let _ = self.0.send(pb::CoordinatorToWorkerMsg {
                    inner: Some(Inner::WorkUnitEos(true)),
                });
            }
        }

        self.join_set.spawn(async move {
            let _guard = WorkUnitEosOnDrop(tx);
            futures::future::try_join_all(futures).await?;
            Ok(())
        });
        Ok(())
    }

    /// Specializes the [Arc<dyn ExecutionPlan>] for this stage to provided task index. This implies
    /// trimming down any unnecessary information that the specific `task_i` task is not going to
    /// need, like unexecuted branches in [ChildrenIsolatorUnionExec], or unexecuted variants of
    /// [DistributedLeafExec].
    fn task_specialized_plan(
        &self,
        task_i: usize,
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<WorkUnitFeedDeclaration>)> {
        let session_config = self.task_ctx.session_config();
        let d_cfg = DistributedConfig::from_config_options(session_config.options())?;
        let wuf_registry = &d_cfg.__private_work_unit_feed_registry;

        let mut work_unit_feed_declarations = vec![];
        let d_ctx = DistributedTaskContext {
            task_index: task_i,
            task_count: self.task_count,
        };

        let plan = Arc::clone(self.plan);
        let transformed = plan.transform_down_with_dt_ctx(d_ctx, |plan, d_ctx| {
            if let Some(wuf) = wuf_registry.get_work_unit_feed(&plan) {
                work_unit_feed_declarations.push(WorkUnitFeedDeclaration {
                    id: serialize_uuid(&wuf.id()),
                    partitions: plan.properties().partitioning.partition_count() as u64,
                });
            };

            if let Some(ciu) = plan.downcast_ref::<ChildrenIsolatorUnionExec>() {
                let ciu = ciu.to_task_specialized(d_ctx.task_index);
                return Ok(Transformed::yes(Arc::new(ciu)));
            };

            if let Some(dle) = plan.downcast_ref::<DistributedLeafExec>() {
                let specialized = dle.to_task_specialized(d_ctx.task_index);
                return Ok(Transformed::yes(specialized));
            }

            Ok(Transformed::no(plan))
        })?;
        Ok((transformed.data, work_unit_feed_declarations))
    }
}

fn keep_stream_alive<T: 'static>(notify: Arc<Notify>) -> impl Stream<Item = T> + 'static {
    futures::stream::once(notify.notified_owned()).filter_map(|()| futures::future::ready(None))
}

/// Metrics that measure network details about communications between [DistributedExec] and a worker.
#[derive(Clone)]
pub(super) struct CoordinatorToWorkerMetrics {
    pub(super) plan_bytes_sent: BytesCounterMetric,
    pub(super) plan_send_latency: Arc<LatencyMetric>,
    pub(super) instantiation_time: u64,
}

impl CoordinatorToWorkerMetrics {
    pub(super) fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            // Metric that measures to total sum of bytes worth of subplans sent.
            plan_bytes_sent: MetricBuilder::new(metrics)
                .with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0"))
                .bytes_counter("plan_bytes_sent"),
            // Latency statistics about the network calls issued to the workers for feeding subplans.
            plan_send_latency: Arc::new(LatencyMetric::new(
                "plan_send_latency",
                |b| b.with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0")),
                metrics,
            )),
            instantiation_time: now_ns(),
        }
    }
}
