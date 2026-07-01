use crate::common::{TreeNodeExt, now_ns, task_ctx_with_extension};
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::coordinator::MetricsStore;
use crate::coordinator::latency_metric::LatencyMetric;
use crate::execution_plans::{ChildrenIsolatorUnionExec, DistributedLeafExec};
use crate::passthrough_headers::get_passthrough_headers;
use crate::stage::LocalStage;
use crate::work_unit_feed::{build_work_unit_batch_msg, set_work_unit_send_time};
use crate::{
    BytesCounterMetric, BytesMetricExt, CoordinatorToWorkerMsg,
    DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, DistributedCodec, DistributedConfig,
    DistributedTaskContext, DistributedWorkUnitFeedContext, LoadInfo, NetworkBoundaryExt,
    SetPlanRequest, Stage, TaskEstimator, TaskKey, TaskRoutingContext, WorkUnitFeedDeclaration,
    WorkerToCoordinatorMsg, get_distributed_channel_resolver, get_distributed_worker_resolver,
};
use datafusion::common::DataFusionError;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, Label, MetricBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use rand::Rng;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use url::Url;
use uuid::Uuid;

/// How many [crate::WorkUnitMsg] messages are allowed to be chunked synchronously together in order to
/// send fewer bigger [crate::WorkUnitMsg] batches over the wire, reducing the overhead of sending many
/// small batches. See [StreamExt::ready_chunks] docs for more details about how chunking works.
const WORK_UNIT_FEED_CHUNK_SIZE: usize = 256;

/// Manages communication between coordinator and workers for a single query.
///
/// The [QueryCoordinator]'s lifetime is scoped to a single query , and will instantiate independent
/// [StageCoordinator] scoped to each individual stage.
pub(super) struct QueryCoordinator {
    task_ctx: Arc<TaskContext>,
    coordinator_to_worker_metrics: CoordinatorToWorkerMetrics,
    metrics_store: Option<Arc<MetricsStore>>,
    end_stream_notifier: Arc<Notify>,
    join_set: Mutex<JoinSet<Result<()>>>,
}

impl QueryCoordinator {
    /// Builds a new [QueryCoordinator] scoped to a query.
    pub(super) fn new(
        task_ctx: Arc<TaskContext>,
        metrics_set: &ExecutionPlanMetricsSet,
        metrics_store: Option<Arc<MetricsStore>>,
    ) -> Self {
        Self {
            task_ctx,
            metrics_store,
            coordinator_to_worker_metrics: CoordinatorToWorkerMetrics::new(metrics_set),
            end_stream_notifier: Arc::new(Notify::new()),
            join_set: Mutex::new(JoinSet::new()),
        }
    }

    /// Builds a new [StageCoordinator] that will manage coordinator-worker connections for the given
    /// stage.
    pub(super) fn stage_coordinator<'a>(&'a self, stage: &'a LocalStage) -> StageCoordinator<'a> {
        StageCoordinator {
            plan: &stage.plan,
            query_id: stage.query_id,
            stage_id: stage.num,
            task_count: stage.tasks,
            task_ctx: &self.task_ctx,
            metrics: &self.coordinator_to_worker_metrics,
            metrics_store: &self.metrics_store,
            end_stream_notifier: &self.end_stream_notifier,
            join_set: &self.join_set,
        }
    }

    /// Returns the [SessionConfig] for the current query.
    pub(super) fn session_config(&self) -> &SessionConfig {
        self.task_ctx.session_config()
    }

    /// returns a guard that, when dropped, it signals all the coordinator->worker connections that
    /// the query is finished, ending them, and propagating the EOS to the workers so that they can
    /// clean up any remaining state.
    pub(super) fn end_query_guard(&self) -> NotifyGuard {
        NotifyGuard(Arc::clone(&self.end_stream_notifier))
    }

    /// Blocks until all background tasks have finished (e.g., sending WorkUnit feeds, or collecting
    /// metrics)
    pub(super) async fn drain_pending_tasks(self) -> Result<()> {
        let join_set = std::mem::take(self.join_set.lock().unwrap().deref_mut());
        for res in join_set.join_all().await {
            res?;
        }
        Ok(())
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
    metrics_store: &'a Option<Arc<MetricsStore>>,
    end_stream_notifier: &'a Arc<Notify>,
    join_set: &'a Mutex<JoinSet<Result<()>>>,
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
        UnboundedSender<CoordinatorToWorkerMsg>,
        UnboundedReceiver<WorkerToCoordinatorMsg>,
    )> {
        let session_config = self.task_ctx.session_config();
        let codec = DistributedCodec::new_combined_with_user(session_config);

        let (specialized, work_unit_feed_declarations) = self.task_specialized_plan(task_i)?;

        let plan_proto =
            PhysicalPlanNode::try_from_physical_plan(specialized, &codec)?.encode_to_vec();
        let plan_size = plan_proto.len();

        let task_key = TaskKey {
            query_id: self.query_id,
            stage_id: self.stage_id,
            task_number: task_i,
        };

        let msg = CoordinatorToWorkerMsg::SetPlanRequest(SetPlanRequest {
            task_key,
            task_count: self.task_count,
            plan_proto,
            work_unit_feed_declarations,
            target_worker_url: url.clone(),
            query_start_time_ns: self.metrics.instantiation_time,
        });

        let (coordinator_to_worker_tx, coordinator_to_worker_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (worker_to_coordinator_tx, worker_to_coordinator_rx) =
            tokio::sync::mpsc::unbounded_channel();

        let channel_resolver = get_distributed_channel_resolver(self.task_ctx.as_ref());

        let mut headers = get_config_extension_propagation_headers(session_config)?;
        headers.extend(get_passthrough_headers(session_config));

        let coordinator_to_worker_stream = futures::stream::once(async { msg })
            .chain(UnboundedReceiverStream::new(coordinator_to_worker_rx))
            .map(set_work_unit_send_time)
            // Keep the request side of the channel open until the query ends: this tail emits
            // no messages and only completes, once the `Notify` fires. Workers interpret this
            // EOS of this stream as a query finished/aborted signal. The flow looks like this:
            // 1. The query ends normally, as all Arrow RecordBatches are already streamed.
            // 2. The end stream notifier guard is dropped in `DistributedExec::execute()`.
            // 3. Here, `end_stream_notifier` fires and the coordinator->worker channel is
            //    gracefully ended.
            // 4. The coordinator->worker channel EOS is received in `impl_coordinator_channel.rs`.
            // 5. The metrics are send back in the worker->coordinator channel, and then that
            //    channel is closed.
            .chain(keep_stream_alive(Arc::clone(self.end_stream_notifier)))
            .boxed();

        let metrics = self.metrics.clone();

        self.join_set.lock().unwrap().spawn(async move {
            let start = Instant::now();
            let mut client = channel_resolver.get_worker_client_for_url(&url).await?;
            let mut worker_to_coordinator_stream = client
                .coordinator_channel(headers, coordinator_to_worker_stream)
                .await?;
            metrics.plan_send_latency.record(&start);
            metrics.plan_bytes_sent.add_bytes(plan_size);
            while let Some(msg) = worker_to_coordinator_stream.try_next().await? {
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
        mut worker_to_coordinator_rx: UnboundedReceiver<WorkerToCoordinatorMsg>,
    ) -> UnboundedReceiver<LoadInfo> {
        let task_key = TaskKey {
            query_id: self.query_id,
            stage_id: self.stage_id,
            task_number: task_i,
        };
        let task_metrics = self.metrics_store.clone();
        let (load_info_tx, load_info_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut load_info_tx_opt = Some(load_info_tx);

        // Cannot use self.join_set because that's tied to the lifetime of the query, and the
        // metrics collection process might outlive the query's lifetime.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            while let Some(msg) = worker_to_coordinator_rx.recv().await {
                match msg {
                    WorkerToCoordinatorMsg::TaskMetrics(v) => {
                        if let Some(task_metrics) = &task_metrics {
                            task_metrics.insert(task_key, v);
                        }
                    }
                    WorkerToCoordinatorMsg::LoadInfo(load_info) => {
                        if let Some(tx) = &load_info_tx_opt {
                            let _ = tx.send(load_info);
                        }
                    }
                    WorkerToCoordinatorMsg::LoadInfoEos => {
                        let _ = load_info_tx_opt.take();
                    }
                }
            }
        });
        load_info_rx
    }

    /// Spawns a background task in charge of sending messages to workers. Some things that are sent
    /// to workers here are:
    /// - WorkUnits collected from [WorkUnitFeeds] present in the plan.
    pub(super) fn coordinator_to_worker_task(
        &mut self,
        task_i: usize,
        tx: UnboundedSender<CoordinatorToWorkerMsg>,
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

        struct WorkUnitEosOnDrop(UnboundedSender<CoordinatorToWorkerMsg>);
        impl Drop for WorkUnitEosOnDrop {
            fn drop(&mut self) {
                let _ = self.0.send(CoordinatorToWorkerMsg::WorkUnitEos);
            }
        }

        self.join_set.lock().unwrap().spawn(async move {
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
                    id: wuf.id(),
                    partitions: plan.properties().partitioning.partition_count(),
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

    /// Returns as many URLs as the task count for the stage this [StageCoordinator]
    /// is managing. These URLs can be:
    /// - assigned randomly, if the user did not provide any custom routing.
    /// - chosen by the user, if they provided an implementation for the
    ///   [TaskEstimator::route_tasks] method.
    pub(super) fn routed_urls(&self) -> Result<Vec<Url>> {
        let session_config = self.task_ctx.session_config();
        let d_cfg = DistributedConfig::from_config_options(session_config.options())?;
        let worker_resolver = get_distributed_worker_resolver(session_config)?;
        let task_estimator = &d_cfg.__private_task_estimator;

        let routed_urls = match task_estimator.route_tasks(&TaskRoutingContext {
            task_ctx: Arc::clone(self.task_ctx),
            plan: self.plan,
            task_count: self.task_count,
        }) {
            Ok(Some(routed_urls)) => routed_urls,
            // If the user has not defined custom routing with a `route_tasks` implementation, we
            // default to round-robin task assignation from a randomized starting point.
            Ok(None) => {
                let available_urls = worker_resolver.get_urls()?;
                let start_idx = rand::rng().random_range(0..available_urls.len());
                (0..self.task_count)
                    .map(|i| available_urls[(start_idx + i) % available_urls.len()].clone())
                    .collect()
            }
            Err(e) => return exec_err!("error routing tasks to workers: {e}"),
        };

        if routed_urls.len() != self.task_count {
            return exec_err!(
                "number of tasks ({}) was not equal to number of urls ({}) at execution time",
                self.task_count,
                routed_urls.len()
            );
        }
        Ok(routed_urls)
    }

    pub(super) fn find_input_stage_with_single_url(&self) -> Option<Url> {
        let mut single_stage_url = None;
        self.plan
            .apply(|plan| {
                let Some(nb) = plan.as_network_boundary() else {
                    return Ok(TreeNodeRecursion::Continue);
                };

                if let Stage::Remote(remote) = nb.input_stage()
                    && remote.workers.len() == 1
                {
                    single_stage_url = Some(remote.workers[0].clone());
                    return Ok(TreeNodeRecursion::Stop);
                }

                Ok(TreeNodeRecursion::Jump)
            })
            .expect("Cannot fail");

        single_stage_url
    }
}

fn keep_stream_alive<T: 'static>(notify: Arc<Notify>) -> impl Stream<Item = T> + 'static {
    futures::stream::once(notify.notified_owned()).filter_map(|()| futures::future::ready(None))
}

pub(super) struct NotifyGuard(Arc<Notify>);

impl Drop for NotifyGuard {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

/// Metrics that measure network details about communications between [DistributedExec] and a worker.
#[derive(Clone)]
pub(super) struct CoordinatorToWorkerMetrics {
    pub(super) plan_bytes_sent: BytesCounterMetric,
    pub(super) plan_send_latency: Arc<LatencyMetric>,
    pub(super) instantiation_time: usize,
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
