use crate::codec::{encode_execution_plan, roundtrip_pb};
use crate::common::{TreeNodeExt, now_ns, task_ctx_with_extension};
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::coordinator::DynamicFilterRegistry;
use crate::coordinator::Store;
use crate::coordinator::latency_metric::LatencyMetric;
use crate::dynamic_filtering::{
    discover_dynamic_filter_producers,
    maybe_roundtrip_plan_to_sever_in_memory_dynamic_filter_relationships,
};
use crate::events::{RouteTasksEvent, RouteTasksHandlers};
use crate::execution_plans::{ChildrenIsolatorUnionExec, DistributedLeafExec};
use crate::passthrough_headers::get_passthrough_headers;
use crate::stage::LocalStage;
use crate::work_unit_feed::WorkUnitFeedRegistry;
use crate::work_unit_feed::{build_work_unit_batch_msg, set_work_unit_send_time};
use crate::{
    CoordinatorToWorkerMsg, DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, DistributedTaskContext,
    DistributedWorkUnitFeedContext, LoadInfo, LocalWorkerContext, MaybeEncoded, NetworkBoundaryExt,
    SetPlanRequest, TaskCompletedDynamicFilters, TaskKey, TaskMetrics, WorkUnitFeedDeclaration,
    WorkerToCoordinatorMsg, get_distributed_channel_resolver,
};
use datafusion::common::DataFusionError;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{HashSet, Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, Label, MetricBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::Count;
use datafusion::prelude::SessionConfig;
use futures::{Stream, StreamExt, TryStreamExt};
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
    metrics: ExecutionPlanMetricsSet,
    coordinator_to_worker_metrics: CoordinatorToWorkerMetrics,
    metrics_store: Option<Arc<Store<TaskMetrics>>>,
    completed_dynamic_filter_store: Option<Arc<Store<TaskCompletedDynamicFilters>>>,
    dynamic_filter_registry: Arc<DynamicFilterRegistry>,
    end_stream_notifier: Arc<Notify>,
    join_set: Mutex<JoinSet<Result<()>>>,
}

impl QueryCoordinator {
    /// Builds a new [QueryCoordinator] scoped to a query.
    pub(super) fn new(
        task_ctx: Arc<TaskContext>,
        metrics_set: &ExecutionPlanMetricsSet,
        metrics_store: Option<Arc<Store<TaskMetrics>>>,
        completed_dynamic_filter_store: Option<Arc<Store<TaskCompletedDynamicFilters>>>,
    ) -> Self {
        Self {
            task_ctx,
            metrics: metrics_set.clone(),
            metrics_store,
            completed_dynamic_filter_store,
            dynamic_filter_registry: Arc::new(DynamicFilterRegistry::new()),
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
            metrics_set: &self.metrics,
            metrics: &self.coordinator_to_worker_metrics,
            metrics_store: &self.metrics_store,
            completed_dynamic_filter_store: &self.completed_dynamic_filter_store,
            dynamic_filter_registry: &self.dynamic_filter_registry,
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
struct SpecializedTaskPlan {
    plan: Arc<dyn ExecutionPlan>,
    work_unit_feed_declarations: Vec<WorkUnitFeedDeclaration>,
    dynamic_filter_remote_producer_ids: Vec<u64>,
}

pub(super) struct StageCoordinator<'a> {
    plan: &'a Arc<dyn ExecutionPlan>,
    query_id: Uuid,
    stage_id: usize,
    task_count: usize,
    task_ctx: &'a Arc<TaskContext>,
    metrics_set: &'a ExecutionPlanMetricsSet,
    metrics: &'a CoordinatorToWorkerMetrics,
    metrics_store: &'a Option<Arc<Store<TaskMetrics>>>,
    completed_dynamic_filter_store: &'a Option<Arc<Store<TaskCompletedDynamicFilters>>>,
    dynamic_filter_registry: &'a Arc<DynamicFilterRegistry>,
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

        let SpecializedTaskPlan {
            plan,
            work_unit_feed_declarations,
            dynamic_filter_remote_producer_ids,
        } = self.task_specialized_plan(task_i)?;

        let task_key = TaskKey {
            query_id: self.query_id,
            stage_id: self.stage_id,
            task_number: task_i,
        };
        self.dynamic_filter_registry
            .register_task(&plan, task_key)?;

        let set_plan_request = SetPlanRequest {
            task_key,
            task_count: self.task_count,
            plan: MaybeEncoded::Decoded(plan),
            dynamic_filter_remote_producer_ids,
            work_unit_feed_declarations,
            target_worker_url: url.clone(),
            query_start_time_ns: self.metrics.instantiation_time,
        };

        let (coordinator_to_worker_tx, coordinator_to_worker_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (worker_to_coordinator_tx, worker_to_coordinator_rx) =
            tokio::sync::mpsc::unbounded_channel();

        let mut headers = get_config_extension_propagation_headers(session_config)?;
        headers.extend(get_passthrough_headers(session_config));

        let coordinator_to_worker_stream = UnboundedReceiverStream::new(coordinator_to_worker_rx)
            .map(set_work_unit_send_time)
            // Keep the request side of the channel open until the query ends: this tail emits
            // no messages and only completes, once the `Notify` fires. Workers interpret this
            // EOS of this stream as a query finished/aborted signal. The flow looks like this:
            // 1. The query ends normally, as all Arrow RecordBatches are already streamed.
            // 2. The end stream notifier guard is dropped in `DistributedExec::execute()`.
            // 3. Here, `end_stream_notifier` fires and the coordinator->worker channel is
            //    gracefully ended.
            // 4. The coordinator->worker channel EOS is received in `impl_coordinator_channel.rs`.
            // 5. The metrics and final dynamic filters are sent back in the
            //    worker->coordinator channel, and then that channel is closed.
            .chain(keep_stream_alive(Arc::clone(self.end_stream_notifier)))
            .boxed();

        let metrics = self.metrics.clone();
        let metrics_set = self.metrics_set.clone();
        let task_ctx = Arc::clone(self.task_ctx);

        self.join_set.lock().unwrap().spawn(async move {
            let start = Instant::now();

            let mut client = match LocalWorkerContext::from_ctx(&task_ctx) {
                Some(lw) if lw.self_url == url => {
                    metrics.local_coordinator_channels.add(1);
                    Ok(lw.to_worker_channel())
                }
                _ => {
                    metrics.remote_coordinator_channels.add(1);
                    let ch_resolver = get_distributed_channel_resolver(task_ctx.as_ref());
                    ch_resolver.get_worker_client_for_url(&url).await
                }
            }?;
            let mut worker_to_coordinator_stream = client
                .coordinator_channel(
                    headers,
                    set_plan_request,
                    coordinator_to_worker_stream,
                    metrics_set,
                    &task_ctx,
                )
                .await?;
            metrics.plan_send_latency.record(&start);
            while let Some(msg) = worker_to_coordinator_stream.try_next().await? {
                if worker_to_coordinator_tx.send(msg).is_err() {
                    break; // receiver dropped
                }
            }
            Ok::<_, DataFusionError>(())
        });

        Ok((coordinator_to_worker_tx, worker_to_coordinator_rx))
    }

    pub(super) fn seal_dynamic_filter_stage(&self) {
        self.dynamic_filter_registry.seal_stage(self.stage_id);
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
        let completed_dynamic_filter_store = self.completed_dynamic_filter_store.clone();
        let dynamic_filter_registry = Arc::clone(self.dynamic_filter_registry);
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
                    WorkerToCoordinatorMsg::TaskCompletedDynamicFilters(filters) => {
                        if let Some(store) = &completed_dynamic_filter_store {
                            store.insert(task_key, filters);
                        }
                    }
                    WorkerToCoordinatorMsg::ProducedDynamicFilter(filter) => {
                        dynamic_filter_registry.add_report(task_key, *filter);
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
        let wuf_registry = session_config
            .get_extension::<WorkUnitFeedRegistry>()
            .unwrap_or_default();

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
    fn task_specialized_plan(&self, task_i: usize) -> Result<SpecializedTaskPlan> {
        let session_config = self.task_ctx.session_config();
        let wuf_registry = session_config
            .get_extension::<WorkUnitFeedRegistry>()
            .unwrap_or_default();

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

                // WorkUnitFeeds are transitioned to remote mode during proto conversion.
                // Right now, there's no other way for a WorkUnitFeed to be transitioned to
                // remote mode so that it can pull WorkUnits over the WorkerChannel.
                //
                // Doing this roundtrip here is not super clean, but it transitions the feed with
                // very little specialized code.
                let plan = roundtrip_pb(plan, self.task_ctx)?;
                return Ok(Transformed::yes(plan));
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
        let plan = maybe_roundtrip_plan_to_sever_in_memory_dynamic_filter_relationships(
            Arc::clone(&transformed.data),
            self.task_ctx,
        )?;
        let dynamic_filter_remote_producer_ids = dynamic_filter_remote_producer_ids(&plan)?;
        Ok(SpecializedTaskPlan {
            plan,
            work_unit_feed_declarations,
            dynamic_filter_remote_producer_ids,
        })
    }

    /// Returns as many URLs as the task count for the stage this [StageCoordinator]
    /// is managing. These URLs can be:
    /// - assigned randomly, if the user did not provide any custom routing.
    /// - chosen by the user, if they provided an implementation for the
    ///   [RouteTasksHandler::route_tasks] method.
    pub(super) fn routed_urls(&self) -> Result<Vec<Url>> {
        let ev = RouteTasksEvent {
            task_ctx: Arc::clone(self.task_ctx),
            plan: self.plan,
            task_count: self.task_count,
        };
        let Some(routed) = RouteTasksHandlers::handle(ev).transpose()? else {
            return exec_err!("No task routing handler was able to resolve URLs for stage");
        };

        if routed.urls.len() != self.task_count {
            return exec_err!(
                "number of tasks ({}) was not equal to number of urls ({}) at execution time",
                self.task_count,
                routed.urls.len()
            );
        }
        Ok(routed.urls)
    }
}

/// Returns producer IDs with at least one remote consumer.
///
/// If a producer ID is present in the dynamic-filter anchors of any [`NetworkBoundary`], the plan
/// contains at least one remote consumer and the producer's updates must be forwarded to the
/// coordinator.
///
/// [`NetworkBoundary`]: crate::NetworkBoundary
fn dynamic_filter_remote_producer_ids(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<u64>> {
    let producer_ids: HashSet<_> = discover_dynamic_filter_producers(plan)?
        .into_iter()
        .map(|producer| producer.id)
        .collect();
    let mut anchor_ids = HashSet::new();
    plan.apply(|node| {
        if !node.is_network_boundary() {
            return Ok(TreeNodeRecursion::Continue);
        }
        node.apply_expressions(&mut |root| {
            root.apply(|expression| {
                if expression
                    .downcast_ref::<DynamicFilterPhysicalExpr>()
                    .is_some()
                {
                    let Some(id) = expression.expression_id() else {
                        return datafusion::common::internal_err!(
                            "DynamicFilterPhysicalExpr did not have an expression ID"
                        );
                    };
                    anchor_ids.insert(id);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut remote_producer_ids: Vec<_> = producer_ids.intersection(&anchor_ids).copied().collect();
    remote_producer_ids.sort_unstable();
    Ok(remote_producer_ids)
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
    pub(super) local_coordinator_channels: Count,
    pub(super) remote_coordinator_channels: Count,
    pub(super) plan_send_latency: Arc<LatencyMetric>,
    pub(super) instantiation_time: usize,
}

// Use a helper function instead of a closure due to a panic in the rustc compiler where it
// would incorrectly allocate memory for the metrics that reuses the same buffer across calls to builder.
// This is fixed in rustc 1.98
fn with_task_id_label(builder: MetricBuilder) -> MetricBuilder {
    builder.with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0"))
}

impl CoordinatorToWorkerMetrics {
    pub(super) fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            local_coordinator_channels: MetricBuilder::new(metrics)
                .global_counter("local_coordinator_channels"),
            remote_coordinator_channels: MetricBuilder::new(metrics)
                .global_counter("remote_coordinator_channels"),
            // Latency statistics about the network calls issued to the workers for feeding subplans.
            plan_send_latency: Arc::new(LatencyMetric::new(
                "plan_send_latency",
                with_task_id_label,
                metrics,
            )),
            instantiation_time: now_ns(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NetworkShuffleExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::repartition::RepartitionExec;

    #[test]
    fn identifies_only_producers_with_network_boundary_consumers() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let local_filter = dynamic_filter();
        let local_probe = Arc::new(FilterExec::try_new(local_filter.clone(), empty(&schema))?)
            as Arc<dyn ExecutionPlan>;
        let local_join = join_with_filter(&schema, local_probe, Arc::clone(&local_filter))?;
        assert!(dynamic_filter_remote_producer_ids(&local_join)?.is_empty());

        let remote_filter = dynamic_filter();
        let repartition = Arc::new(RepartitionExec::try_new(
            empty(&schema),
            Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 1),
        )?) as Arc<dyn ExecutionPlan>;
        let remote_probe = Arc::new(
            NetworkShuffleExec::try_new(repartition, 1)?
                .with_dynamic_filter_anchors(vec![remote_filter.clone()]),
        ) as Arc<dyn ExecutionPlan>;
        let remote_join = join_with_filter(&schema, remote_probe, Arc::clone(&remote_filter))?;
        assert_eq!(
            dynamic_filter_remote_producer_ids(&remote_join)?,
            vec![remote_filter.expression_id().unwrap()]
        );
        Ok(())
    }

    fn dynamic_filter() -> Arc<DynamicFilterPhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ))
    }

    fn empty(schema: &Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::clone(schema)))
    }

    fn join_with_filter(
        schema: &Arc<Schema>,
        probe: Arc<dyn ExecutionPlan>,
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            HashJoinExec::try_new(
                empty(schema),
                probe,
                vec![(Arc::new(Column::new("a", 0)), Arc::new(Column::new("a", 0)))],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )?
            .with_dynamic_filter_expr(dynamic_filter)?,
        ))
    }

    /// Regression test for a rustc miscompilation (present at least through
    /// 1.96, fixed in 1.98) of [`CoordinatorToWorkerMetrics::new`].
    ///
    /// When the per-metric label builder was passed to [`LatencyMetric::new`]
    /// as a closure (`|b| b.with_label(..)`) and the crate was compiled in
    /// release mode with `panic = "abort"` at opt-level >= 2, the compiler
    /// reused the first `MetricBuilder`'s freshly-emptied labels `Vec` header
    /// on the second, back-to-back invocation. The `_avg` latency metric's
    /// labels vec then aliased the `_max` metric's heap buffer (pushing into
    /// it at index 1 without allocating), so two live [`Metric`]s owned one
    /// buffer and metrics teardown double-freed / read freed memory — a
    /// use-after-free that only reproduced in optimized abort builds. Passing
    /// the builder as a named `fn` ([`with_task_id_label`]) sidesteps it.
    ///
    /// `CoordinatorToWorkerMetrics::new` registers the latency `_max`/`_avg` pair, each of which
    /// must own a distinct heap buffer holding exactly its single `task_id`
    /// label. The miscompile is observable as the `_avg` buffer aliasing the
    /// `_max` buffer with length 2.
    ///
    /// NOTE: the miscompile only manifests in `--release` builds compiled with
    /// `panic = "abort"`; a default debug `cargo test` run passes on affected
    /// and fixed toolchains alike. This test therefore guards the invariant
    /// and documents the required builder shape; to exercise the miscompile
    /// itself, build the reproducing configuration.
    #[test]
    fn coordinator_metrics_have_distinct_label_buffers() {
        // The exact codegen depends on the surrounding inlining context, so
        // repeat rather than trusting a single construction.
        for iteration in 0..10_000 {
            let metrics = ExecutionPlanMetricsSet::new();
            let _coordinator = CoordinatorToWorkerMetrics::new(&metrics);

            let set = metrics.clone_inner();
            let labeled: Vec<(usize, usize)> = set
                .iter()
                .filter(|metric| !metric.labels().is_empty())
                .map(|metric| (metric.labels().as_ptr() as usize, metric.labels().len()))
                .collect();

            assert_eq!(
                labeled.len(),
                2,
                "iteration {iteration}: expected 2 labeled metrics \
                 (plan_send_latency_max, plan_send_latency_avg), \
                 got {labeled:x?}"
            );

            for (ptr, len) in &labeled {
                assert_eq!(
                    *len, 1,
                    "iteration {iteration}: labeled metric at {ptr:#x} carries {len} labels, \
                     expected 1 — label-buffer aliasing miscompilation"
                );
            }

            let mut ptrs: Vec<usize> = labeled.iter().map(|(ptr, _)| *ptr).collect();
            ptrs.sort_unstable();
            let distinct = ptrs.windows(2).all(|w| w[0] != w[1]);
            assert!(
                distinct,
                "iteration {iteration}: labeled metrics share a label buffer: {labeled:x?} \
                 — rustc metric-builder miscompilation (use a named fn, not a closure)"
            );
        }
    }
}
