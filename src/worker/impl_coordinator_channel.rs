use crate::codec::encode_physical_expr;
use crate::common::{
    TreeNodeExt, discover_dynamic_filter_consumers, discover_dynamic_filter_producers,
};
use crate::events::{WorkerPlanRewriteEvent, WorkerPlanRewriteHandlers};
use crate::execution_plans::SamplerExec;
use crate::protocol::LocalWorkerContext;
use crate::work_unit_feed::{RemoteWorkUnitFeedRegistry, set_work_unit_received_time};
use crate::worker::task_data::TaskDataMetrics;
use crate::{
    CoordinatorToWorkerMsg, DistributedConfig, DistributedExt, DistributedTaskContext,
    ProducedDynamicFilter, SetPlanRequest, TaskCompletedDynamicFilters, TaskData,
    TaskDynamicFilter, TaskMetrics, Worker, WorkerQueryContext, WorkerToCoordinatorMsg,
};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, HashSet, Result, exec_datafusion_err, internal_err};
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, StreamExt, TryStreamExt};
use http::HeaderMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, watch};

impl Worker {
    pub async fn coordinator_channel(
        &self,
        headers: HeaderMap,
        request: SetPlanRequest,
        stream: BoxStream<'static, Result<CoordinatorToWorkerMsg>>,
    ) -> Result<BoxStream<'static, Result<WorkerToCoordinatorMsg>>> {
        let key = request.task_key;

        let entry = self
            .task_data_entries
            .get_with(key, async { Default::default() })
            .await;

        let mut remote_work_unit_feed_registry = RemoteWorkUnitFeedRegistry::default();
        for decl in request.work_unit_feed_declarations {
            remote_work_unit_feed_registry.add(decl.id, decl.partitions);
        }

        let (metrics_tx, metrics_rx) = oneshot::channel();
        let (dynamic_filters_tx, dynamic_filters_rx) = oneshot::channel();
        let mut load_info_rxs = vec![];

        let task_data = || async {
            let mut cfg = SessionConfig::default()
                .with_extension(Arc::new(remote_work_unit_feed_registry.receivers))
                .with_extension(Arc::new(DistributedTaskContext {
                    task_index: request.task_key.task_number,
                    task_count: request.task_count,
                }))
                .with_extension(Arc::new(LocalWorkerContext {
                    local_worker: self.clone(),
                    self_url: request.target_worker_url,
                }))
                .with_distributed_option_extension_from_headers::<DistributedConfig>(&headers)?;

            let d_cfg = DistributedConfig::from_config_options(cfg.options())?;
            let shuffle_batch_size = d_cfg.shuffle_batch_size;
            let collect_metrics = d_cfg.collect_metrics;
            if shuffle_batch_size != 0 {
                cfg = cfg.with_batch_size(shuffle_batch_size);
            }

            let session_state = self
                .session_builder
                .build_session_state(WorkerQueryContext {
                    builder: SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(cfg)
                        .with_runtime_env(Arc::clone(&self.runtime)),
                    headers,
                })
                .await?;

            let task_ctx = session_state.task_ctx();
            let plan = request.plan.decode(&task_ctx)?;

            let ev = WorkerPlanRewriteEvent {
                plan,
                session_config: session_state.config(),
            };
            let plan = WorkerPlanRewriteHandlers::handle(ev)?.plan;
            load_info_rxs =
                SamplerExec::kick_off_first_sampler(Arc::clone(&plan), Arc::clone(&task_ctx))?;

            // Initialize partition count to the number of partitions in the stage
            Ok::<_, DataFusionError>(TaskData {
                base_plan: plan,
                final_plan: Arc::new(OnceLock::new()),
                task_ctx,
                metrics_tx: match collect_metrics {
                    true => Arc::new(std::sync::Mutex::new(Some(metrics_tx))),
                    false => Arc::new(std::sync::Mutex::new(None)),
                },
                task_data_metrics: Arc::new(TaskDataMetrics::new(request.query_start_time_ns)),
            })
        };

        let task_data_result = task_data().await.map_err(Arc::new);

        entry
            .write(task_data_result.clone())
            .map_err(|e| exec_datafusion_err!("{e}"))?;

        let task_data = task_data_result.map_err(DataFusionError::Shared)?;

        let dynamic_filter_report_ids: HashSet<_> =
            request.dynamic_filter_report_ids.iter().copied().collect();
        let producer_filters = discover_dynamic_filter_producers(&task_data.base_plan)?
            .into_iter()
            .filter(|producer| dynamic_filter_report_ids.contains(&producer.id));
        let (producer_cancel_tx, producer_cancel_rx) = watch::channel(false);
        let producer_task_ctx = Arc::clone(&task_data.task_ctx);

        // Continue reading remaining messages (work unit feed data) in the background.
        let mut work_unit_senders = Some(remote_work_unit_feed_registry.senders);
        let task_data_entries = Arc::clone(&self.task_data_entries);

        // This tokio task takes ownership of the final-report senders that keep the
        // worker->coordinator stream alive. As soon as this task ends, the runtime metrics and
        // final dynamic filters are sent back and the worker->coordinator stream ends. The flow
        // is the following:
        // 1. The query ends normally, as all Arrow RecordBatches are already streamed.
        // 2. In DistributedExec::execute(), the end query guard is dropped.
        // 3. In StageCoordinator::send_plan_task(), `end_stream_notifier` fires and the
        //    coordinator->worker channel is gracefully ended.
        // 4. The coordinator->worker channel EOS is received by this same function, ending the
        //    while loop inside this `tokio::spawn` below.
        // 5. The metrics and final dynamic filters are sent back in the worker->coordinator
        //    channel, and then that channel is closed.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            let mut stream = stream.map_ok(set_work_unit_received_time);
            while let Some(Ok(msg)) = stream.next().await {
                match msg {
                    CoordinatorToWorkerMsg::WorkUnitBatch(work_unit_batch) => {
                        let Some(work_unit_senders) = work_unit_senders.as_mut() else {
                            continue;
                        };
                        for wu in work_unit_batch.batch {
                            let id = wu.id;
                            let partition = wu.partition;
                            let Some(tx) = work_unit_senders.get(&(wu.id, partition)) else {
                                continue;
                            };
                            if tx.send(Ok(wu)).is_err() {
                                // Channel closed, this sender needs to be dropped, as none will ever
                                // be listening on the other side.
                                work_unit_senders.remove(&(id, partition));
                                continue;
                            }
                        }
                    }
                    CoordinatorToWorkerMsg::WorkUnitEos => {
                        // No further work unit message will be received here, so drop all the
                        // sender sides so that receiver sides see an EOS upon draining the
                        // remaining messages.
                        //
                        // The [WorkUnitEos] message just applies work units, and it's not a global
                        // EOS signal for the coordinator->worker stream, as there might be more
                        // messages of different nature in that stream.
                        let _ = work_unit_senders.take();
                    }
                    CoordinatorToWorkerMsg::ApplyDynamicFilter(_) => {
                        // Runtime application is introduced independently from the routing
                        // protocol. Until then, accepting the message is intentionally a no-op.
                    }
                }
            }

            // Producer completion is an optimization signal, not a condition for ending a task.
            // Wake any filters that never completed before finalizing the task reports.
            let _ = producer_cancel_tx.send(true);

            let metrics_tx = task_data.metrics_tx.lock().unwrap().take();
            let mut dynamic_filters = TaskCompletedDynamicFilters::default();
            if let Some(Ok(plan)) = task_data.final_plan.get() {
                let d_ctx = DistributedTaskContext {
                    task_index: key.task_number,
                    task_count: request.task_count,
                };
                let task_data_metrics = &task_data.task_data_metrics;
                task_data_metrics.mark_execution_finished();
                if let Some(metrics_tx) = metrics_tx {
                    send_metrics_via_channel(metrics_tx, plan, d_ctx, task_data_metrics);
                }
                dynamic_filters = build_task_completed_dynamic_filters(plan, &task_data.task_ctx)
                    .unwrap_or_default();
            }
            let _ = dynamic_filters_tx.send(dynamic_filters);
            task_data_entries.invalidate(&key).await
        });

        let load_info_stream = FuturesUnordered::from_iter(load_info_rxs)
            .filter_map(async |load_info_or_channel_dropped| {
                // This error can only happen if the LoadInfo sender was dropped, which is fine.
                let load_info = load_info_or_channel_dropped.ok()?;
                Some(WorkerToCoordinatorMsg::LoadInfo(load_info))
            })
            .chain(futures::stream::once(async move {
                WorkerToCoordinatorMsg::LoadInfoEos
            }));

        // Stream back metrics when the coordinator channel reaches EOS. At that point the
        // coordinator has closed the query-scoped request stream, so any remaining task state can
        // be finalized even if some partition streams were not dropped through the normal path.
        let metrics_stream = metrics_rx.into_stream();
        let metrics_stream = metrics_stream.filter_map(async |task_metrics_or_channel_dropped| {
            let task_metrics = task_metrics_or_channel_dropped.ok()?;
            Some(WorkerToCoordinatorMsg::TaskMetrics(task_metrics))
        });

        let dynamic_filters_stream = dynamic_filters_rx.into_stream().filter_map(
            async |dynamic_filters_or_channel_dropped| {
                let dynamic_filters = dynamic_filters_or_channel_dropped.ok()?;
                Some(WorkerToCoordinatorMsg::TaskCompletedDynamicFilters(
                    dynamic_filters,
                ))
            },
        );

        let produced_dynamic_filters_stream =
            futures::stream::select_all(producer_filters.into_iter().map(|producer| {
                produced_dynamic_filter_stream(
                    producer.id,
                    producer.expression,
                    Arc::clone(&producer_task_ctx),
                    producer_cancel_rx.clone(),
                )
            }));

        Ok(futures::stream::select(
            produced_dynamic_filters_stream,
            futures::stream::select(
                load_info_stream,
                futures::stream::select(metrics_stream, dynamic_filters_stream),
            ),
        )
        .map(Ok)
        .boxed())
    }
}

fn produced_dynamic_filter_stream(
    expression_id: u64,
    expression: Arc<dyn PhysicalExpr>,
    task_ctx: Arc<TaskContext>,
    cancel_rx: watch::Receiver<bool>,
) -> BoxStream<'static, WorkerToCoordinatorMsg> {
    futures::stream::unfold(
        Some((expression, task_ctx, cancel_rx)),
        move |state| async move {
            let (expression, task_ctx, mut cancel_rx) = state?;
            let dynamic_filter = expression
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .expect("producer discovery returns DynamicFilterPhysicalExpr");

            loop {
                // `wait_update()` uses a Tokio watch channel, so multiple generations can
                // naturally coalesce while this observer is busy. The stream promises the latest
                // observed state rather than one message per generation; completion is awaited
                // separately because it does not advance the generation.
                let completed = tokio::select! {
                    _ = dynamic_filter.wait_update() => false,
                    _ = dynamic_filter.wait_complete() => true,
                    _ = cancel_rx.wait_for(|cancelled| *cancelled) => return None,
                };

                let Ok(serialized) = encode_physical_expr(&expression, &task_ctx) else {
                    // Dynamic filtering is an optimization. Ignore an unserializable update and
                    // continue observing the producer in case a later state can be serialized.
                    if completed {
                        return None;
                    }
                    continue;
                };
                let is_complete = matches!(
                    serialized.expr_type.as_ref(),
                    Some(ExprType::DynamicFilter(dynamic_filter)) if dynamic_filter.is_complete
                );
                let message = WorkerToCoordinatorMsg::ProducedDynamicFilter(Box::new(
                    ProducedDynamicFilter {
                        expression_id,
                        expression: serialized,
                    },
                ));
                let next = (!is_complete).then_some((expression, task_ctx, cancel_rx));
                return Some((message, next));
            }
        },
    )
    .boxed()
}

fn build_task_completed_dynamic_filters(
    plan: &Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<datafusion::execution::TaskContext>,
) -> Result<TaskCompletedDynamicFilters> {
    let mut filters = vec![];
    for consumer in discover_dynamic_filter_consumers(plan)? {
        // Serializing the complete DynamicFilterPhysicalExpr preserves both its current
        // predicate and its completion state through DataFusion's native proto hook.
        let expression = encode_physical_expr(&consumer.expression, task_ctx)?;
        let Some(ExprType::DynamicFilter(dynamic_filter)) = expression.expr_type.as_ref() else {
            return internal_err!("discovered dynamic filter did not serialize as one");
        };
        // A cancelled or short-circuited task can leave filters incomplete. Do not report those
        // as final values for display.
        if dynamic_filter.is_complete {
            filters.push(TaskDynamicFilter {
                expression_id: consumer.id,
                expression,
            });
        }
    }
    Ok(TaskCompletedDynamicFilters { filters })
}

/// Collects metrics from the plan in pre-order traversal order and sends them via the
/// coordinator channel oneshot.
fn send_metrics_via_channel(
    metrics_tx: Sender<TaskMetrics>,
    plan: &Arc<dyn ExecutionPlan>,
    dt_ctx: DistributedTaskContext,
    task_data_metrics: &Arc<TaskDataMetrics>,
) {
    let mut pre_order_plan_metrics = vec![];
    let _ = plan.apply_with_dt_ctx(dt_ctx, |node, _| {
        pre_order_plan_metrics.push(node.metrics().unwrap_or_default());
        Ok(TreeNodeRecursion::Continue)
    });

    // Ignore send errors — the coordinator channel may have been dropped (e.g. query cancelled).
    let _ = metrics_tx.send(TaskMetrics {
        pre_order_plan_metrics,
        task_metrics: task_data_metrics.to_metrics_set(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_expr::expressions::{Column, lit};
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn streams_dynamic_filter_updates_and_completion() -> Result<()> {
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        let expression_id = dynamic_filter.expression_id().unwrap();
        let expression = Arc::clone(&dynamic_filter) as Arc<dyn PhysicalExpr>;
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let mut stream = produced_dynamic_filter_stream(
            expression_id,
            expression,
            SessionContext::new().task_ctx(),
            cancel_rx,
        );

        let (update, ()) = tokio::join!(stream.next(), async {
            tokio::task::yield_now().await;
            dynamic_filter.update(lit(false)).unwrap();
        });
        let update = produced_filter(update.expect("expected update"));
        let ExprType::DynamicFilter(update) = update.expression.expr_type.as_ref().unwrap() else {
            panic!("expected dynamic filter");
        };
        let update_generation = update.generation;
        assert!(!update.is_complete);

        let (completion, ()) = tokio::join!(stream.next(), async {
            tokio::task::yield_now().await;
            dynamic_filter.mark_complete();
        });
        let completion = produced_filter(completion.expect("expected completion"));
        let ExprType::DynamicFilter(completion) = completion.expression.expr_type.as_ref().unwrap()
        else {
            panic!("expected dynamic filter");
        };
        assert_eq!(completion.generation, update_generation);
        assert!(completion.is_complete);
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_stops_dynamic_filter_updates() {
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        let expression_id = dynamic_filter.expression_id().unwrap();
        let expression = dynamic_filter as Arc<dyn PhysicalExpr>;
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let mut stream = produced_dynamic_filter_stream(
            expression_id,
            expression,
            SessionContext::new().task_ctx(),
            cancel_rx,
        );

        cancel_tx.send(true).unwrap();
        assert!(stream.next().await.is_none());
    }

    fn produced_filter(message: WorkerToCoordinatorMsg) -> Box<ProducedDynamicFilter> {
        let WorkerToCoordinatorMsg::ProducedDynamicFilter(filter) = message else {
            panic!("expected produced dynamic filter");
        };
        filter
    }
}
