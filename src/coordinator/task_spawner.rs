use crate::common::{serialize_uuid, task_ctx_with_extension};
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::coordinator::MetricsStore;
use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::passthrough_headers::get_passthrough_headers;
use crate::protobuf::tonic_status_to_datafusion_error;
use crate::stage::LocalStage;
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::coordinator_to_worker_msg::Inner;
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::{
    DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, DistributedConfig, DistributedTaskContext,
    DistributedWorkUnitFeedContext, TaskKey, get_distributed_channel_resolver,
};
use datafusion::common::Result;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::JoinSet;
use datafusion::common::{DataFusionError, exec_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::{
    Count, ExecutionPlanMetricsSet, Label, MetricBuilder, MetricValue, Time,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use futures::future::BoxFuture;
use http::Extensions;
use prost::Message;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataMap;
use url::Url;
use uuid::Uuid;

/// Metrics that measure network details about communications between [DistributedExec] and a
/// worker.
#[derive(Clone)]
pub(super) struct CoordinatorToWorkerMetrics {
    pub(super) plan_bytes_sent: Count,
    pub(super) plan_send_latency: Arc<LatencyMetric>,
}

impl CoordinatorToWorkerMetrics {
    pub(super) fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            // Metric that measures to total sum of bytes worth of subplans sent.
            plan_bytes_sent: MetricBuilder::new(metrics)
                .with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0"))
                .global_counter("plan_bytes_sent"),
            // Latency statistics about the network calls issued to the workers for feeding subplans.
            plan_send_latency: Arc::new(LatencyMetric::new(
                "plan_send_latency",
                |b| b.with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0")),
                metrics,
            )),
        }
    }
}

/// Builder for the different kind of tasks that handle the communications between the
/// [DistributedExec] node to the workers. This struct is responsible for instantiating the tasks
/// as boxed futures so that [DistributedExec] can tokio-spawn them at will.
///
/// This struct is responsible for:
/// - Building tasks that communicate a serialized plan to multiple workers for further execution.
/// - Building tasks that stream partition feeds from local [WorkUnitFeedExec] nodes to their
///   remote counterparts.
pub(super) struct CoordinatorToWorkerTaskSpawner<'a> {
    plan: &'a Arc<dyn ExecutionPlan>,
    plan_proto: Vec<u8>,
    query_id: Uuid,
    stage_id: usize,
    task_count: usize,
    metrics: &'a CoordinatorToWorkerMetrics,
    task_metrics: &'a Option<Arc<MetricsStore>>,
    join_set: &'a mut JoinSet<Result<()>>,
}

impl<'a> CoordinatorToWorkerTaskSpawner<'a> {
    /// Builds a new [CoordinatorToWorkerTaskSpawner] based on the [Stage] that needs to be
    /// fanned out to multiple workers.
    pub(super) fn new(
        stage: &'a LocalStage,
        metrics: &'a CoordinatorToWorkerMetrics,
        task_metrics: &'a Option<Arc<MetricsStore>>,
        codec: &'a dyn PhysicalExtensionCodec,
        join_set: &'a mut JoinSet<Result<()>>,
    ) -> Result<Self> {
        let plan_proto = PhysicalPlanNode::try_from_physical_plan(Arc::clone(&stage.plan), codec)?
            .encode_to_vec();

        Ok(Self {
            plan: &stage.plan,
            plan_proto,
            query_id: stage.query_id,
            stage_id: stage.num,
            task_count: stage.tasks,
            metrics,
            task_metrics,
            join_set,
        })
    }

    /// Sends a serialized plan to a specific worker and sets up the bidirectional gRPC stream.
    /// Returns the sender for outbound coordinator-to-worker messages and the receiver for
    /// inbound worker-to-coordinator messages.
    pub(super) fn send_plan_task(
        &mut self,
        ctx: Arc<TaskContext>,
        task_i: usize,
        url: Url,
    ) -> Result<(
        UnboundedSender<pb::CoordinatorToWorkerMsg>,
        UnboundedReceiver<pb::WorkerToCoordinatorMsg>,
    )> {
        let d_cfg = DistributedConfig::from_config_options(ctx.session_config().options())?;
        /// Searches recursively for nodes exposing [crate::WorkUnitFeed]s, and executes their
        /// feeds, keeping into account that some of them might be executed within a
        /// [ChildrenIsolatorUnionExec] context. This means that some of them are irrelevant for
        /// the current [task_i], and we don't want to account for them here.
        ///
        /// It places in the `out` argument all the collected [WorkUnitFeedDeclaration]s necessary
        /// for sending the plan.
        fn gather_work_unit_feed_declarations(
            plan: &Arc<dyn ExecutionPlan>,
            ctx: DistributedTaskContext,
            d_cfg: &DistributedConfig,
            out: &mut Vec<WorkUnitFeedDeclaration>,
        ) {
            let wuf = if let Some(wuf) = d_cfg
                .__private_work_unit_feed_registry
                .get_work_unit_feed(plan)
            {
                wuf
            } else if let Some(ciu) = plan.as_any().downcast_ref::<ChildrenIsolatorUnionExec>() {
                for (child_i, ctx) in &ciu.task_idx_map[ctx.task_index] {
                    let child = &ciu.children[*child_i];
                    // Just recurse to children that will actually get executed by this
                    // ChildrenIsolatorUnionExec.
                    gather_work_unit_feed_declarations(child, ctx.clone(), d_cfg, out)
                }
                return;
            } else {
                for child in plan.children() {
                    gather_work_unit_feed_declarations(child, ctx.clone(), d_cfg, out)
                }
                return;
            };

            out.push(WorkUnitFeedDeclaration {
                id: serialize_uuid(&wuf.id()),
                partitions: plan.properties().partitioning.partition_count() as u64,
            })
        }

        let mut work_unit_feed_declarations = vec![];
        gather_work_unit_feed_declarations(
            self.plan,
            DistributedTaskContext {
                task_index: task_i,
                task_count: self.task_count,
            },
            d_cfg,
            &mut work_unit_feed_declarations,
        );

        let task_key = TaskKey {
            query_id: serialize_uuid(&self.query_id),
            stage_id: self.stage_id as u64,
            task_number: task_i as u64,
        };
        let msg = pb::CoordinatorToWorkerMsg {
            inner: Some(Inner::SetPlanRequest(pb::SetPlanRequest {
                plan_proto: self.plan_proto.clone(),
                task_count: self.task_count as u64,
                task_key: Some(task_key.clone()),
                work_unit_feed_declarations,
            })),
        };
        let plan_size = self.plan_proto.len();

        let (coordinator_to_worker_tx, coordinator_to_worker_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (worker_to_coordinator_tx, worker_to_coordinator_rx) =
            tokio::sync::mpsc::unbounded_channel();

        let channel_resolver = get_distributed_channel_resolver(ctx.as_ref());

        let mut headers = get_config_extension_propagation_headers(ctx.session_config())?;
        headers.extend(get_passthrough_headers(ctx.session_config()));

        let request = Request::from_parts(
            MetadataMap::from_headers(headers),
            Extensions::default(),
            futures::stream::once(async { msg })
                .chain(UnboundedReceiverStream::new(coordinator_to_worker_rx)),
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
            metrics.plan_bytes_sent.add(plan_size);
            let mut worker_to_coordinator_stream = response.into_inner();
            while let Some(msg_or_err) = worker_to_coordinator_stream.next().await {
                let msg = match msg_or_err {
                    Ok(msg) => msg,
                    Err(err) => {
                        return Err(tonic_status_to_datafusion_error(err).unwrap_or_else(|| {
                            exec_datafusion_err!("Unknown error on worker to coordinator stream")
                        }));
                    }
                };
                if worker_to_coordinator_tx.send(msg).is_err() {
                    break; // receiver dropped
                }
            }
            Ok::<_, DataFusionError>(())
        });

        Ok((coordinator_to_worker_tx, worker_to_coordinator_rx))
    }

    pub(super) fn metrics_collection_task(
        &mut self,
        task_i: usize,
        mut worker_to_coordinator_rx: UnboundedReceiver<pb::WorkerToCoordinatorMsg>,
    ) {
        let task_key = TaskKey {
            query_id: serialize_uuid(&self.query_id),
            stage_id: self.stage_id as u64,
            task_number: task_i as u64,
        };
        let task_metrics = self.task_metrics.clone();
        tokio::spawn(async move {
            while let Some(msg) = worker_to_coordinator_rx.recv().await {
                let Some(inner) = msg.inner else { continue };

                match inner {
                    pb::worker_to_coordinator_msg::Inner::TaskMetrics(pre_order_metrics) => {
                        if let Some(task_metrics) = &task_metrics {
                            task_metrics.insert(task_key.clone(), pre_order_metrics.metrics);
                        }
                    }
                }
            }
        });
    }

    /// Instantiates and returns the task that based on the different local [WorkUnitFeedExec]
    /// nodes, sends their inner [WorkUnitFeeds] over the network to their remote counterparts.
    /// The returned task is just a future that does nothing unless polled.
    ///
    /// Once this function is called, all the [WorkUnitFeedExec]s feeds will be consumed.
    pub(super) fn work_unit_feed_task(
        &mut self,
        ctx: Arc<TaskContext>,
        task_i: usize,
        tx: UnboundedSender<pb::CoordinatorToWorkerMsg>,
    ) -> Result<()> {
        let d_cfg = DistributedConfig::from_config_options(ctx.session_config().options())?;
        /// Recurses into the plan looking for [WorkUnitFeedExec] nodes that should be handled by
        /// the provided [task_i]. Because of [ChildrenIsolatorUnionExec]s being present in the
        /// plan, there might be some present [WorkUnitFeedExec] that will not necessarily get
        /// executed, so we don't want to stream any [WorkUnit] to those.
        ///
        /// It places in `out` the list of futures that should be polled for driving the [WorkUnit]
        /// network streams forward.
        fn gather_work_unit_feed_tasks(
            plan: &Arc<dyn ExecutionPlan>,
            dt_ctx: DistributedTaskContext,
            t_ctx: &Arc<TaskContext>,
            d_cfg: &DistributedConfig,
            tx: &UnboundedSender<pb::CoordinatorToWorkerMsg>,
            out: &mut Vec<BoxFuture<'static, Result<()>>>,
        ) -> Result<()> {
            let wuf = if let Some(wuf) = d_cfg
                .__private_work_unit_feed_registry
                .get_work_unit_feed(plan)
            {
                wuf
            } else if let Some(ciu) = plan.as_any().downcast_ref::<ChildrenIsolatorUnionExec>() {
                for (child_i, dt_ctx) in &ciu.task_idx_map[dt_ctx.task_index] {
                    // Just recurse to children that will actually get executed by this
                    // ChildrenIsolatorUnionExec.
                    let child = &ciu.children[*child_i];
                    gather_work_unit_feed_tasks(child, dt_ctx.clone(), t_ctx, d_cfg, tx, out)?;
                }
                return Ok(());
            } else {
                for child in plan.children() {
                    gather_work_unit_feed_tasks(child, dt_ctx.clone(), t_ctx, d_cfg, tx, out)?
                }
                return Ok(());
            };

            let partitions = plan.properties().partitioning.partition_count();
            let start_partition = partitions * dt_ctx.task_index;
            let end_partition = start_partition + partitions;

            let dist_feed_ctx = DistributedWorkUnitFeedContext {
                fan_out_tasks: dt_ctx.task_count,
            };
            let t_ctx = Arc::new(task_ctx_with_extension(t_ctx, dist_feed_ctx));

            // There should be as many partition feeds as [num partitions] * [num tasks], so that
            // each task index handles a non-overlapping set of partition feeds.
            for (partition, feed_idx) in (start_partition..end_partition).enumerate() {
                // By calling `.take()` the respective partition feed is consumed, and further
                // consumptions are allowed. Calling `.take()` on the same partition feed again
                // will fail.
                let mut work_unit_feed = wuf.feed(feed_idx, Arc::clone(&t_ctx))?;
                let tx = tx.clone();
                let id = wuf.id();
                out.push(Box::pin(async move {
                    // At this point, the partition feed contains a stream of decoded messages,
                    // so they must be encoded in order to send them over the wire.
                    while let Some(data_or_err) = work_unit_feed.next().await {
                        if tx
                            .send(pb::CoordinatorToWorkerMsg {
                                inner: Some(Inner::WorkUnit(pb::WorkUnit {
                                    id: serialize_uuid(&id),
                                    partition: partition as u64,
                                    body: data_or_err?.encode_to_bytes(),
                                })),
                            })
                            .is_err()
                        {
                            break; // channel closed.
                        };
                    }
                    Ok::<_, DataFusionError>(())
                }));
            }
            Ok(())
        }

        let mut futures = vec![];
        gather_work_unit_feed_tasks(
            self.plan,
            DistributedTaskContext {
                task_index: task_i,
                task_count: self.task_count,
            },
            &ctx,
            d_cfg,
            &tx,
            &mut futures,
        )?;
        self.join_set.spawn(async move {
            futures::future::try_join_all(futures).await?;
            Ok(())
        });
        Ok(())
    }
}

/// DataFusion metrics system is pretty limited from an API standpoint. This intermediate struct
/// bridges the gaps that are not satisfied by upstream API for measuring latency.
pub(super) struct LatencyMetric {
    max: Time,
    avg: Time,
    max_latency_micros: AtomicU64,
    sum_latency_micros: AtomicU64,
    count_latency_micros: AtomicU64,
}

impl Drop for LatencyMetric {
    fn drop(&mut self) {
        self.max.add_duration(Duration::from_micros(
            self.max_latency_micros.load(Ordering::Relaxed),
        ));
        self.avg.add_duration(Duration::from_micros(
            self.sum_latency_micros.load(Ordering::Relaxed)
                / self.count_latency_micros.load(Ordering::Relaxed).max(1),
        ));
    }
}

impl LatencyMetric {
    pub(super) fn new(
        name: impl Display,
        builder: impl Fn(MetricBuilder) -> MetricBuilder,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let max = Time::new();
        builder(MetricBuilder::new(metrics)).build(MetricValue::Time {
            name: format!("{name}_max").into(),
            time: max.clone(),
        });
        let avg = Time::new();
        builder(MetricBuilder::new(metrics)).build(MetricValue::Time {
            name: format!("{name}_avg").into(),
            time: avg.clone(),
        });
        Self {
            max,
            avg,
            max_latency_micros: AtomicU64::new(0),
            sum_latency_micros: AtomicU64::new(0),
            count_latency_micros: AtomicU64::new(0),
        }
    }

    fn record(&self, start: &Instant) {
        let micros = start.elapsed().as_micros() as u64;
        self.max_latency_micros.fetch_max(micros, Ordering::Relaxed);
        self.sum_latency_micros.fetch_add(micros, Ordering::Relaxed);
        self.count_latency_micros.fetch_add(1, Ordering::Relaxed);
    }
}
