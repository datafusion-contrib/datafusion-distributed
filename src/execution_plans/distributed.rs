use crate::common::{require_one_child, serialize_uuid};
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::distributed_planner::NetworkBoundaryExt;
use crate::networking::get_distributed_worker_resolver;
use crate::passthrough_headers::get_passthrough_headers;
use crate::protobuf::{DistributedCodec, tonic_status_to_datafusion_error};
use crate::stage::{ExecutionTask, Stage};
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::worker::generated::worker::{
    CoordinatorToWorkerMsg, SetPlanRequest, TaskKey, WorkUnit, coordinator_to_worker_msg::Inner,
};
use crate::{
    DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, WorkUnitFeedExec, WorkerResolver,
    get_distributed_channel_resolver,
};
use datafusion::common::instant::Instant;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err, internal_err};
use datafusion::common::{exec_datafusion_err, internal_datafusion_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Label, MetricBuilder, MetricValue, Time,
};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use futures::future::BoxFuture;
use http::Extensions;
use prost::Message;
use rand::Rng;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataMap;
use url::Url;
use uuid::Uuid;

/// [ExecutionPlan] that executes the inner plan in distributed mode.
/// Before executing it, two modifications are lazily performed on the plan:
/// 1. Assigns worker URLs to all the stages. A random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    pub plan: Arc<dyn ExecutionPlan>,
    pub prepared_plan: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
    metrics: ExecutionPlanMetricsSet,
}

struct PreparedPlan {
    plan: Arc<dyn ExecutionPlan>,
    join_set: JoinSet<Result<()>>,
}

impl DistributedExec {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan,
            prepared_plan: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Returns the plan which is lazily prepared on execute() and actually gets executed.
    /// It is updated on every call to execute(). Returns an error if .execute() has not been called.
    pub(crate) fn prepared_plan(&self) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.prepared_plan
            .lock()
            .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {}", e))?
            .clone()
            .ok_or_else(|| {
                internal_datafusion_err!("No prepared plan found. Was execute() called?")
            })
    }

    /// Prepares the distributed plan for execution, which implies:
    /// 1. Perform some worker assignation, choosing randomly from the given URLs and assigning one
    ///    URL per task.
    /// 2. Sending the sliced subplans to the assigned URLs. For each URL assigned to a task, a
    ///    network call feeding the subplan is necessary.
    /// 3. In each network boundary, set the input plan to `None`. That way, network boundaries
    ///    become nodes without children and traversing them will not go further down in.
    fn prepare_plan(&self, ctx: &Arc<TaskContext>) -> Result<PreparedPlan> {
        let worker_resolver = get_distributed_worker_resolver(ctx.session_config())?;
        let codec = DistributedCodec::new_combined_with_user(ctx.session_config());

        let urls = worker_resolver.get_urls()?;

        let metrics = CoordinatorToWorkerMetrics {
            // Metric that measures to total sum of bytes worth of subplans sent.
            plan_bytes_sent: MetricBuilder::new(&self.metrics)
                .with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0"))
                .global_counter("plan_bytes_sent"),
            // Latency statistics about the network calls issued to the workers for feeding subplans.
            plan_send_latency: Arc::new(LatencyMetric::new(
                "plan_send_latency",
                |b| b.with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0")),
                &self.metrics,
            )),
        };

        let mut join_set = JoinSet::new();
        let prepared = Arc::clone(&self.plan).transform_up(|plan| {
            // The following logic is just applied on network boundaries.
            let Some(plan) = plan.as_network_boundary() else {
                return Ok(Transformed::no(plan));
            };

            let stage = plan.input_stage();

            let send_task_builder =
                CoordinatorToWorkerTaskBuilder::new(stage, metrics.clone(), &codec)?;

            // Right now, we assign random workers to tasks. This might change in the future.
            let start_idx = rand::rng().random_range(0..urls.len());

            let mut tasks = Vec::with_capacity(stage.tasks.len());
            for i in 0..stage.tasks.len() {
                let url = urls[(start_idx + i) % urls.len()].clone();
                tasks.push(ExecutionTask {
                    url: Some(url.clone()),
                });
                let (send_plan_task, tx) =
                    send_task_builder.send_plan_task(Arc::clone(ctx), i, url)?;
                let work_unit_feed_task =
                    send_task_builder.work_unit_feed_task(Arc::clone(ctx), i, tx)?;
                // Spawns the task that feeds this subplan to this worker. There will be as
                // many as this spawned tasks as workers.
                join_set.spawn(send_plan_task);
                join_set.spawn(work_unit_feed_task);
            }

            Ok(Transformed::yes(plan.with_input_stage(Stage {
                query_id: stage.query_id,
                num: stage.num,
                plan: None,
                tasks,
            })?))
        })?;
        Ok(PreparedPlan {
            plan: prepared.data,
            join_set,
        })
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedExec")
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        "DistributedExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedExec {
            plan: require_one_child(&children)?,
            prepared_plan: self.prepared_plan.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition > 0 {
            // The DistributedExec node calls try_assign_urls() lazily upon calling .execute(). This means
            // that .execute() must only be called once, as we cannot afford to perform several
            // random URL assignation while calling multiple partitions, as they will differ,
            // producing an invalid plan
            return exec_err!(
                "DistributedExec must only have 1 partition, but it was called with partition index {partition}"
            );
        }

        let PreparedPlan { plan, join_set } = self.prepare_plan(&context)?;
        {
            let mut guard = self
                .prepared_plan
                .lock()
                .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {e}"))?;
            *guard = Some(plan.clone());
        }
        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), 1);
        let tx = builder.tx();
        // Spawn the task that pulls data from child...
        builder.spawn(async move {
            let mut stream = plan.execute(partition, context)?;
            while let Some(msg) = stream.next().await {
                if tx.send(msg).await.is_err() {
                    break; // channel closed
                }
            }
            Ok(())
        });
        // ...in parallel to the one that feeds the plan to workers.
        builder.spawn(async move {
            for res in join_set.join_all().await {
                res?;
            }
            Ok(())
        });
        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Metrics that measure network details about communications between [DistributedExec] and a
/// worker.
#[derive(Clone)]
struct CoordinatorToWorkerMetrics {
    plan_bytes_sent: Count,
    plan_send_latency: Arc<LatencyMetric>,
}

/// Builder for the different kind of tasks that handle the communications between the
/// [DistributedExec] node to the workers. This struct is responsible for instantiating the tasks
/// as boxed futures so that [DistributedExec] can tokio-spawn them at will.
///
/// This struct is responsible for:
/// - Building tasks that communicate a serialized plan to multiple workers for further execution.
/// - Building tasks that stream partition feeds from local [WorkUnitFeedExec] nodes to their
///   remote counterparts.
struct CoordinatorToWorkerTaskBuilder<'a> {
    work_unit_feed_execs: Vec<&'a WorkUnitFeedExec>,
    plan_proto: Vec<u8>,
    query_id: Uuid,
    stage_id: usize,
    task_count: usize,
    metrics: CoordinatorToWorkerMetrics,
}

impl<'a> CoordinatorToWorkerTaskBuilder<'a> {
    /// Builds a new [CoordinatorToWorkerTaskBuilder] based on the [Stage] that needs to be
    /// fanned out to multiple workers.
    fn new(
        stage: &'a Stage,
        metrics: CoordinatorToWorkerMetrics,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Self> {
        let Some(plan) = &stage.plan else {
            return internal_err!("Plan is not set for stage {}", stage.num);
        };

        let plan_proto =
            PhysicalPlanNode::try_from_physical_plan(Arc::clone(plan), codec)?.encode_to_vec();

        let mut work_unit_feed_execs = vec![];

        plan.apply(|plan| {
            if let Some(pf_exec) = plan.as_any().downcast_ref::<WorkUnitFeedExec>() {
                work_unit_feed_execs.push(pf_exec);
            };
            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(Self {
            plan_proto,
            work_unit_feed_execs,
            query_id: stage.query_id,
            stage_id: stage.num,
            task_count: stage.tasks.len(),
            metrics,
        })
    }

    /// Instantiates and returns the task sends a serialized plan to specific worker. The returned
    /// task is just a future that does nothing unless polled.
    fn send_plan_task(
        &self,
        ctx: Arc<TaskContext>,
        task_i: usize,
        url: Url,
    ) -> Result<(
        BoxFuture<'static, Result<()>>,
        UnboundedSender<CoordinatorToWorkerMsg>,
    )> {
        let channel_resolver = get_distributed_channel_resolver(ctx.as_ref());

        let mut headers = get_config_extension_propagation_headers(ctx.session_config())?;
        headers.extend(get_passthrough_headers(ctx.session_config()));

        let msg = CoordinatorToWorkerMsg {
            inner: Some(Inner::SetPlanRequest(SetPlanRequest {
                plan_proto: self.plan_proto.clone(),
                task_count: self.task_count as u64,
                task_key: Some(TaskKey {
                    query_id: serialize_uuid(&self.query_id),
                    stage_id: self.stage_id as u64,
                    task_number: task_i as u64,
                }),
                work_unit_feed_declarations: self
                    .work_unit_feed_execs
                    .iter()
                    .map(|node| WorkUnitFeedDeclaration {
                        id: serialize_uuid(&node.id),
                        partitions: node.properties().partitioning.partition_count() as u64,
                    })
                    .collect(),
            })),
        };
        let plan_size = self.plan_proto.len();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let request = Request::from_parts(
            MetadataMap::from_headers(headers),
            Extensions::default(),
            futures::stream::once(async { msg }).chain(UnboundedReceiverStream::new(rx)),
        );

        let metrics = self.metrics.clone();
        let send_plan_task = async move {
            let start = Instant::now();
            let mut client = channel_resolver.get_worker_client_for_url(&url).await?;
            client.coordinator_channel(request).await.map_err(|e| {
                tonic_status_to_datafusion_error(&e).unwrap_or_else(|| {
                    exec_datafusion_err!("Error sending plan to worker {url}: {e}")
                })
            })?;
            metrics.plan_send_latency.record(&start);
            metrics.plan_bytes_sent.add(plan_size);
            Ok::<_, DataFusionError>(())
        };

        Ok((Box::pin(send_plan_task), tx))
    }

    /// Instantiates and returns the task that based on the different local [WorkUnitFeedExec]
    /// nodes, sends their inner [WorkUnitFeeds] over the network to their remote counterparts.
    /// The returned task is just a future that does nothing unless polled.
    ///
    /// Once this function is called, all the [WorkUnitFeedExec]s feeds will be consumed.
    fn work_unit_feed_task(
        &self,
        ctx: Arc<TaskContext>,
        task_i: usize,
        tx: UnboundedSender<CoordinatorToWorkerMsg>,
    ) -> Result<BoxFuture<'static, Result<()>>> {
        let mut futures = vec![];

        // The subplan belong to the stage this [CoordinatorToWorkerTaskBuilder] is handling
        // might contain multiple [WorkUnitFeedExec]s, and all must be streamed from here to
        // workers.
        for work_unit_feed_exec in &self.work_unit_feed_execs {
            let partitions_per_task = work_unit_feed_exec
                .properties()
                .partitioning
                .partition_count();
            let start_partition = task_i * partitions_per_task;
            let end_partition = start_partition + partitions_per_task;
            // There should be as many partition feeds as [num partitions] * [num tasks], so that
            // each task index handles a non-overlapping set of partition feeds.
            for (partition, feed_idx) in (start_partition..end_partition).enumerate() {
                // By calling `.take()` the respective partition feed is consumed, and further
                // consumptions are allowed. Calling `.take()` on the same partition feed again
                // will fail.
                let mut work_unit_feed = work_unit_feed_exec
                    .provider
                    .feed(feed_idx, Arc::clone(&ctx))?;
                let tx = tx.clone();
                let id = work_unit_feed_exec.id;
                futures.push(Box::pin(async move {
                    // At this point, the partition feed contains a stream of decoded messages,
                    // so they must be encoded in order to send them over the wire.
                    while let Some(data_or_err) = work_unit_feed.next().await {
                        if tx
                            .send(CoordinatorToWorkerMsg {
                                inner: Some(Inner::WorkUnit(WorkUnit {
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
        }

        Ok(Box::pin(async move {
            futures::future::try_join_all(futures).await?;
            Ok(())
        }))
    }
}

/// DataFusion metrics system is pretty limited from an API standpoint. This intermediate struct
/// bridges the gaps that are not satisfied by upstream API for measuring latency.
struct LatencyMetric {
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
    fn new(
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
