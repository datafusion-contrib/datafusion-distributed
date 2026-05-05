use crate::common::{require_one_child, serialize_uuid, task_ctx_with_extension};
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::distributed_planner::{NetworkBoundaryExt, get_distributed_task_estimator};
use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::networking::get_distributed_worker_resolver;
use crate::passthrough_headers::get_passthrough_headers;
use crate::protobuf::{DistributedCodec, tonic_status_to_datafusion_error};
use crate::stage::{ExecutionTask, Stage};
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::worker::generated::worker::{
    CoordinatorToWorkerMsg, SetPlanRequest, TaskKey, WorkUnit, WorkerToCoordinatorMsg,
    coordinator_to_worker_msg::Inner, worker_to_coordinator_msg,
};
use crate::{
    DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, DistributedConfig, DistributedTaskContext,
    DistributedWorkUnitFeedContext, TaskRoutingContext, WorkerResolver,
    get_distributed_channel_resolver,
};
use datafusion::common::HashMap;
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
use tokio::sync::watch;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataMap;
use url::Url;
use uuid::Uuid;

/// Stores the metrics collected from all worker tasks, and notifies waiters when new entries arrive.
#[derive(Debug, Clone)]
pub struct MetricsStore {
    tx: watch::Sender<HashMap<TaskKey, Vec<pb::MetricsSet>>>,
    rx: watch::Receiver<HashMap<TaskKey, Vec<pb::MetricsSet>>>,
}

impl MetricsStore {
    fn new() -> Self {
        let (tx, rx) = watch::channel(HashMap::new());
        Self { tx, rx }
    }

    pub fn insert(&self, key: TaskKey, metrics: Vec<pb::MetricsSet>) {
        self.tx.send_modify(|map| {
            map.insert(key, metrics);
        });
    }

    pub fn get(&self, key: &TaskKey) -> Option<Vec<pb::MetricsSet>> {
        self.rx.borrow().get(key).cloned()
    }

    #[cfg(test)]
    pub(crate) fn from_entries(
        entries: impl IntoIterator<Item = (TaskKey, Vec<pb::MetricsSet>)>,
    ) -> Self {
        let map: HashMap<_, _> = entries.into_iter().collect();
        let (tx, rx) = watch::channel(map);
        Self { tx, rx }
    }
}

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
    pub task_metrics: Arc<MetricsStore>,
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
            task_metrics: Arc::new(MetricsStore::new()),
        }
    }

    /// Waits until all worker tasks have reported their metrics back via the coordinator channel.
    ///
    /// Metrics are delivered asynchronously after query execution completes, so callers that need
    /// complete metrics (e.g. for observability or display) should await this before inspecting
    /// [`Self::task_metrics`] or calling [`rewrite_distributed_plan_with_metrics`].
    ///
    /// [`rewrite_distributed_plan_with_metrics`]: crate::rewrite_distributed_plan_with_metrics
    pub async fn wait_for_metrics(&self) {
        let mut expected_keys: Vec<TaskKey> = Vec::new();
        let _ = self.plan.apply(|plan| {
            if let Some(boundary) = plan.as_network_boundary() {
                let stage = boundary.input_stage();
                for i in 0..stage.tasks.len() {
                    expected_keys.push(TaskKey {
                        query_id: stage.query_id.as_bytes().to_vec(),
                        stage_id: stage.num as u64,
                        task_number: i as u64,
                    });
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if expected_keys.is_empty() {
            return;
        }
        let mut rx = self.task_metrics.rx.clone();
        let _ = rx
            .wait_for(|map| expected_keys.iter().all(|key| map.contains_key(key)))
            .await;
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

    /// Prepares the distributed plan for execution.
    /// In particular, this means we must take the following steps at each network boundary node:
    /// 1. Assign tasks to URLs. Follow the user-specified routing defined in the TaskEstimator
    ///    implementation, or default to random round-robin assignment.
    /// 2. Send the sliced subplans to the assigned URLs. For each task assigned to a URL, it is here
    ///    that we now must actually send that subplan to the URL over the wire.
    /// 3. Set network boundary input plans to `None`. This way, network boundaries become nodes
    ///    without children, so we stop further traversal from happening in the future.
    /// 4. Spawn a background task per worker that waits for the worker to finish and collects
    ///    its metrics into [DistributedExec::task_metrics] via the coordinator channel.
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
            // The following logic is only relevant to network boundaries.
            let Some(plan) = plan.as_network_boundary() else {
                return Ok(Transformed::no(plan));
            };

            let task_estimator = get_distributed_task_estimator(ctx.session_config())?;

            let stage = plan.input_stage();

            let mut spawner =
                CoordinatorToWorkerTaskSpawner::new(stage, &metrics, &codec, &mut join_set)?;

            // Default routing assigns tasks to URLs round-robin from a random starting point.
            let start_idx = rand::rng().random_range(0..urls.len());

            // If the user has not defined custom routing through the route_task API, we
            // default to round-robin task assignation with a randomized starting point.
            //
            // If the user has defined custom routing through the route_task interface,
            // we attempt to route the task to the specified URL.
            let routed_urls = match task_estimator.route_tasks(&TaskRoutingContext {
                task_ctx: Arc::clone(ctx),
                plan: stage.plan.as_ref().ok_or_else(|| {
                    internal_datafusion_err!(
                        "network boundary stage missing input plan during routing"
                    )
                })?,
                tasks: &stage.tasks,
                urls: &urls,
            }) {
                Ok(Some(routed_urls)) => routed_urls,
                Ok(None) => (0..stage.tasks.len())
                    .map(|i| urls[(start_idx + i) % urls.len()].clone())
                    .collect(),
                Err(e) => return Err(exec_datafusion_err!("error routing tasks to workers: {e}")),
            };

            if routed_urls.len() != stage.tasks.len() {
                return Err(exec_datafusion_err!(
                    "number of tasks ({}) was not equal to number of urls ({}) at execution time",
                    routed_urls.len(),
                    stage.tasks.len()
                ));
            }

            let mut tasks = Vec::with_capacity(stage.tasks.len());
            for (i, routed_url) in routed_urls.into_iter().enumerate() {
                tasks.push(ExecutionTask {
                    url: Some(routed_url.clone()),
                });
                // Spawn a task that sends the subplan to the chosen URL.
                // There will be as many spawned tasks as workers.
                let (tx, worker_rx) = spawner.send_plan_task(Arc::clone(ctx), i, routed_url)?;
                spawner.metrics_collection_task(i, worker_rx, Arc::clone(&self.task_metrics));
                spawner.work_unit_feed_task(Arc::clone(ctx), i, tx)?;
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
            task_metrics: Arc::clone(&self.task_metrics),
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
type WorkerResponseRx =
    tokio::sync::mpsc::UnboundedReceiver<Result<WorkerToCoordinatorMsg, tonic::Status>>;

struct CoordinatorToWorkerTaskSpawner<'a> {
    plan: &'a Arc<dyn ExecutionPlan>,
    plan_proto: Vec<u8>,
    query_id: Uuid,
    stage_id: usize,
    task_count: usize,
    metrics: &'a CoordinatorToWorkerMetrics,
    join_set: &'a mut JoinSet<Result<()>>,
}

impl<'a> CoordinatorToWorkerTaskSpawner<'a> {
    /// Builds a new [CoordinatorToWorkerTaskSpawner] based on the [Stage] that needs to be
    /// fanned out to multiple workers.
    fn new(
        stage: &'a Stage,
        metrics: &'a CoordinatorToWorkerMetrics,
        codec: &'a dyn PhysicalExtensionCodec,
        join_set: &'a mut JoinSet<Result<()>>,
    ) -> Result<Self> {
        let Some(plan) = &stage.plan else {
            return internal_err!("Plan is not set for stage {}", stage.num);
        };

        let plan_proto =
            PhysicalPlanNode::try_from_physical_plan(Arc::clone(plan), codec)?.encode_to_vec();

        Ok(Self {
            plan,
            plan_proto,
            query_id: stage.query_id,
            stage_id: stage.num,
            task_count: stage.tasks.len(),
            metrics,
            join_set,
        })
    }

    /// Sends a serialized plan to a specific worker and sets up the bidirectional gRPC stream.
    /// Returns the sender for outbound coordinator-to-worker messages and the receiver for
    /// inbound worker-to-coordinator messages.
    fn send_plan_task(
        &mut self,
        ctx: Arc<TaskContext>,
        task_i: usize,
        url: Url,
    ) -> Result<(UnboundedSender<CoordinatorToWorkerMsg>, WorkerResponseRx)> {
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
        let msg = CoordinatorToWorkerMsg {
            inner: Some(Inner::SetPlanRequest(SetPlanRequest {
                plan_proto: self.plan_proto.clone(),
                task_count: self.task_count as u64,
                task_key: Some(task_key.clone()),
                work_unit_feed_declarations,
                target_worker_url: url.to_string(),
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
            let mut stream = response.into_inner();
            while let Some(msg) = stream.next().await {
                if worker_to_coordinator_tx.send(msg).is_err() {
                    break; // receiver dropped
                }
            }
            Ok::<_, DataFusionError>(())
        });

        Ok((coordinator_to_worker_tx, worker_to_coordinator_rx))
    }

    /// Receives worker-to-coordinator messages and inserts any collected metrics into the store.
    /// Runs in a detached spawn so it is not cancelled when the output stream is dropped early.
    fn metrics_collection_task(
        &mut self,
        task_i: usize,
        mut worker_to_coordinator_rx: WorkerResponseRx,
        task_metrics_collection: Arc<MetricsStore>,
    ) {
        let task_key = TaskKey {
            query_id: serialize_uuid(&self.query_id),
            stage_id: self.stage_id as u64,
            task_number: task_i as u64,
        };
        tokio::spawn(async move {
            while let Some(Ok(msg)) = worker_to_coordinator_rx.recv().await {
                let Some(worker_to_coordinator_msg::Inner::TaskMetrics(pre_order_metrics)) =
                    msg.inner
                else {
                    continue;
                };
                task_metrics_collection.insert(task_key.clone(), pre_order_metrics.metrics);
            }
        });
    }

    /// Instantiates and returns the task that based on the different local [WorkUnitFeedExec]
    /// nodes, sends their inner [WorkUnitFeeds] over the network to their remote counterparts.
    /// The returned task is just a future that does nothing unless polled.
    ///
    /// Once this function is called, all the [WorkUnitFeedExec]s feeds will be consumed.
    fn work_unit_feed_task(
        &mut self,
        ctx: Arc<TaskContext>,
        task_i: usize,
        tx: UnboundedSender<CoordinatorToWorkerMsg>,
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
            tx: &UnboundedSender<CoordinatorToWorkerMsg>,
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
