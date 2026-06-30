use crate::DistributedConfig;
use crate::common::{
    DistributedCancellationToken, on_drop_stream, require_one_child, serialize_uuid,
    task_ctx_with_extension,
};
use crate::coordinator::metrics_store::MetricsStore;
use crate::coordinator::prepare_static_plan::prepare_static_plan;
use crate::distributed_planner::NetworkBoundaryExt;
use crate::networking::get_distributed_worker_transport;
use crate::worker::generated::worker::TaskKey;
use datafusion::common::internal_datafusion_err;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

/// [ExecutionPlan] that executes the inner plan in distributed mode.
/// Before executing it, two modifications are lazily performed on the plan:
/// 1. Assigns worker URLs to all the stages. Unless explicitly set in
///    [crate::TaskEstimator::route_tasks], a random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    /// Initial [ExecutionPlan] present before execution.
    /// - If the plan was distributed statically, this will be the final distributed plan with all
    ///   the appropriate network boundaries in it.
    /// - If the plan is going to be distributed dynamically during execution, this is the initial
    ///   non-distributed plan.
    base_plan: Arc<dyn ExecutionPlan>,
    /// Resulting [ExecutionPlan] after execution ready for visualization purposes.
    /// - If the plan was distributed statically, this is equal to the base plan.
    /// - If the plan is going to be distributed dynamically during execution, this is the resulting
    ///   plan re-calculated based on runtime statistics.
    plan_for_viz: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
    /// The head stage meant to be executed locally on [DistributedExec::execute].
    head_stage: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
    /// DataFusion metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Storage where metrics collected from workers at runtime will place their results as they
    /// finish their respective remote tasks.
    pub(crate) metrics_store: Option<Arc<MetricsStore>>,
}

pub(super) struct PreparedPlan {
    /// The head stage meant to be executed locally by the coordinator.
    pub(super) head_stage: Arc<dyn ExecutionPlan>,
    /// A final representation of the plan for visualization purposes.
    pub(super) plan_for_viz: Arc<dyn ExecutionPlan>,
}

impl DistributedExec {
    pub fn new(base_plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            base_plan,
            plan_for_viz: Arc::new(Mutex::new(None)),
            head_stage: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
            metrics_store: None,
        }
    }

    /// Enables task metrics collection from remote workers.
    pub fn with_metrics_collection(mut self, enabled: bool) -> Self {
        self.metrics_store = match enabled {
            true => Some(Arc::new(MetricsStore::new())),
            false => None,
        };
        self
    }

    /// Exposes the metrics store so a transport that collects worker `TaskMetrics` out-of-band can
    /// file them where [`rewrite_distributed_plan_with_metrics`] reads them.
    ///
    /// [`rewrite_distributed_plan_with_metrics`]: crate::rewrite_distributed_plan_with_metrics
    pub fn metrics_store(&self) -> Option<Arc<MetricsStore>> {
        self.metrics_store.clone()
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
        let Some(task_metrics) = &self.metrics_store else {
            return;
        };
        let Some(plan) = self.plan_for_viz.lock().unwrap().as_ref().cloned() else {
            return;
        };
        let _ = plan.apply(|plan| {
            if let Some(boundary) = plan.as_network_boundary() {
                let stage = boundary.input_stage();
                for i in 0..stage.task_count() {
                    expected_keys.push(TaskKey {
                        query_id: serialize_uuid(&stage.query_id()),
                        stage_id: stage.num() as u64,
                        task_number: i as u64,
                    });
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if expected_keys.is_empty() {
            return;
        }
        let mut rx = task_metrics.rx.clone();
        let _ = rx
            .wait_for(|map| expected_keys.iter().all(|key| map.contains_key(key)))
            .await;
    }

    /// Returns the plan which is lazily prepared on `execute()` and actually gets executed.
    /// It is updated on every call to `execute()`. Returns an error if `.execute()` has not been
    /// called.
    pub(crate) fn plan_for_viz(&self) -> Result<Arc<dyn ExecutionPlan>> {
        self.plan_for_viz
            .lock()
            .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {}", e))?
            .clone()
            .ok_or_else(|| {
                internal_datafusion_err!("No prepared plan found. Was execute() called?")
            })
    }

    /// Returns the head stage that was actually executed. Unlike [`Self::plan_for_viz`] (which is
    /// reconstructed for visualization, with `Stage::Local` boundaries and rebuilt ancestor
    /// `Arc`s), this returns the original `Arc` instances whose metrics were populated during
    /// execution.
    pub(crate) fn head_stage(&self) -> Result<Arc<dyn ExecutionPlan>> {
        self.head_stage
            .lock()
            .map_err(|e| internal_datafusion_err!("Failed to lock head stage: {}", e))?
            .clone()
            .ok_or_else(|| internal_datafusion_err!("No head stage found. Was execute() called?"))
    }

    /// Routes and dispatches every stage through the registered transport, then returns the head
    /// stage for the caller to drive synchronously, with no `execute()` background task.
    ///
    /// This is the extension point for an in-process transport that owns the runtime and drives the
    /// head stage itself. The transport's dispatch must complete synchronously: a transport that
    /// spawns background delivery work, or a plan that declares work-unit feeds (which are pumped by
    /// that background work), is rejected rather than left to stall.
    pub fn prepare_in_process_plan(
        &self,
        ctx: &Arc<TaskContext>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let d_cfg = DistributedConfig::from_config_options(ctx.session_config().options())?;
        let registry = &d_cfg.__private_work_unit_feed_registry;
        let mut has_feeds = false;
        self.base_plan.apply(|plan| {
            if registry.get_work_unit_feed(plan).is_some() {
                has_feeds = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if has_feeds {
            return exec_err!(
                "the plan declares work-unit feeds, which are not delivered on the in-process path"
            );
        }

        let dispatcher = get_distributed_worker_transport(ctx.session_config()).dispatcher();
        let mut join_set = JoinSet::new();
        let result = prepare_static_plan(
            &self.base_plan,
            ctx,
            dispatcher.as_ref(),
            &mut join_set,
            &self.metrics,
            self.metrics_store.as_ref(),
        )?;
        // Dropping the join set aborts anything still on it, which would kill an async delivery
        // mid-flight and surface as a hang far from the cause. Reject instead.
        if !join_set.is_empty() {
            return exec_err!(
                "the registered transport spawned background delivery work; \
                 prepare_in_process_plan requires a transport whose dispatch completes synchronously"
            );
        }
        self.plan_for_viz
            .lock()
            .expect("poisoned lock")
            .replace(result.plan_for_viz);
        self.head_stage
            .lock()
            .expect("poisoned lock")
            .replace(Arc::clone(&result.head_stage));
        Ok(result.head_stage)
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

    fn properties(&self) -> &Arc<PlanProperties> {
        self.base_plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.base_plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedExec {
            base_plan: require_one_child(&children)?,
            plan_for_viz: Arc::new(Mutex::new(None)),
            head_stage: Arc::new(Mutex::new(None)),
            metrics: self.metrics.clone(),
            metrics_store: self.metrics_store.clone(),
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

        let base_plan = Arc::clone(&self.base_plan);
        let plan_for_viz = Arc::clone(&self.plan_for_viz);
        let head_stage = Arc::clone(&self.head_stage);
        let metrics = self.metrics.clone();
        let metrics_store = self.metrics_store.clone();

        // One token per execution. In-process transports watch it to tear down the moment this
        // output stream drops, whether the consumer read to the end or abandoned it early (a LIMIT
        // gather). It rides the dispatch context so every worker fragment shares it.
        let token = CancellationToken::new();
        let context = Arc::new(task_ctx_with_extension(
            &context,
            DistributedCancellationToken(token.clone()),
        ));

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), 1);
        let tx = builder.tx();

        builder.spawn(async move {
            let dispatcher =
                get_distributed_worker_transport(context.session_config()).dispatcher();
            // The query owns the join set: background plan-delivery and metrics tasks spawned by the
            // dispatcher run on it, so draining it below is what waits for the workers to finish.
            let mut join_set = JoinSet::new();

            let result = prepare_static_plan(
                &base_plan,
                &context,
                dispatcher.as_ref(),
                &mut join_set,
                &metrics,
                metrics_store.as_ref(),
            )?;

            plan_for_viz
                .lock()
                .expect("poisoned lock")
                .replace(result.plan_for_viz);
            head_stage
                .lock()
                .expect("poisoned lock")
                .replace(Arc::clone(&result.head_stage));
            let mut stream = result.head_stage.execute(partition, context)?;
            while let Some(msg) = stream.next().await {
                if tx.send(msg).await.is_err() {
                    break; // channel closed
                }
            }
            drop(tx);
            for res in join_set.join_all().await {
                res?;
            }
            // Dropping the dispatcher ends the coordinator->worker streams, propagating EOS so the
            // workers can clean up. It is held until here so those streams stay open during draining.
            drop(dispatcher);
            Ok(())
        });

        let stream = builder.build();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            on_drop_stream(stream, move || token.cancel()),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
