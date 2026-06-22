use crate::common::{require_one_child, serialize_uuid};
use crate::coordinator::metrics_store::MetricsStore;
use crate::coordinator::prepare_static_plan::prepare_static_plan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::distributed_planner::NetworkBoundaryExt;
use crate::worker::generated::worker::TaskKey;
use datafusion::common::internal_datafusion_err;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};

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

        let query_coordinator = QueryCoordinator::new(
            Arc::clone(&context),
            &self.metrics,
            self.metrics_store.clone(),
        );

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), 1);
        let tx = builder.tx();

        builder.spawn(async move {
            let guard = query_coordinator.end_query_guard();

            let result = prepare_static_plan(&query_coordinator, &base_plan)?;

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
            drop(stream);
            drop(guard);
            drop(tx);
            query_coordinator.drain_pending_tasks().await?;
            Ok(())
        });

        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
