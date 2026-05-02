use crate::common::{require_one_child, serialize_uuid};
use crate::coordinator::metrics_store::MetricsStore;
use crate::coordinator::prepare_static_plan::prepare_static_plan;
use crate::distributed_planner::NetworkBoundaryExt;
use crate::worker::generated::worker::TaskKey;
use datafusion::common::internal_datafusion_err;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;

/// [ExecutionPlan] that executes the inner plan in distributed mode.
/// Before executing it, two modifications are lazily performed on the plan:
/// 1. Assigns worker URLs to all the stages. A random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    plan: Arc<dyn ExecutionPlan>,
    prepared_plan: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
    metrics: ExecutionPlanMetricsSet,
    pub(crate) task_metrics: Option<Arc<MetricsStore>>,
}

pub(super) struct PreparedPlan {
    pub(super) head_stage: Arc<dyn ExecutionPlan>,
    pub(super) join_set: JoinSet<Result<()>>,
}

impl DistributedExec {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan,
            prepared_plan: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
            task_metrics: None,
        }
    }

    /// Enables task metrics collection from remote workers.
    pub fn with_metrics_collection(mut self, enabled: bool) -> Self {
        self.task_metrics = match enabled {
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
        let Some(task_metrics) = &self.task_metrics else {
            return;
        };
        let _ = self.plan.apply(|plan| {
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
    pub(crate) fn prepared_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        self.prepared_plan
            .lock()
            .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {}", e))?
            .clone()
            .ok_or_else(|| {
                internal_datafusion_err!("No prepared plan found. Was execute() called?")
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
            task_metrics: self.task_metrics.clone(),
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

        let PreparedPlan {
            head_stage,
            join_set,
        } = prepare_static_plan(&self.plan, &self.metrics, &self.task_metrics, &context)?;
        {
            let mut guard = self
                .prepared_plan
                .lock()
                .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {e}"))?;
            *guard = Some(head_stage.clone());
        }
        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), 1);
        let tx = builder.tx();
        // Spawn the task that pulls data from child...
        builder.spawn(async move {
            let mut stream = head_stage.execute(partition, context)?;
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
