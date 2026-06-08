use crate::DistributedTaskContext;
use datafusion::common::{Result, Statistics, exec_err, not_impl_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::fmt::Formatter;
use std::sync::Arc;

/// Represents a leaf node ready to be distributed across N tasks, where the variant of the node
/// belonging to each task is stored in a `Vec` of N positions.
///
/// While sending this plan over the wire to a remote worker, only the appropriate variant is sent.
///
/// This [ExecutionPlan] implementation is typically returned by
/// [crate::TaskEstimator::scale_up_leaf_node], which will be called for scaling up a node for
/// distribution. The process typically looks like this:
///
/// 1. The distributed planner calls [crate::TaskEstimator::scale_up_leaf_node] providing a leaf
///    node and the amount of tasks in which it should be distributed:
///
/// ```text
/// ┌──────────────┐
/// │DataSourceExec│ + 3 tasks
/// └──────────────┘
/// ```
///
/// 2. The [crate::TaskEstimator] implementation, either user provided or a default one, returns
///    a [DistributedLeafExec] adhering to this task count:
///
/// ```text
/// ┌────────────────────────────────────────────────┐
/// │              DistributedLeafExec               │
/// │                                                │
/// │┌──────────────┐┌──────────────┐┌──────────────┐│
/// ││DataSourceExec││DataSourceExec││DataSourceExec││
/// ││  for task 0  ││  for task 1  ││  for task 2  ││
/// │└──────────────┘└──────────────┘└──────────────┘│
/// └────────────────────────────────────────────────┘
/// ```
///
/// 3. The [crate::DistributedExec] node, upon being executed, will send the different variants of
///    the leaf node to the respective workers, instead of sending the full [DistributedLeafExec]:
///
/// ```text
/// ┌──────────────────┐┌──────────────────┐┌──────────────────┐
/// │     Worker 0     ││     Worker 1     ││     Worker 2     │
/// │                  ││                  ││                  │
/// │       ...        ││       ...        ││       ...        │
/// │                  ││                  ││                  │
/// │ ┌──────────────┐ ││ ┌──────────────┐ ││ ┌──────────────┐ │
/// │ │   SomeExec   │ ││ │   SomeExec   │ ││ │   SomeExec   │ │
/// │ │              │ ││ │              │ ││ │              │ │
/// │ └──────────────┘ ││ └──────────────┘ ││ └──────────────┘ │
/// │ ┌──────────────┐ ││ ┌──────────────┐ ││ ┌──────────────┐ │
/// │ │DataSourceExec│ ││ │DataSourceExec│ ││ │DataSourceExec│ │
/// │ │  for task 0  │ ││ │  for task 1  │ ││ │  for task 2  │ │
/// │ └──────────────┘ ││ └──────────────┘ ││ └──────────────┘ │
/// └──────────────────┘└──────────────────┘└──────────────────┘
/// ```
///
/// This way, the different workers get to execute different versions of the same plan, each
/// handling its own range of non-overlapping data.
#[derive(Debug)]
pub struct DistributedLeafExec {
    pub(crate) original: Arc<dyn ExecutionPlan>,
    pub(crate) variants: Vec<Arc<dyn ExecutionPlan>>,
}

impl DistributedLeafExec {
    /// Builds a new [DistributedLeafExec] based on the provided original plan and its per-task
    /// variants. Provided variants must expose the same partition count as the original plan.
    pub fn new(
        original: Arc<dyn ExecutionPlan>,
        variants: impl IntoIterator<Item = Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            original,
            variants: variants.into_iter().collect(),
        }
    }

    /// Returns the variant belonging to provided task index.
    pub(crate) fn to_task_specialized(&self, task_i: usize) -> Arc<dyn ExecutionPlan> {
        Arc::clone(&self.variants[task_i])
    }
}

impl DisplayAs for DistributedLeafExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedLeafExec: ")?;
        self.original.fmt_as(t, f)
    }
}

impl ExecutionPlan for DistributedLeafExec {
    fn name(&self) -> &str {
        // Delegate to the original so that metrics lookups (which compare plan.name() against
        // T::static_name()) work transparently with the wrapped plan type. For example,
        // node_metrics::<DataSourceExec> finds DistributedLeafExec nodes in the distributed
        // plan while also finding DataSourceExec nodes in the single-node plan.
        self.original.name()
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        datafusion::catalog::memory::DataSourceExec::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.original.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("DistributedLeafExec does not accept children")
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let d_ctx = DistributedTaskContext::from_ctx(&context);
        if d_ctx.task_count == 1 {
            return self.original.execute(partition, context);
        }

        let Some(plan) = self.variants.get(d_ctx.task_index) else {
            return exec_err!(
                "Task index {} out of range for a per_task vector of length {}",
                d_ctx.task_index,
                self.variants.len()
            );
        };

        plan.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.original.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.original.partition_statistics(partition)
    }
}
