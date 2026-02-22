use crate::common::require_one_child;
use datafusion::common::{Result, not_impl_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

/// [ExecutionPlan] implementation that acts as a simple placeholder for the distributed planner
/// to know when a `NetworkBoundary` should be injected.
///
/// This structure is part of the public API, and it's what allows users of this library to decide
/// where should the network boundaries be placed, and what task count should be used for the
/// stage below.
///
/// Note that there are restrictions around where can the [NetworkBoundaryPlaceholder]s be placed,
/// for example:
/// - A [NetworkBoundaryKind::Broadcast] needs to be placed right above a `BroadcastExec` node.
/// - A [NetworkBoundaryKind::Shuffle] needs to be placed right about a `RepartitionExec` node.
///
/// Failure to do so will result in planning errors.
#[derive(Debug)]
pub struct NetworkBoundaryPlaceholder {
    /// The kind of network boundary that should be injected.
    pub kind: NetworkBoundaryKind,
    /// The task count for the input stage of this network boundary.
    ///
    /// Note that the task count for this network boundary is decided by the other network boundary
    /// immediately above, and not this one.
    pub input_task_count: usize,
    /// The input [ExecutionPlan] that will run remotely on the stage below.
    pub input: Arc<dyn ExecutionPlan>,
}

/// The type of network boundary that should be injected:
/// - [NetworkBoundaryKind::Shuffle] -> `NetworkShuffleExec`
/// - [NetworkBoundaryKind::Coalesce] -> `NetworkCoalesceExec`
/// - [NetworkBoundaryKind::Broadcast] -> `NetworkBroadcastExec`
#[derive(Debug, Clone)]
pub enum NetworkBoundaryKind {
    Shuffle,
    Coalesce,
    Broadcast,
}

impl DisplayAs for NetworkBoundaryPlaceholder {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "NetworkBoundaryPlaceholder: kind={:?}, input_tasks={}",
            self.kind, self.input_task_count
        )
    }
}

impl ExecutionPlan for NetworkBoundaryPlaceholder {
    fn name(&self) -> &str {
        "NetworkBoundaryPlaceholder"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            kind: self.kind.clone(),
            input_task_count: self.input_task_count,
            input: require_one_child(children)?,
        }))
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        not_impl_err!("NetworkBoundaryPlaceholder does not support execution.")
    }
}
