use crate::execution_plans::NetworkBroadcastExec;
use crate::{NetworkCoalesceExec, NetworkShuffleExec, Stage};
use datafusion::common::plan_err;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Necessary information for building a [Stage] during distributed planning.
///
/// [NetworkBoundary]s return this piece of data so that the distributed planner know how to
/// build the next [Stage] from which the [NetworkBoundary] is going to receive data.
///
/// Some network boundaries might perform some modifications in their children, like scaling
/// up the number of partitions, or injecting a specific [ExecutionPlan] on top.
pub struct InputStageInfo {
    /// The head plan of the [Stage] that is about to be built.
    pub plan: Arc<dyn ExecutionPlan>,
    /// The amount of tasks the [Stage] will have.
    pub task_count: usize,
}

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
    /// Returns the information necessary for building the next stage from which this
    /// [NetworkBoundary] is going to collect data.
    fn get_input_stage_info(&self, task_count: usize)
    -> datafusion::common::Result<InputStageInfo>;

    /// re-assigns a different number of input tasks to the current [NetworkBoundary].
    ///
    /// This will be called if upon building a stage, a [crate::distributed_planner::distributed_physical_optimizer_rule::DistributedPlanError::LimitTasks] error
    /// is returned, prompting the [NetworkBoundary] to choose a different number of input tasks.
    fn with_input_task_count(
        &self,
        input_tasks: usize,
    ) -> datafusion::common::Result<Arc<dyn NetworkBoundary>>;

    /// Returns the input tasks assigned to this [NetworkBoundary].
    fn input_task_count(&self) -> usize;

    /// Called when a [Stage] is correctly formed. The [NetworkBoundary] can use this
    /// information to perform any internal transformations necessary for distributed execution.
    ///
    /// Typically, [NetworkBoundary]s will use this call for transitioning from "Pending" to "ready".
    fn with_input_stage(
        &self,
        input_stage: Stage,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>>;

    /// Returns the assigned input [Stage], if any.
    fn input_stage(&self) -> Option<&Stage>;

    /// The planner might decide to remove this [NetworkBoundary] from the plan if it decides that
    /// it's not going to bring any benefit. The [NetworkBoundary] will be replaced with whatever
    /// this function returns.
    fn rollback(&self) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let children = self.children();
        if children.len() != 1 {
            return plan_err!(
                "Expected distributed node {} to have exactly 1 children, but got {}",
                self.name(),
                children.len()
            );
        }
        Ok(Arc::clone(children.first().unwrap()))
    }
}

/// Extension trait for downcasting dynamic types to [NetworkBoundary].
pub trait NetworkBoundaryExt {
    /// Downcasts self to a [NetworkBoundary] if possible.
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary>;
    /// Returns whether self is a [NetworkBoundary] or not.
    fn is_network_boundary(&self) -> bool {
        self.as_network_boundary().is_some()
    }
}

impl NetworkBoundaryExt for dyn ExecutionPlan {
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary> {
        if let Some(node) = self.as_any().downcast_ref::<NetworkShuffleExec>() {
            Some(node)
        } else if let Some(node) = self.as_any().downcast_ref::<NetworkCoalesceExec>() {
            Some(node)
        } else if let Some(node) = self.as_any().downcast_ref::<NetworkBroadcastExec>() {
            Some(node)
        } else {
            None
        }
    }
}
