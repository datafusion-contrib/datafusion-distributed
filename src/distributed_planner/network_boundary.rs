use crate::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec, Stage};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
    /// Called when a [Stage] is correctly formed. The [NetworkBoundary] can use this
    /// information to perform any internal transformations necessary for distributed execution.
    ///
    /// Typically, [NetworkBoundary]s will use this call for transitioning from "Pending" to "ready".
    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>>;

    /// Returns the assigned input [Stage], if any.
    fn input_stage(&self) -> &Stage;
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

/// Scales up the head node of the input stage of a network boundary. Different network boundaries
/// have different needs for scaling up their input, like for example, scaling up a RepartitionExec
/// during shuffles.
pub(crate) fn network_boundary_scale_input(
    input: Arc<dyn ExecutionPlan>,
    consumer_partitions: usize,
    consumer_task_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let transformed = NetworkShuffleExec::scale_input(
        Arc::clone(&input),
        consumer_partitions,
        consumer_task_count,
    )?;
    if transformed.transformed {
        return Ok(transformed.data);
    }
    let transformed = NetworkBroadcastExec::scale_input(
        Arc::clone(&input),
        consumer_partitions,
        consumer_task_count,
    )?;
    if transformed.transformed {
        return Ok(transformed.data);
    }
    let transformed = NetworkCoalesceExec::scale_input(
        Arc::clone(&input),
        consumer_partitions,
        consumer_task_count,
    )?;
    if transformed.transformed {
        return Ok(transformed.data);
    }

    Ok(input)
}
