use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::{BroadcastExec, NetworkBoundaryExt};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_cost_model::{
    CalculateCostOptions, Complexity, Cost, CostModelExtension, LinearComplexity,
};
use std::sync::Arc;

/// Calculates the cost of a plan that may contain the nodes shipped by this crate.
pub(crate) fn calculate_cost(plan: &Arc<dyn ExecutionPlan>) -> Result<Cost> {
    datafusion_cost_model::calculate_cost(
        plan,
        &CalculateCostOptions::default().with_extension(DistributedCostModelExtension),
    )
}

/// Teaches the cost model about the execution plans shipped by this crate.
struct DistributedCostModelExtension;

impl CostModelExtension for DistributedCostModelExtension {
    fn complexity_cpu(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        // BroadcastExec does not do any computation, it does not even read the data.
        // ChildrenIsolatorUnionExec, like UnionExec, just combines its input streams.
        if node.is::<BroadcastExec>() || node.is::<ChildrenIsolatorUnionExec>() {
            return Some(Complexity::Constant(1.));
        }
        None
    }

    fn complexity_memory(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        // BroadcastExec retains batches so several consumers can replay the same input partition.
        if node.is::<BroadcastExec>() {
            return Some(Complexity::Linear(LinearComplexity::AllOutputColumns));
        }
        None
    }

    fn complexity_network(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        if !node.is_network_boundary() {
            return None;
        }
        let bytes = Complexity::Linear(LinearComplexity::AllOutputColumns);
        // A broadcast boundary replicates its input to every consumer task, so the bytes that
        // cross the network scale with the consumer task count, unlike shuffle/coalesce which move
        // each row once. The broadcast boundary's only child is always a BroadcastExec (enforced
        // by NetworkBroadcastExec::try_new), which carries the replication factor.
        if let Some(bcast) = node
            .children()
            .first()
            .copied()
            .and_then(|c| c.downcast_ref::<BroadcastExec>())
        {
            return Some(bytes.multiply(Complexity::Constant(bcast.consumer_task_count() as f32)));
        }
        Some(bytes)
    }
}
