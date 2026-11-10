use crate::distributed_planner::statistics::complexity::{Complexity, LinearComplexity};
use crate::{BroadcastExec, NetworkBoundaryExt};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Calculates the network cost for the provided node, without recursing into children.
pub(super) fn complexity_network(node: &Arc<dyn ExecutionPlan>) -> Complexity {
    if node.is_network_boundary() {
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
            return bytes.multiply(Complexity::Constant(bcast.consumer_task_count() as f32));
        }
        return bytes;
    }

    Complexity::Constant(0.)
}
