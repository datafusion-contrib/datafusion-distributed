use crate::NetworkBoundaryExt;
use crate::distributed_planner::statistics::complexity::{Complexity, LinearComplexity};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Calculates the memory cost for the provided node, without recursing into children.
pub(super) fn complexity_network(node: &Arc<dyn ExecutionPlan>) -> Complexity {
    if node.is_network_boundary() {
        return Complexity::Linear(LinearComplexity::AllOutputColumns);
    }

    Complexity::Constant(0)
}
