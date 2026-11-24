#[cfg(doc)]
use crate::CalculateCostOptions;
use crate::Complexity;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Describes the cost of [ExecutionPlan] nodes the built-in model does not know about.
///
/// Extensions are registered through [CalculateCostOptions] and consulted for each node before
/// the built-in model. Returning [None] falls back to the built-in complexity for that node, which
/// assumes an O(N) pass over the output for unknown CPU work and no memory or network usage.
pub trait CostModelExtension: Send + Sync {
    /// CPU complexity of `node`, without recursing into its children.
    fn complexity_cpu(&self, _node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        None
    }

    /// Memory complexity of `node`, without recursing into its children.
    fn complexity_memory(&self, _node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        None
    }

    /// Network complexity of `node`, without recursing into its children.
    fn complexity_network(&self, _node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        None
    }
}

impl CostModelExtension for Arc<dyn CostModelExtension> {
    fn complexity_cpu(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        self.as_ref().complexity_cpu(node)
    }
    fn complexity_memory(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        self.as_ref().complexity_memory(node)
    }
    fn complexity_network(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        self.as_ref().complexity_network(node)
    }
}

impl CostModelExtension for Box<dyn CostModelExtension> {
    fn complexity_cpu(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        self.as_ref().complexity_cpu(node)
    }
    fn complexity_memory(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        self.as_ref().complexity_memory(node)
    }
    fn complexity_network(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Complexity> {
        self.as_ref().complexity_network(node)
    }
}
