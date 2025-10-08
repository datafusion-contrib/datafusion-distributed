use std::{any::Any, sync::Arc};
use datafusion::physical_plan::ExecutionPlan;
use crate::distributed_physical_optimizer_rule::{NetworkBoundary, NetworkBoundaryExt};


// Use HRTB to work with any lifetime
type DowncastFn = for<'a> fn(&'a dyn ExecutionPlan) -> Option<&'a dyn NetworkBoundaryExt>;

pub struct NetworkBoundaryRegistry {
    downcast_fns: Vec<DowncastFn>,
}

impl NetworkBoundaryRegistry {
    pub fn new() -> Self {
        Self {
            downcast_fns: Vec::new(),
        }
    }
    
    pub fn register<T: NetworkBoundaryExt + 'static>(mut self) -> Self {
        // Use a proper function pointer instead of closure
        fn downcast_impl<T: NetworkBoundaryExt + 'static>(plan: &dyn ExecutionPlan) -> Option<&dyn NetworkBoundaryExt> {
            if plan.as_any().is::<T>() {
                plan.as_any().downcast_ref::<T>().map(|t| t as &dyn NetworkBoundaryExt)
            } else {
                None
            }
        }
        
        self.downcast_fns.push(downcast_impl::<T>);
        self
    }
    
    pub fn try_downcast<'a>(&self, plan: &'a dyn ExecutionPlan) -> Option<&'a dyn NetworkBoundary> {
        for downcast_fn in &self.downcast_fns {
            if let Some(boundary_ext) = downcast_fn(plan) {
                if let Some(boundary) = boundary_ext.as_network_boundary() {
                    return Some(boundary);
                }
            }
        }
        // None
        plan.as_network_boundary()
    }
}

struct MyWrapper{
    plan: Arc<dyn ExecutionPlan>,
}

impl NetworkBoundaryExt for MyWrapper {
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary> {
        None
    }
    fn is_network_boundary(&self) -> bool {
        self.as_network_boundary().is_some()
    }
}

// Usage example:
mod tests {
    use crate::{execution_plans::network_boundary_builder::{MyWrapper, NetworkBoundaryRegistry}};

    fn test() {
        let registry = NetworkBoundaryRegistry::new()
            .register::<MyWrapper>();
    }
}
