use std::fmt::Formatter;
use std::sync::Arc;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PlanProperties;
use crate::distributed_physical_optimizer_rule::{NetworkBoundary, NetworkBoundaryExt};
use delegate::delegate;
use datafusion::error::Result;
use datafusion::physical_plan::SendableRecordBatchStream;
use std::any::Any;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::DisplayAs;

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
        None
    }
}

#[derive(Debug)]
struct MyWrapper{
    inner: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for MyWrapper {
    fn fmt_as(&self, fmt_type: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MyWrapper")
    }
}

impl ExecutionPlan for MyWrapper {
    fn name(&self) -> &str {
        "MetricsWrapperExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        unimplemented!("MetricsWrapperExec does not implement properties")
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("MetricsWrapperExec does not implement with_new_children")
    }

    fn execute(
        &self,
        _partition: usize,
        _contex: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("MetricsWrapperExec does not implement execute")
    }

    // metrics returns the wrapped metrics.
    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }
}

impl NetworkBoundaryExt for MyWrapper {
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary> {
        println!("calling as_network_boundary on inner");
       self.inner.as_network_boundary() 
    }
    fn is_network_boundary(&self) -> bool {
        println!("calling is_network_boundary on inner");
        self.inner.as_network_boundary().is_some()
    }
}

// Usage example:
mod tests {
    use std::sync::Arc;

    use datafusion::physical_plan::empty::EmptyExec;
    use arrow::datatypes::Schema;
    use arrow::datatypes::Field;
    use arrow::datatypes::DataType;

    use crate::{execution_plans::network_boundary_builder::{MyWrapper, NetworkBoundaryRegistry}};

    #[test]
    fn test_asdfasdf() {
        let registry = NetworkBoundaryRegistry::new()
            .register::<MyWrapper>();


            let plan = EmptyExec::new(
                Arc::new(Schema::new(
                    vec![Field::new("a", DataType::Int64, false)],
                )),
            );

            let plan2 = MyWrapper {
                inner: Arc::new(plan)
            };

        let b = registry.try_downcast(&plan2);
    }
}
