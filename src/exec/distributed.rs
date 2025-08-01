use std::{any::Any, fmt::Formatter, pin::Pin, sync::Arc};

use datafusion::{
    execution::{RecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan},
};

use crate::{remote::WorkerDiscovery, stage::ExecutionStage, task::WorkerAddr};

/// This execution plan provides the capability to execute its input plan in
/// a distributed manner across multiple nodes in a cluster.
///
/// When execute() is called, it will use the provided `WorkerDiscovery` to
/// discover worker nodes and distribute the execution of the input plan
/// across those nodes.
#[derive(Debug)]
pub struct DistributedExec {
    /// This is the unmodified input plan that this execution plan will execute
    /// We keep it for display purporses
    input: Arc<dyn ExecutionPlan>,
    /// This is the worker discovery mechanism that will be used to provide
    /// worker addresses
    discovery: Arc<dyn WorkerDiscovery>,
    /// This contains stages as defined by the `crate::stage::planner::StageOptimizer`
    stage: Arc<ExecutionStage>,
}

impl DistributedExec {
    /// Creates a new `DistributedExec` with the given input plan.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        discovery: Arc<dyn WorkerDiscovery>,
        stage: Arc<ExecutionStage>,
    ) -> Self {
        DistributedExec {
            input,
            discovery,
            stage,
        }
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "DistributedExec: {}", self.input.name()),
            DisplayFormatType::Verbose => write!(
                f,
                "DistributedExec: input = {}, properties = {:?}",
                self.input.name(),
                self.input.properties()
            ),
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        "DistributedExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "DistributedExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(DistributedExec {
            input: children[0].clone(),
            discovery: self.discovery.clone(),
            stage: self.stage.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<Pin<Box<dyn RecordBatchStream + Send>>> {
        todo!()
    }
}
