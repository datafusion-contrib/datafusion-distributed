use std::sync::Arc;

use datafusion::{
    common::{internal_datafusion_err, internal_err},
    error::{DataFusionError, Result},
};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};

use super::{task::ExecutionTaskMessage, ExecutionTask};

impl TryFrom<Arc<ExecutionTask>> for ExecutionTaskMessage {
    type Error = DataFusionError;

    fn try_from(task: Arc<ExecutionTask>) -> Result<Self> {
        let proto_plan =
            PhysicalPlanNode::try_from_physical_plan(task.plan.clone(), task.codec.as_ref())?;

        let proto_inputs = task
            .inputs
            .iter()
            .map(|input| input.as_any().downcast_ref::<Arc<ExecutionTask>>())
            .collect::<Option<Vec<_>>>()
            .ok_or(internal_datafusion_err!(
                "Failed to downcast inputs to ExecutionTask"
            ))?
            .into_iter()
            .map(|input| ExecutionTaskMessage::try_from(input.clone()).map(Box::new))
            .collect::<Result<Vec<_>>>()?;

        Ok(ExecutionTaskMessage {
            stage_num: Some(task.stage_num as u32),
            plan: Some(Box::new(proto_plan)),
            worker_addr: task.worker_addr.clone(),
            partition_group: task
                .partition_group
                .iter()
                .map(|p| *p as u32)
                .collect::<Vec<u32>>(),
            inputs: proto_inputs,
        })
    }
}
