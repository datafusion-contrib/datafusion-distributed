use std::sync::Arc;

use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec},
    protobuf::PhysicalPlanNode,
};
use prost::Message;

use crate::task::ExecutionTask;

use super::ExecutionStage;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionStageMsg {
    /// Our stage number
    #[prost(uint64, tag = "1")]
    pub num: u64,
    /// Our stage name
    #[prost(string, tag = "2")]
    pub name: String,
    /// The physical execution plan that this stage will execute.
    #[prost(message, optional, boxed, tag = "3")]
    pub plan: Option<Box<PhysicalPlanNode>>,
    /// The input stages to this stage
    #[prost(repeated, message, tag = "4")]
    pub inputs: Vec<Box<ExecutionStageMsg>>,
    /// Our tasks which tell us how finely grained to execute the partitions in
    /// the plan
    #[prost(message, repeated, tag = "5")]
    pub tasks: Vec<ExecutionTask>,
}

impl TryFrom<&ExecutionStage> for ExecutionStageMsg {
    type Error = DataFusionError;

    fn try_from(stage: &ExecutionStage) -> Result<Self, Self::Error> {
        let codec = stage
            .codec
            .clone()
            .unwrap_or(Arc::new(DefaultPhysicalExtensionCodec {}));

        let proto_plan =
            PhysicalPlanNode::try_from_physical_plan(stage.plan.clone(), codec.as_ref())?;
        let inputs = stage
            .child_stages_iter()
            .map(|s| Box::new(ExecutionStageMsg::try_from(s).unwrap()))
            .collect();

        Ok(ExecutionStageMsg {
            num: stage.num as u64,
            name: stage.name(),
            plan: Some(Box::new(proto_plan)),
            inputs,
            tasks: stage.tasks.clone(),
        })
    }
}

impl TryFrom<ExecutionStage> for ExecutionStageMsg {
    type Error = DataFusionError;

    fn try_from(stage: ExecutionStage) -> Result<Self, Self::Error> {
        ExecutionStageMsg::try_from(&stage)
    }
}
