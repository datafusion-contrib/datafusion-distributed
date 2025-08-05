use std::sync::Arc;

use datafusion::{
    common::internal_datafusion_err,
    error::{DataFusionError, Result},
    execution::{runtime_env::RuntimeEnv, FunctionRegistry},
    physical_plan::ExecutionPlan,
};
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, PhysicalExtensionCodec},
    protobuf::PhysicalPlanNode,
};
use prost::Message;

use crate::{plan::DistributedCodec, task::ExecutionTask};

use super::ExecutionStage;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionStageProto {
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
    pub inputs: Vec<Box<ExecutionStageProto>>,
    /// Our tasks which tell us how finely grained to execute the partitions in
    /// the plan
    #[prost(message, repeated, tag = "5")]
    pub tasks: Vec<ExecutionTask>,
}

impl TryFrom<&ExecutionStage> for ExecutionStageProto {
    type Error = DataFusionError;

    fn try_from(stage: &ExecutionStage) -> Result<Self, Self::Error> {
        let codec = stage.codec.clone().unwrap_or(Arc::new(DistributedCodec {}));

        let proto_plan =
            PhysicalPlanNode::try_from_physical_plan(stage.plan.clone(), codec.as_ref())?;
        let inputs = stage
            .child_stages_iter()
            .map(|s| Box::new(ExecutionStageProto::try_from(s).unwrap()))
            .collect();

        Ok(ExecutionStageProto {
            num: stage.num as u64,
            name: stage.name(),
            plan: Some(Box::new(proto_plan)),
            inputs,
            tasks: stage.tasks.clone(),
        })
    }
}

impl TryFrom<ExecutionStage> for ExecutionStageProto {
    type Error = DataFusionError;

    fn try_from(stage: ExecutionStage) -> Result<Self, Self::Error> {
        ExecutionStageProto::try_from(&stage)
    }
}

pub fn stage_from_proto(
    msg: ExecutionStageProto,
    registry: &dyn FunctionRegistry,
    runtime: &RuntimeEnv,
    codec: Arc<dyn PhysicalExtensionCodec>,
) -> Result<ExecutionStage> {
    let plan_node = msg.plan.ok_or(internal_datafusion_err!(
        "ExecutionStageMsg is missing the plan"
    ))?;

    let plan = plan_node.try_into_physical_plan(registry, runtime, codec.as_ref())?;

    let inputs = msg
        .inputs
        .into_iter()
        .map(|s| {
            stage_from_proto(*s, registry, runtime, codec.clone())
                .map(|s| Arc::new(s) as Arc<dyn ExecutionPlan>)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ExecutionStage {
        num: msg.num as usize,
        name: msg.name,
        plan,
        inputs,
        tasks: msg.tasks,
        codec: Some(codec),
        depth: std::sync::atomic::AtomicU64::new(0),
    })
}

// add tests for round trip to and from a proto message for ExecutionStage
/* TODO: broken for now
#[cfg(test)]

mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::{
            array::{RecordBatch, StringArray, UInt8Array},
            datatypes::{DataType, Field, Schema},
        },
        catalog::memory::DataSourceExec,
        common::{internal_datafusion_err, internal_err},
        datasource::MemTable,
        error::{DataFusionError, Result},
        execution::context::SessionContext,
        prelude::SessionConfig,
    };
    use datafusion_proto::{
        physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec},
        protobuf::PhysicalPlanNode,
    };
    use prost::Message;
    use uuid::Uuid;

    use crate::stage::{proto::stage_from_proto, ExecutionStage, ExecutionStageProto};

    // create a simple mem table
    fn create_mem_table() -> Arc<MemTable> {
        let fields = vec![
            Field::new("id", DataType::UInt8, false),
            Field::new("data", DataType::Utf8, false),
        ];
        let schema = Arc::new(Schema::new(fields));

        let partitions = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt8Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["foo", "bar"])),
            ],
        )
        .unwrap();

        Arc::new(MemTable::try_new(schema, vec![vec![partitions]]).unwrap())
    }

    #[tokio::test]
    async fn test_execution_stage_proto_round_trip() -> Result<()> {
        let ctx = SessionContext::new();
        let mem_table = create_mem_table();
        ctx.register_table("mem_table", mem_table).unwrap();

        let physical_plan = ctx
            .sql("SELECT id, count(*) FROM mem_table group by data")
            .await?
            .create_physical_plan()
            .await?;

        // Wrap it in an ExecutionStage
        let stage = ExecutionStage {
            num: 1,
            name: "TestStage".to_string(),
            plan: physical_plan,
            inputs: vec![],
            tasks: vec![],
            codec: Some(Arc::new(DefaultPhysicalExtensionCodec {})),
            depth: std::sync::atomic::AtomicU64::new(0),
        };

        // Convert to proto message
        let stage_msg = ExecutionStageProto::try_from(&stage)?;

        // Serialize to bytes
        let mut buf = Vec::new();
        stage_msg
            .encode(&mut buf)
            .map_err(|e| internal_datafusion_err!("couldn't encode {e:#?}"))?;

        // Deserialize from bytes
        let decoded_msg = ExecutionStageProto::decode(&buf[..])
            .map_err(|e| internal_datafusion_err!("couldn't decode {e:#?}"))?;

        // Convert back to ExecutionStage
        let round_trip_stage = stage_from_proto(
            decoded_msg,
            &ctx,
            ctx.runtime_env().as_ref(),
            Arc::new(DefaultPhysicalExtensionCodec {}),
        )?;

        // Compare original and round-tripped stages
        assert_eq!(stage.num, round_trip_stage.num);
        assert_eq!(stage.name, round_trip_stage.name);
        Ok(())
    }
}*/
