use crate::execution_plans::{ExecutionTask, InputStage, StageExec};
use bytes::Bytes;
use datafusion::common::exec_err;
use datafusion::{
    common::internal_datafusion_err,
    error::{DataFusionError, Result},
    execution::{FunctionRegistry, runtime_env::RuntimeEnv},
};
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, PhysicalExtensionCodec},
    protobuf::PhysicalPlanNode,
};
use prost::Message;
use std::fmt::Display;
use url::Url;

/// A key that uniquely identifies a stage in a query
#[derive(Clone, Hash, Eq, PartialEq, ::prost::Message)]
pub struct StageKey {
    /// Our query id
    #[prost(string, tag = "1")]
    pub query_id: String,
    /// Our stage id
    #[prost(uint64, tag = "2")]
    pub stage_id: u64,
    /// The task number within the stage
    #[prost(uint64, tag = "3")]
    pub task_number: u64,
}

impl Display for StageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StageKey_QueryID_{}_StageID_{}_TaskNumber_{}",
            self.query_id, self.stage_id, self.task_number
        )
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageExecProto {
    /// Our query id
    #[prost(bytes, tag = "1")]
    query_id: Vec<u8>,
    /// Our stage number
    #[prost(uint64, tag = "2")]
    num: u64,
    /// Our stage name
    #[prost(string, tag = "3")]
    name: String,
    /// The physical execution plan that this stage will execute.
    #[prost(message, optional, boxed, tag = "4")]
    plan: Option<Box<PhysicalPlanNode>>,
    /// The input stages to this stage
    #[prost(repeated, message, tag = "5")]
    inputs: Vec<StageInputProto>,
    /// Our tasks which tell us how finely grained to execute the partitions in
    /// the plan
    #[prost(message, repeated, tag = "6")]
    tasks: Vec<ExecutionTaskProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageInputProto {
    #[prost(uint64, tag = "1")]
    num: u64,
    #[prost(message, repeated, tag = "2")]
    tasks: Vec<ExecutionTaskProto>,
    #[prost(bytes, tag = "3")]
    stage: Bytes,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionTaskProto {
    /// The url of the worker that will execute this task.  A None value is interpreted as
    /// unassigned.
    #[prost(string, optional, tag = "1")]
    url_str: Option<String>,
}

fn encode_tasks(tasks: &[ExecutionTask]) -> Vec<ExecutionTaskProto> {
    tasks
        .iter()
        .map(|task| ExecutionTaskProto {
            url_str: task.url.as_ref().map(|v| v.to_string()),
        })
        .collect()
}

/// Encodes an [InputStage] as protobuf [Bytes]:
/// - If the input is [InputStage::Decoded], it will serialize the inner plan as protobuf bytes.
/// - If the input is [InputStage::Encoded], it will pass through the [Bytes] in a zero-copy manner.
pub(crate) fn proto_from_input_stage(
    input_stage: &InputStage,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Bytes, DataFusionError> {
    match input_stage {
        InputStage::Decoded(v) => {
            let stage = StageExec::from_dyn(v);
            Ok(proto_from_stage(stage, codec)?.encode_to_vec().into())
        }
        InputStage::Encoded { proto, .. } => Ok(proto.clone()),
    }
}

/// Converts a [StageExec] into a [StageExecProto], which makes it suitable to be serialized and
/// sent over the wire.
///
/// If the input [InputStage]s of the provided [StageExec] are already encoded as protobuf [Bytes],
/// they will not be decoded and re-encoded, the [Bytes] are just passthrough as-is in a zero copy
/// manner.
pub(crate) fn proto_from_stage(
    stage: &StageExec,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<StageExecProto, DataFusionError> {
    let proto_plan = PhysicalPlanNode::try_from_physical_plan(stage.plan.clone(), codec)?;
    let inputs = stage
        .input_stages_iter()
        .map(|s| match s {
            InputStage::Decoded(s) => {
                let Some(s) = s.as_any().downcast_ref::<StageExec>() else {
                    return exec_err!(
                        "Programming error: StageExec input must always be other StageExec"
                    );
                };

                Ok(StageInputProto {
                    num: s.num as u64,
                    tasks: encode_tasks(&s.tasks),
                    stage: proto_from_stage(s, codec)?.encode_to_vec().into(),
                })
            }
            InputStage::Encoded { num, tasks, proto } => Ok(StageInputProto {
                num: *num as u64,
                tasks: encode_tasks(tasks),
                stage: proto.clone(),
            }),
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StageExecProto {
        query_id: stage.query_id.as_bytes().to_vec(),
        num: stage.num as u64,
        name: stage.name(),
        plan: Some(Box::new(proto_plan)),
        inputs,
        tasks: encode_tasks(&stage.tasks),
    })
}

/// Decodes the provided protobuf [Bytes] as a [StageExec]. Rather than recursively decoding all the
/// input [InputStage]s, it performs a shallow decoding of just the first [StageExec] level, leaving
/// all the inputs in [InputStage::Encoded] state.
///
/// This prevents decoding and then re-encoding the whole plan recursively, and only decodes the
/// things that are strictly needed.
pub(crate) fn stage_from_proto(
    msg: Bytes,
    registry: &dyn FunctionRegistry,
    runtime: &RuntimeEnv,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<StageExec> {
    fn decode_tasks(tasks: Vec<ExecutionTaskProto>) -> Result<Vec<ExecutionTask>> {
        tasks
            .into_iter()
            .map(|task| {
                Ok(ExecutionTask {
                    url: task
                        .url_str
                        .map(|u| {
                            Url::parse(&u).map_err(|_| internal_datafusion_err!("Invalid URL: {u}"))
                        })
                        .transpose()?,
                })
            })
            .collect()
    }
    let msg = StageExecProto::decode(msg)
        .map_err(|e| internal_datafusion_err!("Cannot decode StageExecProto: {e}"))?;
    let plan_node = msg.plan.ok_or(internal_datafusion_err!(
        "ExecutionStageMsg is missing the plan"
    ))?;

    let plan = plan_node.try_into_physical_plan(registry, runtime, codec)?;

    let inputs = msg
        .inputs
        .into_iter()
        .map(|s| {
            Ok(InputStage::Encoded {
                num: s.num as usize,
                tasks: decode_tasks(s.tasks)?,
                proto: s.stage,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StageExec {
        query_id: msg
            .query_id
            .try_into()
            .map_err(|_| internal_datafusion_err!("Invalid query_id in ExecutionStageProto"))?,
        num: msg.num as usize,
        name: msg.name,
        plan,
        inputs,
        tasks: decode_tasks(msg.tasks)?,
        depth: 0,
    })
}

// add tests for round trip to and from a proto message for ExecutionStage
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::StageExec;
    use crate::protobuf::{proto_from_stage, stage_from_proto};
    use datafusion::{
        arrow::{
            array::{RecordBatch, StringArray, UInt8Array},
            datatypes::{DataType, Field, Schema},
        },
        common::internal_datafusion_err,
        datasource::MemTable,
        error::Result,
        execution::context::SessionContext,
    };
    use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
    use prost::Message;
    use uuid::Uuid;

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
    #[ignore]
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
        let stage = StageExec {
            query_id: Uuid::new_v4(),
            num: 1,
            name: "TestStage".to_string(),
            plan: physical_plan,
            inputs: vec![],
            tasks: vec![],
            depth: 0,
        };

        // Convert to proto message
        let stage_msg = proto_from_stage(&stage, &DefaultPhysicalExtensionCodec {})?;

        // Serialize to bytes
        let mut buf = Vec::new();
        stage_msg
            .encode(&mut buf)
            .map_err(|e| internal_datafusion_err!("couldn't encode {e:#?}"))?;

        // Convert back to ExecutionStage
        let round_trip_stage = stage_from_proto(
            buf.into(),
            &ctx,
            ctx.runtime_env().as_ref(),
            &DefaultPhysicalExtensionCodec {},
        )?;

        // Compare original and round-tripped stages
        assert_eq!(stage.num, round_trip_stage.num);
        assert_eq!(stage.name, round_trip_stage.name);
        Ok(())
    }
}
