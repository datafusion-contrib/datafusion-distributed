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
use std::fmt::Display;

use datafusion::physical_plan::metrics::MetricsSet;
use crate::task::ExecutionTask;
use crate::metrics::proto::{ProtoLabel, ProtoMetric, ProtoMetricsSet};
use std::collections::HashMap;
use super::ExecutionStage;

impl Display for StageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StageKey_QueryID_{}_StageID_{}_TaskNumber_{}", self.query_id, self.stage_id, self.task_number)
    }
}

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

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionStageProto {
    /// Our query id
    #[prost(bytes, tag = "1")]
    pub query_id: Vec<u8>,
    /// Our stage number
    #[prost(uint64, tag = "2")]
    pub num: u64,
    /// Our stage name
    #[prost(string, tag = "3")]
    pub name: String,
    /// The physical execution plan that this stage will execute.
    #[prost(message, optional, boxed, tag = "4")]
    pub plan: Option<Box<PhysicalPlanNode>>,
    /// The input stages to this stage
    #[prost(repeated, message, tag = "5")]
    pub inputs: Vec<ExecutionStageProto>,
    /// Our tasks which tell us how finely grained to execute the partitions in
    /// the plan
    #[prost(message, repeated, tag = "6")]
    pub tasks: Vec<ExecutionTask>,
    /// task_metrics is meant to be a HashMap<StageKey, Vec<MetricsSet>>. Since non-primitive types
    /// are not supported as keys, we use a Vec instead. The values in this map depend on the
    /// current state of execution.
    ///
    /// 1. A non-executed `ExecutionStage` will have no metrics.
    /// 2. After a `ExecutionStage` task is executed locally we expect it to have metrics
    ///    for the task and all of its children (which may contain other stages and tasks).
    /// 3. When a `ExecutionStage` (all tasks) are executed remotely, we expect it to have
    ///    metrics all child tasks including those from other stages.
    #[prost(message, repeated, tag="7")]
    pub task_metrics: Vec<TaskMetrics>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskMetricsSet {
    /// Our tasks which tell us how finely grained to execute the partitions in
    /// the plan
    #[prost(message, repeated, tag = "1")]
    pub tasks: Vec<TaskMetrics>,
}

/// TaskMetrics represents the metrics for a single task. It contains a list of metrics for
/// all plan nodes in the task.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskMetrics {
    /// stage_key uniquely identifies this task in the entire `plan`.
    ///
    /// This field is always present. It's marked optional due to protobuf rules.
    #[prost(message, optional, tag="1")]
    pub stage_key: Option<StageKey>,
    /// metrics[i] is the set of metrics for plan node `i` where plan nodes are ordered by the
    /// traversal order in `datafusion::physical_plan::{ExecutionPlanVisitor}`. Note that the plan
    /// corresponds to the `ExecutionStage` in the `stage_key`, which is not necessarily the
    /// containing `ExecutionStage`.
    #[prost(message, repeated, tag="2")]
    pub metrics: Vec<ProtoMetricsSet>,
}

// TODO: move this somewhere else
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlightAppMetadata {
    #[prost(oneof = "AppMetadata", tags = "1")]
    pub content: Option<AppMetadata>,
}

#[derive(Clone, PartialEq, ::prost::Oneof)]
pub enum AppMetadata {
    #[prost(message, tag="1")]
    TaskMetricsSet(TaskMetricsSet),
}

pub fn proto_from_stage(
    stage: &ExecutionStage,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<ExecutionStageProto, DataFusionError> {
    let proto_plan = PhysicalPlanNode::try_from_physical_plan(stage.plan.clone(), codec)?;
    let inputs = stage
        .child_stages_iter()
        .map(|s| proto_from_stage(s, codec))
        .collect::<Result<Vec<_>>>()?;

    Ok(ExecutionStageProto {
        query_id: stage.query_id.as_bytes().to_vec(),
        num: stage.num as u64,
        name: stage.name(),
        plan: Some(Box::new(proto_plan)),
        inputs,
        tasks: stage.tasks.clone(),
        task_metrics: Default::default(),
    })
}

pub fn stage_from_proto(
    msg: ExecutionStageProto,
    registry: &dyn FunctionRegistry,
    runtime: &RuntimeEnv,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<ExecutionStage> {
    let plan_node = msg.plan.ok_or(internal_datafusion_err!(
        "ExecutionStageMsg is missing the plan"
    ))?;

    let plan = plan_node.try_into_physical_plan(registry, runtime, codec)?;

    let inputs = msg
        .inputs
        .into_iter()
        .map(|s| {
            stage_from_proto(s, registry, runtime, codec)
                .map(|s| Arc::new(s) as Arc<dyn ExecutionPlan>)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ExecutionStage {
        query_id: msg
            .query_id
            .try_into()
            .map_err(|_| internal_datafusion_err!("Invalid query_id in ExecutionStageProto"))?,
        num: msg.num as usize,
        name: msg.name,
        plan,
        inputs,
        tasks: msg.tasks,
        depth: 0,
        task_metrics: Default::default(),
    })
}

// add tests for round trip to and from a proto message for ExecutionStage
#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

    use crate::stage::proto::proto_from_stage;
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
        let stage = ExecutionStage {
            query_id: Uuid::new_v4(),
            num: 1,
            name: "TestStage".to_string(),
            plan: physical_plan,
            inputs: vec![],
            tasks: vec![],
            depth: 0,
            task_metrics: Default::default(),
        };

        // Convert to proto message
        let stage_msg = proto_from_stage(&stage, &DefaultPhysicalExtensionCodec {})?;

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
            &DefaultPhysicalExtensionCodec {},
        )?;

        // Compare original and round-tripped stages
        assert_eq!(stage.num, round_trip_stage.num);
        assert_eq!(stage.name, round_trip_stage.name);
        Ok(())
    }
}
