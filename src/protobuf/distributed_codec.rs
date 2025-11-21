use super::get_distributed_user_codecs;
use crate::NetworkBoundary;
use crate::execution_plans::{NetworkCoalesceExec, NetworkCoalesceReady, NetworkShuffleReadyExec};
use crate::stage::{ExecutionTask, MaybeEncodedPlan, Stage};
use crate::{NetworkShuffleExec, PartitionIsolatorExec};
use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{ComposedPhysicalExtensionCodec, PhysicalExtensionCodec};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::sync::Arc;
use url::Url;

/// DataFusion [PhysicalExtensionCodec] implementation that allows serializing and
/// deserializing the custom ExecutionPlans in this project
#[derive(Debug)]
pub struct DistributedCodec;

impl DistributedCodec {
    pub fn new_combined_with_user(cfg: &SessionConfig) -> impl PhysicalExtensionCodec + use<> {
        let mut codecs: Vec<Arc<dyn PhysicalExtensionCodec>> = vec![Arc::new(DistributedCodec {})];
        codecs.extend(get_distributed_user_codecs(cfg));
        ComposedPhysicalExtensionCodec::new(codecs)
    }
}

impl PhysicalExtensionCodec for DistributedCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let DistributedExecProto {
            node: Some(distributed_exec_node),
        } = DistributedExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?
        else {
            return Err(proto_error(
                "Expected DistributedExecNode in DistributedExecProto",
            ));
        };

        fn parse_stage_proto(
            proto: Option<StageProto>,
            inputs: &[Arc<dyn ExecutionPlan>],
        ) -> Result<Stage, DataFusionError> {
            let Some(proto) = proto else {
                return Err(proto_error("Empty StageProto"));
            };
            let plan_proto = match proto.plan_proto.is_empty() {
                true => None,
                false => Some(proto.plan_proto),
            };

            let plan = match (plan_proto, inputs.first()) {
                (Some(plan_proto), None) => MaybeEncodedPlan::Encoded(plan_proto),
                (None, Some(child)) => MaybeEncodedPlan::Decoded(Arc::clone(child)),
                (Some(_), Some(_)) => {
                    return Err(proto_error(
                        "When building a Stage from protobuf, either an already decoded child or its serialized bytes must be passed, but not both",
                    ));
                }
                (None, None) => {
                    return Err(proto_error(
                        "When building a Stage from protobuf, an already decoded child or its serialized bytes must be passed",
                    ));
                }
            };

            Ok(Stage {
                query_id: uuid::Uuid::from_slice(proto.query_id.as_ref())
                    .map_err(|_| proto_error("Invalid query_id in ExecutionStageProto"))?,
                num: proto.num as usize,
                plan,
                tasks: decode_tasks(proto.tasks)?,
            })
        }

        match distributed_exec_node {
            DistributedExecNode::NetworkHashShuffle(NetworkShuffleExecProto {
                schema,
                partitioning,
                input_stage,
            }) => {
                let schema: Schema = schema
                    .as_ref()
                    .map(|s| s.try_into())
                    .ok_or(proto_error("NetworkShuffleExec is missing schema"))??;

                let partitioning = parse_protobuf_partitioning(
                    partitioning.as_ref(),
                    ctx,
                    &schema,
                    &DistributedCodec {},
                )?
                .ok_or(proto_error("NetworkShuffleExec is missing partitioning"))?;

                Ok(Arc::new(new_network_hash_shuffle_exec(
                    partitioning,
                    Arc::new(schema),
                    parse_stage_proto(input_stage, inputs)?,
                )))
            }
            DistributedExecNode::NetworkCoalesceTasks(NetworkCoalesceExecProto {
                schema,
                partitioning,
                input_stage,
            }) => {
                let schema: Schema = schema
                    .as_ref()
                    .map(|s| s.try_into())
                    .ok_or(proto_error("NetworkCoalesceExec is missing schema"))??;

                let partitioning = parse_protobuf_partitioning(
                    partitioning.as_ref(),
                    ctx,
                    &schema,
                    &DistributedCodec {},
                )?
                .ok_or(proto_error("NetworkCoalesceExec is missing partitioning"))?;

                Ok(Arc::new(new_network_coalesce_tasks_exec(
                    partitioning,
                    Arc::new(schema),
                    parse_stage_proto(input_stage, inputs)?,
                )))
            }
            DistributedExecNode::PartitionIsolator(PartitionIsolatorExecProto { n_tasks }) => {
                if inputs.len() != 1 {
                    return Err(proto_error(format!(
                        "PartitionIsolatorExec expects exactly one child, got {}",
                        inputs.len()
                    )));
                }

                let child = inputs.first().unwrap();

                Ok(Arc::new(PartitionIsolatorExec::new_ready(
                    child.clone(),
                    n_tasks as usize,
                )?))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        fn encode_stage_proto(stage: Option<&Stage>) -> Result<StageProto, DataFusionError> {
            let stage = stage.ok_or(proto_error(
                "Cannot encode a NetworkBoundary that has no stage assinged",
            ))?;
            Ok(StageProto {
                query_id: Bytes::from(stage.query_id.as_bytes().to_vec()),
                num: stage.num as u64,
                tasks: encode_tasks(&stage.tasks),
                plan_proto: match &stage.plan {
                    MaybeEncodedPlan::Decoded(_) => Bytes::new(),
                    MaybeEncodedPlan::Encoded(proto) => proto.clone(),
                },
            })
        }

        if let Some(node) = node.as_any().downcast_ref::<NetworkShuffleExec>() {
            let inner = NetworkShuffleExecProto {
                schema: Some(node.schema().try_into()?),
                partitioning: Some(serialize_partitioning(
                    node.properties().output_partitioning(),
                    &DistributedCodec {},
                )?),
                input_stage: Some(encode_stage_proto(node.input_stage())?),
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::NetworkHashShuffle(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else if let Some(node) = node.as_any().downcast_ref::<NetworkCoalesceExec>() {
            let inner = NetworkCoalesceExecProto {
                schema: Some(node.schema().try_into()?),
                partitioning: Some(serialize_partitioning(
                    node.properties().output_partitioning(),
                    &DistributedCodec {},
                )?),
                input_stage: Some(encode_stage_proto(node.input_stage())?),
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::NetworkCoalesceTasks(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else if let Some(node) = node.as_any().downcast_ref::<PartitionIsolatorExec>() {
            let PartitionIsolatorExec::Ready(ready_node) = node else {
                return Err(proto_error(
                    "deserialized an PartitionIsolatorExec that is not ready",
                ));
            };
            let inner = PartitionIsolatorExecProto {
                n_tasks: ready_node.n_tasks as u64,
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::PartitionIsolator(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else {
            Err(proto_error(format!("Unexpected plan {}", node.name())))
        }
    }
}

/// A key that uniquely identifies a stage in a query.
#[derive(Clone, Hash, Eq, PartialEq, ::prost::Message)]
pub struct StageKey {
    /// Our query id
    #[prost(bytes, tag = "1")]
    pub query_id: Bytes,
    /// Our stage id
    #[prost(uint64, tag = "2")]
    pub stage_id: u64,
    /// The task number within the stage
    #[prost(uint64, tag = "3")]
    pub task_number: u64,
}

impl StageKey {
    /// Creates a new `StageKey`.
    pub fn new(query_id: Bytes, stage_id: u64, task_number: u64) -> StageKey {
        Self {
            query_id,
            stage_id,
            task_number,
        }
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageProto {
    /// Our query id
    #[prost(bytes, tag = "1")]
    pub query_id: Bytes,
    /// Our stage number
    #[prost(uint64, tag = "2")]
    pub num: u64,
    /// Our tasks which tell us how finely grained to execute the partitions in
    /// the plan
    #[prost(message, repeated, tag = "3")]
    pub tasks: Vec<ExecutionTaskProto>,
    /// The child plan already serialized
    #[prost(bytes, tag = "4")]
    pub plan_proto: Bytes, // Apparently, with an optional keyword, we cannot put Bytes here.
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionTaskProto {
    /// The url of the worker that will execute this task.  A None value is interpreted as
    /// unassigned.
    #[prost(string, optional, tag = "1")]
    pub url_str: Option<String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DistributedExecProto {
    #[prost(oneof = "DistributedExecNode", tags = "1, 2, 3, 4, 5")]
    pub node: Option<DistributedExecNode>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum DistributedExecNode {
    #[prost(message, tag = "1")]
    NetworkHashShuffle(NetworkShuffleExecProto),
    #[prost(message, tag = "2")]
    NetworkCoalesceTasks(NetworkCoalesceExecProto),
    #[prost(message, tag = "3")]
    PartitionIsolator(PartitionIsolatorExecProto),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionIsolatorExecProto {
    #[prost(uint64, tag = "1")]
    pub n_tasks: u64,
}

/// Protobuf representation of the [NetworkShuffleExec] physical node. It serves as
/// an intermediate format for serializing/deserializing [NetworkShuffleExec] nodes
/// to send them over the wire.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NetworkShuffleExecProto {
    #[prost(message, optional, tag = "1")]
    schema: Option<protobuf::Schema>,
    #[prost(message, optional, tag = "2")]
    partitioning: Option<protobuf::Partitioning>,
    #[prost(message, optional, tag = "3")]
    input_stage: Option<StageProto>,
}

fn new_network_hash_shuffle_exec(
    partitioning: Partitioning,
    schema: SchemaRef,
    input_stage: Stage,
) -> NetworkShuffleExec {
    NetworkShuffleExec::Ready(NetworkShuffleReadyExec {
        properties: PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ),
        input_stage,
        metrics_collection: Default::default(),
    })
}

/// Protobuf representation of the [NetworkShuffleExec] physical node. It serves as
/// an intermediate format for serializing/deserializing [NetworkShuffleExec] nodes
/// to send them over the wire.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NetworkCoalesceExecProto {
    #[prost(message, optional, tag = "1")]
    schema: Option<protobuf::Schema>,
    #[prost(message, optional, tag = "2")]
    partitioning: Option<protobuf::Partitioning>,
    #[prost(message, optional, tag = "3")]
    input_stage: Option<StageProto>,
}

fn new_network_coalesce_tasks_exec(
    partitioning: Partitioning,
    schema: SchemaRef,
    input_stage: Stage,
) -> NetworkCoalesceExec {
    NetworkCoalesceExec::Ready(NetworkCoalesceReady {
        properties: PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ),
        input_stage,
        metrics_collection: Default::default(),
    })
}

fn encode_tasks(tasks: &[ExecutionTask]) -> Vec<ExecutionTaskProto> {
    tasks
        .iter()
        .map(|task| ExecutionTaskProto {
            url_str: task.url.as_ref().map(|v| v.to_string()),
        })
        .collect()
}

fn decode_tasks(tasks: Vec<ExecutionTaskProto>) -> Result<Vec<ExecutionTask>, DataFusionError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::physical_expr::LexOrdering;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::{
        physical_expr::{Partitioning, PhysicalSortExpr, expressions::Column, expressions::col},
        physical_plan::{ExecutionPlan, displayable, sorts::sort::SortExec, union::UnionExec},
    };

    use datafusion::prelude::SessionContext;

    fn empty_exec() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(SchemaRef::new(Schema::empty())))
    }

    fn dummy_stage() -> Stage {
        Stage {
            query_id: Default::default(),
            num: 0,
            plan: MaybeEncodedPlan::Decoded(empty_exec()),
            tasks: vec![],
        }
    }

    fn schema_i32(name: &str) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]))
    }

    fn repr(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    fn create_context() -> Arc<TaskContext> {
        SessionContext::new().task_ctx()
    }

    #[test]
    fn test_roundtrip_single_flight() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("a");
        let part = Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 4);
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(new_network_hash_shuffle_exec(part, schema, dummy_stage()));

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[empty_exec()], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_isolator_flight() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("b");
        let flight = Arc::new(new_network_hash_shuffle_exec(
            Partitioning::UnknownPartitioning(1),
            schema,
            dummy_stage(),
        ));

        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionIsolatorExec::new_ready(flight.clone(), 1)?);

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[flight], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_isolator_union() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("c");
        let left = Arc::new(new_network_hash_shuffle_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
        ));
        let right = Arc::new(new_network_hash_shuffle_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
        ));

        let union = UnionExec::try_new(vec![left.clone(), right.clone()])?;
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionIsolatorExec::new_ready(union.clone(), 1)?);

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[union], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_isolator_sort_flight() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("d");
        let flight = Arc::new(new_network_hash_shuffle_exec(
            Partitioning::UnknownPartitioning(1),
            schema.clone(),
            dummy_stage(),
        ));

        let sort_expr = PhysicalSortExpr {
            expr: col("d", &schema)?,
            options: Default::default(),
        };
        let sort = Arc::new(SortExec::new(
            LexOrdering::new(vec![sort_expr]).unwrap(),
            flight.clone(),
        ));

        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionIsolatorExec::new_ready(sort.clone(), 1)?);

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[sort], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_single_flight_coalesce() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("e");
        let plan: Arc<dyn ExecutionPlan> = Arc::new(new_network_coalesce_tasks_exec(
            Partitioning::RoundRobinBatch(3),
            schema,
            dummy_stage(),
        ));

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[empty_exec()], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_isolator_flight_coalesce() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("f");
        let flight = Arc::new(new_network_coalesce_tasks_exec(
            Partitioning::UnknownPartitioning(1),
            schema,
            dummy_stage(),
        ));

        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionIsolatorExec::new_ready(flight.clone(), 1)?);

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[flight], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_isolator_union_coalesce() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("g");
        let left = Arc::new(new_network_coalesce_tasks_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
        ));
        let right = Arc::new(new_network_coalesce_tasks_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
        ));

        let union = UnionExec::try_new(vec![left.clone(), right.clone()])?;
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionIsolatorExec::new_ready(union.clone(), 3)?);

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[union], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }
}
