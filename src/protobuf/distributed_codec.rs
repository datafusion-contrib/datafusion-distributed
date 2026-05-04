use super::get_distributed_user_codecs;
use crate::common::{deserialize_uuid, require_one_child, serialize_uuid};
use crate::execution_plans::{
    BroadcastExec, ChildrenIsolatorUnionExec, NetworkBroadcastExec, NetworkCoalesceExec,
    SamplerExec,
};
use crate::stage::{LocalStage, RemoteStage, Stage};
use crate::worker::WorkerConnectionPool;
use crate::{DistributedTaskContext, NetworkBoundary};
use crate::{NetworkShuffleExec, PartitionIsolatorExec};
use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{
    ComposedPhysicalExtensionCodec, DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use itertools::Itertools;
use prost::Message;
use std::sync::Arc;
use url::Url;

/// DataFusion [PhysicalExtensionCodec] implementation that allows serializing and
/// deserializing the custom ExecutionPlans in this project
#[derive(Debug)]
pub struct DistributedCodec;

impl DistributedCodec {
    pub fn new_combined_with_user(cfg: &SessionConfig) -> ComposedPhysicalExtensionCodec {
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
            if let Some(input) = inputs.first().cloned() {
                Ok(Stage::Local(LocalStage {
                    query_id: deserialize_uuid(proto.query_id.as_ref())?,
                    num: proto.num as usize,
                    plan: input,
                    tasks: proto.tasks.len(),
                }))
            } else {
                let mut worker_urls = Vec::with_capacity(proto.tasks.len());
                for task in proto.tasks {
                    let Some(url_str) = task.url_str else {
                        return Err(proto_error("Missing URL in task"));
                    };
                    let Ok(url) = Url::parse(&url_str) else {
                        return Err(proto_error("Invalid URL in task"));
                    };
                    worker_urls.push(url);
                }
                Ok(Stage::Remote(RemoteStage {
                    query_id: deserialize_uuid(proto.query_id.as_ref())?,
                    num: proto.num as usize,
                    workers: worker_urls,
                }))
            }
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
                    &DefaultPhysicalProtoConverter,
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
                    &DefaultPhysicalProtoConverter,
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

                Ok(Arc::new(PartitionIsolatorExec::new(
                    child.clone(),
                    n_tasks as usize,
                )))
            }
            DistributedExecNode::NetworkBroadcast(NetworkBroadcastExecProto {
                schema,
                partitioning,
                input_stage,
            }) => {
                let schema: Schema = schema
                    .as_ref()
                    .map(|s| s.try_into())
                    .ok_or(proto_error("NetworkBroadcastExec is missing schema"))??;

                let partitioning = parse_protobuf_partitioning(
                    partitioning.as_ref(),
                    ctx,
                    &schema,
                    &DistributedCodec {},
                    &DefaultPhysicalProtoConverter,
                )?
                .ok_or(proto_error("NetworkBroadcastExec is missing partitioning"))?;

                Ok(Arc::new(new_network_broadcast_exec(
                    partitioning,
                    Arc::new(schema),
                    parse_stage_proto(input_stage, inputs)?,
                )))
            }
            DistributedExecNode::Broadcast(BroadcastExecProto {
                consumer_task_count,
            }) => {
                if inputs.len() != 1 {
                    return Err(proto_error(format!(
                        "BroadcastExec expects exactly one child, got {}",
                        inputs.len()
                    )));
                }

                let child = inputs.first().unwrap();
                Ok(Arc::new(BroadcastExec::new(
                    child.clone(),
                    consumer_task_count as usize,
                )))
            }
            DistributedExecNode::ChildrenIsolatorUnion(ChildrenIsolatorUnionExecProto {
                partition_count,
                task_idx_map,
            }) => {
                // Building a UnionExec just to get the properties out of it is not the most
                // efficient thing to do. However, it's the easiest way of getting the properties
                // for the ChildrenIsolatorUnionExec without copy-pasting in this project
                // all the machinery that builds them for UnionExec.
                let mut properties = UnionExec::try_new(inputs.to_vec())?
                    .properties()
                    .as_ref()
                    .clone();
                properties.partitioning =
                    Partitioning::UnknownPartitioning(partition_count as usize);

                Ok(Arc::new(ChildrenIsolatorUnionExec {
                    properties: Arc::new(properties),
                    metrics: Default::default(),
                    children: inputs.to_vec(),
                    task_idx_map: task_idx_map
                        .iter()
                        .map(|entry| {
                            entry
                                .child_ctx
                                .iter()
                                .map(|child_ctx| {
                                    (
                                        child_ctx.child_idx as usize,
                                        DistributedTaskContext {
                                            task_index: child_ctx.task_idx as usize,
                                            task_count: child_ctx.task_count as usize,
                                        },
                                    )
                                })
                                .collect_vec()
                        })
                        .collect(),
                }))
            }
            DistributedExecNode::Sampler(SamplerExecProto {}) => {
                Ok(Arc::new(SamplerExec::new(require_one_child(inputs)?)))
            }
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        fn encode_stage_proto(stage: &Stage) -> Result<StageProto, DataFusionError> {
            Ok(match stage {
                Stage::Local(local) => StageProto {
                    query_id: serialize_uuid(&local.query_id).into(),
                    num: local.num as u64,
                    tasks: vec![ExecutionTaskProto::default(); local.tasks],
                },
                Stage::Remote(remote) => {
                    let mut tasks = Vec::with_capacity(remote.workers.len());
                    for worker in &remote.workers {
                        tasks.push(ExecutionTaskProto {
                            url_str: Some(worker.to_string()),
                        })
                    }
                    StageProto {
                        query_id: serialize_uuid(&remote.query_id).into(),
                        num: remote.num as u64,
                        tasks,
                    }
                }
            })
        }

        if let Some(node) = node.as_any().downcast_ref::<NetworkShuffleExec>() {
            let inner = NetworkShuffleExecProto {
                schema: Some(node.schema().try_into()?),
                partitioning: Some(serialize_partitioning(
                    node.properties().output_partitioning(),
                    &DistributedCodec {},
                    &DefaultPhysicalProtoConverter,
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
                    &DefaultPhysicalProtoConverter,
                )?),
                input_stage: Some(encode_stage_proto(node.input_stage())?),
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::NetworkCoalesceTasks(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else if let Some(node) = node.as_any().downcast_ref::<PartitionIsolatorExec>() {
            let inner = PartitionIsolatorExecProto {
                n_tasks: node.n_tasks as u64,
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::PartitionIsolator(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else if let Some(node) = node.as_any().downcast_ref::<NetworkBroadcastExec>() {
            let inner = NetworkBroadcastExecProto {
                schema: Some(node.schema().try_into()?),
                partitioning: Some(serialize_partitioning(
                    node.properties().output_partitioning(),
                    &DistributedCodec {},
                    &DefaultPhysicalProtoConverter,
                )?),
                input_stage: Some(encode_stage_proto(node.input_stage())?),
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::NetworkBroadcast(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else if let Some(node) = node.as_any().downcast_ref::<BroadcastExec>() {
            let inner = BroadcastExecProto {
                consumer_task_count: node.consumer_task_count() as u64,
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::Broadcast(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else if let Some(node) = node.as_any().downcast_ref::<ChildrenIsolatorUnionExec>() {
            let inner = ChildrenIsolatorUnionExecProto {
                partition_count: node.properties().output_partitioning().partition_count() as u64,
                task_idx_map: node
                    .task_idx_map
                    .iter()
                    .map(|v| TaskIdxMapEntryProto {
                        child_ctx: v
                            .iter()
                            .map(|(child_idx, task_ctx)| ChildIdxWithTaskContextProto {
                                child_idx: *child_idx as u64,
                                task_idx: task_ctx.task_index as u64,
                                task_count: task_ctx.task_count as u64,
                            })
                            .collect_vec(),
                    })
                    .collect_vec(),
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::ChildrenIsolatorUnion(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else if let Some(_node) = node.as_any().downcast_ref::<SamplerExec>() {
            let inner = SamplerExecProto {};

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::Sampler(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else {
            Err(proto_error(format!("Unexpected plan {}", node.name())))
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
    #[prost(oneof = "DistributedExecNode", tags = "1, 2, 3, 4, 5, 6, 7")]
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
    #[prost(message, tag = "4")]
    ChildrenIsolatorUnion(ChildrenIsolatorUnionExecProto),
    #[prost(message, tag = "5")]
    NetworkBroadcast(NetworkBroadcastExecProto),
    #[prost(message, tag = "6")]
    Broadcast(BroadcastExecProto),
    #[prost(message, tag = "7")]
    Sampler(SamplerExecProto),
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

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChildrenIsolatorUnionExecProto {
    #[prost(uint64, tag = "1")]
    partition_count: u64,
    #[prost(message, repeated, tag = "2")]
    task_idx_map: Vec<TaskIdxMapEntryProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskIdxMapEntryProto {
    #[prost(message, repeated, tag = "1")]
    child_ctx: Vec<ChildIdxWithTaskContextProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChildIdxWithTaskContextProto {
    #[prost(uint64, tag = "1")]
    child_idx: u64,
    #[prost(uint64, tag = "2")]
    task_idx: u64,
    #[prost(uint64, tag = "3")]
    task_count: u64,
}

fn new_network_hash_shuffle_exec(
    partitioning: Partitioning,
    schema: SchemaRef,
    input_stage: Stage,
) -> NetworkShuffleExec {
    NetworkShuffleExec {
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
        worker_connections: WorkerConnectionPool::new(input_stage.task_count()),
        input_stage,
    }
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
    NetworkCoalesceExec {
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
        worker_connections: WorkerConnectionPool::new(input_stage.task_count()),
        input_stage,
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NetworkBroadcastExecProto {
    #[prost(message, optional, tag = "1")]
    schema: Option<protobuf::Schema>,
    #[prost(message, optional, tag = "2")]
    partitioning: Option<protobuf::Partitioning>,
    #[prost(message, optional, tag = "3")]
    input_stage: Option<StageProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BroadcastExecProto {
    #[prost(uint64, tag = "1")]
    pub consumer_task_count: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SamplerExecProto {}

fn new_network_broadcast_exec(
    partitioning: Partitioning,
    schema: SchemaRef,
    input_stage: Stage,
) -> NetworkBroadcastExec {
    NetworkBroadcastExec {
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
        worker_connections: WorkerConnectionPool::new(input_stage.task_count()),
        input_stage,
    }
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
        Stage::Remote(RemoteStage {
            query_id: Default::default(),
            num: 0,
            workers: vec![],
        })
    }

    fn dummy_stage_with_plan() -> Stage {
        Stage::Local(LocalStage {
            query_id: Default::default(),
            num: 0,
            plan: empty_exec(),
            tasks: 1,
        })
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

        let decoded = codec.try_decode(&buf, &[], &ctx)?;
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

        let plan: Arc<dyn ExecutionPlan> = Arc::new(PartitionIsolatorExec::new(flight.clone(), 1));

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
        let plan: Arc<dyn ExecutionPlan> = Arc::new(PartitionIsolatorExec::new(union.clone(), 1));

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

        let plan: Arc<dyn ExecutionPlan> = Arc::new(PartitionIsolatorExec::new(sort.clone(), 1));

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

        let decoded = codec.try_decode(&buf, &[], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_single_flight_with_plan() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("a");
        let part = Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 4);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(new_network_hash_shuffle_exec(
            part,
            schema,
            dummy_stage_with_plan(),
        ));

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[empty_exec()], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_single_flight_coalesce_with_plan() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("e");
        let plan: Arc<dyn ExecutionPlan> = Arc::new(new_network_coalesce_tasks_exec(
            Partitioning::RoundRobinBatch(3),
            schema,
            dummy_stage_with_plan(),
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

        let plan: Arc<dyn ExecutionPlan> = Arc::new(PartitionIsolatorExec::new(flight.clone(), 1));

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
        let plan: Arc<dyn ExecutionPlan> = Arc::new(PartitionIsolatorExec::new(union.clone(), 3));

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[union], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_children_isolator_union() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("h");
        let left = Arc::new(new_network_hash_shuffle_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
        )) as Arc<dyn ExecutionPlan>;
        let right = Arc::new(new_network_hash_shuffle_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
        )) as Arc<dyn ExecutionPlan>;

        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(ChildrenIsolatorUnionExec::from_children_and_task_counts(
                vec![left.clone(), right.clone()],
                vec![2, 2],
                4,
            )?);

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[left, right], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }
}
