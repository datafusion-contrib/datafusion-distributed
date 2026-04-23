use super::get_distributed_user_codecs;
use crate::common::{deserialize_uuid, serialize_uuid};
use crate::distributed_planner::{
    BroadcastExchangeLayout, CoalesceExchangeLayout, ExchangeLayout, ShuffleExchangeLayout,
};
use crate::execution_plans::{
    BroadcastExec, ChildrenIsolatorUnionExec, LocalExchangeSplitExec, NetworkBroadcastExec,
    NetworkCoalesceExec,
};
use crate::stage::{ExecutionTask, Stage};
use crate::worker::WorkerConnectionPool;
use crate::{DistributedTaskContext, NetworkBoundary};
use crate::{NetworkShuffleExec, PartitionIsolatorExec};
use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, internal_datafusion_err};
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

            Ok(Stage {
                query_id: deserialize_uuid(proto.query_id.as_ref())?,
                num: proto.num as usize,
                plan: inputs.first().cloned(),
                tasks: decode_tasks(proto.tasks)?,
            })
        }

        fn parse_exchange_layout_proto(
            proto: Option<ExchangeAssignmentProto>,
            ctx: &TaskContext,
            schema: &Schema,
        ) -> Result<Arc<ExchangeLayout>, DataFusionError> {
            let Some(proto) = proto else {
                return Err(proto_error("Empty ExchangeAssignmentProto"));
            };

            match proto.assignment {
                Some(exchange_assignment_proto::Assignment::Shuffle(shuffle)) => {
                    let producer_partitioning = parse_protobuf_partitioning(
                        shuffle.producer_partitioning.as_ref(),
                        ctx,
                        schema,
                        &DistributedCodec {},
                        &DefaultPhysicalProtoConverter,
                    )?
                    .ok_or(proto_error(
                        "ShuffleExchangeAssignmentProto is missing producer_partitioning",
                    ))?;

                    let consumer_partition_ranges =
                        decode_ranges(shuffle.consumer_partition_ranges)?;
                    let consumer_task_count = consumer_partition_ranges.len();
                    if consumer_task_count == 0 {
                        return Err(proto_error(
                            "ShuffleExchangeAssignmentProto requires at least one consumer range",
                        ));
                    }

                    Ok(Arc::new(ExchangeLayout::Shuffle(ShuffleExchangeLayout {
                        producer_task_count: shuffle.producer_task_count as usize,
                        consumer_task_count,
                        producer_partitioning,
                        consumer_partition_ranges,
                    })))
                }
                Some(exchange_assignment_proto::Assignment::Coalesce(coalesce)) => {
                    let producer_task_ranges = decode_ranges(coalesce.producer_task_ranges)?;
                    let consumer_slot_ranges = decode_ranges(coalesce.consumer_slot_ranges)?;
                    if producer_task_ranges.len() != consumer_slot_ranges.len() {
                        return Err(proto_error(format!(
                            "CoalesceExchangeAssignmentProto range length mismatch: producer_task_ranges={} consumer_slot_ranges={}",
                            producer_task_ranges.len(),
                            consumer_slot_ranges.len()
                        )));
                    }
                    let consumer_task_count = consumer_slot_ranges.len();
                    if consumer_task_count == 0 {
                        return Err(proto_error(
                            "CoalesceExchangeAssignmentProto requires at least one consumer range",
                        ));
                    }

                    Ok(Arc::new(ExchangeLayout::Coalesce(CoalesceExchangeLayout {
                        producer_task_count: coalesce.producer_task_count as usize,
                        consumer_task_count,
                        partitions_per_producer_task: coalesce.partitions_per_producer_task
                            as usize,
                        producer_task_ranges,
                        consumer_slot_ranges,
                    })))
                }
                Some(exchange_assignment_proto::Assignment::Broadcast(broadcast)) => {
                    let consumer_partition_ranges =
                        decode_ranges(broadcast.consumer_partition_ranges)?;
                    let consumer_task_count = consumer_partition_ranges.len();
                    if consumer_task_count == 0 {
                        return Err(proto_error(
                            "BroadcastExchangeAssignmentProto requires at least one consumer range",
                        ));
                    }

                    Ok(Arc::new(ExchangeLayout::Broadcast(
                        BroadcastExchangeLayout {
                            producer_task_count: broadcast.producer_task_count as usize,
                            consumer_task_count,
                            partitions_per_producer_task: broadcast.partitions_per_producer_task
                                as usize,
                            consumer_partition_ranges,
                        },
                    )))
                }
                None => Err(proto_error("Empty ExchangeAssignmentProto.assignment")),
            }
        }

        match distributed_exec_node {
            DistributedExecNode::NetworkHashShuffle(NetworkShuffleExecProto {
                schema,
                partitioning,
                input_stage,
                assignment,
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

                let layout = parse_exchange_layout_proto(assignment, ctx, &schema)?;
                Ok(Arc::new(new_network_hash_shuffle_exec(
                    partitioning,
                    Arc::new(schema),
                    parse_stage_proto(input_stage, inputs)?,
                    layout,
                )?))
            }
            DistributedExecNode::NetworkCoalesceTasks(NetworkCoalesceExecProto {
                schema,
                partitioning,
                input_stage,
                assignment,
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

                let layout = parse_exchange_layout_proto(assignment, ctx, &schema)?;
                Ok(Arc::new(new_network_coalesce_tasks_exec(
                    partitioning,
                    Arc::new(schema),
                    parse_stage_proto(input_stage, inputs)?,
                    layout,
                )?))
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
                assignment,
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

                let layout = parse_exchange_layout_proto(assignment, ctx, &schema)?;
                Ok(Arc::new(new_network_broadcast_exec(
                    partitioning,
                    Arc::new(schema),
                    parse_stage_proto(input_stage, inputs)?,
                    layout,
                )?))
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
            DistributedExecNode::LocalExchangeSplit(LocalExchangeSplitExecProto {
                partitioning,
                base_partition_count,
                local_partition_count,
            }) => {
                if inputs.len() != 1 {
                    return Err(proto_error(format!(
                        "LocalExchangeSplitExec expects exactly one child, got {}",
                        inputs.len()
                    )));
                }

                let child = inputs.first().unwrap();
                let schema = child.schema();
                let partitioning = parse_protobuf_partitioning(
                    partitioning.as_ref(),
                    ctx,
                    schema.as_ref(),
                    &DistributedCodec {},
                    &DefaultPhysicalProtoConverter,
                )?
                .ok_or(proto_error(
                    "LocalExchangeSplitExec is missing partitioning",
                ))?;
                let Partitioning::Hash(hash_exprs, _) = partitioning else {
                    return Err(proto_error(
                        "LocalExchangeSplitExec requires hash partitioning metadata",
                    ));
                };

                Ok(Arc::new(LocalExchangeSplitExec::try_new(
                    child.clone(),
                    hash_exprs,
                    base_partition_count as usize,
                    local_partition_count as usize,
                )?))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        fn encode_stage_proto(stage: &Stage) -> Result<StageProto, DataFusionError> {
            Ok(StageProto {
                query_id: serialize_uuid(&stage.query_id).into(),
                num: stage.num as u64,
                tasks: encode_tasks(&stage.tasks),
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
                assignment: Some(encode_exchange_layout_proto(node.layout().as_ref())?),
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
                assignment: Some(encode_exchange_layout_proto(node.layout().as_ref())?),
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
                assignment: Some(encode_exchange_layout_proto(node.layout().as_ref())?),
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
        } else if let Some(node) = node.as_any().downcast_ref::<LocalExchangeSplitExec>() {
            let inner = LocalExchangeSplitExecProto {
                partitioning: Some(serialize_partitioning(
                    node.properties().output_partitioning(),
                    &DistributedCodec {},
                    &DefaultPhysicalProtoConverter,
                )?),
                base_partition_count: node.base_partition_count() as u64,
                local_partition_count: node.local_partition_count() as u64,
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::LocalExchangeSplit(inner)),
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
    LocalExchangeSplit(LocalExchangeSplitExecProto),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionIsolatorExecProto {
    #[prost(uint64, tag = "1")]
    pub n_tasks: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocalExchangeSplitExecProto {
    #[prost(message, optional, tag = "1")]
    partitioning: Option<protobuf::Partitioning>,
    #[prost(uint64, tag = "2")]
    base_partition_count: u64,
    #[prost(uint64, tag = "3")]
    local_partition_count: u64,
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
    #[prost(message, optional, tag = "4")]
    assignment: Option<ExchangeAssignmentProto>,
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

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionRangeProto {
    #[prost(uint64, tag = "1")]
    start: u64,
    #[prost(uint64, tag = "2")]
    end: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExchangeAssignmentProto {
    #[prost(oneof = "exchange_assignment_proto::Assignment", tags = "1, 2, 3")]
    assignment: Option<exchange_assignment_proto::Assignment>,
}

pub mod exchange_assignment_proto {
    use super::{
        BroadcastExchangeAssignmentProto, CoalesceExchangeAssignmentProto,
        ShuffleExchangeAssignmentProto,
    };

    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Assignment {
        #[prost(message, tag = "1")]
        Shuffle(ShuffleExchangeAssignmentProto),
        #[prost(message, tag = "2")]
        Coalesce(CoalesceExchangeAssignmentProto),
        #[prost(message, tag = "3")]
        Broadcast(BroadcastExchangeAssignmentProto),
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleExchangeAssignmentProto {
    #[prost(uint64, tag = "1")]
    producer_task_count: u64,
    #[prost(message, optional, tag = "2")]
    producer_partitioning: Option<protobuf::Partitioning>,
    #[prost(message, repeated, tag = "3")]
    consumer_partition_ranges: Vec<PartitionRangeProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CoalesceExchangeAssignmentProto {
    #[prost(uint64, tag = "1")]
    producer_task_count: u64,
    #[prost(uint64, tag = "2")]
    partitions_per_producer_task: u64,
    #[prost(message, repeated, tag = "3")]
    producer_task_ranges: Vec<PartitionRangeProto>,
    #[prost(message, repeated, tag = "4")]
    consumer_slot_ranges: Vec<PartitionRangeProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BroadcastExchangeAssignmentProto {
    #[prost(uint64, tag = "1")]
    producer_task_count: u64,
    #[prost(uint64, tag = "2")]
    partitions_per_producer_task: u64,
    #[prost(message, repeated, tag = "3")]
    consumer_partition_ranges: Vec<PartitionRangeProto>,
}

fn new_network_hash_shuffle_exec(
    partitioning: Partitioning,
    schema: SchemaRef,
    input_stage: Stage,
    layout: Arc<ExchangeLayout>,
) -> Result<NetworkShuffleExec> {
    Ok(NetworkShuffleExec {
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
        worker_connections: WorkerConnectionPool::new(input_stage.tasks.len()),
        input_stage,
        metrics_collection: Default::default(),
        layout,
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
    #[prost(message, optional, tag = "4")]
    assignment: Option<ExchangeAssignmentProto>,
}

fn new_network_coalesce_tasks_exec(
    partitioning: Partitioning,
    schema: SchemaRef,
    input_stage: Stage,
    layout: Arc<ExchangeLayout>,
) -> Result<NetworkCoalesceExec> {
    Ok(NetworkCoalesceExec {
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
        worker_connections: WorkerConnectionPool::new(input_stage.tasks.len()),
        input_stage,
        metrics_collection: Default::default(),
        layout,
    })
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NetworkBroadcastExecProto {
    #[prost(message, optional, tag = "1")]
    schema: Option<protobuf::Schema>,
    #[prost(message, optional, tag = "2")]
    partitioning: Option<protobuf::Partitioning>,
    #[prost(message, optional, tag = "3")]
    input_stage: Option<StageProto>,
    #[prost(message, optional, tag = "4")]
    assignment: Option<ExchangeAssignmentProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BroadcastExecProto {
    #[prost(uint64, tag = "1")]
    pub consumer_task_count: u64,
}

fn new_network_broadcast_exec(
    partitioning: Partitioning,
    schema: SchemaRef,
    input_stage: Stage,
    layout: Arc<ExchangeLayout>,
) -> Result<NetworkBroadcastExec> {
    Ok(NetworkBroadcastExec {
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
        worker_connections: WorkerConnectionPool::new(input_stage.tasks.len()),
        input_stage,
        metrics_collection: Default::default(),
        layout,
    })
}

fn encode_ranges(ranges: &[std::ops::Range<usize>]) -> Vec<PartitionRangeProto> {
    ranges
        .iter()
        .map(|range| PartitionRangeProto {
            start: range.start as u64,
            end: range.end as u64,
        })
        .collect()
}

fn decode_ranges(ranges: Vec<PartitionRangeProto>) -> Result<Vec<std::ops::Range<usize>>> {
    ranges
        .into_iter()
        .map(|range| {
            let start = range.start as usize;
            let end = range.end as usize;
            if start > end {
                return Err(proto_error(format!(
                    "Invalid partition range start={} end={}",
                    start, end
                )));
            }
            Ok(start..end)
        })
        .collect()
}

fn encode_exchange_layout_proto(
    layout: &ExchangeLayout,
) -> Result<ExchangeAssignmentProto, DataFusionError> {
    let assignment = match layout {
        ExchangeLayout::Shuffle(assignment) => {
            exchange_assignment_proto::Assignment::Shuffle(ShuffleExchangeAssignmentProto {
                producer_task_count: assignment.producer_task_count as u64,
                producer_partitioning: Some(serialize_partitioning(
                    &assignment.producer_partitioning,
                    &DistributedCodec {},
                    &DefaultPhysicalProtoConverter,
                )?),
                consumer_partition_ranges: encode_ranges(&assignment.consumer_partition_ranges),
            })
        }
        ExchangeLayout::Coalesce(assignment) => {
            exchange_assignment_proto::Assignment::Coalesce(CoalesceExchangeAssignmentProto {
                producer_task_count: assignment.producer_task_count as u64,
                partitions_per_producer_task: assignment.partitions_per_producer_task as u64,
                producer_task_ranges: encode_ranges(&assignment.producer_task_ranges),
                consumer_slot_ranges: encode_ranges(&assignment.consumer_slot_ranges),
            })
        }
        ExchangeLayout::Broadcast(assignment) => {
            exchange_assignment_proto::Assignment::Broadcast(BroadcastExchangeAssignmentProto {
                producer_task_count: assignment.producer_task_count as u64,
                partitions_per_producer_task: assignment.partitions_per_producer_task as u64,
                consumer_partition_ranges: encode_ranges(&assignment.consumer_partition_ranges),
            })
        }
    };

    Ok(ExchangeAssignmentProto {
        assignment: Some(assignment),
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
            plan: None,
            tasks: vec![],
        }
    }

    fn dummy_stage_with_plan() -> Stage {
        Stage {
            query_id: Default::default(),
            num: 0,
            plan: Some(empty_exec()),
            tasks: vec![],
        }
    }

    fn shuffle_exec(
        partitioning: Partitioning,
        schema: SchemaRef,
        input_stage: Stage,
        consumer_task_count: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let layout = ExchangeLayout::try_shuffle(
            partitioning.clone(),
            input_stage.tasks.len(),
            consumer_task_count,
        )
        .unwrap();
        Arc::new(new_network_hash_shuffle_exec(partitioning, schema, input_stage, layout).unwrap())
    }

    fn coalesce_exec(
        partitioning: Partitioning,
        schema: SchemaRef,
        input_stage: Stage,
        consumer_task_count: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let layout = ExchangeLayout::try_coalesce(
            input_stage.tasks.len(),
            consumer_task_count,
            partitioning.partition_count(),
        )
        .unwrap();
        Arc::new(
            new_network_coalesce_tasks_exec(partitioning, schema, input_stage, layout).unwrap(),
        )
    }

    fn broadcast_exec(
        partitioning: Partitioning,
        schema: SchemaRef,
        input_stage: Stage,
        consumer_task_count: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let layout = ExchangeLayout::try_broadcast(
            input_stage.tasks.len(),
            consumer_task_count,
            partitioning.partition_count() * consumer_task_count,
        )
        .unwrap();
        Arc::new(new_network_broadcast_exec(partitioning, schema, input_stage, layout).unwrap())
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
        let plan = shuffle_exec(part, schema, dummy_stage(), 1);

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
        let flight = shuffle_exec(
            Partitioning::UnknownPartitioning(1),
            schema,
            dummy_stage(),
            1,
        );

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
        let left = shuffle_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
            1,
        );
        let right = shuffle_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
            1,
        );

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
        let flight = shuffle_exec(
            Partitioning::UnknownPartitioning(1),
            schema.clone(),
            dummy_stage(),
            1,
        );

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
        let plan = coalesce_exec(Partitioning::RoundRobinBatch(3), schema, dummy_stage(), 1);

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_single_flight_broadcast() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("b");
        let plan = broadcast_exec(
            Partitioning::UnknownPartitioning(3),
            schema,
            dummy_stage(),
            4,
        );

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
        let plan = shuffle_exec(part, schema, dummy_stage_with_plan(), 1);

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[empty_exec()], &ctx)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_single_flight_broadcast_with_plan() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let ctx = create_context();

        let schema = schema_i32("b");
        let plan = broadcast_exec(
            Partitioning::UnknownPartitioning(3),
            schema,
            dummy_stage_with_plan(),
            4,
        );

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
        let plan = coalesce_exec(
            Partitioning::RoundRobinBatch(3),
            schema,
            dummy_stage_with_plan(),
            1,
        );

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
        let flight = coalesce_exec(
            Partitioning::UnknownPartitioning(1),
            schema,
            dummy_stage(),
            1,
        );

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
        let left = coalesce_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
            1,
        );
        let right = coalesce_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
            1,
        );

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
        let left = shuffle_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
            1,
        );
        let right = shuffle_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            dummy_stage(),
            1,
        );

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
