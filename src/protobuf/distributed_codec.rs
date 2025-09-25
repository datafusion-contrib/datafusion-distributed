use super::get_distributed_user_codecs;
use crate::common::ComposedPhysicalExtensionCodec;
use crate::execution_plans::{NetworkCoalesceExec, NetworkCoalesceReady, NetworkShuffleReadyExec};
use crate::{NetworkShuffleExec, PartitionIsolatorExec};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::sync::Arc;

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
        registry: &dyn FunctionRegistry,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let DistributedExecProto {
            node: Some(distributed_exec_node),
        } = DistributedExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?
        else {
            return Err(proto_error(
                "Expected DistributedExecNode in DistributedExecProto",
            ));
        };

        match distributed_exec_node {
            DistributedExecNode::NetworkHashShuffle(NetworkShuffleExecProto {
                schema,
                partitioning,
                stage_num,
            }) => {
                let schema: Schema = schema
                    .as_ref()
                    .map(|s| s.try_into())
                    .ok_or(proto_error("NetworkShuffleExec is missing schema"))??;

                let partitioning = parse_protobuf_partitioning(
                    partitioning.as_ref(),
                    registry,
                    &schema,
                    &DistributedCodec {},
                )?
                .ok_or(proto_error("NetworkShuffleExec is missing partitioning"))?;

                Ok(Arc::new(new_network_hash_shuffle_exec(
                    partitioning,
                    Arc::new(schema),
                    stage_num as usize,
                )))
            }
            DistributedExecNode::NetworkCoalesceTasks(NetworkCoalesceExecProto {
                schema,
                partitioning,
                stage_num,
                input_tasks,
            }) => {
                let schema: Schema = schema
                    .as_ref()
                    .map(|s| s.try_into())
                    .ok_or(proto_error("NetworkCoalesceExec is missing schema"))??;

                let partitioning = parse_protobuf_partitioning(
                    partitioning.as_ref(),
                    registry,
                    &schema,
                    &DistributedCodec {},
                )?
                .ok_or(proto_error("NetworkCoalesceExec is missing partitioning"))?;

                Ok(Arc::new(new_network_coalesce_tasks_exec(
                    partitioning,
                    Arc::new(schema),
                    stage_num as usize,
                    input_tasks as usize,
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
        if let Some(node) = node.as_any().downcast_ref::<NetworkShuffleExec>() {
            let NetworkShuffleExec::Ready(ready_node) = node else {
                return Err(proto_error(
                    "deserialized an NetworkShuffleExec that is not ready",
                ));
            };
            let inner = NetworkShuffleExecProto {
                schema: Some(node.schema().try_into()?),
                partitioning: Some(serialize_partitioning(
                    node.properties().output_partitioning(),
                    &DistributedCodec {},
                )?),
                stage_num: ready_node.stage_num as u64,
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::NetworkHashShuffle(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else if let Some(node) = node.as_any().downcast_ref::<NetworkCoalesceExec>() {
            let NetworkCoalesceExec::Ready(ready_node) = node else {
                return Err(proto_error(
                    "deserialized an NetworkCoalesceExec that is not ready",
                ));
            };

            let inner = NetworkCoalesceExecProto {
                schema: Some(node.schema().try_into()?),
                partitioning: Some(serialize_partitioning(
                    node.properties().output_partitioning(),
                    &DistributedCodec {},
                )?),
                stage_num: ready_node.stage_num as u64,
                input_tasks: ready_node.input_tasks as u64,
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

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DistributedExecProto {
    #[prost(oneof = "DistributedExecNode", tags = "1, 2, 3")]
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
    #[prost(uint64, tag = "3")]
    stage_num: u64,
}

fn new_network_hash_shuffle_exec(
    partitioning: Partitioning,
    schema: SchemaRef,
    stage_num: usize,
) -> NetworkShuffleExec {
    NetworkShuffleExec::Ready(NetworkShuffleReadyExec {
        properties: PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ),
        stage_num,
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
    #[prost(uint64, tag = "3")]
    stage_num: u64,
    #[prost(uint64, tag = "4")]
    input_tasks: u64,
}

fn new_network_coalesce_tasks_exec(
    partitioning: Partitioning,
    schema: SchemaRef,
    stage_num: usize,
    input_tasks: usize,
) -> NetworkCoalesceExec {
    NetworkCoalesceExec::Ready(NetworkCoalesceReady {
        properties: PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ),
        stage_num,
        input_tasks,
        metrics_collection: Default::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::physical_expr::LexOrdering;
    use datafusion::{
        execution::registry::MemoryFunctionRegistry,
        physical_expr::{Partitioning, PhysicalSortExpr, expressions::Column, expressions::col},
        physical_plan::{ExecutionPlan, displayable, sorts::sort::SortExec, union::UnionExec},
    };

    fn schema_i32(name: &str) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]))
    }

    fn repr(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    #[test]
    fn test_roundtrip_single_flight() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let registry = MemoryFunctionRegistry::new();

        let schema = schema_i32("a");
        let part = Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 4);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(new_network_hash_shuffle_exec(part, schema, 0));

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[], &registry)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_isolator_flight() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let registry = MemoryFunctionRegistry::new();

        let schema = schema_i32("b");
        let flight = Arc::new(new_network_hash_shuffle_exec(
            Partitioning::UnknownPartitioning(1),
            schema,
            0,
        ));

        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionIsolatorExec::new_ready(flight.clone(), 1)?);

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[flight], &registry)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_isolator_union() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let registry = MemoryFunctionRegistry::new();

        let schema = schema_i32("c");
        let left = Arc::new(new_network_hash_shuffle_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            0,
        ));
        let right = Arc::new(new_network_hash_shuffle_exec(
            Partitioning::RoundRobinBatch(2),
            schema.clone(),
            1,
        ));

        let union = Arc::new(UnionExec::new(vec![left.clone(), right.clone()]));
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionIsolatorExec::new_ready(union.clone(), 1)?);

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let decoded = codec.try_decode(&buf, &[union], &registry)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }

    #[test]
    fn test_roundtrip_isolator_sort_flight() -> datafusion::common::Result<()> {
        let codec = DistributedCodec;
        let registry = MemoryFunctionRegistry::new();

        let schema = schema_i32("d");
        let flight = Arc::new(new_network_hash_shuffle_exec(
            Partitioning::UnknownPartitioning(1),
            schema.clone(),
            0,
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

        let decoded = codec.try_decode(&buf, &[sort], &registry)?;
        assert_eq!(repr(&plan), repr(&decoded));

        Ok(())
    }
}
