use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::{
    common::{internal_datafusion_err, internal_err, Result},
    execution::FunctionRegistry,
    physical_plan::{displayable, ExecutionPlan},
};
use datafusion_proto::{
    physical_plan::{
        from_proto::parse_protobuf_partitioning, to_proto::serialize_partitioning,
        DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
    },
    protobuf,
};
use prost::Message;

use crate::{
    analyze::{DistributedAnalyzeExec, DistributedAnalyzeRootExec},
    isolator::PartitionIsolatorExec,
    logging::trace,
    max_rows::MaxRowsExec,
    protobuf::{
        df_ray_exec_node::Payload, DfRayExecNode, DfRayStageReaderExecNode,
        DistributedAnalyzeExecNode, DistributedAnalyzeRootExecNode, MaxRowsExecNode,
        PartitionIsolatorExecNode, RecordBatchExecNode,
    },
    record_batch_exec::RecordBatchExec,
    stage_reader::DFRayStageReaderExec,
    util::{batch_to_ipc, ipc_to_batch},
};

#[derive(Debug)]
/// Physical Extension Codec for for DataFusion for Ray plans
pub struct DFRayCodec {}

impl PhysicalExtensionCodec for DFRayCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Ok(node) = DfRayExecNode::decode(buf) {
            let payload = node
                .payload
                .ok_or(internal_datafusion_err!("no payload when decoding proto"))?;

            match payload {
                Payload::StageReaderExec(node) => {
                    let schema: Schema = node
                        .schema
                        .as_ref()
                        .ok_or(internal_datafusion_err!("missing schema in proto"))?
                        .try_into()?;

                    let part = parse_protobuf_partitioning(
                        node.partitioning.as_ref(),
                        registry,
                        &schema,
                        &DefaultPhysicalExtensionCodec {},
                    )?
                    .ok_or(internal_datafusion_err!("missing partitioning in proto"))?;

                    Ok(Arc::new(DFRayStageReaderExec::try_new(
                        part,
                        Arc::new(schema),
                        node.stage_id,
                    )?))
                }
                Payload::MaxRowsExec(node) => {
                    if inputs.len() != 1 {
                        Err(internal_datafusion_err!(
                            "MaxRowsExec requires one input, got {}",
                            inputs.len()
                        ))
                    } else {
                        Ok(Arc::new(MaxRowsExec::new(
                            inputs[0].clone(),
                            node.max_rows as usize,
                        )))
                    }
                }
                Payload::IsolatorExec(node) => {
                    if inputs.len() != 1 {
                        Err(internal_datafusion_err!(
                            "PartitionIsolatorExec requires one input"
                        ))
                    } else {
                        Ok(Arc::new(PartitionIsolatorExec::new(
                            inputs[0].clone(),
                            node.partition_count as usize,
                        )))
                    }
                }
                Payload::DistributedAnalyzeExec(distributed_analyze_exec_node) => {
                    if inputs.len() != 1 {
                        Err(internal_datafusion_err!(
                            "DistributedAnalyzeExec requires one input"
                        ))
                    } else {
                        Ok(Arc::new(DistributedAnalyzeExec::new(
                            inputs[0].clone(),
                            distributed_analyze_exec_node.verbose,
                            distributed_analyze_exec_node.show_statistics,
                        )))
                    }
                }
                Payload::DistributedAnalyzeRootExec(distributed_analyze_root_exec_node) => {
                    if inputs.len() != 1 {
                        Err(internal_datafusion_err!(
                            "DistributedAnalyzeRootExec requires one input"
                        ))
                    } else {
                        Ok(Arc::new(DistributedAnalyzeRootExec::new(
                            inputs[0].clone(),
                            distributed_analyze_root_exec_node.verbose,
                            distributed_analyze_root_exec_node.show_statistics,
                        )))
                    }
                }
                Payload::RecordBatchExec(rb_exec) => {
                    // deserialize the record batch stored in the opaque bytes field
                    let batch = ipc_to_batch(&rb_exec.batch).map_err(|e| {
                        internal_datafusion_err!("Failed to decode RecordBatch: {:#?}", e)
                    })?;

                    Ok(Arc::new(RecordBatchExec::new(batch)))
                }
            }
        } else {
            internal_err!("cannot decode proto extension in dfray codec")
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        trace!(
            "try encoding node: {}",
            displayable(node.as_ref()).one_line()
        );

        let payload = if let Some(reader) = node.as_any().downcast_ref::<DFRayStageReaderExec>() {
            let schema: protobuf::Schema = reader.schema().try_into()?;
            let partitioning: protobuf::Partitioning = serialize_partitioning(
                reader.properties().output_partitioning(),
                &DefaultPhysicalExtensionCodec {},
            )?;

            let pb = DfRayStageReaderExecNode {
                schema: Some(schema),
                partitioning: Some(partitioning),
                stage_id: reader.stage_id,
            };

            Payload::StageReaderExec(pb)
        } else if let Some(pi) = node.as_any().downcast_ref::<PartitionIsolatorExec>() {
            let pb = PartitionIsolatorExecNode {
                partition_count: pi.partition_count as u64,
            };

            Payload::IsolatorExec(pb)
        } else if let Some(max) = node.as_any().downcast_ref::<MaxRowsExec>() {
            let pb = MaxRowsExecNode {
                max_rows: max.max_rows as u64,
            };
            Payload::MaxRowsExec(pb)
        } else if let Some(exec) = node.as_any().downcast_ref::<DistributedAnalyzeExec>() {
            let pb = DistributedAnalyzeExecNode {
                verbose: exec.verbose,
                show_statistics: exec.show_statistics,
            };
            Payload::DistributedAnalyzeExec(pb)
        } else if let Some(exec) = node.as_any().downcast_ref::<DistributedAnalyzeRootExec>() {
            let pb = DistributedAnalyzeRootExecNode {
                verbose: exec.verbose,
                show_statistics: exec.show_statistics,
            };
            Payload::DistributedAnalyzeRootExec(pb)
        } else if let Some(exec) = node.as_any().downcast_ref::<RecordBatchExec>() {
            let pb = RecordBatchExecNode {
                batch: batch_to_ipc(&exec.batch).map_err(|e| {
                    internal_datafusion_err!("Failed to encode RecordBatch: {:#?}", e)
                })?,
            };
            Payload::RecordBatchExec(pb)
        } else {
            return internal_err!("Not supported node to encode to proto");
        };

        let pb = DfRayExecNode {
            payload: Some(payload),
        };
        pb.encode(buf)
            .map_err(|e| internal_datafusion_err!("Failed to encode protobuf: {}", e))?;

        trace!(
            "DONE encoding node: {}",
            displayable(node.as_ref()).one_line()
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use datafusion::{
        physical_plan::{displayable, Partitioning},
        prelude::SessionContext,
    };
    use datafusion_proto::physical_plan::AsExecutionPlan;

    use super::*;
    use crate::{
        isolator::PartitionIsolatorExec, max_rows::MaxRowsExec, stage_reader::DFRayStageReaderExec,
    };

    fn create_test_schema() -> Arc<arrow::datatypes::Schema> {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", DataType::Int32, false),
            arrow::datatypes::Field::new("b", DataType::Int32, false),
        ]))
    }

    fn verify_round_trip(exec: Arc<dyn ExecutionPlan>) {
        let ctx = SessionContext::new();
        let codec = DFRayCodec {};

        // serialize execution plan to proto
        let proto: protobuf::PhysicalPlanNode =
            protobuf::PhysicalPlanNode::try_from_physical_plan(exec.clone(), &codec)
                .expect("to proto");

        // deserialize proto back to execution plan
        let runtime = ctx.runtime_env();
        let result_exec_plan: Arc<dyn ExecutionPlan> = proto
            .try_into_physical_plan(&ctx, runtime.as_ref(), &codec)
            .expect("from proto");

        let input = displayable(exec.as_ref())
            .set_show_schema(true)
            .indent(true)
            .to_string();
        let round_trip = displayable(result_exec_plan.as_ref())
            .set_show_schema(true)
            .indent(true)
            .to_string();

        assert_eq!(input, round_trip);
    }

    #[test]
    fn stage_reader_round_trip() {
        let schema = create_test_schema();
        let part = Partitioning::UnknownPartitioning(2);
        let exec = Arc::new(DFRayStageReaderExec::try_new(part, schema, 1).unwrap());
        let codec = DFRayCodec {};
        let mut buf = vec![];
        codec.try_encode(exec.clone(), &mut buf).unwrap();
        let ctx = SessionContext::new();
        let decoded = codec.try_decode(&buf, &[], &ctx).unwrap();
        assert_eq!(exec.schema(), decoded.schema());
    }

    #[test]
    fn max_rows_round_trip() {
        let schema = create_test_schema();
        let part = Partitioning::UnknownPartitioning(2);
        let reader_exec = Arc::new(DFRayStageReaderExec::try_new(part, schema, 1).unwrap());
        let exec = Arc::new(MaxRowsExec::new(reader_exec, 10));

        verify_round_trip(exec);
    }

    #[test]
    fn partition_isolator_round_trip() {
        let schema = create_test_schema();
        let part = Partitioning::UnknownPartitioning(2);
        let reader_exec = Arc::new(DFRayStageReaderExec::try_new(part, schema, 1).unwrap());
        let exec = Arc::new(PartitionIsolatorExec::new(reader_exec, 4));

        verify_round_trip(exec);
    }

    #[test]
    fn max_rows_and_reader_round_trip() {
        let schema = create_test_schema();
        let part = Partitioning::UnknownPartitioning(2);
        let exec = Arc::new(MaxRowsExec::new(
            Arc::new(DFRayStageReaderExec::try_new(part, schema, 1).unwrap()),
            10,
        ));

        verify_round_trip(exec);
    }
}
