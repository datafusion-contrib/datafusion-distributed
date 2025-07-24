//! Serialization and deserialization codec for distributed DataFusion execution plans
//!
//! This module provides the `DDCodec` implementation that enables serialization and
//! deserialization of custom DataFusion physical execution plans used in distributed
//! query processing. The codec extends DataFusion's standard serialization capabilities
//! to support distributed-specific execution plan nodes.
//!
//! # Purpose
//!
//! In distributed DataFusion, query execution plans need to be serialized and sent
//! across the network to worker nodes. The standard DataFusion codec doesn't know
//! about our custom execution plan nodes, so this codec provides that capability.
//!
//! # Supported Execution Plan Nodes
//!
//! The codec can serialize and deserialize the following custom nodes:
//! - **`DDStageReaderExec`**: Reads data from remote worker stages
//! - **`PartitionIsolatorExec`**: Isolates specific partitions for distributed execution
//! - **`MaxRowsExec`**: Limits the number of rows returned from a query
//! - **`DistributedAnalyzeExec`**: Distributed query analysis and profiling
//! - **`DistributedAnalyzeRootExec`**: Root node for distributed analysis trees
//! - **`RecordBatchExec`**: Executes pre-computed record batches (for local results)
//!
//! # Protocol Integration
//!
//! Uses Protocol Buffers (protobuf) for efficient binary serialization with:
//! - **Compact encoding**: Minimizes network transfer overhead
//! - **Schema evolution**: Supports backward/forward compatibility
//! - **Type safety**: Ensures correct deserialization of complex plan trees
//! - **Cross-language support**: Compatible with other protobuf implementations
//!
//! # Delegation Pattern
//!
//! The codec follows a delegation pattern where:
//! 1. **Custom nodes**: Handled directly by this codec
//! 2. **Standard nodes**: Delegated to DataFusion's default codec
//! 3. **Fallback**: Graceful error handling for unknown node types
//!
//! # Usage in Distributed Execution
//!
//! This codec is used throughout the distributed system:
//! - **Task Distribution**: Serializing plans to send to workers
//! - **Result Aggregation**: Deserializing partial results from workers  
//! - **Plan Caching**: Storing serialized plans for reuse
//! - **Network Transfer**: Minimizing data transfer overhead
//!
//! # Example Integration
//!
//! ```rust,ignore
//! use distributed_datafusion::codec::DDCodec;
//! use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
//!
//! // Create codec with default DataFusion codec as fallback
//! let codec = DDCodec::new(Arc::new(DefaultPhysicalExtensionCodec {}));
//!
//! // Serialize a custom execution plan
//! let mut buf = Vec::new();
//! codec.try_encode(execution_plan, &mut buf)?;
//!
//! // Send buf over network...
//!
//! // Deserialize on worker node
//! let decoded_plan = codec.try_decode(&buf, &inputs, &registry)?;
//! ```

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
        dd_exec_node::Payload, DdExecNode, DdStageReaderExecNode, DistributedAnalyzeExecNode,
        DistributedAnalyzeRootExecNode, MaxRowsExecNode, PartitionIsolatorExecNode,
        RecordBatchExecNode,
    },
    record_batch_exec::RecordBatchExec,
    stage_reader::DDStageReaderExec,
    util::{batch_to_ipc, ipc_to_batch},
};

#[derive(Debug)]
pub struct DDCodec {
    /// Fallback codec for standard DataFusion execution plan nodes
    ///
    /// This sub-codec handles serialization of standard DataFusion nodes that
    /// are not specific to distributed execution. Typically set to
    /// `DefaultPhysicalExtensionCodec` to provide full DataFusion compatibility.
    sub_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl DDCodec {
    pub fn new(sub_codec: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self { sub_codec }
    }
}

impl Default for DDCodec {
    fn default() -> Self {
        Self::new(Arc::new(DefaultPhysicalExtensionCodec {}))
    }
}

impl PhysicalExtensionCodec for DDCodec {
    /// Deserializes binary data into a distributed DataFusion execution plan
    ///
    /// This method attempts to decode protobuf-serialized execution plan data into
    /// concrete execution plan objects. It handles both custom distributed nodes
    /// and standard DataFusion nodes through delegation.
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // First, try to decode as a custom distributed DataFusion node
        if let Ok(node) = DdExecNode::decode(buf) {
            // Extract the payload containing the specific node type
            let payload = node
                .payload
                .ok_or(internal_datafusion_err!("no payload when decoding proto"))?;

            match payload {
                // Reconstruct DDStageReaderExec from protobuf data
                Payload::StageReaderExec(node) => {
                    // Deserialize schema information
                    let schema: Schema = node
                        .schema
                        .as_ref()
                        .ok_or(internal_datafusion_err!("missing schema in proto"))?
                        .try_into()?;

                    // Deserialize partitioning information
                    let part = parse_protobuf_partitioning(
                        node.partitioning.as_ref(),
                        registry,
                        &schema,
                        &DefaultPhysicalExtensionCodec {},
                    )?
                    .ok_or(internal_datafusion_err!("missing partitioning in proto"))?;

                    // Create the stage reader with reconstructed metadata
                    Ok(Arc::new(DDStageReaderExec::try_new(
                        part,
                        Arc::new(schema),
                        node.stage_id,
                    )?))
                }
                // Reconstruct MaxRowsExec with row limit configuration
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
                // Reconstruct PartitionIsolatorExec with partition count
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
                // Reconstruct DistributedAnalyzeExec with analysis configuration
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
                // Reconstruct DistributedAnalyzeRootExec with root analysis configuration
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
                // Reconstruct RecordBatchExec with pre-computed data
                Payload::RecordBatchExec(rb_exec) => {
                    // Deserialize the record batch from IPC format
                    let batch = ipc_to_batch(&rb_exec.batch).map_err(|e| {
                        internal_datafusion_err!("Failed to decode RecordBatch: {:#?}", e)
                    })?;

                    Ok(Arc::new(RecordBatchExec::new(batch)))
                }
            }
        } else if let Ok(ext) = self.sub_codec.try_decode(buf, inputs, registry) {
            // Delegate to sub-codec for standard DataFusion nodes
            trace!(
                "Delegated decoding to sub codec for node: {}",
                displayable(ext.as_ref()).one_line()
            );
            Ok(ext)
        } else {
            // Neither custom nor sub-codec could handle the data
            internal_err!("cannot decode proto extension in distributed datafusion codec")
        }
    }

    /// Serializes a distributed DataFusion execution plan into binary protobuf data
    ///
    /// This method attempts to serialize execution plan objects into protobuf format
    /// for network transfer or storage. It handles both custom distributed nodes
    /// and standard DataFusion nodes through delegation.
    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        trace!(
            "try encoding node: {}",
            displayable(node.as_ref()).one_line()
        );

        // Determine the appropriate payload based on node type
        let payload = if let Some(reader) = node.as_any().downcast_ref::<DDStageReaderExec>() {
            // Serialize DDStageReaderExec with schema and partitioning
            let schema: protobuf::Schema = reader.schema().try_into()?;
            let partitioning: protobuf::Partitioning = serialize_partitioning(
                reader.properties().output_partitioning(),
                &DefaultPhysicalExtensionCodec {},
            )?;

            let pb = DdStageReaderExecNode {
                schema: Some(schema),
                partitioning: Some(partitioning),
                stage_id: reader.stage_id,
            };

            Some(Payload::StageReaderExec(pb))
        } else if let Some(pi) = node.as_any().downcast_ref::<PartitionIsolatorExec>() {
            // Serialize PartitionIsolatorExec with partition count
            let pb = PartitionIsolatorExecNode {
                partition_count: pi.partition_count as u64,
            };

            Some(Payload::IsolatorExec(pb))
        } else if let Some(max) = node.as_any().downcast_ref::<MaxRowsExec>() {
            // Serialize MaxRowsExec with row limit
            let pb = MaxRowsExecNode {
                max_rows: max.max_rows as u64,
            };
            Some(Payload::MaxRowsExec(pb))
        } else if let Some(exec) = node.as_any().downcast_ref::<DistributedAnalyzeExec>() {
            // Serialize DistributedAnalyzeExec with analysis configuration
            let pb = DistributedAnalyzeExecNode {
                verbose: exec.verbose,
                show_statistics: exec.show_statistics,
            };
            Some(Payload::DistributedAnalyzeExec(pb))
        } else if let Some(exec) = node.as_any().downcast_ref::<DistributedAnalyzeRootExec>() {
            // Serialize DistributedAnalyzeRootExec with root analysis configuration
            let pb = DistributedAnalyzeRootExecNode {
                verbose: exec.verbose,
                show_statistics: exec.show_statistics,
            };
            Some(Payload::DistributedAnalyzeRootExec(pb))
        } else if let Some(exec) = node.as_any().downcast_ref::<RecordBatchExec>() {
            // Serialize RecordBatchExec with IPC-encoded batch data
            let pb = RecordBatchExecNode {
                batch: batch_to_ipc(&exec.batch).map_err(|e| {
                    internal_datafusion_err!("Failed to encode RecordBatch: {:#?}", e)
                })?,
            };
            Some(Payload::RecordBatchExec(pb))
        } else {
            // Node is not a custom distributed type, will delegate to sub-codec
            trace!(
                "Node {} is not a custom DDExecNode, delegating to sub codec",
                displayable(node.as_ref()).one_line()
            );
            None
        };

        match payload {
            Some(payload) => {
                // Encode custom node as DdExecNode protobuf message
                let pb = DdExecNode {
                    payload: Some(payload),
                };
                pb.encode(buf)
                    .map_err(|e| internal_datafusion_err!("Failed to encode protobuf: {:#?}", e))
            }
            None => {
                // Delegate to sub-codec for standard DataFusion nodes
                self.sub_codec.try_encode(node, buf)
            }
        }
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
        isolator::PartitionIsolatorExec, max_rows::MaxRowsExec, stage_reader::DDStageReaderExec,
    };

    fn create_test_schema() -> Arc<arrow::datatypes::Schema> {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", DataType::Int32, false),
            arrow::datatypes::Field::new("b", DataType::Int32, false),
        ]))
    }

    fn verify_round_trip(exec: Arc<dyn ExecutionPlan>) {
        let ctx = SessionContext::new();
        let codec = DDCodec::new(Arc::new(DefaultPhysicalExtensionCodec {}));

        // Serialize execution plan to protobuf format
        let proto: protobuf::PhysicalPlanNode =
            protobuf::PhysicalPlanNode::try_from_physical_plan(exec.clone(), &codec)
                .expect("to proto");

        // Deserialize protobuf data back to execution plan
        let runtime = ctx.runtime_env();
        let result_exec_plan: Arc<dyn ExecutionPlan> = proto
            .try_into_physical_plan(&ctx, runtime.as_ref(), &codec)
            .expect("from proto");

        // Generate detailed string representations for comparison
        let input = displayable(exec.as_ref())
            .set_show_schema(true)
            .indent(true)
            .to_string();
        let round_trip = displayable(result_exec_plan.as_ref())
            .set_show_schema(true)
            .indent(true)
            .to_string();

        // Verify exact match between original and round-trip plans
        assert_eq!(input, round_trip);
    }

    #[test]
    fn stage_reader_round_trip() {
        let schema = create_test_schema();
        let part = Partitioning::UnknownPartitioning(2);
        let exec = Arc::new(DDStageReaderExec::try_new(part, schema, 1).unwrap());
        let codec = DDCodec::new(Arc::new(DefaultPhysicalExtensionCodec {}));
        let mut buf = vec![];

        // Test direct codec encode/decode
        codec.try_encode(exec.clone(), &mut buf).unwrap();
        let ctx = SessionContext::new();
        let decoded = codec.try_decode(&buf, &[], &ctx).unwrap();

        // Verify schema preservation
        assert_eq!(exec.schema(), decoded.schema());
    }

    #[test]
    fn max_rows_round_trip() {
        let schema = create_test_schema();
        let part = Partitioning::UnknownPartitioning(2);
        let reader_exec = Arc::new(DDStageReaderExec::try_new(part, schema, 1).unwrap());
        let exec = Arc::new(MaxRowsExec::new(reader_exec, 10));

        verify_round_trip(exec);
    }

    #[test]
    fn partition_isolator_round_trip() {
        let schema = create_test_schema();
        let part = Partitioning::UnknownPartitioning(2);
        let reader_exec = Arc::new(DDStageReaderExec::try_new(part, schema, 1).unwrap());
        let exec = Arc::new(PartitionIsolatorExec::new(reader_exec, 4));

        verify_round_trip(exec);
    }

    #[test]
    fn max_rows_and_reader_round_trip() {
        let schema = create_test_schema();
        let part = Partitioning::UnknownPartitioning(2);
        let exec = Arc::new(MaxRowsExec::new(
            Arc::new(DDStageReaderExec::try_new(part, schema, 1).unwrap()),
            10,
        ));

        verify_round_trip(exec);
    }
}
