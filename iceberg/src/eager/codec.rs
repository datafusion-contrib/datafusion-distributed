use crate::EagerIcebergDataSource;
use crate::common::scan_config::ScanConfig;
use crate::eager::planned_files::PlannedFiles;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, Statistics, exec_datafusion_err, internal_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::proto_error;
use iceberg::io::{FileIOBuilder, StorageFactory};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use prost::Message;

use crate::proto::generated::iceberg as pb;

/// Physical plan codec for [`EagerIcebergDataSource`].
#[derive(Debug, Clone)]
pub struct EagerIcebergCodec {
    storage_factory: Arc<dyn StorageFactory>,
    iceberg_runtime: iceberg::Runtime,
}

impl EagerIcebergCodec {
    /// Creates a codec using the storage and runtime configured on this process.
    pub fn new(
        storage_factory: Arc<dyn StorageFactory>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Self {
        Self {
            storage_factory,
            iceberg_runtime,
        }
    }
}

impl Default for EagerIcebergCodec {
    fn default() -> Self {
        Self::new(
            Arc::new(OpenDalResolvingStorageFactory::new()),
            iceberg::Runtime::current(),
        )
    }
}

impl PhysicalExtensionCodec for EagerIcebergCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "EagerIcebergDataSource should have no children, got {}",
                inputs.len()
            );
        }
        let source = pb::AssignedScan::decode(buf).map_err(|error| {
            proto_error(format!("failed to decode EagerIcebergDataSource: {error}"))
        })?;
        let scan = self.decode_config(source.config, ctx, proto_converter)?;
        if source.partitions.is_empty()
            || source.partitions.len() != scan.partitioning.partition_count()
        {
            return internal_err!("Iceberg assigned files do not match scan partitioning");
        }
        let groups = source
            .partitions
            .into_iter()
            .map(|group| {
                group
                    .tasks
                    .into_iter()
                    .map(|bytes| {
                        rmp_serde::from_slice(&bytes).map_err(|error| {
                            exec_datafusion_err!("Invalid assigned Iceberg file: {error}")
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let mut tasks = Vec::new();
        let partitions = groups
            .into_iter()
            .map(|group| {
                let start = tasks.len();
                tasks.extend(group);
                (start..tasks.len()).collect()
            })
            .collect::<Vec<_>>();
        let files = Arc::new(PlannedFiles::from_tasks(tasks, ctx)?);
        Ok(DataSourceExec::from_data_source(EagerIcebergDataSource {
            statistics: Arc::new(Statistics::new_unknown(&scan.schema)),
            scan,
            files,
            partitions: partitions.into(),
        }))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(exec) = node.downcast_ref::<DataSourceExec>() else {
            return internal_err!(
                "expected DataSourceExec wrapping EagerIcebergDataSource, got {}",
                node.name()
            );
        };
        let Some(source) = exec.data_source().downcast_ref::<EagerIcebergDataSource>() else {
            return internal_err!("expected DataSourceExec wrapping EagerIcebergDataSource");
        };
        let partitions = (0..source.partitions.len())
            .map(|partition| {
                let tasks = source
                    .planned_partition_tasks(partition)?
                    .map(|file| {
                        rmp_serde::to_vec_named(file).map_err(|error| {
                            exec_datafusion_err!("Cannot encode assigned Iceberg file: {error}")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(pb::FileGroup { tasks })
            })
            .collect::<Result<Vec<_>>>()?;
        let proto = pb::AssignedScan {
            config: Some(self.encode_config(&source.scan, proto_converter)?),
            partitions,
        };
        proto.encode(buf).map_err(|error| {
            proto_error(format!("failed to encode EagerIcebergDataSource: {error}"))
        })
    }
}

impl EagerIcebergCodec {
    fn decode_config(
        &self,
        proto: Option<pb::ScanConfig>,
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<ScanConfig> {
        let proto = proto.ok_or_else(|| proto_error("Missing Iceberg scan config"))?;
        let schema = proto
            .schema
            .ok_or_else(|| proto_error("EagerIcebergDataSource is missing its schema"))?;
        let schema = SchemaRef::new((&schema).try_into()?);
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        let partitioning = parse_protobuf_partitioning(
            proto.partitioning.as_ref(),
            &decode_ctx,
            &schema,
            proto_converter,
        )?
        .ok_or_else(|| proto_error("EagerIcebergDataSource is missing its partitioning"))?;
        let fetch = proto
            .fetch
            .map(usize::try_from)
            .transpose()
            .map_err(|_| proto_error("Iceberg fetch limit does not fit in usize"))?;
        let iceberg_file_io = FileIOBuilder::new(Arc::clone(&self.storage_factory))
            .with_props(proto.storage_properties)
            .build();
        Ok(ScanConfig {
            schema,
            partitioning,
            fetch,
            metrics: Default::default(),
            iceberg_file_io,
            iceberg_runtime: self.iceberg_runtime.clone(),
        })
    }

    fn encode_config(
        &self,
        scan: &ScanConfig,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<pb::ScanConfig> {
        Ok(pb::ScanConfig {
            schema: Some(datafusion_proto::protobuf::Schema::try_from(
                scan.schema.as_ref(),
            )?),
            partitioning: Some(serialize_partitioning(
                &scan.partitioning,
                self,
                proto_converter,
            )?),
            fetch: scan.fetch.map(|value| value as u64),
            storage_properties: scan.iceberg_file_io.config().props().clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::IcebergTestHarness;

    #[tokio::test]
    async fn rejects_missing_config_and_invalid_assigned_groups() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness.scan().await?;
        let exec = plan.downcast_ref::<DataSourceExec>().unwrap();
        let source = exec
            .data_source()
            .downcast_ref::<crate::LazyIcebergDataSource>()
            .unwrap();
        let codec = EagerIcebergCodec::default();
        let context = TaskContext::default();
        let proto_converter = datafusion_proto::physical_plan::DefaultPhysicalProtoConverter {};
        let config = codec.encode_config(&source.scan, &proto_converter)?;
        let missing = pb::AssignedScan {
            config: None,
            partitions: vec![],
        }
        .encode_to_vec();
        assert!(
            codec
                .try_decode(&missing, &[], &context, &proto_converter)
                .is_err()
        );
        let mut malformed =
            vec![pb::FileGroup { tasks: vec![] }; source.scan.partitioning.partition_count()];
        malformed[0].tasks.push(vec![0xc1]); // Invalid MessagePack descriptor.
        for partitions in [vec![], malformed] {
            let bytes = pb::AssignedScan {
                config: Some(config.clone()),
                partitions,
            }
            .encode_to_vec();
            assert!(
                codec
                    .try_decode(&bytes, &[], &context, &proto_converter)
                    .is_err()
            );
        }
        Ok(())
    }
}
