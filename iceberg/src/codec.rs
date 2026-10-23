use std::sync::Arc;

use crate::{AssignedFiles, ScanInput};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, exec_datafusion_err, internal_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{WorkUnitFeed, WorkUnitFeedProto};
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
use crate::{IcebergDataSource, IcebergWorkUnitFeed};

/// Physical plan codec for [`IcebergDataSource`].
#[derive(Debug, Clone)]
pub struct IcebergCodec {
    storage_factory: Arc<dyn StorageFactory>,
    iceberg_runtime: iceberg::Runtime,
}

impl IcebergCodec {
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

impl Default for IcebergCodec {
    fn default() -> Self {
        Self::new(
            Arc::new(OpenDalResolvingStorageFactory::new()),
            iceberg::Runtime::current(),
        )
    }
}

impl PhysicalExtensionCodec for IcebergCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "IcebergDataSource should have no children, got {}",
                inputs.len()
            );
        }

        let proto = pb::IcebergDataSource::decode(buf)
            .map_err(|error| proto_error(format!("failed to decode IcebergDataSource: {error}")))?;
        let schema = proto
            .schema
            .ok_or_else(|| proto_error("IcebergDataSource is missing its schema"))?;
        let schema = SchemaRef::new((&schema).try_into()?);
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        let partitioning = parse_protobuf_partitioning(
            proto.partitioning.as_ref(),
            &decode_ctx,
            &schema,
            proto_converter,
        )?
        .ok_or_else(|| proto_error("IcebergDataSource is missing its partitioning"))?;
        let input = match (proto.feed, proto.assigned_files) {
            (Some(feed), None) => {
                ScanInput::WorkUnitFeed(Box::new(WorkUnitFeed::<IcebergWorkUnitFeed>::from_proto(
                    WorkUnitFeedProto { id: feed.id },
                )?))
            }
            (None, Some(files)) => {
                if files.partitions.len() != partitioning.partition_count()
                    || files.partitions.is_empty()
                {
                    return internal_err!("Iceberg assigned files do not match scan partitioning");
                }
                let groups = files
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
                ScanInput::AssignedFiles(AssignedFiles::from_partitions(
                    groups,
                    ctx,
                    files.may_filter_rows,
                )?)
            }
            _ => {
                return internal_err!(
                    "Iceberg scan requires exactly one input: assigned files or feed"
                );
            }
        };
        let fetch = proto
            .fetch
            .map(usize::try_from)
            .transpose()
            .map_err(|_| proto_error("Iceberg fetch limit does not fit in usize"))?;
        let iceberg_file_io = FileIOBuilder::new(Arc::clone(&self.storage_factory))
            .with_props(proto.storage_properties)
            .build();

        Ok(DataSourceExec::from_data_source(IcebergDataSource {
            schema,
            partitioning,
            fetch,
            metrics: Default::default(),
            column_stats: None,
            table_snapshot: None,
            iceberg_file_io,
            iceberg_runtime: self.iceberg_runtime.clone(),
            input,
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
                "expected DataSourceExec wrapping IcebergDataSource, got {}",
                node.name()
            );
        };
        let Some(source) = exec.data_source().downcast_ref::<IcebergDataSource>() else {
            return internal_err!("expected DataSourceExec wrapping IcebergDataSource");
        };

        let (feed, assigned_files) = match source.input() {
            ScanInput::WorkUnitFeed(feed) => (
                Some(pb::WorkUnitFeed {
                    id: feed.to_proto().id,
                }),
                None,
            ),
            ScanInput::AssignedFiles(files) => {
                let partitions = (0..files.partition_count())
                    .map(|partition| {
                        let tasks = files
                            .partition(partition)?
                            .map(|file| {
                                rmp_serde::to_vec_named(file).map_err(|error| {
                                    exec_datafusion_err!(
                                        "Cannot encode assigned Iceberg file: {error}"
                                    )
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Ok(pb::FileGroup { tasks })
                    })
                    .collect::<Result<Vec<_>>>()?;
                (
                    None,
                    Some(pb::AssignedFiles {
                        partitions,
                        may_filter_rows: files.planned.may_filter_rows(),
                    }),
                )
            }
        };
        let proto = pb::IcebergDataSource {
            schema: Some(datafusion_proto::protobuf::Schema::try_from(
                source.schema.as_ref(),
            )?),
            feed,
            assigned_files,
            partitioning: Some(serialize_partitioning(
                &source.partitioning,
                self,
                proto_converter,
            )?),
            fetch: source.fetch.map(|value| value as u64),
            storage_properties: source.iceberg_file_io.config().props().clone(),
        };
        proto
            .encode(buf)
            .map_err(|error| proto_error(format!("failed to encode IcebergDataSource: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::Statistics;
    use datafusion::datasource::source::DataSource;

    use super::*;
    use crate::test_utils::IcebergTestHarness;

    #[tokio::test]
    async fn roundtrips_data_source_plan() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_table_option("fixture.storage", "roundtrip's value")
            .with_table_option("fixture.region", "test-region")
            .build()
            .await?;
        let plan = harness.physical_plan("SELECT * FROM taxi LIMIT 10").await?;
        let decoded_plan = harness.roundtrip_plan(Arc::clone(&plan))?;
        let source_plan = iceberg_plan(&plan)?;
        let decoded_source_plan = iceberg_plan(&decoded_plan)?;
        let source = iceberg_source(&source_plan)?;
        let decoded = iceberg_source(&decoded_source_plan)?;

        assert_eq!(source.schema, decoded.schema);
        assert_eq!(
            source.partitioning.to_string(),
            decoded.partitioning.to_string()
        );
        assert_eq!(source.fetch, decoded.fetch);
        assert_eq!(
            source.feed().unwrap().to_proto(),
            decoded.feed().unwrap().to_proto()
        );
        assert_eq!(
            source.iceberg_file_io.config().props(),
            decoded.iceberg_file_io.config().props()
        );
        let properties = decoded.iceberg_file_io.config().props();
        assert_eq!(
            properties.get("fixture.storage").map(String::as_str),
            Some("roundtrip's value")
        );
        assert_eq!(
            properties.get("fixture.region").map(String::as_str),
            Some("test-region")
        );
        assert_eq!(
            decoded.partition_statistics(None)?.as_ref(),
            &Statistics::new_unknown(&decoded.schema)
        );
        Ok(())
    }

    fn iceberg_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
            && exec
                .data_source()
                .downcast_ref::<IcebergDataSource>()
                .is_some()
        {
            return Ok(Arc::clone(plan));
        }
        for child in plan.children() {
            if let Ok(plan) = iceberg_plan(child) {
                return Ok(plan);
            }
        }
        internal_err!("fixture query contains no IcebergDataSource")
    }

    fn iceberg_source(plan: &Arc<dyn ExecutionPlan>) -> Result<&IcebergDataSource> {
        let Some(exec) = plan.downcast_ref::<DataSourceExec>() else {
            return internal_err!("expected a DataSourceExec");
        };
        exec.data_source()
            .downcast_ref::<IcebergDataSource>()
            .ok_or_else(|| proto_error("expected an IcebergDataSource"))
    }
}
