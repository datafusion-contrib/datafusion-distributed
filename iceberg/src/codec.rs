use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::{Result, internal_err};
use datafusion::datasource::source::DataSource;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::{self, proto_error};
use iceberg::io::{FileIOBuilder, StorageConfig, StorageFactory};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use prost::Message;

use crate::{IcebergDataSource, IcebergWorkUnitFeed};
use datafusion_distributed::{WorkUnitFeed, WorkUnitFeedProto};

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

#[derive(Clone, PartialEq, ::prost::Message)]
struct IcebergDataSourceProto {
    #[prost(message, optional, tag = "1")]
    schema: Option<protobuf::Schema>,
    #[prost(message, optional, tag = "2")]
    feed: Option<WorkUnitFeedProto>,
    #[prost(uint64, tag = "3")]
    partitions: u64,
    #[prost(enumeration = "PartitioningKind", tag = "4")]
    partitioning: i32,
    #[prost(uint64, optional, tag = "5")]
    fetch: Option<u64>,
    #[prost(btree_map = "string, string", tag = "6")]
    storage_properties: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
enum PartitioningKind {
    Unknown = 0,
    RoundRobin = 1,
}

impl PhysicalExtensionCodec for IcebergCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "IcebergDataSource should have no children, got {}",
                inputs.len()
            );
        }

        let proto = IcebergDataSourceProto::decode(buf)
            .map_err(|error| proto_error(format!("failed to decode IcebergDataSource: {error}")))?;
        let schema = proto
            .schema
            .ok_or_else(|| proto_error("IcebergDataSource is missing its schema"))?;
        let schema = SchemaRef::new((&schema).try_into()?);
        let feed = proto
            .feed
            .ok_or_else(|| proto_error("IcebergDataSource is missing its work-unit feed"))?;
        let feed = WorkUnitFeed::<IcebergWorkUnitFeed>::from_proto(feed)?;
        let storage_config =
            StorageConfig::from_props(proto.storage_properties.into_iter().collect());
        let file_io = FileIOBuilder::new(Arc::clone(&self.storage_factory))
            .with_props(storage_config.props())
            .build();
        let partitions = usize::try_from(proto.partitions)
            .map_err(|_| proto_error("Iceberg partition count does not fit in usize"))?;
        if partitions == 0 {
            return Err(proto_error(
                "Iceberg partition count must be greater than zero",
            ));
        }
        let partitioning = match PartitioningKind::try_from(proto.partitioning).map_err(|_| {
            proto_error(format!(
                "unknown Iceberg partitioning kind {}",
                proto.partitioning
            ))
        })? {
            PartitioningKind::Unknown => Partitioning::UnknownPartitioning(partitions),
            PartitioningKind::RoundRobin => Partitioning::RoundRobinBatch(partitions),
        };
        let fetch = proto
            .fetch
            .map(usize::try_from)
            .transpose()
            .map_err(|_| proto_error("Iceberg fetch limit does not fit in usize"))?;
        Ok(DataSourceExec::from_data_source(
            IcebergDataSource::from_remote(
                schema,
                partitioning,
                fetch,
                file_io,
                self.iceberg_runtime.clone(),
                feed,
            ),
        ))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(exec) = node.downcast_ref::<DataSourceExec>() else {
            return internal_err!(
                "expected DataSourceExec wrapping IcebergDataSource, got {}",
                node.name()
            );
        };
        let Some(source) = exec.data_source().downcast_ref::<IcebergDataSource>() else {
            return internal_err!("expected DataSourceExec wrapping IcebergDataSource");
        };

        let (partitioning, partitions) = match source.output_partitioning() {
            Partitioning::UnknownPartitioning(partitions) => {
                (PartitioningKind::Unknown, partitions)
            }
            Partitioning::RoundRobinBatch(partitions) => (PartitioningKind::RoundRobin, partitions),
            Partitioning::Hash(_, _) => {
                return internal_err!(
                    "IcebergDataSource hash partitioning cannot be serialized safely"
                );
            }
        };
        if partitions == 0 {
            return internal_err!("Iceberg partition count must be greater than zero");
        }
        let partitions = u64::try_from(partitions)
            .map_err(|_| proto_error("Iceberg partition count does not fit in u64"))?;
        let fetch = source
            .fetch()
            .map(u64::try_from)
            .transpose()
            .map_err(|_| proto_error("Iceberg fetch limit does not fit in u64"))?;
        let proto = IcebergDataSourceProto {
            schema: Some(protobuf::Schema::try_from(source.schema_ref().as_ref())?),
            feed: Some(source.feed().to_proto()),
            partitions,
            partitioning: partitioning as i32,
            fetch,
            storage_properties: source
                .storage_config()
                .props()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        };
        proto
            .encode(buf)
            .map_err(|error| proto_error(format!("failed to encode IcebergDataSource: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::prelude::SessionContext;
    use crate::test_utils::{FixtureStorageFactory, IcebergTestHarness};

    #[tokio::test]
    async fn roundtrips_data_source_plan() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let dataframe = harness.context().sql("SELECT * FROM taxi").await?;
        let plan = dataframe.create_physical_plan().await?;
        let source_plan = iceberg_plan(&plan)?;
        let source = iceberg_source(&source_plan)?;

        let codec = IcebergCodec::new(
            Arc::new(FixtureStorageFactory::default()),
            iceberg::Runtime::current(),
        );
        let mut bytes = Vec::new();
        codec.try_encode(Arc::clone(&source_plan), &mut bytes)?;
        let decoded = codec.try_decode(&bytes, &[], &harness.context().task_ctx())?;

        let decoded = iceberg_source(&decoded)?;
        assert_eq!(source.schema_ref(), decoded.schema_ref());
        assert_eq!(
            source.output_partitioning().to_string(),
            decoded.output_partitioning().to_string()
        );
        assert_eq!(source.fetch(), decoded.fetch());
        assert_eq!(source.feed().to_proto(), decoded.feed().to_proto());
        assert_eq!(source.storage_config(), decoded.storage_config());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_zero_partitions_and_unknown_partitioning() -> Result<()> {
        let codec = test_codec();
        let context = SessionContext::new().task_ctx();

        let error = codec
            .try_decode(
                &test_proto(0, PartitioningKind::Unknown as i32)?.encode_to_vec(),
                &[],
                &context,
            )
            .expect_err("zero partitions must be rejected");
        assert!(
            error
                .to_string()
                .contains("Iceberg partition count must be greater than zero")
        );

        let error = codec
            .try_decode(&test_proto(1, 99)?.encode_to_vec(), &[], &context)
            .expect_err("unknown partitioning must be rejected");
        assert!(
            error
                .to_string()
                .contains("unknown Iceberg partitioning kind 99")
        );
        Ok(())
    }

    #[tokio::test]
    async fn handles_integer_and_storage_property_boundaries() -> Result<()> {
        let codec = test_codec();
        let context = SessionContext::new().task_ctx();
        let mut proto = test_proto(u64::MAX, PartitioningKind::RoundRobin as i32)?;
        proto.fetch = Some(u64::MAX);
        proto.storage_properties.insert(
            "s3.endpoint".to_string(),
            "http://127.0.0.1:9000".to_string(),
        );

        #[cfg(target_pointer_width = "64")]
        {
            let decoded = codec.try_decode(&proto.encode_to_vec(), &[], &context)?;
            let source = iceberg_source(&decoded)?;
            assert_eq!(source.output_partitioning().partition_count(), usize::MAX);
            assert_eq!(source.fetch(), Some(usize::MAX));
            assert_eq!(
                source.storage_config().props().get("s3.endpoint"),
                Some(&"http://127.0.0.1:9000".to_string())
            );

            let mut encoded = Vec::new();
            codec.try_encode(decoded, &mut encoded)?;
            let encoded = IcebergDataSourceProto::decode(encoded.as_slice()).map_err(|error| {
                proto_error(format!("failed to decode test data source: {error}"))
            })?;
            assert_eq!(encoded.partitions, u64::MAX);
            assert_eq!(encoded.fetch, Some(u64::MAX));
            assert_eq!(encoded.storage_properties, proto.storage_properties);
        }

        #[cfg(target_pointer_width = "32")]
        {
            let error = codec
                .try_decode(&proto.encode_to_vec(), &[], &context)
                .expect_err("u64::MAX cannot fit in a 32-bit usize");
            assert!(
                error
                    .to_string()
                    .contains("Iceberg partition count does not fit in usize")
            );
        }

        Ok(())
    }

    fn test_codec() -> IcebergCodec {
        IcebergCodec::new(
            Arc::new(FixtureStorageFactory::default()),
            iceberg::Runtime::current(),
        )
    }

    fn test_proto(partitions: u64, partitioning: i32) -> Result<IcebergDataSourceProto> {
        Ok(IcebergDataSourceProto {
            schema: Some(protobuf::Schema::try_from(&Schema::empty())?),
            feed: Some(WorkUnitFeedProto { id: vec![0; 16] }),
            partitions,
            partitioning,
            fetch: None,
            storage_properties: BTreeMap::new(),
        })
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
