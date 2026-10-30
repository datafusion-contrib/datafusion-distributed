use crate::IcebergFileIOCodec;
use crate::common::scan_config::ScanConfig;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, internal_err};
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
use crate::{IcebergWorkUnitFeed, LazyIcebergDataSource};

/// Physical plan codec for [`LazyIcebergDataSource`].
#[derive(Debug, Clone)]
pub struct LazyIcebergCodec {
    storage_factory: Arc<dyn StorageFactory>,
    iceberg_runtime: iceberg::Runtime,
    file_io_codec: Option<Arc<dyn IcebergFileIOCodec>>,
}

impl LazyIcebergCodec {
    /// Creates a codec using the storage and runtime configured on this process.
    pub fn new(
        storage_factory: Arc<dyn StorageFactory>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Self {
        Self {
            storage_factory,
            iceberg_runtime,
            file_io_codec: None,
        }
    }

    /// Overrides file IO encoding and decoding while retaining the standard plan codec.
    /// For session registration, use [`crate::IcebergFileIOExt`].
    pub fn with_file_io_codec(mut self, codec: Arc<dyn IcebergFileIOCodec>) -> Self {
        self.file_io_codec = Some(codec);
        self
    }
}

impl Default for LazyIcebergCodec {
    fn default() -> Self {
        Self::new(
            Arc::new(OpenDalResolvingStorageFactory::new()),
            iceberg::Runtime::current(),
        )
    }
}

impl PhysicalExtensionCodec for LazyIcebergCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "LazyIcebergDataSource should have no children, got {}",
                inputs.len()
            );
        }

        let proto = pb::IcebergDataSource::decode(buf).map_err(|error| {
            proto_error(format!("failed to decode LazyIcebergDataSource: {error}"))
        })?;
        let schema = proto
            .schema
            .ok_or_else(|| proto_error("LazyIcebergDataSource is missing its schema"))?;
        let schema = SchemaRef::new((&schema).try_into()?);
        let feed = proto
            .feed
            .ok_or_else(|| proto_error("LazyIcebergDataSource is missing its work-unit feed"))?;
        let feed =
            WorkUnitFeed::<IcebergWorkUnitFeed>::from_proto(WorkUnitFeedProto { id: feed.id })?;
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        let partitioning = parse_protobuf_partitioning(
            proto.partitioning.as_ref(),
            &decode_ctx,
            &schema,
            proto_converter,
        )?
        .ok_or_else(|| proto_error("LazyIcebergDataSource is missing its partitioning"))?;
        let fetch = proto
            .fetch
            .map(usize::try_from)
            .transpose()
            .map_err(|_| proto_error("Iceberg fetch limit does not fit in usize"))?;
        let iceberg_file_io = match &self.file_io_codec {
            Some(codec) => codec.decode(proto.storage_properties, ctx)?,
            None => FileIOBuilder::new(Arc::clone(&self.storage_factory))
                .with_props(proto.storage_properties)
                .build(),
        };

        Ok(DataSourceExec::from_data_source(LazyIcebergDataSource {
            scan: ScanConfig {
                schema,
                partitioning,
                fetch,
                metrics: Default::default(),
                iceberg_file_io,
                iceberg_runtime: self.iceberg_runtime.clone(),
            },
            column_stats: None,
            table_snapshot: None,
            feed,
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
                "expected DataSourceExec wrapping LazyIcebergDataSource, got {}",
                node.name()
            );
        };
        let Some(source) = exec.data_source().downcast_ref::<LazyIcebergDataSource>() else {
            return internal_err!("expected DataSourceExec wrapping LazyIcebergDataSource");
        };

        let feed = source.feed.to_proto();
        let proto = pb::IcebergDataSource {
            schema: Some(datafusion_proto::protobuf::Schema::try_from(
                source.scan.schema.as_ref(),
            )?),
            feed: Some(pb::WorkUnitFeed { id: feed.id }),
            partitioning: Some(serialize_partitioning(
                &source.scan.partitioning,
                self,
                proto_converter,
            )?),
            fetch: source.scan.fetch.map(|value| value as u64),
            storage_properties: match &self.file_io_codec {
                Some(codec) => codec.encode(&source.scan.iceberg_file_io)?,
                None => source.scan.iceberg_file_io.config().props().clone(),
            },
        };
        proto.encode(buf).map_err(|error| {
            proto_error(format!("failed to encode LazyIcebergDataSource: {error}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::Statistics;
    use datafusion::datasource::source::DataSource;
    use datafusion_distributed_iceberg_test_tools::create_taxi_table;
    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::table::Table;

    use super::*;
    use crate::common::df_err;
    use crate::table_provider::scan::{IcebergDataSourceOptions, plan_scan};

    #[tokio::test]
    async fn roundtrips_data_source_plan() -> Result<()> {
        let taxi = create_taxi_table().await?;
        let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory))
            .with_props([
                ("fixture.storage", "roundtrip's value"),
                ("fixture.region", "test-region"),
            ])
            .build();
        let table = Table::builder()
            .metadata(taxi.table().metadata_ref())
            .identifier(taxi.ident().clone())
            .file_io(file_io)
            .runtime(iceberg::Runtime::current())
            .build()
            .map_err(df_err)?;
        let options = IcebergDataSourceOptions {
            fetch: Some(10),
            ..Default::default()
        };
        let plan = plan_scan(
            table,
            taxi.schema()?,
            options,
            Arc::new(TaskContext::default()),
        )
        .await?;
        let codec =
            LazyIcebergCodec::new(Arc::new(LocalFsStorageFactory), iceberg::Runtime::current());
        let proto_converter = DefaultPhysicalProtoConverter {};
        let mut buf = Vec::new();
        codec.try_encode(Arc::clone(&plan), &mut buf, &proto_converter)?;
        let decoded_plan =
            codec.try_decode(&buf, &[], &TaskContext::default(), &proto_converter)?;
        let source = iceberg_source(&plan)?;
        let decoded = iceberg_source(&decoded_plan)?;

        assert_eq!(source.scan.schema, decoded.scan.schema);
        assert_eq!(
            source.scan.partitioning.to_string(),
            decoded.scan.partitioning.to_string()
        );
        assert_eq!(source.scan.fetch, decoded.scan.fetch);
        assert_eq!(source.feed.to_proto(), decoded.feed.to_proto());
        assert_eq!(
            source.scan.iceberg_file_io.config().props(),
            decoded.scan.iceberg_file_io.config().props()
        );
        let properties = decoded.scan.iceberg_file_io.config().props();
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
            &Statistics::new_unknown(&decoded.scan.schema)
        );
        Ok(())
    }

    fn iceberg_source(plan: &Arc<dyn ExecutionPlan>) -> Result<&LazyIcebergDataSource> {
        let Some(exec) = plan.downcast_ref::<DataSourceExec>() else {
            return internal_err!("expected a DataSourceExec");
        };
        exec.data_source()
            .downcast_ref::<LazyIcebergDataSource>()
            .ok_or_else(|| proto_error("expected an LazyIcebergDataSource"))
    }
}
