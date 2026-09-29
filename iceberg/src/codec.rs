use std::fmt;
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
use crate::{IcebergDataSource, IcebergWorkUnitFeed};

type RuntimeResolver = dyn Fn(&TaskContext) -> Result<iceberg::Runtime> + Send + Sync;

/// Physical plan codec for [`IcebergDataSource`].
#[derive(Clone)]
pub struct IcebergCodec {
    storage_factory: Arc<dyn StorageFactory>,
    runtime_resolver: Arc<RuntimeResolver>,
}

impl fmt::Debug for IcebergCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergCodec")
            .field("storage_factory", &self.storage_factory)
            .finish_non_exhaustive()
    }
}

impl IcebergCodec {
    /// Creates a codec using the storage and runtime configured on this process.
    pub fn new(
        storage_factory: Arc<dyn StorageFactory>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Self {
        Self::new_with_runtime_resolver(storage_factory, move |_| Ok(iceberg_runtime.clone()))
    }

    /// Creates a codec that selects a worker-local runtime for each decoded scan.
    ///
    /// The resolver receives the decoding task's context, so it can read a runtime
    /// from a worker-local session extension. It is not called during encoding;
    /// resolver errors fail decoding. CPU/I/O routing is entirely caller-defined,
    /// and runtime handles are never serialized. The caller must keep the selected
    /// Tokio runtimes alive until scan execution completes.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::common::exec_datafusion_err;
    /// # use datafusion_distributed_iceberg::IcebergCodec;
    /// # use iceberg::io::StorageFactory;
    /// # fn example(storage_factory: Arc<dyn StorageFactory>) {
    /// let codec = IcebergCodec::new_with_runtime_resolver(storage_factory, |ctx| {
    ///     ctx.session_config()
    ///         .get_extension::<iceberg::Runtime>()
    ///         .map(|runtime| runtime.as_ref().clone())
    ///         .ok_or_else(|| exec_datafusion_err!("missing worker Iceberg runtime"))
    /// });
    /// # }
    /// ```
    pub fn new_with_runtime_resolver(
        storage_factory: Arc<dyn StorageFactory>,
        runtime_resolver: impl Fn(&TaskContext) -> Result<iceberg::Runtime> + Send + Sync + 'static,
    ) -> Self {
        Self {
            storage_factory,
            runtime_resolver: Arc::new(runtime_resolver),
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
        let feed = proto
            .feed
            .ok_or_else(|| proto_error("IcebergDataSource is missing its work-unit feed"))?;
        let feed =
            WorkUnitFeed::<IcebergWorkUnitFeed>::from_proto(WorkUnitFeedProto { id: feed.id })?;
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        let partitioning = parse_protobuf_partitioning(
            proto.partitioning.as_ref(),
            &decode_ctx,
            &schema,
            proto_converter,
        )?
        .ok_or_else(|| proto_error("IcebergDataSource is missing its partitioning"))?;
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
            iceberg_runtime: (self.runtime_resolver)(ctx)?,
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
                "expected DataSourceExec wrapping IcebergDataSource, got {}",
                node.name()
            );
        };
        let Some(source) = exec.data_source().downcast_ref::<IcebergDataSource>() else {
            return internal_err!("expected DataSourceExec wrapping IcebergDataSource");
        };

        let feed = source.feed.to_proto();
        let proto = pb::IcebergDataSource {
            schema: Some(datafusion_proto::protobuf::Schema::try_from(
                source.schema.as_ref(),
            )?),
            feed: Some(pb::WorkUnitFeed { id: feed.id }),
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
    use datafusion::common::{Statistics, exec_err};
    use datafusion::datasource::source::DataSource;
    use datafusion::prelude::SessionConfig;
    use datafusion_distributed::{DistributedCodec, DistributedExt};
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use tokio::runtime::{Builder, Handle, Runtime as TokioRuntime};

    use super::*;
    use crate::common::df_err;
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
        assert_eq!(source.feed.to_proto(), decoded.feed.to_proto());
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

    #[test]
    fn fixed_runtime_is_preserved() -> Result<()> {
        let io = runtime()?;
        let cpu = runtime()?;
        let codec = IcebergCodec::new(
            Arc::new(OpenDalResolvingStorageFactory::new()),
            iceberg::Runtime::new_with_split(&io, &cpu),
        );
        io.block_on(async {
            let proto = encoded_scan(&codec).await?;
            let ctx = task_context(iceberg::Runtime::new(&io));
            assert_decoded_runtime(&proto, &codec, &ctx, &io, &cpu).await
        })
    }

    #[test]
    fn default_runtime_is_captured_at_construction() -> Result<()> {
        let startup = runtime()?;
        let worker = runtime()?;
        let codec = {
            let _guard = startup.enter();
            IcebergCodec::default()
        };
        worker.block_on(async {
            let proto = encoded_scan(&codec).await?;
            assert_decoded_runtime(&proto, &codec, &TaskContext::default(), &startup, &startup)
                .await
        })
    }

    #[test]
    fn resolves_each_decode_with_query_cpu_and_shared_io() -> Result<()> {
        let codec = IcebergCodec::new_with_runtime_resolver(
            Arc::new(OpenDalResolvingStorageFactory::new()),
            |ctx| {
                Ok(ctx
                    .session_config()
                    .get_extension::<iceberg::Runtime>()
                    .expect("worker runtime extension")
                    .as_ref()
                    .clone())
            },
        );
        let config = SessionConfig::new().with_distributed_user_codec(codec);
        let codec = DistributedCodec::new_combined_with_user(&config);
        let io = runtime()?;
        let cpu_a = runtime()?;
        let cpu_b = runtime()?;
        io.block_on(async {
            let proto = encoded_scan(&codec).await?;
            let ctx_a = task_context(iceberg::Runtime::new_with_split(&io, &cpu_a));
            let ctx_b = task_context(iceberg::Runtime::new_with_split(&io, &cpu_b));
            assert_decoded_runtime(&proto, &codec, &ctx_a, &io, &cpu_a).await?;
            assert_decoded_runtime(&proto, &codec, &ctx_b, &io, &cpu_b).await
        })
    }

    #[tokio::test]
    async fn propagates_runtime_resolver_errors_only_on_decode() -> Result<()> {
        let codec = IcebergCodec::new_with_runtime_resolver(
            Arc::new(OpenDalResolvingStorageFactory::new()),
            |_| exec_err!("query runtime unavailable"),
        );
        let proto = encoded_scan(&codec).await?;
        let error = proto
            .try_into_physical_plan(&TaskContext::default(), &codec)
            .unwrap_err();
        assert!(
            error.to_string().contains("query runtime unavailable"),
            "{error}"
        );
        Ok(())
    }

    fn runtime() -> Result<TokioRuntime> {
        Ok(Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?)
    }

    fn task_context(runtime: iceberg::Runtime) -> TaskContext {
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_extension(Arc::new(runtime)))
    }

    async fn encoded_scan(codec: &dyn PhysicalExtensionCodec) -> Result<PhysicalPlanNode> {
        let harness = IcebergTestHarness::new().await?;
        PhysicalPlanNode::try_from_physical_plan(harness.scan().await?, codec)
    }

    async fn assert_decoded_runtime(
        proto: &PhysicalPlanNode,
        codec: &dyn PhysicalExtensionCodec,
        ctx: &TaskContext,
        io: &TokioRuntime,
        cpu: &TokioRuntime,
    ) -> Result<()> {
        let decoded = proto.try_into_physical_plan(ctx, codec)?;
        let runtime = &iceberg_source(&decoded)?.iceberg_runtime;
        assert_eq!(
            runtime
                .io()
                .spawn(async { Handle::current().id() })
                .await
                .map_err(df_err)?,
            io.handle().id(),
        );
        assert_eq!(
            runtime
                .cpu()
                .spawn(async { Handle::current().id() })
                .await
                .map_err(df_err)?,
            cpu.handle().id(),
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
