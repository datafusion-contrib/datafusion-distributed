use crate::config_extension_ext::{
    set_distributed_option_extension, set_distributed_option_extension_from_headers,
};
use crate::distributed_planner::set_distributed_task_estimator;
use crate::networking::{set_distributed_channel_resolver, set_distributed_worker_resolver};
use crate::passthrough_headers::set_passthrough_headers;
use crate::protobuf::{set_distributed_user_codec, set_distributed_user_codec_arc};
use crate::{ChannelResolver, DistributedConfig, TaskEstimator, WorkerResolver};
use arrow_ipc::CompressionType;
use datafusion::common::DataFusionError;
use datafusion::config::ConfigExtension;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use delegate::delegate;
use http::HeaderMap;
use std::sync::Arc;

/// Extends DataFusion with distributed capabilities.
pub trait DistributedExt: Sized {
    /// Adds the provided [ConfigExtension] to the distributed context. The [ConfigExtension] will
    /// be serialized using gRPC metadata and sent across tasks. Users are expected to call this
    /// method with their own extensions to be able to access them in any place in the
    /// plan.
    ///
    /// This method also adds the provided [ConfigExtension] to the current session option
    /// extensions, the same as calling [SessionConfig::with_option_extension].
    ///
    /// Example:
    ///
    /// ```rust
    /// # use async_trait::async_trait;
    /// # use datafusion::common::{extensions_options, DataFusionError};
    /// # use datafusion::config::ConfigExtension;
    /// # use datafusion::execution::{SessionState, SessionStateBuilder};
    /// # use datafusion::prelude::SessionConfig;
    /// # use datafusion_distributed::{DistributedExt, WorkerSessionBuilder, WorkerQueryContext};
    ///
    /// extensions_options! {
    ///     pub struct CustomExtension {
    ///         pub foo: String, default = "".to_string()
    ///         pub bar: usize, default = 0
    ///         pub baz: bool, default = false
    ///     }
    /// }
    ///
    /// impl ConfigExtension for CustomExtension {
    ///     const PREFIX: &'static str = "custom";
    /// }
    ///
    /// let mut my_custom_extension = CustomExtension::default();
    /// // Now, the CustomExtension will be able to cross network boundaries. Upon making an Arrow
    /// // Flight request, it will be sent through gRPC metadata.
    /// let state = SessionStateBuilder::new()
    ///     .with_distributed_option_extension(my_custom_extension)
    ///     .build();
    ///
    /// async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    ///     // This function can be provided to a Worker to tell it how to
    ///     // build sessions that retrieve the CustomExtension from gRPC metadata.
    ///     Ok(ctx
    ///         .builder
    ///         .with_distributed_option_extension_from_headers::<CustomExtension>(&ctx.headers)?
    ///         .build())
    /// }
    /// ```
    fn with_distributed_option_extension<T: ConfigExtension + Default>(self, t: T) -> Self;

    /// Same as [DistributedExt::with_distributed_option_extension] but with an in-place mutation
    fn set_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T);

    /// Adds the provided [ConfigExtension] to the distributed context. The [ConfigExtension] will
    /// be serialized using gRPC metadata and sent across tasks. Users are expected to call this
    /// method with their own extensions to be able to access them in any place in the
    /// plan.
    ///
    /// - If there was a [ConfigExtension] of the same type already present, it's updated with an
    ///   in-place mutation based on the headers that came over the wire.
    /// - If there was no [ConfigExtension] set before, it will get added, as if
    ///   [SessionConfig::with_option_extension] was being called.
    ///
    /// Example:
    ///
    /// ```rust
    /// # use async_trait::async_trait;
    /// # use datafusion::common::{extensions_options, DataFusionError};
    /// # use datafusion::config::ConfigExtension;
    /// # use datafusion::execution::{SessionState, SessionStateBuilder};
    /// # use datafusion::prelude::SessionConfig;
    /// # use datafusion_distributed::{DistributedExt, WorkerSessionBuilder, WorkerQueryContext};
    ///
    /// extensions_options! {
    ///     pub struct CustomExtension {
    ///         pub foo: String, default = "".to_string()
    ///         pub bar: usize, default = 0
    ///         pub baz: bool, default = false
    ///     }
    /// }
    ///
    /// impl ConfigExtension for CustomExtension {
    ///     const PREFIX: &'static str = "custom";
    /// }
    ///
    /// let mut my_custom_extension = CustomExtension::default();
    /// // Now, the CustomExtension will be able to cross network boundaries. Upon making an Arrow
    /// // Flight request, it will be sent through gRPC metadata.
    /// let state = SessionStateBuilder::new()
    ///     .with_distributed_option_extension(my_custom_extension)
    ///     .build();
    ///
    /// async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    ///     // This function can be provided to a Worker to tell it how to
    ///     // build sessions that retrieve the CustomExtension from gRPC metadata.
    ///     Ok(ctx
    ///         .builder
    ///         .with_distributed_option_extension_from_headers::<CustomExtension>(&ctx.headers)?
    ///         .build())
    /// }
    /// ```
    fn with_distributed_option_extension_from_headers<T: ConfigExtension + Default>(
        self,
        headers: &HeaderMap,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_option_extension_from_headers] but with an in-place mutation
    fn set_distributed_option_extension_from_headers<T: ConfigExtension + Default>(
        &mut self,
        headers: &HeaderMap,
    ) -> Result<(), DataFusionError>;

    /// Injects a user-defined [PhysicalExtensionCodec] that is capable of encoding/decoding
    /// custom execution nodes. Multiple user-defined [PhysicalExtensionCodec] can be added
    /// by calling this method several times.
    ///
    /// Example:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::common::DataFusionError;
    /// # use datafusion::execution::{SessionState, FunctionRegistry, SessionStateBuilder, TaskContext};
    /// # use datafusion::physical_plan::ExecutionPlan;
    /// # use datafusion::prelude::SessionConfig;
    /// # use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    /// # use datafusion_distributed::{DistributedExt, WorkerQueryContext};
    ///
    /// #[derive(Debug)]
    /// struct CustomExecCodec;
    ///
    /// impl PhysicalExtensionCodec for CustomExecCodec {
    ///     fn try_decode(&self, buf: &[u8], inputs: &[Arc<dyn ExecutionPlan>], ctx: &TaskContext) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    ///         todo!()
    ///     }
    ///
    ///     fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> datafusion::common::Result<()> {
    ///         todo!()
    ///     }
    /// }
    ///
    /// let state = SessionStateBuilder::new()
    ///     .with_distributed_user_codec(CustomExecCodec)
    ///     .build();
    ///
    /// async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    ///     // This function can be provided to a Worker to tell it how to
    ///     // encode/decode CustomExec nodes.
    ///     Ok(SessionStateBuilder::new()
    ///         .with_distributed_user_codec(CustomExecCodec)
    ///         .build())
    /// }
    /// ```
    fn with_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(self, codec: T) -> Self;

    /// Same as [DistributedExt::with_distributed_user_codec] but with an in-place mutation
    fn set_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T);

    /// Same as [DistributedExt::with_distributed_user_codec] but with a dynamic argument.
    fn with_distributed_user_codec_arc(self, codec: Arc<dyn PhysicalExtensionCodec>) -> Self;

    /// Same as [DistributedExt::set_distributed_user_codec] but with a dynamic argument.
    fn set_distributed_user_codec_arc(&mut self, codec: Arc<dyn PhysicalExtensionCodec>);

    /// This is what tells Distributed DataFusion the URLs of the workers available for serving queries.
    ///
    /// It injects a [WorkerResolver] implementation for Distributed DataFusion to resolve worker
    /// nodes in the cluster. When running in distributed mode, setting a [WorkerResolver] is required.
    ///
    /// Even if this is required to be present in the [SessionContext] that first initiates and
    /// plans the query, it's not necessary to be present in a Worker's session state builder,
    /// as no planning happens there.
    ///
    /// Example:
    ///
    /// ```
    /// # use arrow_flight::flight_service_client::FlightServiceClient;
    /// # use async_trait::async_trait;
    /// # use datafusion::common::DataFusionError;
    /// # use datafusion::execution::{SessionState, SessionStateBuilder};
    /// # use datafusion::prelude::SessionConfig;
    /// # use url::Url;
    /// # use std::sync::Arc;
    /// # use datafusion_distributed::{BoxCloneSyncChannel, WorkerResolver, DistributedExt, DistributedPhysicalOptimizerRule, WorkerQueryContext};
    ///
    /// struct CustomWorkerResolver;
    ///
    /// #[async_trait]
    /// impl WorkerResolver for CustomWorkerResolver {
    ///     fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
    ///         todo!()
    ///     }
    /// }
    ///
    /// // This tweaks the SessionState so that it can plan for distributed queries and execute them.
    /// let state = SessionStateBuilder::new()
    ///     .with_distributed_worker_resolver(CustomWorkerResolver)
    ///     // the DistributedPhysicalOptimizerRule also needs to be passed so that query plans
    ///     // get distributed.
    ///     .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
    ///     .build();
    /// ```
    fn with_distributed_worker_resolver<T: WorkerResolver + Send + Sync + 'static>(
        self,
        resolver: T,
    ) -> Self;

    /// Same as [DistributedExt::with_distributed_channel_resolver] but with an in-place mutation.
    fn set_distributed_worker_resolver<T: WorkerResolver + Send + Sync + 'static>(
        &mut self,
        resolver: T,
    );

    /// This is what tells Distributed DataFusion how to build an Arrow Flight client out of a worker URL.
    ///
    /// There's a default implementation that caches the Arrow Flight client instances so that there's
    /// only one per URL, but users can decide to override that behavior in favor of their own solution.
    ///
    /// Example:
    ///
    /// ```
    /// # use arrow_flight::flight_service_client::FlightServiceClient;
    /// # use async_trait::async_trait;
    /// # use datafusion::common::DataFusionError;
    /// # use datafusion::execution::{SessionState, SessionStateBuilder};
    /// # use datafusion::prelude::SessionConfig;
    /// # use url::Url;
    /// # use std::sync::Arc;
    /// # use datafusion_distributed::{BoxCloneSyncChannel, ChannelResolver, DistributedExt, DistributedPhysicalOptimizerRule, WorkerQueryContext};
    ///
    /// struct CustomChannelResolver;
    ///
    /// #[async_trait]
    /// impl ChannelResolver for CustomChannelResolver {
    ///     async fn get_flight_client_for_url(&self, url: &Url) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
    ///         // Build a custom FlightServiceClient wrapped with tower layers or something similar.
    ///         todo!()
    ///     }
    /// }
    ///
    /// // This tweaks the SessionState so that it can plan for distributed queries and execute them.
    /// let state = SessionStateBuilder::new()
    ///     .with_distributed_channel_resolver(CustomChannelResolver)
    ///     // the DistributedPhysicalOptimizerRule also needs to be passed so that query plans
    ///     // get distributed.
    ///     .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
    ///     .build();
    ///
    /// // This function can be provided to a Worker so that, upon receiving a distributed
    /// // part of a plan, it knows how to resolve gRPC channels from URLs for making network calls to other nodes.
    /// async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    ///     // If you have a custom channel resolver, it should also be passed in the
    ///     // Worker session builder.
    ///     Ok(ctx
    ///         .builder
    ///         .with_distributed_channel_resolver(CustomChannelResolver)
    ///         .build())
    /// }
    /// ```
    fn with_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(
        self,
        resolver: T,
    ) -> Self;

    /// Same as [DistributedExt::with_distributed_channel_resolver] but with an in-place mutation.
    fn set_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(
        &mut self,
        resolver: T,
    );

    /// Adds a distributed task count estimator. [TaskEstimator]s are executed on each node
    /// sequentially until one returns an estimation on the number of tasks that should be
    /// used for the stage containing that node.
    ///
    /// Many nodes might decide to provide an estimation, so a reconciliation between all of them
    /// is performed internally during planning.
    ///
    /// ```text
    ///     ┌───────────────────────┐
    ///     │SortPreservingMergeExec│
    ///     └───────────────────────┘
    ///                 ▲
    /// ┌ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ─ ─ Stage 2
    ///     ┌───────────┴───────────┐    │
    /// │   │       SortExec        │
    ///     └───────────────────────┘    │
    /// │   ┌───────────────────────┐
    ///     │     AggregateExec     │    │
    /// │   └───────────────────────┘
    ///  ─ ─ ─ ─ ─ ─ ─ ─▲─ ─ ─ ─ ─ ─ ─ ─ ┘
    /// ┌ ─ ─ ─ ─ ─ ─ ─ ┴ ─ ─ ─ ─ ─ ─ ─ ─ Stage 1
    ///     ┌───────────────────────┐    │
    /// │   │      FilterExec       │
    ///     └───────────────────────┘    │
    /// │   ┌───────────────────────┐       a TaskEstimator estimates the amount of tasks
    ///     │       SomeExec        │◀───┼──  based on how much data will be pulled.
    /// │   └───────────────────────┘
    ///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
    /// ```
    fn with_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(
        self,
        estimator: T,
    ) -> Self;

    /// Same as [DistributedExt::with_distributed_task_estimator] but with an in-place mutation.
    fn set_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(
        &mut self,
        estimator: T,
    );

    /// Sets the maximum number of files each task in a stage with a FileScanConfig node will
    /// handle. Reducing this number will increment the amount of tasks. By default, this
    /// is close to the number of cores in the machine.
    ///
    /// ```text
    ///     ┌───────────────────────┐
    ///     │SortPreservingMergeExec│
    ///     └───────────────────────┘
    ///                 ▲
    /// ┌ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ─ ─ Stage 2
    ///     ┌───────────┴───────────┐    │
    /// │   │       SortExec        │
    ///     └───────────────────────┘    │
    /// │   ┌───────────────────────┐
    ///     │     AggregateExec     │    │
    /// │   └───────────────────────┘
    ///  ─ ─ ─ ─ ─ ─ ─ ─▲─ ─ ─ ─ ─ ─ ─ ─ ┘
    /// ┌ ─ ─ ─ ─ ─ ─ ─ ┴ ─ ─ ─ ─ ─ ─ ─ ─ Stage 1
    ///     ┌───────────────────────┐    │
    /// │   │      FilterExec       │
    ///     └───────────────────────┘    │
    /// │   ┌───────────────────────┐        Sets the max number of files
    ///     │    FileScanConfig     │◀───┼─   each task will handle. Less
    /// │   └───────────────────────┘        files_per_task == more tasks
    ///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
    ///```
    fn with_distributed_files_per_task(
        self,
        files_per_task: usize,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_files_per_task] but with an in-place mutation.
    fn set_distributed_files_per_task(
        &mut self,
        files_per_task: usize,
    ) -> Result<(), DataFusionError>;

    /// The number of tasks in each stage is calculated in a bottom-to-top fashion.
    ///
    /// Bottom stages containing leaf nodes will provide an estimation of the amount of tasks
    /// for those stages, but upper stages might see a reduction (or increment) in the amount
    /// of tasks based on the cardinality effect bottom stages have in the data.
    ///
    /// For example: If there are two stages, and the leaf stage is estimated to use 10 tasks,
    ///  the upper stage might use less (e.g. 5) if it sees that the leaf stage is returning
    ///  less data because of filters or aggregations.
    ///
    /// This function sets the scale factor for when encountering these nodes that change the
    /// cardinality of the data. For example, if a stage with 10 tasks contains an AggregateExec
    /// node, and the scale factor is 2.0, the following stage will use  10 / 2.0 = 5 tasks.
    ///
    /// ```text
    ///     ┌───────────────────────┐
    ///     │SortPreservingMergeExec│
    ///     └───────────────────────┘
    ///                 ▲
    /// ┌ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ─ ─ Stage 2 (N/scale_factor tasks)
    ///     ┌───────────┴───────────┐    │
    /// │   │       SortExec        │
    ///     └───────────────────────┘    │
    /// │   ┌───────────────────────┐
    ///     │     AggregateExec     │    │
    /// │   └───────────────────────┘
    ///  ─ ─ ─ ─ ─ ─ ─ ─▲─ ─ ─ ─ ─ ─ ─ ─ ┘
    /// ┌ ─ ─ ─ ─ ─ ─ ─ ┴ ─ ─ ─ ─ ─ ─ ─ ─ Stage 1 (N tasks)
    ///     ┌───────────────────────┐    │       A filter reduces cardinality,
    /// │   │      FilterExec       │◀────────therefore the next stage will have
    ///     └───────────────────────┘    │    less tasks according to this factor
    /// │   ┌───────────────────────┐
    ///     │    FileScanConfig     │    │
    /// │   └───────────────────────┘
    ///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
    /// ```
    fn with_distributed_cardinality_effect_task_scale_factor(
        self,
        factor: f64,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_cardinality_effect_task_scale_factor] but with
    /// an in-place mutation.
    fn set_distributed_cardinality_effect_task_scale_factor(
        &mut self,
        factor: f64,
    ) -> Result<(), DataFusionError>;

    /// Enables metrics collection across network boundaries so that all the metrics gather in
    /// each node are accessible from the head stage that started running the query.
    fn with_distributed_metrics_collection(self, enabled: bool) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_metrics_collection] but with an in-place mutation.
    fn set_distributed_metrics_collection(&mut self, enabled: bool) -> Result<(), DataFusionError>;

    /// Enables children isolator unions for distributing UNION operations across as many tasks as
    /// the sum of all the tasks required for each child.
    ///
    /// For example, if there is a UNION with 3 children, requiring one task each, it will result
    /// in a plan with 3 tasks where each task runs one child:
    ///
    /// ```text
    /// ┌─────────────────────────────┐┌─────────────────────────────┐┌─────────────────────────────┐
    /// │           Task 1            ││           Task 2            ││           Task 3            │
    /// │┌───────────────────────────┐││┌───────────────────────────┐││┌───────────────────────────┐│
    /// ││ ChildrenIsolatorUnionExec ││││ ChildrenIsolatorUnionExec ││││ ChildrenIsolatorUnionExec ││
    /// │└───▲─────────▲─────────▲───┘││└───▲─────────▲─────────▲───┘││└───▲─────────▲─────────▲───┘│
    /// │    │                        ││              │              ││                        │    │
    /// │┌───┴───┐ ┌  ─│ ─   ┌  ─│ ─  ││┌  ─│ ─   ┌───┴───┐ ┌  ─│ ─  ││┌  ─│ ─   ┌  ─│ ─   ┌───┴───┐│
    /// ││Child 1│  Child 2│  Child 3│││ Child 1│ │Child 2│  Child 3│││ Child 1│  Child 2│ │Child 3││
    /// │└───────┘ └  ─  ─   └  ─  ─  ││└  ─  ─   └───────┘ └  ─  ─  ││└  ─  ─   └  ─  ─   └───────┘│
    /// └─────────────────────────────┘└─────────────────────────────┘└─────────────────────────────┘
    /// ```
    fn with_distributed_children_isolator_unions(
        self,
        enabled: bool,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_children_isolator_unions] but with an in-place mutation.
    fn set_distributed_children_isolator_unions(
        &mut self,
        enabled: bool,
    ) -> Result<(), DataFusionError>;

    /// Enables broadcast joins for CollectLeft hash joins. When enabled, the build side of
    /// a CollectLeft join is broadcast to all consumer tasks instead of being coalesced
    /// into a single partition.
    ///
    /// Note: This option is disabled by default until the implementation is smarter about when to
    /// broadcast.
    fn with_distributed_broadcast_joins(self, enabled: bool) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_broadcast_joins_enabled] but with an in-place mutation.
    fn set_distributed_broadcast_joins(&mut self, enabled: bool) -> Result<(), DataFusionError>;

    /// The compression type to use for sending data over the wire.
    ///
    /// The default is [CompressionType::LZ4_FRAME].
    fn with_distributed_compression(
        self,
        compression: Option<CompressionType>,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_compression] but with an in-place mutation.
    fn set_distributed_compression(
        &mut self,
        compression: Option<CompressionType>,
    ) -> Result<(), DataFusionError>;

    /// How many rows to collect in each record batch before sending it over the wire in a
    /// shuffle operation. This value defaults to the same as `datafusion.execution.batch_size`.
    ///
    /// Setting it to something smaller than `datafusion.execution.batch_size` has no effect.
    ///
    /// It's preferable to set `datafusion.execution.batch_size` directly instead of this
    /// parameter if the specific use case allows it.
    fn with_distributed_shuffle_batch_size(
        self,
        batch_size: usize,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_shuffle_batch_size] but with an in-place mutation.
    fn set_distributed_shuffle_batch_size(
        &mut self,
        batch_size: usize,
    ) -> Result<(), DataFusionError>;

    /// Sets arbitrary HTTP headers that will be forwarded unchanged to worker nodes.
    /// These headers are included in outgoing Arrow Flight requests to workers.
    ///
    /// Returns an error if any header name starts with the reserved prefix
    /// `x-datafusion-distributed-config-`, which is used internally.
    ///
    /// Example:
    ///
    /// ```rust
    /// # use datafusion::execution::SessionStateBuilder;
    /// # use datafusion_distributed::DistributedExt;
    /// # use http::HeaderMap;
    ///
    /// let mut passthrough = HeaderMap::new();
    /// passthrough.insert("x-custom-priority", "high".parse().unwrap());
    ///
    /// let state = SessionStateBuilder::new()
    ///     .with_distributed_passthrough_headers(passthrough)
    ///     .unwrap()
    ///     .build();
    /// ```
    fn with_distributed_passthrough_headers(
        self,
        headers: HeaderMap,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_passthrough_headers] but with an in-place mutation.
    fn set_distributed_passthrough_headers(
        &mut self,
        headers: HeaderMap,
    ) -> Result<(), DataFusionError>;

    /// Sets the maximum tasks that will be assigned for each stage.
    ///
    /// If not specified, the number of workers returned by the provided [WorkerResolver] is taken.
    fn with_distributed_max_tasks_per_stage(
        self,
        max_tasks_per_stage: usize,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_max_tasks_per_stage] but with an in-place mutation.
    fn set_distributed_max_tasks_per_stage(
        &mut self,
        max_tasks_per_stage: usize,
    ) -> Result<(), DataFusionError>;

    /// Sets the maximum time spent establishing a gRPC connection to a worker.
    fn with_distributed_grpc_connect_timeout_ms(
        self,
        timeout_ms: usize,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_grpc_connect_timeout_ms] but with an in-place mutation.
    fn set_distributed_grpc_connect_timeout_ms(
        &mut self,
        timeout_ms: usize,
    ) -> Result<(), DataFusionError>;

    /// Sets the total timeout for outbound `do_get` RPCs.
    ///
    /// This is a full-stream deadline for the whole request, not an idle timeout.
    fn with_distributed_grpc_request_timeout_ms(
        self,
        timeout_ms: usize,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_grpc_request_timeout_ms] but with an in-place mutation.
    fn set_distributed_grpc_request_timeout_ms(
        &mut self,
        timeout_ms: usize,
    ) -> Result<(), DataFusionError>;

    /// Sets how long a worker waits for task data to become available before failing the request.
    fn with_distributed_wait_plan_timeout_ms(
        self,
        timeout_ms: usize,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_wait_plan_timeout_ms] but with an in-place mutation.
    fn set_distributed_wait_plan_timeout_ms(
        &mut self,
        timeout_ms: usize,
    ) -> Result<(), DataFusionError>;

    /// Sets the TCP keepalive period for worker-to-worker connections.
    fn with_distributed_grpc_tcp_keepalive_ms(
        self,
        keepalive_ms: usize,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_grpc_tcp_keepalive_ms] but with an in-place mutation.
    fn set_distributed_grpc_tcp_keepalive_ms(
        &mut self,
        keepalive_ms: usize,
    ) -> Result<(), DataFusionError>;
}

impl DistributedExt for SessionConfig {
    fn set_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T) {
        set_distributed_option_extension(self, t)
    }

    fn set_distributed_option_extension_from_headers<T: ConfigExtension + Default>(
        &mut self,
        headers: &HeaderMap,
    ) -> Result<(), DataFusionError> {
        set_distributed_option_extension_from_headers::<T>(self, headers)?;
        Ok(())
    }

    fn set_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T) {
        set_distributed_user_codec(self, codec)
    }

    fn set_distributed_user_codec_arc(&mut self, codec: Arc<dyn PhysicalExtensionCodec>) {
        set_distributed_user_codec_arc(self, codec)
    }

    fn set_distributed_worker_resolver<T: WorkerResolver + Send + Sync + 'static>(
        &mut self,
        resolver: T,
    ) {
        set_distributed_worker_resolver(self, resolver);
    }

    fn set_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(
        &mut self,
        resolver: T,
    ) {
        set_distributed_channel_resolver(self, resolver);
    }

    fn set_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(
        &mut self,
        estimator: T,
    ) {
        set_distributed_task_estimator(self, estimator)
    }

    fn set_distributed_files_per_task(
        &mut self,
        files_per_task: usize,
    ) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.files_per_task = files_per_task;
        Ok(())
    }

    fn set_distributed_cardinality_effect_task_scale_factor(
        &mut self,
        factor: f64,
    ) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.cardinality_task_count_factor = factor;
        Ok(())
    }

    fn set_distributed_metrics_collection(&mut self, enabled: bool) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.collect_metrics = enabled;
        Ok(())
    }

    fn set_distributed_children_isolator_unions(
        &mut self,
        enabled: bool,
    ) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.children_isolator_unions = enabled;
        Ok(())
    }

    fn set_distributed_broadcast_joins(&mut self, enabled: bool) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.broadcast_joins = enabled;
        Ok(())
    }

    fn set_distributed_compression(
        &mut self,
        compression: Option<CompressionType>,
    ) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.compression = match compression {
            Some(CompressionType::ZSTD) => "zstd".to_string(),
            Some(CompressionType::LZ4_FRAME) => "lz4".to_string(),
            _ => "none".to_string(),
        };
        Ok(())
    }

    fn set_distributed_shuffle_batch_size(
        &mut self,
        batch_size: usize,
    ) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.shuffle_batch_size = batch_size;
        Ok(())
    }

    fn set_distributed_passthrough_headers(
        &mut self,
        headers: HeaderMap,
    ) -> Result<(), DataFusionError> {
        set_passthrough_headers(self, headers)
    }

    fn set_distributed_max_tasks_per_stage(
        &mut self,
        max_tasks_per_stage: usize,
    ) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.max_tasks_per_stage = max_tasks_per_stage;
        Ok(())
    }

    fn set_distributed_grpc_connect_timeout_ms(
        &mut self,
        timeout_ms: usize,
    ) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.grpc_connect_timeout_ms = timeout_ms;
        Ok(())
    }

    fn set_distributed_grpc_request_timeout_ms(
        &mut self,
        timeout_ms: usize,
    ) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.grpc_request_timeout_ms = timeout_ms;
        Ok(())
    }

    fn set_distributed_wait_plan_timeout_ms(
        &mut self,
        timeout_ms: usize,
    ) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.wait_plan_timeout_ms = timeout_ms;
        Ok(())
    }

    fn set_distributed_grpc_tcp_keepalive_ms(
        &mut self,
        keepalive_ms: usize,
    ) -> Result<(), DataFusionError> {
        let d_cfg = DistributedConfig::from_config_options_mut(self.options_mut())?;
        d_cfg.grpc_tcp_keepalive_ms = keepalive_ms;
        Ok(())
    }

    delegate! {
        to self {
            #[call(set_distributed_option_extension)]
            #[expr($;self)]
            fn with_distributed_option_extension<T: ConfigExtension + Default>(mut self, t: T) -> Self;

            #[call(set_distributed_option_extension_from_headers)]
            #[expr($?;Ok(self))]
            fn with_distributed_option_extension_from_headers<T: ConfigExtension + Default>(mut self, headers: &HeaderMap) -> Result<Self, DataFusionError>;

            #[call(set_distributed_user_codec)]
            #[expr($;self)]
            fn with_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(mut self, codec: T) -> Self;

            #[call(set_distributed_user_codec_arc)]
            #[expr($;self)]
            fn with_distributed_user_codec_arc(mut self, codec: Arc<dyn PhysicalExtensionCodec>) -> Self;

            #[call(set_distributed_worker_resolver)]
            #[expr($;self)]
            fn with_distributed_worker_resolver<T: WorkerResolver + Send + Sync + 'static>(mut self, resolver: T) -> Self;

            #[call(set_distributed_channel_resolver)]
            #[expr($;self)]
            fn with_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(mut self, resolver: T) -> Self;

            #[call(set_distributed_task_estimator)]
            #[expr($;self)]
            fn with_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(mut self, estimator: T) -> Self;

            #[call(set_distributed_files_per_task)]
            #[expr($?;Ok(self))]
            fn with_distributed_files_per_task(mut self, files_per_task: usize) -> Result<Self, DataFusionError>;

            #[call(set_distributed_cardinality_effect_task_scale_factor)]
            #[expr($?;Ok(self))]
            fn with_distributed_cardinality_effect_task_scale_factor(mut self, factor: f64) -> Result<Self, DataFusionError>;

            #[call(set_distributed_metrics_collection)]
            #[expr($?;Ok(self))]
            fn with_distributed_metrics_collection(mut self, enabled: bool) -> Result<Self, DataFusionError>;

            #[call(set_distributed_children_isolator_unions)]
            #[expr($?;Ok(self))]
            fn with_distributed_children_isolator_unions(mut self, enabled: bool) -> Result<Self, DataFusionError>;

            #[call(set_distributed_broadcast_joins)]
            #[expr($?;Ok(self))]
            fn with_distributed_broadcast_joins(mut self, enabled: bool) -> Result<Self, DataFusionError>;

            #[call(set_distributed_compression)]
            #[expr($?;Ok(self))]
            fn with_distributed_compression(mut self, compression: Option<CompressionType>) -> Result<Self, DataFusionError>;

            #[call(set_distributed_shuffle_batch_size)]
            #[expr($?;Ok(self))]
            fn with_distributed_shuffle_batch_size(mut self, batch_size: usize) -> Result<Self, DataFusionError>;

            #[call(set_distributed_passthrough_headers)]
            #[expr($?;Ok(self))]
            fn with_distributed_passthrough_headers(mut self, headers: HeaderMap) -> Result<Self, DataFusionError>;

            #[call(set_distributed_max_tasks_per_stage)]
            #[expr($?;Ok(self))]
            fn with_distributed_max_tasks_per_stage(mut self, max_tasks_per_stage: usize) -> Result<Self, DataFusionError>;

            #[call(set_distributed_grpc_connect_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_connect_timeout_ms(mut self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            #[call(set_distributed_grpc_request_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_request_timeout_ms(mut self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            #[call(set_distributed_wait_plan_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_wait_plan_timeout_ms(mut self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            #[call(set_distributed_grpc_tcp_keepalive_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_tcp_keepalive_ms(mut self, keepalive_ms: usize) -> Result<Self, DataFusionError>;
        }
    }
}

impl DistributedExt for SessionStateBuilder {
    delegate! {
        to self.config().get_or_insert_default() {
            fn set_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T);
            #[call(set_distributed_option_extension)]
            #[expr($;self)]
            fn with_distributed_option_extension<T: ConfigExtension + Default>(mut self, t: T) -> Self;

            fn set_distributed_option_extension_from_headers<T: ConfigExtension + Default>(&mut self, h: &HeaderMap) -> Result<(), DataFusionError>;
            #[call(set_distributed_option_extension_from_headers)]
            #[expr($?;Ok(self))]
            fn with_distributed_option_extension_from_headers<T: ConfigExtension + Default>(mut self, headers: &HeaderMap) -> Result<Self, DataFusionError>;

            fn set_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T);
            #[call(set_distributed_user_codec)]
            #[expr($;self)]
            fn with_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(mut self, codec: T) -> Self;

            fn set_distributed_user_codec_arc(&mut self, codec: Arc<dyn PhysicalExtensionCodec>);
            #[call(set_distributed_user_codec_arc)]
            #[expr($;self)]
            fn with_distributed_user_codec_arc(mut self, codec: Arc<dyn PhysicalExtensionCodec>) -> Self;

            fn set_distributed_worker_resolver<T: WorkerResolver + Send + Sync + 'static>(&mut self, resolver: T);
            #[call(set_distributed_worker_resolver)]
            #[expr($;self)]
            fn with_distributed_worker_resolver<T: WorkerResolver + Send + Sync + 'static>(mut self, resolver: T) -> Self;

            fn set_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(&mut self, resolver: T);
            #[call(set_distributed_channel_resolver)]
            #[expr($;self)]
            fn with_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(mut self, resolver: T) -> Self;

            fn set_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(&mut self, estimator: T);
            #[call(set_distributed_task_estimator)]
            #[expr($;self)]
            fn with_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(mut self, estimator: T) -> Self;

            fn set_distributed_files_per_task(&mut self, files_per_task: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_files_per_task)]
            #[expr($?;Ok(self))]
            fn with_distributed_files_per_task(mut self, files_per_task: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_cardinality_effect_task_scale_factor(&mut self, factor: f64) -> Result<(), DataFusionError>;
            #[call(set_distributed_cardinality_effect_task_scale_factor)]
            #[expr($?;Ok(self))]
            fn with_distributed_cardinality_effect_task_scale_factor(mut self, factor: f64) -> Result<Self, DataFusionError>;

            fn set_distributed_metrics_collection(&mut self, enabled: bool) -> Result<(), DataFusionError>;
            #[call(set_distributed_metrics_collection)]
            #[expr($?;Ok(self))]
            fn with_distributed_metrics_collection(mut self, enabled: bool) -> Result<Self, DataFusionError>;

            fn set_distributed_children_isolator_unions(&mut self, enabled: bool) -> Result<(), DataFusionError>;
            #[call(set_distributed_children_isolator_unions)]
            #[expr($?;Ok(self))]
            fn with_distributed_children_isolator_unions(mut self, enabled: bool) -> Result<Self, DataFusionError>;

            fn set_distributed_broadcast_joins(&mut self, enabled: bool) -> Result<(), DataFusionError>;
            #[call(set_distributed_broadcast_joins)]
            #[expr($?;Ok(self))]
            fn with_distributed_broadcast_joins(mut self, enabled: bool) -> Result<Self, DataFusionError>;

            fn set_distributed_compression(&mut self, compression: Option<CompressionType>) -> Result<(), DataFusionError>;
            #[call(set_distributed_compression)]
            #[expr($?;Ok(self))]
            fn with_distributed_compression(mut self, compression: Option<CompressionType>) -> Result<Self, DataFusionError>;

            fn set_distributed_shuffle_batch_size(&mut self, batch_size: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_shuffle_batch_size)]
            #[expr($?;Ok(self))]
            fn with_distributed_shuffle_batch_size(mut self, batch_size: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_passthrough_headers(&mut self, headers: HeaderMap) -> Result<(), DataFusionError>;
            #[call(set_distributed_passthrough_headers)]
            #[expr($?;Ok(self))]
            fn with_distributed_passthrough_headers(mut self, headers: HeaderMap) -> Result<Self, DataFusionError>;

            fn set_distributed_max_tasks_per_stage(&mut self, max_tasks_per_stage: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_max_tasks_per_stage)]
            #[expr($?;Ok(self))]
            fn with_distributed_max_tasks_per_stage(mut self, max_tasks_per_stage: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_grpc_connect_timeout_ms(&mut self, timeout_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_grpc_connect_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_connect_timeout_ms(mut self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_grpc_request_timeout_ms(&mut self, timeout_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_grpc_request_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_request_timeout_ms(mut self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_wait_plan_timeout_ms(&mut self, timeout_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_wait_plan_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_wait_plan_timeout_ms(mut self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_grpc_tcp_keepalive_ms(&mut self, keepalive_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_grpc_tcp_keepalive_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_tcp_keepalive_ms(mut self, keepalive_ms: usize) -> Result<Self, DataFusionError>;
        }
    }
}

impl DistributedExt for SessionState {
    delegate! {
        to self.config_mut() {
            fn set_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T);
            #[call(set_distributed_option_extension)]
            #[expr($;self)]
            fn with_distributed_option_extension<T: ConfigExtension + Default>(mut self, t: T) -> Self;

            fn set_distributed_option_extension_from_headers<T: ConfigExtension + Default>(&mut self, h: &HeaderMap) -> Result<(), DataFusionError>;
            #[call(set_distributed_option_extension_from_headers)]
            #[expr($?;Ok(self))]
            fn with_distributed_option_extension_from_headers<T: ConfigExtension + Default>(mut self, headers: &HeaderMap) -> Result<Self, DataFusionError>;

            fn set_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T);
            #[call(set_distributed_user_codec)]
            #[expr($;self)]
            fn with_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(mut self, codec: T) -> Self;

            fn set_distributed_user_codec_arc(&mut self, codec: Arc<dyn PhysicalExtensionCodec>);
            #[call(set_distributed_user_codec_arc)]
            #[expr($;self)]
            fn with_distributed_user_codec_arc(mut self, codec: Arc<dyn PhysicalExtensionCodec>) -> Self;

            fn set_distributed_worker_resolver<T: WorkerResolver + Send + Sync + 'static>(&mut self, resolver: T);
            #[call(set_distributed_worker_resolver)]
            #[expr($;self)]
            fn with_distributed_worker_resolver<T: WorkerResolver + Send + Sync + 'static>(mut self, resolver: T) -> Self;

            fn set_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(&mut self, resolver: T);
            #[call(set_distributed_channel_resolver)]
            #[expr($;self)]
            fn with_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(mut self, resolver: T) -> Self;

            fn set_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(&mut self, estimator: T);
            #[call(set_distributed_task_estimator)]
            #[expr($;self)]
            fn with_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(mut self, estimator: T) -> Self;

            fn set_distributed_files_per_task(&mut self, files_per_task: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_files_per_task)]
            #[expr($?;Ok(self))]
            fn with_distributed_files_per_task(mut self, files_per_task: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_cardinality_effect_task_scale_factor(&mut self, factor: f64) -> Result<(), DataFusionError>;
            #[call(set_distributed_cardinality_effect_task_scale_factor)]
            #[expr($?;Ok(self))]
            fn with_distributed_cardinality_effect_task_scale_factor(mut self, factor: f64) -> Result<Self, DataFusionError>;

            fn set_distributed_metrics_collection(&mut self, enabled: bool) -> Result<(), DataFusionError>;
            #[call(set_distributed_metrics_collection)]
            #[expr($?;Ok(self))]
            fn with_distributed_metrics_collection(mut self, enabled: bool) -> Result<Self, DataFusionError>;

            fn set_distributed_children_isolator_unions(&mut self, enabled: bool) -> Result<(), DataFusionError>;
            #[call(set_distributed_children_isolator_unions)]
            #[expr($?;Ok(self))]
            fn with_distributed_children_isolator_unions(mut self, enabled: bool) -> Result<Self, DataFusionError>;

            fn set_distributed_broadcast_joins(&mut self, enabled: bool) -> Result<(), DataFusionError>;
            #[call(set_distributed_broadcast_joins)]
            #[expr($?;Ok(self))]
            fn with_distributed_broadcast_joins(mut self, enabled: bool) -> Result<Self, DataFusionError>;

            fn set_distributed_compression(&mut self, compression: Option<CompressionType>) -> Result<(), DataFusionError>;
            #[call(set_distributed_compression)]
            #[expr($?;Ok(self))]
            fn with_distributed_compression(mut self, compression: Option<CompressionType>) -> Result<Self, DataFusionError>;

            fn set_distributed_shuffle_batch_size(&mut self, batch_size: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_shuffle_batch_size)]
            #[expr($?;Ok(self))]
            fn with_distributed_shuffle_batch_size(mut self, batch_size: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_passthrough_headers(&mut self, headers: HeaderMap) -> Result<(), DataFusionError>;
            #[call(set_distributed_passthrough_headers)]
            #[expr($?;Ok(self))]
            fn with_distributed_passthrough_headers(mut self, headers: HeaderMap) -> Result<Self, DataFusionError>;

            fn set_distributed_max_tasks_per_stage(&mut self, max_tasks_per_stage: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_max_tasks_per_stage)]
            #[expr($?;Ok(self))]
            fn with_distributed_max_tasks_per_stage(mut self, max_tasks_per_stage: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_grpc_connect_timeout_ms(&mut self, timeout_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_grpc_connect_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_connect_timeout_ms(mut self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_grpc_request_timeout_ms(&mut self, timeout_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_grpc_request_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_request_timeout_ms(mut self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_wait_plan_timeout_ms(&mut self, timeout_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_wait_plan_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_wait_plan_timeout_ms(mut self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_grpc_tcp_keepalive_ms(&mut self, keepalive_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_grpc_tcp_keepalive_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_tcp_keepalive_ms(mut self, keepalive_ms: usize) -> Result<Self, DataFusionError>;
        }
    }
}

impl DistributedExt for SessionContext {
    delegate! {
        to self.state_ref().write().config_mut() {
            fn set_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T);
            #[call(set_distributed_option_extension)]
            #[expr($;self)]
            fn with_distributed_option_extension<T: ConfigExtension + Default>(self, t: T) -> Self;

            fn set_distributed_option_extension_from_headers<T: ConfigExtension + Default>(&mut self, h: &HeaderMap) -> Result<(), DataFusionError>;
            #[call(set_distributed_option_extension_from_headers)]
            #[expr($?;Ok(self))]
            fn with_distributed_option_extension_from_headers<T: ConfigExtension + Default>(self, headers: &HeaderMap) -> Result<Self, DataFusionError>;

            fn set_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T);
            #[call(set_distributed_user_codec)]
            #[expr($;self)]
            fn with_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(self, codec: T) -> Self;

            fn set_distributed_user_codec_arc(&mut self, codec: Arc<dyn PhysicalExtensionCodec>);
            #[call(set_distributed_user_codec_arc)]
            #[expr($;self)]
            fn with_distributed_user_codec_arc(self, codec: Arc<dyn PhysicalExtensionCodec>) -> Self;

            fn set_distributed_worker_resolver<T: WorkerResolver + Send + Sync + 'static>(&mut self, resolver: T);
            #[call(set_distributed_worker_resolver)]
            #[expr($;self)]
            fn with_distributed_worker_resolver<T: WorkerResolver + Send + Sync + 'static>(self, resolver: T) -> Self;

            fn set_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(&mut self, resolver: T);
            #[call(set_distributed_channel_resolver)]
            #[expr($;self)]
            fn with_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(self, resolver: T) -> Self;

            fn set_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(&mut self, estimator: T);
            #[call(set_distributed_task_estimator)]
            #[expr($;self)]
            fn with_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(self, estimator: T) -> Self;

            fn set_distributed_files_per_task(&mut self, files_per_task: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_files_per_task)]
            #[expr($?;Ok(self))]
            fn with_distributed_files_per_task(self, files_per_task: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_cardinality_effect_task_scale_factor(&mut self, factor: f64) -> Result<(), DataFusionError>;
            #[call(set_distributed_cardinality_effect_task_scale_factor)]
            #[expr($?;Ok(self))]
            fn with_distributed_cardinality_effect_task_scale_factor(self, factor: f64) -> Result<Self, DataFusionError>;

            fn set_distributed_metrics_collection(&mut self, enabled: bool) -> Result<(), DataFusionError>;
            #[call(set_distributed_metrics_collection)]
            #[expr($?;Ok(self))]
            fn with_distributed_metrics_collection(self, enabled: bool) -> Result<Self, DataFusionError>;

            fn set_distributed_children_isolator_unions(&mut self, enabled: bool) -> Result<(), DataFusionError>;
            #[call(set_distributed_children_isolator_unions)]
            #[expr($?;Ok(self))]
            fn with_distributed_children_isolator_unions(self, enabled: bool) -> Result<Self, DataFusionError>;

            fn set_distributed_broadcast_joins(&mut self, enabled: bool) -> Result<(), DataFusionError>;
            #[call(set_distributed_broadcast_joins)]
            #[expr($?;Ok(self))]
            fn with_distributed_broadcast_joins(self, enabled: bool) -> Result<Self, DataFusionError>;

            fn set_distributed_compression(&mut self, compression: Option<CompressionType>) -> Result<(), DataFusionError>;
            #[call(set_distributed_compression)]
            #[expr($?;Ok(self))]
            fn with_distributed_compression(self, compression: Option<CompressionType>) -> Result<Self, DataFusionError>;

            fn set_distributed_shuffle_batch_size(&mut self, batch_size: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_shuffle_batch_size)]
            #[expr($?;Ok(self))]
            fn with_distributed_shuffle_batch_size(self, batch_size: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_passthrough_headers(&mut self, headers: HeaderMap) -> Result<(), DataFusionError>;
            #[call(set_distributed_passthrough_headers)]
            #[expr($?;Ok(self))]
            fn with_distributed_passthrough_headers(self, headers: HeaderMap) -> Result<Self, DataFusionError>;

            fn set_distributed_max_tasks_per_stage(&mut self, max_tasks_per_stage: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_max_tasks_per_stage)]
            #[expr($?;Ok(self))]
            fn with_distributed_max_tasks_per_stage(self, max_tasks_per_stage: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_grpc_connect_timeout_ms(&mut self, timeout_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_grpc_connect_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_connect_timeout_ms(self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_grpc_request_timeout_ms(&mut self, timeout_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_grpc_request_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_request_timeout_ms(self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_wait_plan_timeout_ms(&mut self, timeout_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_wait_plan_timeout_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_wait_plan_timeout_ms(self, timeout_ms: usize) -> Result<Self, DataFusionError>;

            fn set_distributed_grpc_tcp_keepalive_ms(&mut self, keepalive_ms: usize) -> Result<(), DataFusionError>;
            #[call(set_distributed_grpc_tcp_keepalive_ms)]
            #[expr($?;Ok(self))]
            fn with_distributed_grpc_tcp_keepalive_ms(self, keepalive_ms: usize) -> Result<Self, DataFusionError>;
        }
    }
}
