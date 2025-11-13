use crate::channel_resolver_ext::set_distributed_channel_resolver;
use crate::config_extension_ext::{
    set_distributed_option_extension, set_distributed_option_extension_from_headers,
};
use crate::distributed_planner::set_distributed_task_estimator;
use crate::protobuf::{set_distributed_user_codec, set_distributed_user_codec_arc};
use crate::{ChannelResolver, DistributedConfig, TaskEstimator};
use datafusion::common::DataFusionError;
use datafusion::config::ConfigExtension;
use datafusion::execution::SessionStateBuilder;
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
    /// # use datafusion_distributed::{DistributedExt, DistributedSessionBuilder, DistributedSessionBuilderContext};
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
    ///     .with_distributed_option_extension(my_custom_extension).unwrap()
    ///     .build();
    ///
    /// async fn build_state(ctx: DistributedSessionBuilderContext) -> Result<SessionState, DataFusionError> {
    ///     // This function can be provided to an ArrowFlightEndpoint in order to tell it how to
    ///     // build sessions that retrieve the CustomExtension from gRPC metadata.
    ///     Ok(SessionStateBuilder::new()
    ///         .with_distributed_option_extension_from_headers::<CustomExtension>(&ctx.headers)?
    ///         .build())
    /// }
    /// ```
    fn with_distributed_option_extension<T: ConfigExtension + Default>(
        self,
        t: T,
    ) -> Result<Self, DataFusionError>;

    /// Same as [DistributedExt::with_distributed_option_extension] but with an in-place mutation
    fn set_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
        t: T,
    ) -> Result<(), DataFusionError>;

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
    /// # use datafusion_distributed::{DistributedExt, DistributedSessionBuilder, DistributedSessionBuilderContext};
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
    ///     .with_distributed_option_extension(my_custom_extension).unwrap()
    ///     .build();
    ///
    /// async fn build_state(ctx: DistributedSessionBuilderContext) -> Result<SessionState, DataFusionError> {
    ///     // This function can be provided to an ArrowFlightEndpoint in order to tell it how to
    ///     // build sessions that retrieve the CustomExtension from gRPC metadata.
    ///     Ok(SessionStateBuilder::new()
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
    /// # use datafusion::execution::{SessionState, FunctionRegistry, SessionStateBuilder};
    /// # use datafusion::physical_plan::ExecutionPlan;
    /// # use datafusion::prelude::SessionConfig;
    /// # use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    /// # use datafusion_distributed::{DistributedExt, DistributedSessionBuilderContext};
    ///
    /// #[derive(Debug)]
    /// struct CustomExecCodec;
    ///
    /// impl PhysicalExtensionCodec for CustomExecCodec {
    ///     fn try_decode(&self, buf: &[u8], inputs: &[Arc<dyn ExecutionPlan>], registry: &dyn FunctionRegistry) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
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
    /// async fn build_state(ctx: DistributedSessionBuilderContext) -> Result<SessionState, DataFusionError> {
    ///     // This function can be provided to an ArrowFlightEndpoint in order to tell it how to
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

    /// Injects a [ChannelResolver] implementation for Distributed DataFusion to resolve worker
    /// nodes. When running in distributed mode, setting a [ChannelResolver] is required.
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
    /// # use datafusion_distributed::{BoxCloneSyncChannel, ChannelResolver, DistributedExt, DistributedPhysicalOptimizerRule, DistributedSessionBuilderContext};
    ///
    /// struct CustomChannelResolver;
    ///
    /// #[async_trait]
    /// impl ChannelResolver for CustomChannelResolver {
    ///     fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
    ///         todo!()
    ///     }
    ///
    ///     async fn get_flight_client_for_url(&self, url: &Url) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
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
    /// // This function can be provided to an ArrowFlightEndpoint so that, upon receiving a distributed
    /// // part of a plan, it knows how to resolve gRPC channels from URLs for making network calls to other nodes.
    /// async fn build_state(ctx: DistributedSessionBuilderContext) -> Result<SessionState, DataFusionError> {
    ///     Ok(SessionStateBuilder::new()
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

    /// Adds a distributed task count estimator. Estimators are executed on leaf nodes
    /// sequentially until one returns an estimation on the amount of tasks that should be
    /// used for the stage containing the leaf node.
    ///
    /// The first one that returns something for a leaf node is the one that decides how many
    /// tasks are used.
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
    /// │   ┌───────────────────────┐       TaskEstimator estimates tasks in
    ///     │       SomeExec        │◀───┼──  stages containing leaf nodes
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
}

impl DistributedExt for SessionStateBuilder {
    fn set_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
        t: T,
    ) -> Result<(), DataFusionError> {
        set_distributed_option_extension(self.config().get_or_insert_default(), t)
    }

    fn set_distributed_option_extension_from_headers<T: ConfigExtension + Default>(
        &mut self,
        headers: &HeaderMap,
    ) -> Result<(), DataFusionError> {
        set_distributed_option_extension_from_headers::<T>(
            self.config().get_or_insert_default(),
            headers,
        )
    }

    fn set_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T) {
        set_distributed_user_codec(self.config().get_or_insert_default(), codec)
    }

    fn set_distributed_user_codec_arc(&mut self, codec: Arc<dyn PhysicalExtensionCodec>) {
        set_distributed_user_codec_arc(self.config().get_or_insert_default(), codec)
    }

    fn set_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(
        &mut self,
        resolver: T,
    ) {
        let cfg = self.config().get_or_insert_default();
        set_distributed_channel_resolver(cfg, resolver);
    }

    fn set_distributed_task_estimator<T: TaskEstimator + Send + Sync + 'static>(
        &mut self,
        estimator: T,
    ) {
        set_distributed_task_estimator(self.config().get_or_insert_default(), estimator)
    }

    fn set_distributed_files_per_task(
        &mut self,
        files_per_task: usize,
    ) -> Result<(), DataFusionError> {
        let cfg = self.config().get_or_insert_default();
        let d_cfg = DistributedConfig::from_config_options_mut(cfg.options_mut())?;
        d_cfg.files_per_task = files_per_task;
        Ok(())
    }

    fn set_distributed_cardinality_effect_task_scale_factor(
        &mut self,
        factor: f64,
    ) -> Result<(), DataFusionError> {
        let cfg = self.config().get_or_insert_default();
        let d_cfg = DistributedConfig::from_config_options_mut(cfg.options_mut())?;
        d_cfg.cardinality_task_count_factor = factor;
        Ok(())
    }

    delegate! {
        to self {
            #[call(set_distributed_option_extension)]
            #[expr($?;Ok(self))]
            fn with_distributed_option_extension<T: ConfigExtension + Default>(mut self, t: T) -> Result<Self, DataFusionError>;

            #[call(set_distributed_option_extension_from_headers)]
            #[expr($?;Ok(self))]
            fn with_distributed_option_extension_from_headers<T: ConfigExtension + Default>(mut self, headers: &HeaderMap) -> Result<Self, DataFusionError>;

            #[call(set_distributed_user_codec)]
            #[expr($;self)]
            fn with_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(mut self, codec: T) -> Self;

            #[call(set_distributed_user_codec_arc)]
            #[expr($;self)]
            fn with_distributed_user_codec_arc(mut self, codec: Arc<dyn PhysicalExtensionCodec>) -> Self;

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
        }
    }
}
