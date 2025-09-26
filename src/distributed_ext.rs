use crate::ChannelResolver;
use crate::channel_resolver_ext::set_distributed_channel_resolver;
use crate::config_extension_ext::{
    set_distributed_option_extension, set_distributed_option_extension_from_headers,
};
use crate::protobuf::{set_distributed_user_codec, set_distributed_user_codec_arc};
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
    /// let mut config = SessionConfig::new()
    ///     .with_distributed_option_extension(my_custom_extension).unwrap();
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
    /// let mut config = SessionConfig::new()
    ///     .with_distributed_option_extension(my_custom_extension).unwrap();
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
    /// let config = SessionConfig::new().with_distributed_user_codec(CustomExecCodec);
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
    /// # use async_trait::async_trait;
    /// # use datafusion::common::DataFusionError;
    /// # use datafusion::execution::{SessionState, SessionStateBuilder};
    /// # use datafusion::prelude::SessionConfig;
    /// # use url::Url;
    /// # use datafusion_distributed::{BoxCloneSyncChannel, ChannelResolver, DistributedExt, DistributedSessionBuilderContext};
    ///
    /// struct CustomChannelResolver;
    ///
    /// #[async_trait]
    /// impl ChannelResolver for CustomChannelResolver {
    ///     fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
    ///         todo!()
    ///     }
    ///
    ///     async fn get_channel_for_url(&self, url: &Url) -> Result<BoxCloneSyncChannel, DataFusionError> {
    ///         todo!()
    ///     }
    /// }
    ///
    /// let config = SessionConfig::new().with_distributed_channel_resolver(CustomChannelResolver);
    ///
    /// async fn build_state(ctx: DistributedSessionBuilderContext) -> Result<SessionState, DataFusionError> {
    ///     // This function can be provided to an ArrowFlightEndpoint so that it knows how to
    ///     // resolve tonic channels from URLs upon making network calls to other nodes.
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
}

impl DistributedExt for SessionConfig {
    fn set_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
        t: T,
    ) -> Result<(), DataFusionError> {
        set_distributed_option_extension(self, t)
    }

    fn set_distributed_option_extension_from_headers<T: ConfigExtension + Default>(
        &mut self,
        headers: &HeaderMap,
    ) -> Result<(), DataFusionError> {
        set_distributed_option_extension_from_headers::<T>(self, headers)
    }

    fn set_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T) {
        set_distributed_user_codec(self, codec)
    }

    fn set_distributed_user_codec_arc(&mut self, codec: Arc<dyn PhysicalExtensionCodec>) {
        set_distributed_user_codec_arc(self, codec)
    }

    fn set_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(
        &mut self,
        resolver: T,
    ) {
        set_distributed_channel_resolver(self, resolver)
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
        }
    }
}

impl DistributedExt for SessionStateBuilder {
    delegate! {
        to self.config().get_or_insert_default() {
            fn set_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T) -> Result<(), DataFusionError>;
            #[call(set_distributed_option_extension)]
            #[expr($?;Ok(self))]
            fn with_distributed_option_extension<T: ConfigExtension + Default>(mut self, t: T) -> Result<Self, DataFusionError>;

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

            fn set_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(&mut self, resolver: T);
            #[call(set_distributed_channel_resolver)]
            #[expr($;self)]
            fn with_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(mut self, resolver: T) -> Self;
        }
    }
}

impl DistributedExt for SessionState {
    delegate! {
        to self.config_mut() {
            fn set_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T) -> Result<(), DataFusionError>;
            #[call(set_distributed_option_extension)]
            #[expr($?;Ok(self))]
            fn with_distributed_option_extension<T: ConfigExtension + Default>(mut self, t: T) -> Result<Self, DataFusionError>;

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

            fn set_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(&mut self, resolver: T);
            #[call(set_distributed_channel_resolver)]
            #[expr($;self)]
            fn with_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(mut self, resolver: T) -> Self;
        }
    }
}

impl DistributedExt for SessionContext {
    delegate! {
        to self.state_ref().write().config_mut() {
            fn set_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T) -> Result<(), DataFusionError>;
            #[call(set_distributed_option_extension)]
            #[expr($?;Ok(self))]
            fn with_distributed_option_extension<T: ConfigExtension + Default>(self, t: T) -> Result<Self, DataFusionError>;

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

            fn set_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(&mut self, resolver: T);
            #[call(set_distributed_channel_resolver)]
            #[expr($;self)]
            fn with_distributed_channel_resolver<T: ChannelResolver + Send + Sync + 'static>(self, resolver: T) -> Self;
        }
    }
}
