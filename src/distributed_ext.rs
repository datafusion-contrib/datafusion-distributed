use crate::config_extension_ext::{
    add_distributed_option_extension, retrieve_distributed_option_extension,
};
use crate::user_codec_ext::add_user_codec;
use datafusion::common::DataFusionError;
use datafusion::config::ConfigExtension;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use delegate::delegate;
use http::HeaderMap;

/// Extends DataFusion with distributed capabilities.
pub trait DistributedExt {
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
    /// let mut config = SessionConfig::new();
    /// let mut opt = CustomExtension::default();
    /// // Now, the CustomExtension will be able to cross network boundaries. Upon making an Arrow
    /// // Flight request, it will be sent through gRPC metadata.
    /// config.add_distributed_option_extension(opt).unwrap();
    ///
    /// async fn build_state(ctx: DistributedSessionBuilderContext) -> Result<SessionState, DataFusionError> {
    ///     let mut state = SessionStateBuilder::new().build();
    ///
    ///     // while providing this MyCustomSessionBuilder to an Arrow Flight endpoint, it will
    ///     // know how to deserialize the CustomExtension from the gRPC metadata.
    ///     state.retrieve_distributed_option_extension::<CustomExtension>(&ctx.headers)?;
    ///     Ok(state)
    /// }
    /// ```
    fn add_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
        t: T,
    ) -> Result<(), DataFusionError>;

    /// Gets the specified [ConfigExtension] from the distributed context and adds it to
    /// the [SessionConfig::options] extensions. The function will build a new [ConfigExtension]
    /// out of the Arrow Flight gRPC metadata present in the [SessionConfig] and will propagate it
    /// to the extension options.
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
    /// let mut config = SessionConfig::new();
    /// let mut opt = CustomExtension::default();
    /// // Now, the CustomExtension will be able to cross network boundaries. Upon making an Arrow
    /// // Flight request, it will be sent through gRPC metadata.
    /// config.add_distributed_option_extension(opt).unwrap();
    ///
    /// async fn build_state(ctx: DistributedSessionBuilderContext) -> Result<SessionState, DataFusionError> {
    ///     let mut state = SessionStateBuilder::new().build();
    ///
    ///     // while providing this MyCustomSessionBuilder to an Arrow Flight endpoint, it will
    ///     // know how to deserialize the CustomExtension from the gRPC metadata.
    ///     state.retrieve_distributed_option_extension::<CustomExtension>(&ctx.headers)?;
    ///     Ok(state)
    /// }
    /// ```
    fn retrieve_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
        headers: &HeaderMap,
    ) -> Result<(), DataFusionError>;

    /// Injects a user-defined codec that is capable of encoding/decoding custom execution nodes.
    ///
    /// Example:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::execution::{SessionState, FunctionRegistry, SessionStateBuilder};
    /// # use datafusion::physical_plan::ExecutionPlan;
    /// # use datafusion::prelude::SessionConfig;
    /// # use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    /// # use datafusion_distributed::DistributedExt;
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
    /// let mut config = SessionConfig::new();
    /// config.add_user_codec(CustomExecCodec)
    /// ```
    fn add_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T);
}

impl DistributedExt for SessionConfig {
    fn add_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
        t: T,
    ) -> Result<(), DataFusionError> {
        add_distributed_option_extension(self, t)
    }

    fn retrieve_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
        headers: &HeaderMap,
    ) -> Result<(), DataFusionError> {
        retrieve_distributed_option_extension::<T>(self, headers)
    }

    fn add_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T) {
        add_user_codec(self, codec)
    }
}

impl DistributedExt for SessionStateBuilder {
    delegate! {
        to self.config().get_or_insert_default() {
            fn add_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T) -> Result<(), DataFusionError>;
            fn retrieve_distributed_option_extension<T: ConfigExtension + Default>(&mut self, h: &HeaderMap) -> Result<(), DataFusionError>;
            fn add_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T);
        }
    }
}

impl DistributedExt for SessionState {
    delegate! {
        to self.config_mut() {
            fn add_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T) -> Result<(), DataFusionError>;
            fn retrieve_distributed_option_extension<T: ConfigExtension + Default>(&mut self, h: &HeaderMap) -> Result<(), DataFusionError>;
            fn add_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T);
        }
    }
}

impl DistributedExt for SessionContext {
    delegate! {
        to self.state_ref().write().config_mut() {
            fn add_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T) -> Result<(), DataFusionError>;
            fn retrieve_distributed_option_extension<T: ConfigExtension + Default>(&mut self, h: &HeaderMap) -> Result<(), DataFusionError>;
            fn add_user_codec<T: PhysicalExtensionCodec + 'static>(&mut self, codec: T);
        }
    }
}
