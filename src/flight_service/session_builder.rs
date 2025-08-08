use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::SessionContext;

/// Trait called by the Arrow Flight endpoint that handles distributed parts of a DataFusion
/// plan for building a DataFusion's [datafusion::prelude::SessionContext].
#[async_trait]
pub trait SessionBuilder {
    /// Takes a [SessionStateBuilder] and adds whatever is necessary for it to work, like
    /// custom extension codecs, custom physical optimization rules, UDFs, UDAFs, config
    /// extensions, etc...
    ///
    /// Example: adding some custom extension plan codecs
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use async_trait::async_trait;
    /// # use datafusion::error::DataFusionError;
    /// # use datafusion::execution::runtime_env::RuntimeEnv;
    /// # use datafusion::execution::{FunctionRegistry, SessionStateBuilder};
    /// # use datafusion::physical_plan::ExecutionPlan;
    /// # use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    /// # use datafusion_distributed::{with_user_codec, SessionBuilder};
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
    /// #[derive(Clone)]
    /// struct CustomSessionBuilder;
    ///
    /// #[async_trait]
    /// impl SessionBuilder for CustomSessionBuilder {
    ///     fn session_state_builder(&self, mut builder: SessionStateBuilder) -> Result<SessionStateBuilder, DataFusionError> {
    ///         // Add your UDFs, optimization rules, etc...
    ///         Ok(with_user_codec(builder, CustomExecCodec))
    ///     }
    /// }
    /// ```
    fn session_state_builder(
        &self,
        builder: SessionStateBuilder,
    ) -> Result<SessionStateBuilder, DataFusionError> {
        Ok(builder)
    }

    /// Modifies the [SessionState] and returns it. Same as [SessionBuilder::session_state_builder]
    /// but operating on an already built [SessionState].
    ///
    /// Example:
    ///
    /// ```rust
    /// # use async_trait::async_trait;
    /// # use datafusion::common::DataFusionError;
    /// # use datafusion::execution::SessionState;
    /// # use datafusion_distributed::SessionBuilder;
    ///
    /// #[derive(Clone)]
    /// struct CustomSessionBuilder;
    ///
    /// #[async_trait]
    /// impl SessionBuilder for CustomSessionBuilder {
    ///     async fn session_state(&self, state: SessionState) -> Result<SessionState, DataFusionError> {
    ///         // mutate the state adding any custom logic
    ///         Ok(state)
    ///     }
    /// }
    /// ```
    async fn session_state(&self, state: SessionState) -> Result<SessionState, DataFusionError> {
        Ok(state)
    }

    /// Modifies the [SessionContext] and returns it. Same as [SessionBuilder::session_state_builder]
    /// or [SessionBuilder::session_state] but operation on an already built [SessionContext].
    ///
    /// Example:
    ///
    /// ```rust
    /// # use async_trait::async_trait;
    /// # use datafusion::common::DataFusionError;
    /// # use datafusion::prelude::SessionContext;
    /// # use datafusion_distributed::SessionBuilder;
    ///
    /// #[derive(Clone)]
    /// struct CustomSessionBuilder;
    ///
    /// #[async_trait]
    /// impl SessionBuilder for CustomSessionBuilder {
    ///     async fn session_context(&self, ctx: SessionContext) -> Result<SessionContext, DataFusionError> {
    ///         // mutate the context adding any custom logic
    ///         Ok(ctx)
    ///     }
    /// }
    /// ```
    async fn session_context(
        &self,
        ctx: SessionContext,
    ) -> Result<SessionContext, DataFusionError> {
        Ok(ctx)
    }
}

/// Noop implementation of the [SessionBuilder]. Used by default if no [SessionBuilder] is provided
/// while building the Arrow Flight endpoint.
#[derive(Debug, Clone)]
pub struct NoopSessionBuilder;

impl SessionBuilder for NoopSessionBuilder {}
