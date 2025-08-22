use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SessionState, SessionStateBuilder};
use http::HeaderMap;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct DistributedSessionBuilderContext {
    pub runtime_env: Arc<RuntimeEnv>,
    pub headers: HeaderMap,
}

/// Trait called by the Arrow Flight endpoint that handles distributed parts of a DataFusion
/// plan for building a DataFusion's [SessionState].
#[async_trait]
pub trait DistributedSessionBuilder {
    /// Builds a custom [SessionState] scoped to a single ArrowFlight gRPC call, allowing the
    /// users to provide a customized DataFusion session with things like custom extension codecs,
    /// custom physical optimization rules, UDFs, UDAFs, config extensions, etc...
    ///
    /// Example:
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use async_trait::async_trait;
    /// # use datafusion::error::DataFusionError;
    /// # use datafusion::execution::{FunctionRegistry, SessionState, SessionStateBuilder};
    /// # use datafusion::physical_plan::ExecutionPlan;
    /// # use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    /// # use datafusion_distributed::{DistributedExt, DistributedSessionBuilder, DistributedSessionBuilderContext};
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
    /// impl DistributedSessionBuilder for CustomSessionBuilder {
    ///     async fn build_session_state(&self, ctx: DistributedSessionBuilderContext) -> Result<SessionState, DataFusionError> {
    ///         let mut builder = SessionStateBuilder::new()
    ///             .with_runtime_env(ctx.runtime_env.clone())
    ///             .with_default_features();
    ///         builder.set_user_codec(CustomExecCodec);
    ///         // Add your UDFs, optimization rules, etc...
    ///
    ///         Ok(builder.build())
    ///     }
    /// }
    /// ```
    async fn build_session_state(
        &self,
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError>;
}

/// Noop implementation of the [DistributedSessionBuilder]. Used by default if no [DistributedSessionBuilder] is provided
/// while building the Arrow Flight endpoint.
#[derive(Debug, Clone)]
pub struct DefaultSessionBuilder;

#[async_trait]
impl DistributedSessionBuilder for DefaultSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError> {
        Ok(SessionStateBuilder::new()
            .with_runtime_env(ctx.runtime_env.clone())
            .with_default_features()
            .build())
    }
}

#[async_trait]
impl<F, Fut> DistributedSessionBuilder for F
where
    F: Fn(DistributedSessionBuilderContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<SessionState, DataFusionError>> + Send + 'static,
{
    async fn build_session_state(
        &self,
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError> {
        self(ctx).await
    }
}
