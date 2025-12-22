use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionState, SessionStateBuilder};
use http::HeaderMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct DistributedSessionBuilderContext {
    pub builder: SessionStateBuilder,
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
    /// # use datafusion::execution::{FunctionRegistry, SessionState, SessionStateBuilder, TaskContext};
    /// # use datafusion::physical_plan::ExecutionPlan;
    /// # use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    /// # use datafusion_distributed::{DistributedExt, DistributedSessionBuilder, DistributedSessionBuilderContext};
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
    /// #[derive(Clone)]
    /// struct CustomSessionBuilder;
    ///
    /// #[async_trait]
    /// impl DistributedSessionBuilder for CustomSessionBuilder {
    ///     async fn build_session_state(&self, ctx: DistributedSessionBuilderContext) -> Result<SessionState, DataFusionError> {
    ///         Ok(ctx
    ///             .builder
    ///             .with_distributed_user_codec(CustomExecCodec)
    ///             // Add your UDFs, optimization rules, etc...
    ///             .build())
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
        Ok(ctx.builder.build())
    }
}

/// Implementation of [DistributedSessionBuilder] for any async function that returns a [Result]
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

pub trait MappedDistributedSessionBuilderExt {
    /// Maps an existing [DistributedSessionBuilder] allowing to add further extensions
    /// to its already built [SessionStateBuilder].
    ///
    /// Useful if there's already a [DistributedSessionBuilder] that needs to be extended
    /// with further capabilities.
    ///
    /// Example:
    ///
    /// ```rust
    /// # use datafusion::execution::SessionStateBuilder;
    /// # use datafusion_distributed::{DefaultSessionBuilder, MappedDistributedSessionBuilderExt};
    ///
    /// let session_builder = DefaultSessionBuilder
    ///     .map(|b: SessionStateBuilder| {
    ///         // Add further things.
    ///         Ok(b.build())
    ///     });
    /// ```
    fn map<F>(self, f: F) -> MappedDistributedSessionBuilder<Self, F>
    where
        Self: Sized,
        F: Fn(SessionStateBuilder) -> Result<SessionState, DataFusionError>;
}

impl<T: DistributedSessionBuilder> MappedDistributedSessionBuilderExt for T {
    fn map<F>(self, f: F) -> MappedDistributedSessionBuilder<Self, F>
    where
        Self: Sized,
    {
        MappedDistributedSessionBuilder {
            inner: self,
            f: Arc::new(f),
        }
    }
}

pub struct MappedDistributedSessionBuilder<T, F> {
    inner: T,
    f: Arc<F>,
}

impl<T: Clone, F> Clone for MappedDistributedSessionBuilder<T, F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            f: self.f.clone(),
        }
    }
}

#[async_trait]
impl<T, F> DistributedSessionBuilder for MappedDistributedSessionBuilder<T, F>
where
    T: DistributedSessionBuilder + Send + Sync + 'static,
    F: Fn(SessionStateBuilder) -> Result<SessionState, DataFusionError> + Send + Sync,
{
    async fn build_session_state(
        &self,
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError> {
        let state = self.inner.build_session_state(ctx).await?;
        let builder = SessionStateBuilder::new_from_existing(state);
        (self.f)(builder)
    }
}
