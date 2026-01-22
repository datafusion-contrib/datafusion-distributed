use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionState, SessionStateBuilder};
use http::HeaderMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct WorkerQueryContext {
    pub builder: SessionStateBuilder,
    pub headers: HeaderMap,
}

/// builds a DataFusion's [SessionState] in each query issued to a worker.
#[async_trait]
pub trait WorkerSessionBuilder {
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
    /// # use datafusion_distributed::{DistributedExt, WorkerSessionBuilder, WorkerQueryContext};
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
    /// impl WorkerSessionBuilder for CustomSessionBuilder {
    ///     async fn build_session_state(&self, ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
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
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError>;
}

/// Noop implementation of the [WorkerSessionBuilder]. Used by default if no [WorkerSessionBuilder]
/// is provided while building the Worker.
#[derive(Debug, Clone)]
pub struct DefaultSessionBuilder;

#[async_trait]
impl WorkerSessionBuilder for DefaultSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        Ok(ctx.builder.build())
    }
}

/// Implementation of [WorkerSessionBuilder] for any async function that returns a [Result]
#[async_trait]
impl<F, Fut> WorkerSessionBuilder for F
where
    F: Fn(WorkerQueryContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<SessionState, DataFusionError>> + Send + 'static,
{
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        self(ctx).await
    }
}

pub trait MappedWorkerSessionBuilderExt {
    /// Maps an existing [WorkerSessionBuilder] allowing to add further extensions
    /// to its already built [SessionStateBuilder].
    ///
    /// Useful if there's already a [WorkerSessionBuilder] that needs to be extended
    /// with further capabilities.
    ///
    /// Example:
    ///
    /// ```rust
    /// # use datafusion::execution::SessionStateBuilder;
    /// # use datafusion_distributed::{DefaultSessionBuilder, MappedWorkerSessionBuilderExt};
    ///
    /// let session_builder = DefaultSessionBuilder
    ///     .map(|b: SessionStateBuilder| {
    ///         // Add further things.
    ///         Ok(b.build())
    ///     });
    /// ```
    fn map<F>(self, f: F) -> MappedWorkerSessionBuilder<Self, F>
    where
        Self: Sized,
        F: Fn(SessionStateBuilder) -> Result<SessionState, DataFusionError>;
}

impl<T: WorkerSessionBuilder> MappedWorkerSessionBuilderExt for T {
    fn map<F>(self, f: F) -> MappedWorkerSessionBuilder<Self, F>
    where
        Self: Sized,
    {
        MappedWorkerSessionBuilder {
            inner: self,
            f: Arc::new(f),
        }
    }
}

pub struct MappedWorkerSessionBuilder<T, F> {
    inner: T,
    f: Arc<F>,
}

impl<T: Clone, F> Clone for MappedWorkerSessionBuilder<T, F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            f: self.f.clone(),
        }
    }
}

#[async_trait]
impl<T, F> WorkerSessionBuilder for MappedWorkerSessionBuilder<T, F>
where
    T: WorkerSessionBuilder + Send + Sync + 'static,
    F: Fn(SessionStateBuilder) -> Result<SessionState, DataFusionError> + Send + Sync,
{
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        let state = self.inner.build_session_state(ctx).await?;
        let builder = SessionStateBuilder::new_from_existing(state);
        (self.f)(builder)
    }
}
