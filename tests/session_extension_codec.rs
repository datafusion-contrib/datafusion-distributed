#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::{DataFusionError, Result, internal_err};
    use datafusion::execution::SessionState;
    use datafusion_distributed::test_utils::localhost::start_localhost_context_with_worker;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DistributedExt, SessionExtensionCodec, Worker, WorkerPlanRewriteEvent,
        WorkerPlanRewriteEventResponse, WorkerQueryContext,
    };
    use std::sync::Arc;

    #[derive(Debug, PartialEq)]
    struct RequestIdentity(u64);

    #[derive(Clone)]
    struct RequestIdentityCodec;

    impl SessionExtensionCodec for RequestIdentityCodec {
        type Extension = RequestIdentity;
        const TYPE_URL: &'static str = "datafusion-distributed.test/request-identity/v1";

        fn encode(&self, extension: &Self::Extension) -> Result<Vec<u8>> {
            Ok(extension.0.to_le_bytes().to_vec())
        }

        fn decode(&self, payload: &[u8]) -> Result<Self::Extension> {
            let bytes = payload.try_into().map_err(|_| {
                DataFusionError::Configuration("invalid request identity payload".to_string())
            })?;
            Ok(RequestIdentity(u64::from_le_bytes(bytes)))
        }
    }

    #[tokio::test]
    async fn session_extension_reaches_worker_sessions() -> Result<()> {
        let (mut ctx, _guard, _) =
            start_localhost_context_with_worker(3, build_worker_session, |builder| {
                Worker::from_session_builder(builder)
                    .with_session_extension_codec(RequestIdentityCodec)
            })
            .await;
        ctx.state_ref()
            .write()
            .config_mut()
            .set_extension(Arc::new(RequestIdentity(42)));
        ctx.set_distributed_session_extension_codec(RequestIdentityCodec);

        register_parquet_tables(&ctx).await?;
        ctx.sql(r#"SELECT "MinTemp" FROM weather WHERE "MinTemp" > 20.0"#)
            .await?
            .collect()
            .await?;
        Ok(())
    }

    async fn build_worker_session(ctx: WorkerQueryContext) -> Result<SessionState> {
        Ok(ctx
            .builder
            .with_distributed_worker_plan_rewrite_handler(require_request_identity)
            .build())
    }

    fn require_request_identity(
        event: WorkerPlanRewriteEvent<'_>,
    ) -> Result<WorkerPlanRewriteEventResponse> {
        let Some(identity) = event.session_config.get_extension::<RequestIdentity>() else {
            return internal_err!("request identity is missing from worker session");
        };
        if identity.as_ref() != &RequestIdentity(42) {
            return internal_err!("worker received the wrong request identity");
        }
        Ok(WorkerPlanRewriteEventResponse::new(event.plan))
    }
}
