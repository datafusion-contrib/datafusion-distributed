#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::{Result, exec_err};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use datafusion_distributed::{
        DistributedExec, DistributedExt, NetworkCoalesceExec, StageRootWrapper, WorkerQueryContext,
    };
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn direct_distributed_exec_applies_registered_stage_wrapper() -> Result<()> {
        let mut ctx = start_configured_in_memory_context(
            2,
            |ctx: WorkerQueryContext| async move {
                Ok(ctx
                    .builder
                    .with_distributed_stage_root_wrapper(RejectingWrapper)
                    .build())
            },
            |worker| worker,
        )
        .await;
        ctx.set_distributed_dynamic_task_count(false)?;
        ctx.set_distributed_stage_root_wrapper(RejectingWrapper);

        let input = SessionContext::new()
            .sql("SELECT 1 AS id")
            .await?
            .create_physical_plan()
            .await?;
        let network = Arc::new(NetworkCoalesceExec::try_new(input, 2, 1)?);
        let plan = Arc::new(DistributedExec::new(Arc::new(CoalescePartitionsExec::new(
            network,
        ))));
        let error = tokio::time::timeout(Duration::from_secs(10), collect(plan, ctx.task_ctx()))
            .await
            .expect("query must finish")
            .expect_err("the configured wrapper must reject the stage");
        assert!(
            error
                .to_string()
                .contains("stage rejected by registered wrapper")
        );
        Ok(())
    }

    #[derive(Debug)]
    struct RejectingWrapper;

    impl StageRootWrapper for RejectingWrapper {
        fn matches(&self, _: &dyn ExecutionPlan) -> bool {
            false
        }

        fn wrap(&self, _: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
            exec_err!("stage rejected by registered wrapper")
        }
    }
}
