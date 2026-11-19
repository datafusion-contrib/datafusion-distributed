#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use async_trait::async_trait;
    use datafusion::common::runtime::SpawnedTask;
    use datafusion::common::{Result, assert_contains, exec_err};
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::test_utils::routing::{
        URLEmitterExtensionCodec, URLEmitterFunction, UrlEmitterRouteTaskHandler,
        url_emitter_desired_task_count, url_emitter_scale_up_leaf_node,
    };
    use datafusion_distributed::{
        DistributedExt, DistributedMetricsFormat, RouteTaskEvent, RouteTaskEventResponse,
        RouteTaskHandler, WorkerQueryContext, WorkerToCoordinatorMsg,
        rewrite_distributed_plan_with_metrics,
    };
    use futures::{StreamExt, TryStreamExt};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    const WAIT: Duration = Duration::from_secs(5);
    const SCAN: &str = "SELECT task_index FROM url_emitter(2, 2, 'startup')";

    #[tokio::test]
    async fn static_task_produces_output_before_sibling_setup() -> Result<()> {
        let gate = TaskGate::default();
        let ctx = with_emitter(start_in_memory_context(2, build_worker).await)
            .with_distributed_dynamic_task_count(false)?
            .with_distributed_route_task_handler(gate.clone());
        ctx.sql("SET datafusion.execution.target_partitions = 2")
            .await?;
        let mut output = ctx.sql(SCAN).await?.execute_stream().await?;

        let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(
            first
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert!(!gate.release.is_cancelled());
        gate.release.cancel();
        let rest = output.try_collect::<Vec<_>>().await?;
        assert_eq!(rest.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn adaptive_task_produces_output_before_sibling_setup() -> Result<()> {
        let gate = TaskGate::default();
        let ctx = with_emitter(start_in_memory_context(2, build_worker).await)
            .with_distributed_dynamic_task_count(true)?
            .with_distributed_route_task_handler(gate.clone());
        ctx.sql("SET datafusion.execution.target_partitions = 2")
            .await?;
        let mut output = ctx.sql(SCAN).await?.execute_stream().await?;

        let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(
            first
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert!(!gate.release.is_cancelled());
        gate.release.cancel();
        let rest = output.try_collect::<Vec<_>>().await?;
        assert_eq!(rest.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn adaptive_task_samples_before_sibling_setup() -> Result<()> {
        let gate = TaskGate::default();
        let ctx = with_emitter(start_in_memory_context(2, build_worker).await)
            .with_distributed_dynamic_task_count(true)?
            .with_distributed_route_task_handler(gate.clone());
        ctx.sql("SET datafusion.execution.target_partitions = 2")
            .await?;
        let query = ctx
            .sql("SELECT task_index FROM url_emitter(2, 2, 'startup') GROUP BY task_index")
            .await?;
        let execution = SpawnedTask::spawn(async move { query.collect().await });

        timeout(WAIT, gate.entered.cancelled()).await.unwrap();
        timeout(WAIT, gate.sampled.cancelled()).await.unwrap();
        assert!(!gate.release.is_cancelled());
        gate.release.cancel();
        let batches = timeout(WAIT, execution).await.unwrap().unwrap()?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn dropping_output_cancels_unstarted_tasks_and_completes_reports() -> Result<()> {
        let gate = TaskGate::default();
        let ctx = with_emitter(start_in_memory_context(2, build_worker).await)
            .with_distributed_dynamic_task_count(true)?
            .with_distributed_metrics_collection(true)?
            .with_distributed_route_task_handler(gate.clone());
        ctx.sql("SET datafusion.execution.target_partitions = 2")
            .await?;
        let plan = ctx.sql(SCAN).await?.create_physical_plan().await?;
        let mut output = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;

        let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
        assert_eq!(first.num_rows(), 1);
        drop(output);

        timeout(WAIT, gate.exited.cancelled()).await.unwrap();
        assert!(!gate.release.is_cancelled());
        timeout(
            WAIT,
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated),
        )
        .await
        .unwrap()?;
        Ok(())
    }

    #[tokio::test]
    async fn limit_cancels_unstarted_tasks_and_completes_reports() -> Result<()> {
        let gate = TaskGate::default();
        let ctx = with_emitter(start_in_memory_context(2, build_worker).await)
            .with_distributed_dynamic_task_count(false)?
            .with_distributed_metrics_collection(true)?
            .with_distributed_route_task_handler(gate.clone());
        ctx.sql("SET datafusion.execution.target_partitions = 2")
            .await?;
        let plan = ctx
            .sql(&format!("{SCAN} LIMIT 1"))
            .await?
            .create_physical_plan()
            .await?;
        let output = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;

        let batches = timeout(WAIT, output.try_collect::<Vec<_>>())
            .await
            .unwrap()?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        timeout(WAIT, gate.exited.cancelled()).await.unwrap();
        assert!(!gate.release.is_cancelled());
        timeout(
            WAIT,
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated),
        )
        .await
        .unwrap()?;
        Ok(())
    }

    #[tokio::test]
    async fn late_setup_error_reaches_the_query() -> Result<()> {
        let gate = TaskGate::default();
        let ctx = with_emitter(start_in_memory_context(2, build_worker).await)
            .with_distributed_dynamic_task_count(false)?
            .with_distributed_route_task_handler(FailSecondTask(gate.clone()));
        ctx.sql("SET datafusion.execution.target_partitions = 2")
            .await?;
        let mut output = ctx.sql(SCAN).await?.execute_stream().await?;

        let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
        assert_eq!(first.num_rows(), 1);
        gate.release.cancel();
        let error = timeout(WAIT, output.try_collect::<Vec<_>>())
            .await
            .unwrap()
            .unwrap_err();
        assert_contains!(error.to_string(), "second task setup failed");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn setup_error_survives_parent_stage_cancellation() -> Result<()> {
        let gate = TaskGate::default();
        let ctx = with_emitter(start_in_memory_context(2, build_worker).await)
            .with_distributed_dynamic_task_count(false)?
            .with_distributed_route_task_handler(FailSecondTask(gate.clone()));
        ctx.sql("SET datafusion.execution.target_partitions = 2")
            .await?;
        let execution = SpawnedTask::spawn(async move {
            ctx.sql("SELECT count(*) FROM url_emitter(2, 2, 'startup') GROUP BY task_index")
                .await?
                .collect()
                .await
        });
        timeout(WAIT, gate.entered.cancelled()).await.unwrap();
        gate.release.cancel();
        let error = timeout(WAIT, execution)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_contains!(error.to_string(), "second task setup failed");
        Ok(())
    }

    fn with_emitter(mut ctx: SessionContext) -> SessionContext {
        ctx.set_distributed_user_codec(URLEmitterExtensionCodec);
        ctx.set_distributed_desired_task_count_handler(url_emitter_desired_task_count);
        ctx.set_distributed_scale_up_leaf_node_handler(url_emitter_scale_up_leaf_node);
        ctx.register_udtf("url_emitter", Arc::new(URLEmitterFunction));
        ctx
    }

    async fn build_worker(ctx: WorkerQueryContext) -> Result<SessionState> {
        Ok(ctx
            .builder
            .with_distributed_user_codec(URLEmitterExtensionCodec)
            .build())
    }

    #[derive(Clone, Default)]
    struct TaskGate {
        entered: CancellationToken,
        release: CancellationToken,
        exited: CancellationToken,
        sampled: CancellationToken,
    }

    #[async_trait]
    impl RouteTaskHandler for TaskGate {
        async fn handle(
            &self,
            event: RouteTaskEvent<'_>,
        ) -> Option<Result<RouteTaskEventResponse>> {
            // Make the stalled routing attempt observable before the ready task produces data.
            if event.task_key.task_number == 0 {
                self.entered.cancelled().await;
            }
            if event.task_key.task_number == 1 {
                let _exited = self.exited.clone().drop_guard();
                self.entered.cancel();
                self.release.cancelled().await;
            }
            let sampled = self.sampled.clone();
            let response = UrlEmitterRouteTaskHandler.handle(event).await?;
            Some(response.map(|mut response| {
                response.worker_to_coordinator_stream = response.worker_to_coordinator_stream
                    .inspect_ok(move |message| {
                        if matches!(message, WorkerToCoordinatorMsg::LoadInfo(info) if info.rows_ready > 0) {
                            sampled.cancel();
                        }
                    })
                    .boxed();
                response
            }))
        }
    }

    struct FailSecondTask(TaskGate);

    #[async_trait]
    impl RouteTaskHandler for FailSecondTask {
        async fn handle(
            &self,
            event: RouteTaskEvent<'_>,
        ) -> Option<Result<RouteTaskEventResponse>> {
            if event.task_key.task_number == 1 {
                self.0.entered.cancel();
                self.0.release.cancelled().await;
                return Some(exec_err!("second task setup failed"));
            }
            UrlEmitterRouteTaskHandler.handle(event).await
        }
    }
}
