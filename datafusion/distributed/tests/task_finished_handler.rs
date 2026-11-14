#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::Result;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::physical_plan::execute_stream;
    use datafusion::physical_plan::metrics::{Count, Metric, MetricValue, MetricsSet};
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, DistributedMetricsFormat,
        MappedWorkerSessionBuilderExt, NetworkBoundaryExt, TaskFinishedEvent,
        TaskFinishedEventResponse, TaskFinishedHandler, TaskOutcome, assert_snapshot,
        rewrite_distributed_plan_with_metrics,
    };
    use futures::{StreamExt, TryStreamExt};
    use std::borrow::Cow;
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn every_task_of_a_completed_query_reports_succeeded() -> Result<(), Box<dyn Error>> {
        let outcomes = RecordedOutcomes::default();
        let session_builder = DefaultSessionBuilder.map({
            let outcomes = outcomes.clone();
            move |builder| {
                Ok(builder
                    .with_distributed_task_finished_handler(outcomes.recorder())
                    .build())
            }
        });
        let ctx = start_configured_in_memory_context(3, session_builder, |worker| worker).await;
        register_parquet_tables(&ctx).await?;

        ctx.sql(r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday""#)
            .await?
            .collect()
            .await?;

        let outcomes = outcomes.wait_for(6).await;
        assert_snapshot!(outcomes, @"
        stage 1 task 0: Succeeded
        stage 1 task 1: Succeeded
        stage 1 task 2: Succeeded
        stage 2 task 0: Succeeded
        stage 2 task 1: Succeeded
        stage 2 task 2: Succeeded
        ");
        Ok(())
    }

    /// Only the row with the highest "MinTemp" (20.9) divides by zero. The other tasks race with
    /// the failure and may report Succeeded or Incomplete, so only the Failed outcome is asserted.
    #[tokio::test]
    async fn the_task_whose_operator_errors_reports_failed() -> Result<(), Box<dyn Error>> {
        let outcomes = RecordedOutcomes::default();
        let session_builder = DefaultSessionBuilder.map({
            let outcomes = outcomes.clone();
            move |builder| {
                Ok(builder
                    .with_distributed_task_finished_handler(outcomes.recorder())
                    .build())
            }
        });
        let ctx = start_configured_in_memory_context(3, session_builder, |worker| worker).await;
        register_parquet_tables(&ctx).await?;

        let result = ctx
            .sql(r#"SELECT 1 / CAST("MinTemp" < 20.9 AS INT) FROM weather"#)
            .await?
            .collect()
            .await;
        assert!(result.is_err());

        let failed = outcomes
            .wait_for(3)
            .await
            .lines()
            .filter(|line| line.contains("Failed"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_snapshot!(failed, @"stage 1 task 0: Failed: Arrow error: Divide by zero error");
        Ok(())
    }

    /// flights_1m is a single file, so all of its rows are assigned to task 0. Dropping the query
    /// after one batch stops task 0 before the end of its rows. Tasks 1 and 2 are assigned no
    /// rows, so their streams reach their end.
    #[tokio::test]
    async fn a_task_stopped_before_its_end_reports_incomplete() -> Result<(), Box<dyn Error>> {
        let outcomes = RecordedOutcomes::default();
        let session_builder = DefaultSessionBuilder.map({
            let outcomes = outcomes.clone();
            move |builder| {
                Ok(builder
                    .with_distributed_task_finished_handler(outcomes.recorder())
                    .build())
            }
        });
        let ctx = start_configured_in_memory_context(3, session_builder, |worker| worker).await;
        register_parquet_tables(&ctx).await?;

        let plan = ctx
            .sql("SELECT * FROM flights_1m")
            .await?
            .create_physical_plan()
            .await?;
        let mut stream = execute_stream(plan, ctx.task_ctx())?;
        stream.next().await.transpose()?;

        drop(stream);

        let outcomes = outcomes.wait_for(3).await;
        assert_snapshot!(outcomes, @"
        stage 1 task 0: Incomplete
        stage 1 task 1: Succeeded
        stage 1 task 2: Succeeded
        ");
        Ok(())
    }

    #[tokio::test]
    async fn handler_metrics_are_merged_into_task_metrics() -> Result<(), Box<dyn Error>> {
        let session_builder = DefaultSessionBuilder.map(|builder| {
            Ok(builder
                .with_distributed_task_finished_handler(|_ev: TaskFinishedEvent<'_>| {
                    let count = Count::new();
                    count.add(1);
                    let mut metrics = MetricsSet::new();
                    metrics.push(Arc::new(Metric::new(
                        MetricValue::Count {
                            name: Cow::Borrowed("finished_tasks"),
                            count,
                        },
                        None,
                    )));
                    Ok(TaskFinishedEventResponse::new(metrics))
                })
                .build())
        });
        let ctx = start_configured_in_memory_context(3, session_builder, |worker| worker).await;
        register_parquet_tables(&ctx).await?;

        let plan = ctx
            .sql(r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday""#)
            .await?
            .create_physical_plan()
            .await?;
        execute_stream(Arc::clone(&plan), ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .await?;

        // Every task's `finished_tasks=1` is merged into its stage's metrics.
        let mut stage_metrics = vec![];
        plan.apply(|node| {
            if let Some(boundary) = node.as_network_boundary() {
                let stage = boundary.input_stage();
                let finished_tasks = stage.metrics().sum_by_name("finished_tasks");
                stage_metrics.push(format!(
                    "Stage {} tasks={} finished_tasks={}",
                    stage.num(),
                    stage.task_count(),
                    finished_tasks.map_or(0, |v| v.as_usize()),
                ));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_snapshot!(stage_metrics.join("\n"), @"
        Stage 2 tasks=3 finished_tasks=3
        Stage 1 tasks=3 finished_tasks=3
        ");
        Ok(())
    }

    /// Records one line per finished task: its stage, task number and outcome.
    #[derive(Clone, Default)]
    struct RecordedOutcomes(Arc<Mutex<Vec<String>>>);

    impl RecordedOutcomes {
        fn recorder(&self) -> impl TaskFinishedHandler {
            let lines = Arc::clone(&self.0);
            move |ev: TaskFinishedEvent<'_>| {
                let outcome = match ev.outcome {
                    TaskOutcome::Succeeded => "Succeeded".to_string(),
                    TaskOutcome::Failed(err) => format!("Failed: {err}"),
                    TaskOutcome::Incomplete => "Incomplete".to_string(),
                    other => format!("{other:?}"),
                };
                lines.lock().unwrap().push(format!(
                    "stage {} task {}: {outcome}",
                    ev.task_key.stage_id, ev.task_key.task_number
                ));
                Ok(TaskFinishedEventResponse::new(MetricsSet::new()))
            }
        }

        /// Waits until `n` tasks have finished, and returns their sorted lines. Handlers run
        /// on the workers after the query ends, so they may still be running when it returns.
        async fn wait_for(&self, n: usize) -> String {
            timeout(Duration::from_secs(10), async {
                while self.0.lock().unwrap().len() < n {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("timed out waiting for TaskFinishedHandler events");
            let mut lines = self.0.lock().unwrap().clone();
            lines.sort();
            lines.join("\n")
        }
    }
}
