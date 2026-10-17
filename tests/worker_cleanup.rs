#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::Result;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion_distributed::{
        CoordinatorChannelResult, CoordinatorToWorkerMsg, ExecuteTaskRequest, MaybeEncoded,
        ProducerHead, SetPlanRequest, TaskKey, Worker, WorkerToCoordinatorMsg,
    };
    use futures::stream::{self, BoxStream};
    use futures::{StreamExt, TryStreamExt};
    use http::HeaderMap;
    use std::sync::{Arc, Weak};
    use std::time::Duration;
    use tokio::sync::mpsc::{Sender, channel};
    use tokio_stream::wrappers::ReceiverStream;
    use url::Url;
    use uuid::Uuid;

    #[tokio::test]
    async fn coordinator_eos_releases_unexecuted_task() -> Result<()> {
        let worker = Worker::default();
        let (control, response) = open_task(&worker, new_task_key()).await?;
        let task_ctx = Arc::downgrade(&response.task_ctx);
        drop(response.task_ctx);

        drop(control);
        drain(response.stream).await?;

        assert_released(task_ctx).await;
        Ok(())
    }

    #[tokio::test]
    async fn coordinator_eos_preserves_existing_consumers_and_final_reports() -> Result<()> {
        let worker = Worker::default();
        let task_key = new_task_key();
        let (control, response) = open_task(&worker, task_key).await?;
        let task_ctx = Arc::downgrade(&response.task_ctx);
        drop(response.task_ctx);
        let (streams, context) = worker
            .execute_task(ExecuteTaskRequest {
                task_key,
                target_partition_start: 0,
                target_partition_end: 1,
                producer_head: ProducerHead::None,
            })
            .await?;

        drop(control);
        let reports = drain(response.stream).await?;
        assert!(
            reports
                .iter()
                .any(|msg| matches!(msg, WorkerToCoordinatorMsg::TaskMetrics(_)))
        );
        assert!(
            reports
                .iter()
                .any(|msg| matches!(msg, WorkerToCoordinatorMsg::TaskCompletedDynamicFilters(_)))
        );
        let batches = stream::iter(streams)
            .flatten()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        drop(context);
        assert_released(task_ctx).await;
        Ok(())
    }

    #[tokio::test]
    async fn closing_replaced_setup_preserves_current_task() -> Result<()> {
        let worker = Worker::default();
        let task_key = new_task_key();
        let (old_control, old_response) = open_task(&worker, task_key).await?;
        let (control, response) = open_task(&worker, task_key).await?;
        let old_context = Arc::downgrade(&old_response.task_ctx);
        drop(old_response.task_ctx);
        drop(old_control);
        drain(old_response.stream).await?;
        assert_released(old_context).await;

        let (_, context) = tokio::time::timeout(
            Duration::from_secs(1),
            worker.execute_task(ExecuteTaskRequest {
                task_key,
                target_partition_start: 0,
                target_partition_end: 1,
                producer_head: ProducerHead::None,
            }),
        )
        .await
        .expect("closing the old channel removed the current task")?;
        assert!(Arc::ptr_eq(&context, &response.task_ctx));
        drop(control);
        drain(response.stream).await?;
        Ok(())
    }

    fn new_task_key() -> TaskKey {
        TaskKey {
            query_id: Uuid::new_v4(),
            stage_id: 0,
            task_number: 0,
        }
    }

    async fn open_task(
        worker: &Worker,
        task_key: TaskKey,
    ) -> Result<(
        Sender<Result<CoordinatorToWorkerMsg>>,
        CoordinatorChannelResult,
    )> {
        let request = SetPlanRequest {
            task_key,
            task_count: 1,
            plan: MaybeEncoded::Decoded(Arc::new(PlaceholderRowExec::new(Arc::new(
                Schema::empty(),
            )))),
            dynamic_filter_remote_producer_ids: vec![],
            work_unit_feed_declarations: vec![],
            target_worker_url: Url::parse("http://worker.test").unwrap(),
            query_start_time_ns: 0,
        };
        let (tx, rx) = channel(1);
        let response = worker
            .coordinator_channel(HeaderMap::new(), request, ReceiverStream::new(rx).boxed())
            .await?;
        Ok((tx, response))
    }

    async fn drain(
        stream: BoxStream<'static, Result<WorkerToCoordinatorMsg>>,
    ) -> Result<Vec<WorkerToCoordinatorMsg>> {
        tokio::time::timeout(Duration::from_secs(5), stream.try_collect())
            .await
            .expect("coordinator channel did not finish")
    }

    async fn assert_released(context: Weak<TaskContext>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while context.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed task retained its context without cache maintenance");
    }
}
