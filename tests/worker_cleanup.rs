#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::Result;
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion_distributed::{MaybeEncoded, SetPlanRequest, TaskKey, Worker};
    use futures::{StreamExt, TryStreamExt};
    use http::HeaderMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::channel;
    use tokio_stream::wrappers::ReceiverStream;
    use url::Url;
    use uuid::Uuid;

    #[tokio::test]
    async fn coordinator_eos_releases_unexecuted_task() -> Result<()> {
        let worker = Worker::default();
        let request = SetPlanRequest {
            task_key: TaskKey {
                query_id: Uuid::nil(),
                stage_id: 0,
                task_number: 0,
            },
            task_count: 1,
            plan: MaybeEncoded::Decoded(Arc::new(PlaceholderRowExec::new(Arc::new(
                Schema::empty(),
            )))),
            dynamic_filter_remote_producer_ids: vec![],
            work_unit_feed_declarations: vec![],
            target_worker_url: Url::parse("http://worker.test").unwrap(),
            query_start_time_ns: 0,
        };
        let (control, incoming) = channel(1);
        let response = worker
            .coordinator_channel(
                HeaderMap::new(),
                request,
                ReceiverStream::new(incoming).boxed(),
            )
            .await?;
        let context = Arc::downgrade(&response.task_ctx);
        drop(response.task_ctx);
        drop(control);

        // Bounds failures without delaying successful cleanup.
        tokio::time::timeout(Duration::from_secs(5), async {
            response.stream.try_for_each(|_| async { Ok(()) }).await?;
            while context.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
        .expect("completed task retained its context without cache maintenance")
    }
}
