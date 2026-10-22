#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::{HashSet, Result, assert_contains, internal_err};
    use datafusion::error::DataFusionError;
    use datafusion::execution::SessionState;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::{
        DistributedExt, MappedWorkerSessionBuilderExt, WorkerQueryContext,
        WorkerStreamExecutedEvent, WorkerStreamExecutedEventResponse,
    };
    use std::sync::Arc;
    use std::sync::Mutex;

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx.builder.build())
    }

    #[derive(Default)]
    struct SeenPartitions {
        task_keys: HashSet<(uuid::Uuid, usize, usize)>,
        partitions: Vec<usize>,
    }

    #[tokio::test]
    async fn worker_stream_executed_handlers_receive_task_key_and_run_in_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let seen = Arc::new(Mutex::new(SeenPartitions::default()));
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        let session_builder = build_state.map({
            let seen = Arc::clone(&seen);
            let order = Arc::clone(&order);
            move |mut builder| {
                builder.set_distributed_worker_stream_executed_handler({
                    let seen = Arc::clone(&seen);
                    let order = Arc::clone(&order);
                    move |event: WorkerStreamExecutedEvent<'_>| {
                        order.lock().unwrap().push("first");
                        let mut seen = seen.lock().unwrap();
                        seen.task_keys.insert((
                            event.task_key.query_id,
                            event.task_key.stage_id,
                            event.task_key.task_number,
                        ));
                        seen.partitions.push(event.partition);
                        Ok(WorkerStreamExecutedEventResponse::new(event.stream))
                    }
                });
                builder.set_distributed_worker_stream_executed_handler({
                    let order = Arc::clone(&order);
                    move |event: WorkerStreamExecutedEvent<'_>| {
                        order.lock().unwrap().push("second");
                        Ok(WorkerStreamExecutedEventResponse::new(event.stream))
                    }
                });
                Ok(builder.build())
            }
        });
        let ctx = start_configured_in_memory_context(3, session_builder, |worker| worker).await;

        let batches = collect_stream_hook_query(&ctx).await?;
        assert_snapshot_contains_rows(&batches);

        let seen = seen.lock().unwrap();
        // The query is distributed across workers, so every reported task key shares the same
        // query id, confirming it is the one that actually identifies this query's tasks.
        let query_ids: HashSet<_> = seen.task_keys.iter().map(|(q, _, _)| *q).collect();
        assert_eq!(query_ids.len(), 1, "every task should share one query id");
        assert!(!seen.partitions.is_empty());

        let order = order.lock().unwrap();
        assert!(!order.is_empty());
        for pair in order.chunks(2) {
            assert_eq!(pair, ["first", "second"]);
        }

        Ok(())
    }

    #[tokio::test]
    async fn worker_stream_executed_handler_error_propagates_to_the_query()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_builder = build_state.map(|mut builder| {
            builder.set_distributed_worker_stream_executed_handler(
                |_event: WorkerStreamExecutedEvent<'_>| internal_err!("stream hook failed"),
            );
            Ok(builder.build())
        });
        let ctx = start_configured_in_memory_context(3, session_builder, |worker| worker).await;

        let err = collect_stream_hook_query(&ctx)
            .await
            .expect_err("stream hook error should propagate to the query");

        assert_contains!(err.to_string(), "stream hook failed");

        Ok(())
    }

    fn assert_snapshot_contains_rows(batches: &str) {
        assert_contains!(batches, "| id |");
        assert_contains!(batches, "| 2  |");
        assert_contains!(batches, "| 3  |");
    }

    async fn collect_stream_hook_query(ctx: &SessionContext) -> Result<String> {
        let _temp_file = register_stream_hook_input_table(ctx).await?;
        let batches = ctx
            .sql("SELECT id FROM stream_hook_input WHERE id > 1 ORDER BY id")
            .await?
            .collect()
            .await?;
        Ok(pretty_format_batches(&batches)?.to_string())
    }

    async fn register_stream_hook_input_table(ctx: &SessionContext) -> Result<std::path::PathBuf> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        register_temp_parquet_table("stream_hook_input", schema, vec![batch], ctx).await
    }
}
