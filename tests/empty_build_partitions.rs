#[cfg(all(feature = "integration", test))]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::common::config::ConfigNonZeroUsize;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, assert_snapshot, display_plan_ascii,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn empty_build_partitions_do_not_block_sibling_probes() -> Result<()> {
        let (ctx, _workers, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let ctx = configure(ctx)?;
        let files = register_tables(&ctx).await?;
        let plan = ctx
            .sql("SELECT b.id FROM build_side b JOIN probe_side p ON b.id = p.id")
            .await?
            .create_physical_plan()
            .await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        insta::with_settings!({
            filters => [(r"[^ \[\],]*(build_side|probe_side)_[^ /\\:]+\.parquet", "$1.parquet")],
        }, {
            assert_snapshot!(display, @"
            ┌───── DistributedExec
            │ CoalescePartitionsExec
            │   [Stage 3] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 3 ── tasks=2, partitions=4
              │ HashJoinExec: mode=Partitioned, join_type=Inner, on=[(id@0, id@0)], projection=[id@0]
              │   [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
              │   [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=2
              └──────────────────────────────────────────────────
                ┌───── Stage 1 ── tasks=2, partitions=8
                │ RepartitionExec: partitioning=Hash([id@0], 8), input_partitions=1
                │   DistributedLeafExec:
                │     t0: DataSourceExec: file_groups={1 group: [[build_side.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
                │     t1: DataSourceExec: file_groups={1 group: [[build_side.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
                └──────────────────────────────────────────────────
                ┌───── Stage 2 ── tasks=2, partitions=8
                │ RepartitionExec: partitioning=Hash([id@0], 8), input_partitions=1
                │   DistributedLeafExec:
                │     t0: DataSourceExec: file_groups={1 group: [[probe_side.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
                │     t1: DataSourceExec: file_groups={1 group: [[probe_side.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
                └──────────────────────────────────────────────────
            ");
        });
        let result =
            tokio::time::timeout(Duration::from_secs(10), collect(plan, ctx.task_ctx())).await;
        for file in files {
            std::fs::remove_file(file)?;
        }
        let batches = result.expect("empty build partitions must not stall sibling probes")?;
        let formatted = pretty_format_batches(&batches)?.to_string();
        assert_snapshot!(formatted, @"
        +----+
        | id |
        +----+
        | 0  |
        +----+
        ");
        Ok(())
    }

    fn configure(ctx: SessionContext) -> Result<SessionContext> {
        {
            let state = ctx.state_ref();
            let mut state = state.write();
            let options = state.config_mut().options_mut();
            options.execution.target_partitions = 4;
            options.execution.batch_size = ConfigNonZeroUsize::try_new(32)?;
            // Eager probe buffering masks the abandoned, never-polled receiver.
            options.execution.hash_join_buffering_capacity = 0;
            options.optimizer.enable_join_dynamic_filter_pushdown = false;
            options.optimizer.hash_join_single_partition_threshold = 0;
            options.optimizer.hash_join_single_partition_threshold_rows = 0;
        }
        // Force shared-connection backpressure with a small generated dataset.
        ctx.with_distributed_broadcast_joins(false)?
            .with_distributed_worker_connection_buffer_budget_bytes(128)
    }

    async fn register_tables(ctx: &SessionContext) -> Result<Vec<PathBuf>> {
        // Only one build partition contains a row. Other join partitions abandon
        // their probes, which share a Flight connection with the matching partition.
        let mut files = vec![];
        for (table, ids) in [("build_side", vec![0]), ("probe_side", (0..4096).collect())] {
            let batch = RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(ids)) as _)])?;
            files.push(register_temp_parquet_table(table, batch.schema(), vec![batch], ctx).await?);
        }
        Ok(files)
    }
}
