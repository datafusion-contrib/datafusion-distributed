// The `URLEmitter` tests emit and assert per-URL worker identity, so they need the distinct dialed
// workers the gRPC in-memory cluster gives, not the single in-process worker.
#[cfg(all(feature = "integration", feature = "flight", test))]
mod tests {
    use std::sync::Arc;

    use arrow::util::pretty::pretty_format_batches;
    use datafusion::{error::DataFusionError, execution::SessionState, physical_plan::collect};
    use datafusion_distributed::{
        DistributedExt, WorkerQueryContext, assert_snapshot, display_plan_ascii,
        test_utils::{
            in_memory_channel_resolver::start_in_memory_context,
            routing::{URLEmitterExtensionCodec, URLEmitterFunction, URLEmitterTaskEstimator},
        },
    };

    const NUM_WORKERS: usize = 5;
    const PARTITIONS: usize = 3;

    #[tokio::test]
    async fn custom_routing() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(5, 5, 'logs')
            ORDER BY task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [task_index@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=5, input_tasks=5
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0] t1:[p1] t2:[p2] t3:[p3] t4:[p4]
          │ SortExec: expr=[task_index@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   DistributedLeafExec:
          │     t0: URLEmitterExec: tasks=5 partitions=1 tag=logs
          │     t1: URLEmitterExec: tasks=5 partitions=1 tag=logs
          │     t2: URLEmitterExec: tasks=5 partitions=1 tag=logs
          │     t3: URLEmitterExec: tasks=5 partitions=1 tag=logs
          │     t4: URLEmitterExec: tasks=5 partitions=1 tag=logs
          └──────────────────────────────────────────────────
        +------------+------------+------+--------------+
        | task_count | task_index | tag  | worker_url   |
        +------------+------------+------+--------------+
        | 5          | 0          | logs | http://url-4 |
        | 5          | 1          | logs | http://url-3 |
        | 5          | 2          | logs | http://url-2 |
        | 5          | 3          | logs | http://url-1 |
        | 5          | 4          | logs | http://url-0 |
        +------------+------------+------+--------------+
        ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn custom_routing_more_partitions() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(8, 5, 'logs')
            ORDER BY task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [task_index@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=10, input_tasks=5
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] t3:[p6..p7] t4:[p8..p9]
          │ SortExec: expr=[task_index@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   DistributedLeafExec:
          │     t0: URLEmitterExec: tasks=5 partitions=2 tag=logs
          │     t1: URLEmitterExec: tasks=5 partitions=2 tag=logs
          │     t2: URLEmitterExec: tasks=5 partitions=2 tag=logs
          │     t3: URLEmitterExec: tasks=5 partitions=2 tag=logs
          │     t4: URLEmitterExec: tasks=5 partitions=2 tag=logs
          └──────────────────────────────────────────────────
        +------------+------------+------+--------------+
        | task_count | task_index | tag  | worker_url   |
        +------------+------------+------+--------------+
        | 5          | 0          | logs | http://url-4 |
        | 5          | 0          | logs | http://url-4 |
        | 5          | 1          | logs | http://url-3 |
        | 5          | 1          | logs | http://url-3 |
        | 5          | 2          | logs | http://url-2 |
        | 5          | 2          | logs | http://url-2 |
        | 5          | 3          | logs | http://url-1 |
        | 5          | 4          | logs | http://url-0 |
        +------------+------------+------+--------------+
        ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn custom_routing_more_tasks() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(3, 5, 'logs')
            ORDER BY task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [task_index@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=5, input_tasks=5
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0] t1:[p1] t2:[p2] t3:[p3] t4:[p4]
          │ SortExec: expr=[task_index@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   DistributedLeafExec:
          │     t0: URLEmitterExec: tasks=5 partitions=1 tag=logs
          │     t1: URLEmitterExec: tasks=5 partitions=1 tag=logs
          │     t2: URLEmitterExec: tasks=5 partitions=1 tag=logs
          │     t3: URLEmitterExec: tasks=5 partitions=1 tag=logs
          │     t4: URLEmitterExec: tasks=5 partitions=1 tag=logs
          └──────────────────────────────────────────────────
        +------------+------------+------+--------------+
        | task_count | task_index | tag  | worker_url   |
        +------------+------------+------+--------------+
        | 5          | 0          | logs | http://url-4 |
        | 5          | 1          | logs | http://url-3 |
        | 5          | 2          | logs | http://url-2 |
        +------------+------------+------+--------------+
        ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn custom_routing_union() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(5, 5, 'left')
            UNION
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(5, 5, 'right')
            ORDER BY tag, task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [tag@2 ASC NULLS LAST, task_index@1 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2]
          │ SortExec: expr=[tag@2 ASC NULLS LAST, task_index@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   AggregateExec: mode=FinalPartitioned, gby=[task_count@0 as task_count, task_index@1 as task_index, tag@2 as tag, worker_url@3 as worker_url], aggr=[]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=5
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] t4:[p0..p11]
            │ RepartitionExec: partitioning=Hash([task_count@0, task_index@1, tag@2, worker_url@3], 12), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[task_count@0 as task_count, task_index@1 as task_index, tag@2 as tag, worker_url@3 as worker_url], aggr=[]
            │     DistributedUnionExec: t0:[c0(0/3)] t1:[c0(1/3)] t2:[c0(2/3)] t3:[c1(0/2)] t4:[c1(1/2)]
            │       DistributedLeafExec:
            │         t0: URLEmitterExec: tasks=5 partitions=2 tag=left
            │         t1: URLEmitterExec: tasks=5 partitions=2 tag=left
            │         t2: URLEmitterExec: tasks=5 partitions=2 tag=left
            │       DistributedLeafExec:
            │         t0: URLEmitterExec: tasks=5 partitions=3 tag=right
            │         t1: URLEmitterExec: tasks=5 partitions=3 tag=right
            └──────────────────────────────────────────────────
        +------------+------------+-------+--------------+
        | task_count | task_index | tag   | worker_url   |
        +------------+------------+-------+--------------+
        | 3          | 0          | left  | http://url-4 |
        | 3          | 1          | left  | http://url-3 |
        | 3          | 2          | left  | http://url-2 |
        | 2          | 0          | right | http://url-1 |
        | 2          | 1          | right | http://url-0 |
        +------------+------------+-------+--------------+
        ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn custom_routing_union_variant() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(3, 8, 'left')
            UNION
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(4, 2, 'right')
            ORDER BY tag, task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [tag@2 ASC NULLS LAST, task_index@1 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2]
          │ SortExec: expr=[tag@2 ASC NULLS LAST, task_index@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   AggregateExec: mode=FinalPartitioned, gby=[task_count@0 as task_count, task_index@1 as task_index, tag@2 as tag, worker_url@3 as worker_url], aggr=[]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=5
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] t4:[p0..p11]
            │ RepartitionExec: partitioning=Hash([task_count@0, task_index@1, tag@2, worker_url@3], 12), input_partitions=4
            │   AggregateExec: mode=Partial, gby=[task_count@0 as task_count, task_index@1 as task_index, tag@2 as tag, worker_url@3 as worker_url], aggr=[]
            │     DistributedUnionExec: t0:[c0(0/4)] t1:[c0(1/4)] t2:[c0(2/4)] t3:[c0(3/4)] t4:[c1]
            │       DistributedLeafExec:
            │         t0: URLEmitterExec: tasks=8 partitions=1 tag=left
            │         t1: URLEmitterExec: tasks=8 partitions=1 tag=left
            │         t2: URLEmitterExec: tasks=8 partitions=1 tag=left
            │         t3: URLEmitterExec: tasks=8 partitions=1 tag=left
            │       DistributedLeafExec:
            │         t0: URLEmitterExec: tasks=2 partitions=4 tag=right
            └──────────────────────────────────────────────────
        +------------+------------+-------+--------------+
        | task_count | task_index | tag   | worker_url   |
        +------------+------------+-------+--------------+
        | 4          | 0          | left  | http://url-4 |
        | 4          | 1          | left  | http://url-3 |
        | 4          | 2          | left  | http://url-2 |
        | 1          | 0          | right | http://url-0 |
        +------------+------------+-------+--------------+
        ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn custom_routing_join() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT
                l.task_count,
                l.task_index AS left_index,
                l.tag AS left_tag,
                l.worker_url AS worker_left,
                r.task_index AS right_index,
                r.tag AS right_tag,
                r.worker_url AS worker_right
            FROM url_emitter(5, 5, 'left') l
            JOIN url_emitter(5, 5, 'right') r
            ON l.task_index = r.task_index
            ORDER BY l.task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [left_index@1 ASC NULLS LAST]
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=15, input_tasks=5
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] t4:[p0..p2]
          │ SortExec: expr=[left_index@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[task_count@0 as task_count, task_index@1 as left_index, tag@2 as left_tag, worker_url@3 as worker_left, task_index@4 as right_index, tag@5 as right_tag, worker_url@6 as worker_right]
          │     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(task_index@1, task_index@0)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=5
          │       [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=5
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p14] t1:[p0..p14] t2:[p0..p14] t3:[p0..p14] t4:[p0..p14]
            │ RepartitionExec: partitioning=Hash([task_index@1], 15), input_partitions=1
            │   DistributedLeafExec:
            │     t0: URLEmitterExec: tasks=5 partitions=1 tag=left
            │     t1: URLEmitterExec: tasks=5 partitions=1 tag=left
            │     t2: URLEmitterExec: tasks=5 partitions=1 tag=left
            │     t3: URLEmitterExec: tasks=5 partitions=1 tag=left
            │     t4: URLEmitterExec: tasks=5 partitions=1 tag=left
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p14] t1:[p0..p14] t2:[p0..p14] t3:[p0..p14] t4:[p0..p14]
            │ RepartitionExec: partitioning=Hash([task_index@0], 15), input_partitions=1
            │   DistributedLeafExec:
            │     t0: URLEmitterExec: tasks=5 partitions=1 tag=right
            │     t1: URLEmitterExec: tasks=5 partitions=1 tag=right
            │     t2: URLEmitterExec: tasks=5 partitions=1 tag=right
            │     t3: URLEmitterExec: tasks=5 partitions=1 tag=right
            │     t4: URLEmitterExec: tasks=5 partitions=1 tag=right
            └──────────────────────────────────────────────────
        +------------+------------+----------+--------------+-------------+-----------+--------------+
        | task_count | left_index | left_tag | worker_left  | right_index | right_tag | worker_right |
        +------------+------------+----------+--------------+-------------+-----------+--------------+
        | 5          | 0          | left     | http://url-4 | 0           | right     | http://url-4 |
        | 5          | 1          | left     | http://url-3 | 1           | right     | http://url-3 |
        | 5          | 2          | left     | http://url-2 | 2           | right     | http://url-2 |
        | 5          | 3          | left     | http://url-1 | 3           | right     | http://url-1 |
        | 5          | 4          | left     | http://url-0 | 4           | right     | http://url-0 |
        +------------+------------+----------+--------------+-------------+-----------+--------------+
        ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn custom_routing_join_variant() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT
                l.task_count,
                l.task_index AS left_index,
                l.tag AS left_tag,
                l.worker_url AS worker_left,
                r.task_index AS right_index,
                r.tag AS right_tag,
                r.worker_url AS worker_right
            FROM url_emitter(12, 9, 'left') l
            JOIN url_emitter(7, 10, 'right') r
            ON l.task_index = r.task_index
            ORDER BY l.task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [left_index@1 ASC NULLS LAST]
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=15, input_tasks=5
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] t4:[p0..p2]
          │ SortExec: expr=[left_index@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[task_count@0 as task_count, task_index@1 as left_index, tag@2 as left_tag, worker_url@3 as worker_left, task_index@4 as right_index, tag@5 as right_tag, worker_url@6 as worker_right]
          │     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(task_index@1, task_index@0)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=5
          │       [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=5
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p14] t1:[p0..p14] t2:[p0..p14] t3:[p0..p14] t4:[p0..p14]
            │ RepartitionExec: partitioning=Hash([task_index@1], 15), input_partitions=3
            │   DistributedLeafExec:
            │     t0: URLEmitterExec: tasks=9 partitions=3 tag=left
            │     t1: URLEmitterExec: tasks=9 partitions=3 tag=left
            │     t2: URLEmitterExec: tasks=9 partitions=3 tag=left
            │     t3: URLEmitterExec: tasks=9 partitions=3 tag=left
            │     t4: URLEmitterExec: tasks=9 partitions=3 tag=left
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p14] t1:[p0..p14] t2:[p0..p14] t3:[p0..p14] t4:[p0..p14]
            │ RepartitionExec: partitioning=Hash([task_index@0], 15), input_partitions=2
            │   DistributedLeafExec:
            │     t0: URLEmitterExec: tasks=10 partitions=2 tag=right
            │     t1: URLEmitterExec: tasks=10 partitions=2 tag=right
            │     t2: URLEmitterExec: tasks=10 partitions=2 tag=right
            │     t3: URLEmitterExec: tasks=10 partitions=2 tag=right
            │     t4: URLEmitterExec: tasks=10 partitions=2 tag=right
            └──────────────────────────────────────────────────
        +------------+------------+----------+--------------+-------------+-----------+--------------+
        | task_count | left_index | left_tag | worker_left  | right_index | right_tag | worker_right |
        +------------+------------+----------+--------------+-------------+-----------+--------------+
        | 5          | 0          | left     | http://url-4 | 0           | right     | http://url-4 |
        | 5          | 0          | left     | http://url-4 | 0           | right     | http://url-4 |
        | 5          | 0          | left     | http://url-4 | 0           | right     | http://url-4 |
        | 5          | 0          | left     | http://url-4 | 0           | right     | http://url-4 |
        | 5          | 0          | left     | http://url-4 | 0           | right     | http://url-4 |
        | 5          | 0          | left     | http://url-4 | 0           | right     | http://url-4 |
        | 5          | 1          | left     | http://url-3 | 1           | right     | http://url-3 |
        | 5          | 1          | left     | http://url-3 | 1           | right     | http://url-3 |
        | 5          | 1          | left     | http://url-3 | 1           | right     | http://url-3 |
        | 5          | 1          | left     | http://url-3 | 1           | right     | http://url-3 |
        | 5          | 1          | left     | http://url-3 | 1           | right     | http://url-3 |
        | 5          | 1          | left     | http://url-3 | 1           | right     | http://url-3 |
        | 5          | 2          | left     | http://url-2 | 2           | right     | http://url-2 |
        | 5          | 2          | left     | http://url-2 | 2           | right     | http://url-2 |
        | 5          | 3          | left     | http://url-1 | 3           | right     | http://url-1 |
        | 5          | 3          | left     | http://url-1 | 3           | right     | http://url-1 |
        | 5          | 4          | left     | http://url-0 | 4           | right     | http://url-0 |
        | 5          | 4          | left     | http://url-0 | 4           | right     | http://url-0 |
        +------------+------------+----------+--------------+-------------+-----------+--------------+
        ",
        );
        Ok(())
    }

    async fn run_query(sql: &str) -> Result<(String, String), DataFusionError> {
        let mut ctx = start_in_memory_context(NUM_WORKERS, build_state).await;
        ctx.set_distributed_task_estimator(URLEmitterTaskEstimator);
        ctx.set_distributed_user_codec(URLEmitterExtensionCodec);
        ctx.register_udtf("url_emitter", Arc::new(URLEmitterFunction));
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = PARTITIONS;

        let df = ctx.sql(sql).await?;
        let plan = df.create_physical_plan().await?;
        let plan_display = display_plan_ascii(plan.as_ref(), false);

        let batches = collect(plan, ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&batches)?;

        Ok((plan_display, formatted.to_string()))
    }

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_distributed_user_codec(URLEmitterExtensionCodec)
            .build())
    }
}
