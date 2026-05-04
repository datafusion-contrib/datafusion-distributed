#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::error::DataFusionError;
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::test_work_unit_feed::{
        RowGeneratorExec, TestWorkUnitFeedExecCodec, TestWorkUnitFeedFunction,
        TestWorkUnitFeedTaskEstimator,
    };
    use datafusion_distributed::{
        DistributedExt, WorkerQueryContext, assert_snapshot, display_plan_ascii,
    };
    use futures::TryStreamExt;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn single_task_no_distribution() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit('source', 1, 'rows(1),rows(1)', 'rows(2)')
            ORDER BY task, partition, letter
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        SortPreservingMergeExec: [task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST]
          SortExec: expr=[task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST], preserve_partitioning=[true]
            RowGeneratorExec: tag=source, tasks=1, partition_ops=[[rows(1), rows(1)], [rows(2)]]
        +--------+------+-----------+--------+
        | tag    | task | partition | letter |
        +--------+------+-----------+--------+
        | source | 0    | 0         | a      |
        | source | 0    | 0         | a      |
        | source | 0    | 1         | a      |
        | source | 0    | 1         | b      |
        +--------+------+-----------+--------+
        ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn two_tasks() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit('source', 2, 'rows(1),rows(1)', 'rows(2)', 'rows(1)', 'rows(2),rows(1)')
            ORDER BY task, partition, letter
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3]
          │ SortExec: expr=[task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST], preserve_partitioning=[true]
          │   RowGeneratorExec: tag=source, tasks=2, partition_ops=[[rows(1), rows(1)], [rows(2)], [rows(1)], [rows(2), rows(1)]]
          └──────────────────────────────────────────────────
        +--------+------+-----------+--------+
        | tag    | task | partition | letter |
        +--------+------+-----------+--------+
        | source | 0    | 0         | a      |
        | source | 0    | 0         | a      |
        | source | 0    | 1         | a      |
        | source | 0    | 1         | b      |
        | source | 1    | 0         | a      |
        | source | 1    | 1         | a      |
        | source | 1    | 1         | a      |
        | source | 1    | 1         | b      |
        +--------+------+-----------+--------+
        ",
        );
        Ok(())
    }

    /// Tests that empty work unit feeds (no work units) produce no rows for that partition,
    /// while other partitions still work correctly through the distributed path.
    #[tokio::test]
    async fn empty_work_unit_feeds() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit('source', 2, 'rows(3)', '', '', 'rows(1)')
            ORDER BY task, partition, letter
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3]
          │ SortExec: expr=[task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST], preserve_partitioning=[true]
          │   RowGeneratorExec: tag=source, tasks=2, partition_ops=[[rows(3)], [], [], [rows(1)]]
          └──────────────────────────────────────────────────
        +--------+------+-----------+--------+
        | tag    | task | partition | letter |
        +--------+------+-----------+--------+
        | source | 0    | 0         | a      |
        | source | 0    | 0         | b      |
        | source | 0    | 0         | c      |
        | source | 1    | 1         | a      |
        +--------+------+-----------+--------+
        ",
        );
        Ok(())
    }

    /// Tests distribution across three tasks.
    #[tokio::test]
    async fn three_tasks() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit('source', 3, 'rows(2)', 'rows(1)', 'rows(3)', 'rows(1)', 'rows(2)', 'rows(1)')
            ORDER BY task, partition, letter
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5]
          │ SortExec: expr=[task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST], preserve_partitioning=[true]
          │   RowGeneratorExec: tag=source, tasks=3, partition_ops=[[rows(2)], [rows(1)], [rows(3)], [rows(1)], [rows(2)], [rows(1)]]
          └──────────────────────────────────────────────────
        +--------+------+-----------+--------+
        | tag    | task | partition | letter |
        +--------+------+-----------+--------+
        | source | 0    | 0         | a      |
        | source | 0    | 0         | b      |
        | source | 0    | 1         | a      |
        | source | 1    | 0         | a      |
        | source | 1    | 0         | b      |
        | source | 1    | 0         | c      |
        | source | 1    | 1         | a      |
        | source | 2    | 0         | a      |
        | source | 2    | 0         | b      |
        | source | 2    | 1         | a      |
        +--------+------+-----------+--------+
        ",
        );
        Ok(())
    }

    /// Tests a UNION ALL of two work unit feed sources — each produces an independent
    /// WorkUnitFeedExec node, and both must receive their feeds correctly in the same stage.
    #[tokio::test]
    async fn union_of_two_feeds() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit('left', 2, 'rows(2)', 'rows(1)', 'rows(3)', 'rows(1)')
            UNION ALL
            SELECT * FROM test_work_unit('right', 2, 'rows(1)', 'rows(2)', 'rows(1)', 'rows(1)')
            ORDER BY tag, task, partition, letter
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [tag@0 ASC NULLS LAST, task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p4..p7] t2:[p8..p11]
          │ DistributedUnionExec: t0:[c0] t1:[c1(0/2)] t2:[c1(1/2)]
          │   SortExec: expr=[tag@0 ASC NULLS LAST, task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST], preserve_partitioning=[true]
          │     RowGeneratorExec: tag=left, tasks=2, partition_ops=[[rows(2)], [rows(1)], [rows(3)], [rows(1)]]
          │   SortExec: expr=[tag@0 ASC NULLS LAST, task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST], preserve_partitioning=[true]
          │     RowGeneratorExec: tag=right, tasks=2, partition_ops=[[rows(1)], [rows(2)], [rows(1)], [rows(1)]]
          └──────────────────────────────────────────────────
        +-------+------+-----------+--------+
        | tag   | task | partition | letter |
        +-------+------+-----------+--------+
        | left  | 0    | 0         | a      |
        | left  | 0    | 0         | b      |
        | left  | 0    | 1         | a      |
        | left  | 0    | 2         | a      |
        | left  | 0    | 2         | b      |
        | left  | 0    | 2         | c      |
        | left  | 0    | 3         | a      |
        | right | 0    | 0         | a      |
        | right | 0    | 1         | a      |
        | right | 0    | 1         | b      |
        | right | 1    | 0         | a      |
        | right | 1    | 1         | a      |
        +-------+------+-----------+--------+
        ",
        );
        Ok(())
    }

    /// Tests aggregation over a work unit feed source — verifies that standard DataFusion
    /// operators correctly process rows produced from distributed work unit feeds.
    #[tokio::test]
    async fn aggregation_over_feed() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT COUNT(*) as cnt, letter
            FROM test_work_unit('source', 2, 'rows(3)', 'rows(2)', 'rows(1)', 'rows(4)')
            GROUP BY letter
            ORDER BY letter
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [letter@1 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2]
          │ SortExec: expr=[letter@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as cnt, letter@0 as letter]
          │     AggregateExec: mode=FinalPartitioned, gby=[letter@0 as letter], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5]
            │ RepartitionExec: partitioning=Hash([letter@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[letter@0 as letter], aggr=[count(Int64(1))]
            │     RowGeneratorExec: tag=source, tasks=2, partition_ops=[[rows(3)], [rows(2)], [rows(1)], [rows(4)]]
            └──────────────────────────────────────────────────
        +-----+--------+
        | cnt | letter |
        +-----+--------+
        | 4   | a      |
        | 3   | b      |
        | 2   | c      |
        | 1   | d      |
        +-----+--------+
        ",
        );
        Ok(())
    }

    /// Tests a JOIN between two work unit feed sources — each side has its own
    /// WorkUnitFeedExec in a separate stage and feeds must be delivered independently to both.
    #[tokio::test]
    async fn join_of_two_feeds() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SET datafusion.optimizer.hash_join_single_partition_threshold = 0;
            SET datafusion.optimizer.hash_join_single_partition_threshold_rows = 0;
            SELECT a.task as a_task, a.letter as a_letter, b.task as b_task, b.letter as b_letter
            FROM test_work_unit('orders', 2, 'rows(2)', 'rows(1)', 'rows(1)', 'rows(2)') a
            INNER JOIN test_work_unit('customers', 2, 'rows(1)', 'rows(1)', 'rows(2)', 'rows(1)') b
            ON a.letter = b.letter
            ORDER BY a_task, a_letter, b_task, b_letter
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [a_task@0 ASC NULLS LAST, a_letter@1 ASC NULLS LAST, b_task@2 ASC NULLS LAST, b_letter@3 ASC NULLS LAST]
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p2] t1:[p0..p2]
          │ SortExec: expr=[a_task@0 ASC NULLS LAST, b_letter@3 ASC NULLS LAST, b_task@2 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[task@2 as a_task, letter@3 as a_letter, task@0 as b_task, letter@1 as b_letter]
          │     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(letter@1, letter@1)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │       [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5]
            │ RepartitionExec: partitioning=Hash([letter@1], 6), input_partitions=2
            │   RowGeneratorExec: tag=customers, tasks=2, partition_ops=[[rows(1)], [rows(1)], [rows(2)], [rows(1)]]
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p5] t1:[p0..p5]
            │ RepartitionExec: partitioning=Hash([letter@1], 6), input_partitions=2
            │   RowGeneratorExec: tag=orders, tasks=2, partition_ops=[[rows(2)], [rows(1)], [rows(1)], [rows(2)]]
            └──────────────────────────────────────────────────
        +--------+----------+--------+----------+
        | a_task | a_letter | b_task | b_letter |
        +--------+----------+--------+----------+
        | 0      | a        | 0      | a        |
        | 0      | a        | 0      | a        |
        | 0      | a        | 0      | a        |
        | 0      | a        | 0      | a        |
        | 0      | a        | 1      | a        |
        | 0      | a        | 1      | a        |
        | 0      | a        | 1      | a        |
        | 0      | a        | 1      | a        |
        | 0      | b        | 1      | b        |
        | 1      | a        | 0      | a        |
        | 1      | a        | 0      | a        |
        | 1      | a        | 0      | a        |
        | 1      | a        | 0      | a        |
        | 1      | a        | 1      | a        |
        | 1      | a        | 1      | a        |
        | 1      | a        | 1      | a        |
        | 1      | a        | 1      | a        |
        | 1      | b        | 1      | b        |
        +--------+----------+--------+----------+
        ",
        );
        Ok(())
    }

    /// UNION ALL of three feed sources — the ChildrenIsolatorUnionExec must map
    /// three children across tasks, each with its own WorkUnitFeedExec.
    #[tokio::test]
    async fn triple_union() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit('x', 2, 'rows(2)', 'rows(1)')
            UNION ALL
            SELECT * FROM test_work_unit('y', 2, 'rows(1)', 'rows(3)')
            UNION ALL
            SELECT * FROM test_work_unit('z', 2, 'rows(1)', 'rows(1)')
            ORDER BY tag, task, partition, letter
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [tag@0 ASC NULLS LAST, task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5]
          │ DistributedUnionExec: t0:[c0] t1:[c1] t2:[c2]
          │   SortExec: expr=[tag@0 ASC NULLS LAST, task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST], preserve_partitioning=[true]
          │     RowGeneratorExec: tag=x, tasks=2, partition_ops=[[rows(2)], [rows(1)]]
          │   SortExec: expr=[tag@0 ASC NULLS LAST, task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST], preserve_partitioning=[true]
          │     RowGeneratorExec: tag=y, tasks=2, partition_ops=[[rows(1)], [rows(3)]]
          │   SortExec: expr=[tag@0 ASC NULLS LAST, task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST], preserve_partitioning=[true]
          │     RowGeneratorExec: tag=z, tasks=2, partition_ops=[[rows(1)], [rows(1)]]
          └──────────────────────────────────────────────────
        +-----+------+-----------+--------+
        | tag | task | partition | letter |
        +-----+------+-----------+--------+
        | x   | 0    | 0         | a      |
        | x   | 0    | 0         | b      |
        | x   | 0    | 1         | a      |
        | y   | 0    | 0         | a      |
        | y   | 0    | 1         | a      |
        | y   | 0    | 1         | b      |
        | y   | 0    | 1         | c      |
        | z   | 0    | 0         | a      |
        | z   | 0    | 1         | a      |
        +-----+------+-----------+--------+
        ");
        Ok(())
    }

    /// UNION ALL mixing a work unit feed source with a plain VALUES subquery.
    /// Only one child of the ChildrenIsolatorUnionExec has a WorkUnitFeedExec.
    #[tokio::test]
    async fn union_feed_with_non_feed() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit('feed', 2, 'rows(2)', 'rows(1)', 'rows(1)', 'rows(2)')
            UNION ALL
            SELECT 'static' as tag, 0 as task, 0 as partition, 'x' as letter
            ORDER BY tag, task, partition, letter
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [tag@0 ASC NULLS LAST, task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5]
          │ DistributedUnionExec: t0:[c0(0/2)] t1:[c0(1/2)] t2:[c1]
          │   SortExec: expr=[tag@0 ASC NULLS LAST, task@1 ASC NULLS LAST, partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST], preserve_partitioning=[true]
          │     RowGeneratorExec: tag=feed, tasks=2, partition_ops=[[rows(2)], [rows(1)], [rows(1)], [rows(2)]]
          │   ProjectionExec: expr=[static as tag, 0 as task, 0 as partition, x as letter]
          │     PlaceholderRowExec
          └──────────────────────────────────────────────────
        +--------+------+-----------+--------+
        | tag    | task | partition | letter |
        +--------+------+-----------+--------+
        | feed   | 0    | 0         | a      |
        | feed   | 0    | 0         | b      |
        | feed   | 0    | 1         | a      |
        | feed   | 1    | 0         | a      |
        | feed   | 1    | 1         | a      |
        | feed   | 1    | 1         | b      |
        | static | 0    | 0         | x      |
        +--------+------+-----------+--------+
        ");
        Ok(())
    }

    /// Aggregation over a UNION of two feeds — combines CIU + multiple WorkUnitFeedExec
    /// nodes + aggregation, stressing the full pipeline.
    #[tokio::test]
    async fn aggregation_over_union_of_feeds() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT tag, letter, COUNT(*) as cnt
            FROM (
                SELECT * FROM test_work_unit('left', 2, 'rows(3)', 'rows(2)', 'rows(1)', 'rows(2)')
                UNION ALL
                SELECT * FROM test_work_unit('right', 2, 'rows(2)', 'rows(1)', 'rows(1)', 'rows(3)')
            )
            GROUP BY tag, letter
            ORDER BY tag, letter
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [tag@0 ASC NULLS LAST, letter@1 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2]
          │ SortExec: expr=[tag@0 ASC NULLS LAST, letter@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[tag@0 as tag, letter@1 as letter, count(Int64(1))@2 as cnt]
          │     AggregateExec: mode=FinalPartitioned, gby=[tag@0 as tag, letter@1 as letter], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5]
            │ RepartitionExec: partitioning=Hash([tag@0, letter@1], 6), input_partitions=4
            │   AggregateExec: mode=Partial, gby=[tag@0 as tag, letter@1 as letter], aggr=[count(Int64(1))]
            │     DistributedUnionExec: t0:[c0] t1:[c1(0/2)] t2:[c1(1/2)]
            │       RowGeneratorExec: tag=left, tasks=2, partition_ops=[[rows(3)], [rows(2)], [rows(1)], [rows(2)]]
            │       RowGeneratorExec: tag=right, tasks=2, partition_ops=[[rows(2)], [rows(1)], [rows(1)], [rows(3)]]
            └──────────────────────────────────────────────────
        +-------+--------+-----+
        | tag   | letter | cnt |
        +-------+--------+-----+
        | left  | a      | 4   |
        | left  | b      | 3   |
        | left  | c      | 1   |
        | right | a      | 4   |
        | right | b      | 2   |
        | right | c      | 1   |
        +-------+--------+-----+
        ");
        Ok(())
    }

    /// JOIN where one side is a feed and the other is an aggregation over a different feed.
    /// Tests that feeds work correctly when placed at different depths in the plan tree.
    #[tokio::test]
    async fn join_feed_with_aggregated_feed() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SET datafusion.optimizer.hash_join_single_partition_threshold = 0;
            SET datafusion.optimizer.hash_join_single_partition_threshold_rows = 0;
            SELECT a.tag as a_tag, a.letter, b.cnt
            FROM test_work_unit('detail', 2, 'rows(2)', 'rows(1)', 'rows(1)', 'rows(2)') a
            INNER JOIN (
                SELECT letter, COUNT(*) as cnt
                FROM test_work_unit('summary', 2, 'rows(3)', 'rows(2)', 'rows(1)', 'rows(4)')
                GROUP BY letter
            ) b ON a.letter = b.letter
            ORDER BY a_tag, a.letter, b.cnt
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [a_tag@0 ASC NULLS LAST, letter@1 ASC NULLS LAST, cnt@2 ASC NULLS LAST]
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p2] t1:[p0..p2]
          │ SortExec: expr=[a_tag@0 ASC NULLS LAST, letter@1 ASC NULLS LAST, cnt@2 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[tag@0 as a_tag, letter@1 as letter, cnt@2 as cnt]
          │     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(letter@1, letter@0)], projection=[tag@0, letter@1, cnt@3]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │       ProjectionExec: expr=[letter@0 as letter, count(Int64(1))@1 as cnt]
          │         AggregateExec: mode=FinalPartitioned, gby=[letter@0 as letter], aggr=[count(Int64(1))]
          │           [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5]
            │ RepartitionExec: partitioning=Hash([letter@1], 6), input_partitions=2
            │   RowGeneratorExec: tag=detail, tasks=2, partition_ops=[[rows(2)], [rows(1)], [rows(1)], [rows(2)]]
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p5] t1:[p0..p5]
            │ RepartitionExec: partitioning=Hash([letter@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[letter@0 as letter], aggr=[count(Int64(1))]
            │     RowGeneratorExec: tag=summary, tasks=2, partition_ops=[[rows(3)], [rows(2)], [rows(1)], [rows(4)]]
            └──────────────────────────────────────────────────
        +--------+--------+-----+
        | a_tag  | letter | cnt |
        +--------+--------+-----+
        | detail | a      | 4   |
        | detail | a      | 4   |
        | detail | a      | 4   |
        | detail | a      | 4   |
        | detail | b      | 3   |
        | detail | b      | 3   |
        +--------+--------+-----+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn broadcast_join_over_feeds() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SET distributed.broadcast_joins=true;
            SELECT
                a.tag as a_tag, a.task as a_task, a.partition as a_partition, a.letter,
                b.tag as b_tag, b.task as b_task, b.partition as b_partition
            FROM test_work_unit('probe', 2, 'rows(3)', 'rows(1)', 'rows(2)', 'rows(1)') a
            INNER JOIN test_work_unit('build', 2, 'rows(1)', 'rows(2)', 'rows(1)', 'rows(3)') b
            ON a.letter = b.letter
            ORDER BY a_task, a_partition, a.letter, b_task, b_partition
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [a_task@1 ASC NULLS LAST, a_partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST, b_task@5 ASC NULLS LAST, b_partition@6 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p1] t1:[p2..p3]
          │ SortExec: expr=[a_task@1 ASC NULLS LAST, a_partition@2 ASC NULLS LAST, letter@3 ASC NULLS LAST, b_task@5 ASC NULLS LAST, b_partition@6 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[tag@0 as a_tag, task@1 as a_task, partition@2 as a_partition, letter@3 as letter, tag@4 as b_tag, task@5 as b_task, partition@6 as b_partition]
          │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(letter@3, letter@3)], projection=[tag@0, task@1, partition@2, letter@3, tag@4, task@5, partition@6]
          │       CoalescePartitionsExec
          │         [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=2, stage_partitions=4, input_tasks=2
          │       RowGeneratorExec: tag=build, tasks=2, partition_ops=[[rows(1)], [rows(2)], [rows(1)], [rows(3)]]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p4..p7]
            │ BroadcastExec: input_partitions=2, consumer_tasks=2, output_partitions=4
            │   RowGeneratorExec: tag=probe, tasks=2, partition_ops=[[rows(3)], [rows(1)], [rows(2)], [rows(1)]]
            └──────────────────────────────────────────────────
        +-------+--------+-------------+--------+-------+--------+-------------+
        | a_tag | a_task | a_partition | letter | b_tag | b_task | b_partition |
        +-------+--------+-------------+--------+-------+--------+-------------+
        | probe | 0      | 0           | a      | build | 0      | 0           |
        | probe | 0      | 0           | a      | build | 0      | 1           |
        | probe | 0      | 0           | a      | build | 1      | 0           |
        | probe | 0      | 0           | a      | build | 1      | 1           |
        | probe | 0      | 0           | b      | build | 0      | 1           |
        | probe | 0      | 0           | b      | build | 1      | 1           |
        | probe | 0      | 0           | c      | build | 1      | 1           |
        | probe | 0      | 1           | a      | build | 0      | 0           |
        | probe | 0      | 1           | a      | build | 0      | 1           |
        | probe | 0      | 1           | a      | build | 1      | 0           |
        | probe | 0      | 1           | a      | build | 1      | 1           |
        | probe | 1      | 0           | a      | build | 0      | 0           |
        | probe | 1      | 0           | a      | build | 0      | 1           |
        | probe | 1      | 0           | a      | build | 1      | 0           |
        | probe | 1      | 0           | a      | build | 1      | 1           |
        | probe | 1      | 0           | b      | build | 0      | 1           |
        | probe | 1      | 0           | b      | build | 1      | 1           |
        | probe | 1      | 1           | a      | build | 0      | 0           |
        | probe | 1      | 1           | a      | build | 0      | 1           |
        | probe | 1      | 1           | a      | build | 1      | 0           |
        | probe | 1      | 1           | a      | build | 1      | 1           |
        +-------+--------+-------------+--------+-------+--------+-------------+
        ");
        Ok(())
    }

    /// `wait()` ops in a feed must actually delay the producing stream.
    /// Verifies the [`crate::test_utils::test_work_unit_feed::WorkUnitOp::Wait`]
    /// op is wired into the producer's stream.
    #[tokio::test]
    async fn wait_op_delays_query() -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        let (_, results) =
            run_query(r#"SELECT * FROM test_work_unit('a', 1, 'rows(1), wait(800), rows(1)')"#)
                .await?;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(800),
            "expected query to take at least 800ms (the wait), but took {elapsed:?}"
        );
        // Sanity check that both rows came through after the wait.
        let data_rows = results.lines().filter(|l| l.starts_with("| a ")).count();
        assert_eq!(
            data_rows, 2,
            "expected 2 rows from feed 'a', got:\n{results}"
        );
        Ok(())
    }

    /// An `err()` op in the feed must surface as a query-level error.
    /// This exercises error propagation from a coordinator-side
    /// [`WorkUnitFeedProvider`] through the local + remote feed pipeline up
    /// to the user calling `try_collect` on the result stream.
    #[tokio::test]
    async fn err_op_in_single_task_propagates() -> Result<(), Box<dyn std::error::Error>> {
        let res =
            run_query(r#"SELECT * FROM test_work_unit('a', 1, 'rows(1), err(boom_single)')"#).await;
        let err = res.expect_err("query should have failed");
        let msg = err.to_string();
        assert!(
            msg.contains("boom_single"),
            "expected error to mention 'boom_single', got: {msg}"
        );
        Ok(())
    }

    /// Same as [`err_op_in_single_task_propagates`] but with two tasks, so the
    /// erroring feed actually goes through the coordinator → worker gRPC path.
    /// Guards against errors being silently swallowed as EOF on the worker side.
    #[tokio::test]
    async fn err_op_in_distributed_feed_propagates() -> Result<(), Box<dyn std::error::Error>> {
        let res = run_query(
            r#"
            SELECT * FROM test_work_unit('a', 2, 'rows(1)', 'rows(1), err(boom_distributed)')
            "#,
        )
        .await;
        let err = res.expect_err("distributed query should have failed");
        let msg = err.to_string();
        assert!(
            msg.contains("boom_distributed"),
            "expected error to mention 'boom_distributed', got: {msg}"
        );
        Ok(())
    }

    /// An `err()` op in one of two independent feeds in the same query must still
    /// surface. The other feed is otherwise valid — we want to make sure the
    /// failing feed taints the whole query rather than the result silently
    /// missing the rows from the failing side.
    #[tokio::test]
    async fn err_in_one_of_two_feeds_propagates() -> Result<(), Box<dyn std::error::Error>> {
        let res = run_query(
            r#"
            SELECT * FROM test_work_unit('left', 2, 'rows(1)', 'rows(1)', 'rows(1)', 'rows(1)')
            UNION ALL
            SELECT * FROM test_work_unit('right', 2, 'rows(1)', 'err(boom_union)', 'rows(1)', 'rows(1)')
            "#,
        )
        .await;
        let err = res.expect_err("query should have failed because of the right feed");
        let msg = err.to_string();
        assert!(
            msg.contains("boom_union"),
            "expected error to mention 'boom_union', got: {msg}"
        );
        Ok(())
    }

    /// A `wait()` op in one feed should not stop another, independent feed in the
    /// same query from making progress, and the final result should still be
    /// correct. Acts as a regression guard against the producer side blocking
    /// other feeds while sleeping on a single partition.
    #[tokio::test]
    async fn wait_in_one_feed_does_not_corrupt_other_feed_results()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, results) = run_query(
            r#"
            SELECT * FROM test_work_unit('fast', 2, 'rows(1)', 'rows(1)', 'rows(1)', 'rows(1)')
            UNION ALL
            SELECT * FROM test_work_unit('slow', 2, 'wait(500), rows(1)', 'rows(1)', 'rows(1)', 'rows(1)')
            ORDER BY tag, task, partition, letter
            "#,
        )
        .await?;
        // 4 rows from `fast` + 4 rows from `slow` = 8 data rows.
        let data_rows = results
            .lines()
            .filter(|l| l.starts_with("| fast ") || l.starts_with("| slow "))
            .count();
        assert_eq!(
            data_rows, 8,
            "expected 8 data rows across both feeds, got:\n{results}"
        );
        Ok(())
    }

    async fn run_query(sql: &str) -> Result<(String, String), DataFusionError> {
        let (mut ctx, _guard, _) = start_localhost_context(3, build_state).await;
        ctx.set_distributed_work_unit_feed(|p: &RowGeneratorExec| Some(&p.feed));
        ctx.set_distributed_user_codec(TestWorkUnitFeedExecCodec);
        ctx.set_distributed_task_estimator(TestWorkUnitFeedTaskEstimator);
        ctx.register_udtf("test_work_unit", Arc::new(TestWorkUnitFeedFunction));

        let mut df_opt = None;
        for sql in sql.split(";") {
            if sql.trim().is_empty() {
                continue;
            }
            let df = ctx.sql(sql).await?;
            df_opt = Some(df);
        }
        let Some(df) = df_opt else {
            return Err(DataFusionError::Plan("Empty 'sql' parameter".to_string()));
        };
        let plan = df.create_physical_plan().await?;
        let plan_display = display_plan_ascii(plan.as_ref(), false);

        let batches = execute_stream(plan, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let formatted = pretty_format_batches(&batches)?;

        Ok((plan_display, formatted.to_string()))
    }

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_distributed_user_codec(TestWorkUnitFeedExecCodec)
            .build())
    }
}
