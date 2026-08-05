//! Reproducers for joins that collect their build (left) side producing wrong results when
//! placed in a multi-task stage without the build side being broadcast.
//!
//! A CollectLeft HashJoin, a NestedLoopJoin, and a CrossJoin all require the *complete* build
//! side in every task. `insert_broadcast_execs` only guarantees that for join types that don't
//! emit build-side rows (and only when broadcast joins are enabled), but the task-count logic
//! in `inject_network_boundaries` does not cap the remaining shapes to a single task. The
//! build-side scan then gets sliced across tasks like any other leaf, and each task joins its
//! slice of the build side against its slice of the probe side.
//!
//! The tables are laid out so the slicing is visible: `build_side` holds ids 0..100 split
//! sequentially across 4 files, while `probe_side` holds the same ids (each repeated 50 times)
//! rotated one file forward. A task therefore sees *different* ids from each table, and any
//! cross-task match is silently lost.
//!
//! These shapes are now handled by plan shaping:
//! `normalize_collect_joins` rewrites build-side-emitting CollectLeft HashJoins to
//! PartitionMode::Partitioned and swaps build-side-emitting NestedLoopJoins so the emitting
//! side becomes the probe side, while the task-count gate in `inject_network_boundaries`
//! caps whatever has no distributed rewrite (Full NestedLoopJoins, null-aware anti joins,
//! and any of these joins when broadcasts are disabled) to a single task. Every test here
//! asserts distributed results match single-node execution.

#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::Result;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::test_utils::property_based::compare_result_set;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, assert_snapshot, display_plan_ascii,
    };
    use parquet::arrow::ArrowWriter;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tokio::sync::OnceCell;

    const NUM_WORKERS: usize = 4;
    const PARTITIONS: usize = 3;
    const FILES_PER_TABLE: i64 = 4;
    const IDS_PER_FILE: i64 = 25;
    const PROBE_DUPLICATES: usize = 50;

    static INIT: OnceCell<()> = OnceCell::const_new();

    /// Case 1: CollectLeft HashJoin with a build-side-emitting join type (LeftSemi).
    /// Broadcast joins are ON, but `insert_broadcast_execs` skips LeftSemi, and nothing caps
    /// the stage to one task. Every id matches on a single node; distributed, a build id only
    /// survives if its probe rows landed in the same task.
    #[tokio::test]
    async fn collect_left_semi_hash_join_is_correct() {
        let plan = assert_distributed_matches_single_node(
            "SELECT id FROM build_side WHERE id IN (SELECT id FROM probe_side)",
            true,
        )
        .await
        .unwrap();
        assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=4, partitions=3
          │ HashJoinExec: mode=Partitioned, join_type=LeftSemi, on=[(id@0, id@0)]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          │   [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=4, partitions=12
            │ RepartitionExec: partitioning=Hash([id@0], 12), input_partitions=2
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=4, partitions=12
            │ RepartitionExec: partitioning=Hash([id@0], 12), input_partitions=2
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ]
            │     t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ]
            │     t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ]
            │     t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ]
            └──────────────────────────────────────────────────
        ")
    }

    /// Case 2: anti join (`NOT IN`). Single-node: every build id exists in probe_side,
    /// so zero rows. Distributed, each task only sees a slice of probe_side, so most build ids
    /// look unmatched and phantom rows are emitted.
    #[tokio::test]
    async fn not_in_anti_hash_join_is_correct() {
        let plan = assert_distributed_matches_single_node(
            "SELECT id FROM build_side WHERE id NOT IN (SELECT id FROM probe_side)",
            true,
        )
        .await
        .unwrap();
        assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(id@0, id@0)]
          CoalescePartitionsExec
            DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet, /target/multi_task_collect_join_repros/build_side/part-1.parquet], [/target/multi_task_collect_join_repros/build_side/part-2.parquet, /target/multi_task_collect_join_repros/build_side/part-3.parquet]]}, projection=[id], file_type=parquet
          DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet, /target/multi_task_collect_join_repros/probe_side/part-1.parquet], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet, /target/multi_task_collect_join_repros/probe_side/part-3.parquet]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ id@0 >= 0 AND id@0 <= 99 AND id@0 IN (SET) ([<values>]) ], pruning_predicate=id_null_count@1 != row_count@2 AND id_max@0 >= 0 AND id_null_count@1 != row_count@2 AND id_min@3 <= 99, required_guarantees=[id in (0, 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 3, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 4, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 5, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 6, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 7, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 8, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 9, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99)]
        ")
    }

    /// Case 3: NestedLoopJoin with a build-side-emitting join type (LeftSemi), produced
    /// by a correlated EXISTS with a non-equi predicate (`p.id > b.id - 1 AND p.id < b.id + 1`
    /// is `p.id = b.id` for integers, but expressed as inequalities so no hash join is possible).
    #[tokio::test]
    #[ignore = "Until we upgrade to DF 54.1.0 or greater, this is flaky. See apache/datafusion#22791"]
    async fn nested_loop_left_semi_join_is_correct() {
        let plan = assert_distributed_matches_single_node(
            "SELECT b.id FROM build_side b WHERE EXISTS ( \
                SELECT 1 FROM probe_side p WHERE p.id > b.id - 1 AND p.id < b.id + 1)",
            true,
        )
        .await
        .unwrap();
        assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ┌───── DistributedExec
        │ NestedLoopJoinExec: join_type=RightSemi, filter=id@0 > join_proj_push_down_1@1 AND id@0 < join_proj_push_down_2@2, projection=[id@0]
        │   CoalescePartitionsExec
        │     [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=2, stage_partitions=2, input_tasks=4
        │   ProjectionExec: expr=[id@0 as id, id@0 - 1 as join_proj_push_down_1, id@0 + 1 as join_proj_push_down_2]
        │     CoalescePartitionsExec
        │       DistributedLeafExec:
        │         t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
        │         t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
        │         t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
        │         t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=4, partitions=8
          │ BroadcastExec: input_partitions=2, consumer_tasks=1, output_partitions=2
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
          │     t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
          │     t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
          │     t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
          └──────────────────────────────────────────────────
        ")
    }

    /// Case 4: Full NestedLoopJoin. Emits unmatched rows from BOTH sides, so no
    /// broadcast orientation can ever be correct. Single-node: every row matches, no NULL
    /// padding. Distributed: cross-task matches are lost and spurious NULL-padded rows appear.
    #[tokio::test]
    #[ignore = "Until we upgrade to DF 54.1.0 or greater, this is flaky. See apache/datafusion#22791"]
    async fn nested_loop_full_join_is_correct() {
        let plan = assert_distributed_matches_single_node(
            "SELECT b.id, p.id FROM build_side b FULL JOIN probe_side p \
                ON p.id > b.id - 1 AND p.id < b.id + 1",
            true,
        )
        .await
        .unwrap();
        assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        NestedLoopJoinExec: join_type=Full, filter=id@0 > join_proj_push_down_1@1 AND id@0 < join_proj_push_down_2@2, projection=[id@0, id@3]
          ProjectionExec: expr=[id@0 as id, id@0 - 1 as join_proj_push_down_1, id@0 + 1 as join_proj_push_down_2]
            CoalescePartitionsExec
              DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet, /target/multi_task_collect_join_repros/build_side/part-1.parquet], [/target/multi_task_collect_join_repros/build_side/part-2.parquet, /target/multi_task_collect_join_repros/build_side/part-3.parquet]]}, projection=[id], file_type=parquet
          DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet, /target/multi_task_collect_join_repros/probe_side/part-1.parquet], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet, /target/multi_task_collect_join_repros/probe_side/part-3.parquet]]}, projection=[id], file_type=parquet
        ")
    }

    /// Full HASH join (equi keys): converted to Partitioned like the other
    /// build-side-emitting types — key co-location gives complete match information on both
    /// sides at once. Contrast with the non-equi Full NLJ above, which has no distributed
    /// rewrite and stays capped to a single task.
    #[tokio::test]
    async fn converted_full_hash_join_is_correct() {
        let plan = assert_distributed_matches_single_node(
            "SELECT b.id, p.id FROM build_side b FULL JOIN probe_side p ON b.id = p.id",
            true,
        )
        .await
        .unwrap();
        assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=4, partitions=12
          │ HashJoinExec: mode=Partitioned, join_type=Full, on=[(id@0, id@0)]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          │   [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=4, partitions=12
            │ RepartitionExec: partitioning=Hash([id@0], 12), input_partitions=2
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=4, partitions=12
            │ RepartitionExec: partitioning=Hash([id@0], 12), input_partitions=2
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            └──────────────────────────────────────────────────
        ")
    }

    /// Case 5: CrossJoin with broadcast joins DISABLED, so no BroadcastExec exists at
    /// all. Single-node: all 100 x 5000 = 500_000 pairs contribute to the sum. Distributed:
    /// each task only pairs its slice of each side, so most pairs are never produced.
    /// (A bare `count(*)` is folded to a constant from parquet statistics, so sum an
    /// expression the optimizer cannot answer from metadata.)
    #[tokio::test]
    async fn cross_join_broadcast_disabled_is_correct() {
        let plan = assert_distributed_matches_single_node(
            "SELECT sum(b.id + p.id) AS pair_sum FROM build_side b CROSS JOIN probe_side p",
            false,
        )
        .await
        .unwrap();
        assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ProjectionExec: expr=[sum(b.id + p.id)@0 as pair_sum]
          AggregateExec: mode=Final, gby=[], aggr=[sum(b.id + p.id)]
            CoalescePartitionsExec
              AggregateExec: mode=Partial, gby=[], aggr=[sum(b.id + p.id)]
                RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
                  CrossJoinExec
                    CoalescePartitionsExec
                      DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet, /target/multi_task_collect_join_repros/build_side/part-1.parquet], [/target/multi_task_collect_join_repros/build_side/part-2.parquet, /target/multi_task_collect_join_repros/build_side/part-3.parquet]]}, projection=[id], file_type=parquet
                    DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet, /target/multi_task_collect_join_repros/probe_side/part-1.parquet], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet, /target/multi_task_collect_join_repros/probe_side/part-3.parquet]]}, projection=[id], file_type=parquet
        ")
    }

    /// Joins with a single partition on both sides do not need to be and should not be
    /// rewritten. Doing so risks breaking assumptions DataFusion has made to safely apply
    /// optimizations to single-partition cases. This tests the hash join case
    #[tokio::test]
    async fn single_partition_full_hash_join_is_correct() {
        let plan = assert_distributed_matches_single_node(
            r"SELECT b.id AS bid, p.id AS pid FROM
                (SELECT id FROM build_side ORDER BY id LIMIT 5) b
                FULL JOIN
                (SELECT id FROM probe_side ORDER BY id LIMIT 1000000) p
                ON b.id = p.id
             ORDER BY bid NULLS LAST, pid NULLS LAST",
            true,
        )
        .await
        .unwrap();
        assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ┌───── DistributedExec
        │ SortExec: expr=[bid@0 ASC NULLS LAST, pid@1 ASC NULLS LAST], preserve_partitioning=[false]
        │   ProjectionExec: expr=[id@0 as bid, id@1 as pid]
        │     HashJoinExec: mode=CollectLeft, join_type=Full, on=[(id@0, id@0)]
        │       SortPreservingMergeExec: [id@0 ASC NULLS LAST], fetch=5
        │         [Stage 1] => NetworkCoalesceExec: output_partitions=8, input_tasks=4
        │       SortPreservingMergeExec: [id@0 ASC NULLS LAST], fetch=1000000
        │         [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=4, partitions=8
          │ SortExec: TopK(fetch=5), expr=[id@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          │     t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          │     t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          │     t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=4, partitions=8
          │ SortExec: TopK(fetch=1000000), expr=[id@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          │     t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          │     t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          │     t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          └──────────────────────────────────────────────────
        ")
    }

    /// Joins with a single partition on both sides do not need to be and should not be
    /// rewritten. Doing so risks breaking assumptions DataFusion has made to safely apply
    /// optimizations to single-partition cases. This tests the nested loop join case
    #[tokio::test]
    async fn single_partition_nested_loop_join_is_correct() {
        let plan = assert_distributed_matches_single_node(
            r"SELECT b.id AS bid, p.id AS pid FROM
                (SELECT id FROM build_side WHERE id % 25 = 0) b
                LEFT JOIN
                (SELECT id FROM probe_side ORDER BY id LIMIT 1000000) p
                ON p.id <= b.id
             ORDER BY bid NULLS LAST, pid NULLS LAST",
            true,
        )
        .await
        .unwrap();
        assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ┌───── DistributedExec
        │ SortExec: expr=[bid@0 ASC NULLS LAST, pid@1 ASC NULLS LAST], preserve_partitioning=[false]
        │   ProjectionExec: expr=[id@0 as bid, id@1 as pid]
        │     NestedLoopJoinExec: join_type=Left, filter=id@1 <= id@0
        │       CoalescePartitionsExec
        │         [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        │       SortPreservingMergeExec: [id@0 ASC NULLS LAST], fetch=1000000
        │         [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=4, partitions=12
          │ FilterExec: id@0 % 25 = 0
          │   RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=id@0 % 25 = 0
          │       t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-0.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=id@0 % 25 = 0
          │       t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=id@0 % 25 = 0
          │       t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=id@0 % 25 = 0
          └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=4, partitions=8
          │ SortExec: TopK(fetch=1000000), expr=[id@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          │     t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          │     t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          │     t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[id@0 ASC NULLS LAST]
          └──────────────────────────────────────────────────
        ")
    }

    #[tokio::test]
    async fn build_side_fetch_is_preserved_by_normalize() {
        let plan = assert_distributed_matches_single_node(
            "SELECT b.id, p.id FROM (SELECT id FROM build_side LIMIT 50) b \
             FULL JOIN probe_side p ON b.id = p.id",
            true,
        )
        .await
        .unwrap();
        assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=4, partitions=12
          │ HashJoinExec: mode=Partitioned, join_type=Full, on=[(id@0, id@0)]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          │   [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=4, partitions=12
            │ RepartitionExec: partitioning=Hash([id@0], 12), input_partitions=1
            │   CoalescePartitionsExec: fetch=50
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], limit=50, file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], limit=50, file_type=parquet
            │       t2: DataSourceExec: file_groups={3 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], limit=50, file_type=parquet
            │       t3: DataSourceExec: file_groups={3 groups: [[/target/multi_task_collect_join_repros/build_side/part-1.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-2.parquet:<int>..<int>, /target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/build_side/part-3.parquet:<int>..<int>]]}, projection=[id], limit=50, file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=4, partitions=12
            │ RepartitionExec: partitioning=Hash([id@0], 12), input_partitions=2
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t1: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-0.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-2.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t2: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            │     t3: DataSourceExec: file_groups={2 groups: [[/target/multi_task_collect_join_repros/probe_side/part-1.parquet:<int>..<int>], [/target/multi_task_collect_join_repros/probe_side/part-3.parquet:<int>..<int>]]}, projection=[id], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    fn data_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("target/multi_task_collect_join_repros")
    }

    async fn ensure_data() {
        INIT.get_or_init(|| async {
            let dir = data_dir();
            let _ = fs::remove_dir_all(&dir);
            // (table, rows per id, file rotation)
            for (table, duplicates, rotation) in [
                ("build_side", 1usize, 0i64),
                ("probe_side", PROBE_DUPLICATES, 1),
            ] {
                let table_dir = dir.join(table);
                fs::create_dir_all(&table_dir).unwrap();
                for file_idx in 0..FILES_PER_TABLE {
                    let chunk = (file_idx + rotation) % FILES_PER_TABLE;
                    let ids = (chunk * IDS_PER_FILE..(chunk + 1) * IDS_PER_FILE)
                        .flat_map(|id| std::iter::repeat_n(id, duplicates))
                        .collect::<Vec<_>>();
                    write_ids(&table_dir.join(format!("part-{file_idx}.parquet")), &ids);
                }
            }
        })
        .await;
    }

    fn write_ids(path: &Path, ids: &[i64]) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    async fn register_tables(ctx: &SessionContext) -> Result<()> {
        for table in ["build_side", "probe_side"] {
            ctx.register_parquet(
                table,
                data_dir().join(table).to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        }
        Ok(())
    }

    async fn make_distributed_ctx(broadcast_joins: bool) -> Result<SessionContext> {
        let ctx = start_in_memory_context(NUM_WORKERS, DefaultSessionBuilder).await;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = PARTITIONS;
        let ctx = ctx
            .with_distributed_file_scan_config_bytes_per_partition(1)?
            .with_distributed_broadcast_joins(broadcast_joins)?;
        register_tables(&ctx).await?;
        Ok(ctx)
    }

    async fn run(
        ctx: &SessionContext,
        query: &str,
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<RecordBatch>)> {
        let df = ctx.sql(query).await?;
        let plan = df.create_physical_plan().await?;
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        Ok((plan, batches))
    }

    /// Runs `query` on both contexts and asserts the distributed context produces the same
    /// results as single-node execution. The task-count gate caps these join shapes to a
    /// single task (and stage validation guarantees nothing unsafe slips through), so the
    /// query must both plan and return correct results.
    ///
    /// Returns the distributed plan
    async fn assert_distributed_matches_single_node(
        query: &str,
        broadcast_joins: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        ensure_data().await;

        let s_ctx = SessionContext::new();
        // Pin the baseline to the same partitioning as the distributed context: DataFusion
        // 54's NestedLoopJoin emits spurious unmatched rows in Full/Left joins, and how many
        // depends on the probe-side partition count (fixed upstream during the DataFusion 55
        // cycle, apache/datafusion#22791). With different target_partitions the two contexts
        // disagree for reasons that have nothing to do with distribution.
        s_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = PARTITIONS;
        register_tables(&s_ctx).await?;
        let d_ctx = make_distributed_ctx(broadcast_joins).await?;

        let (_, s_batches) = run(&s_ctx, query).await?;
        let (d_plan, d_batches) = run(&d_ctx, query).await?;
        println!(
            "distributed plan:\n{}",
            display_plan_ascii(d_plan.as_ref(), false)
        );

        let s_rows: usize = s_batches.iter().map(|b| b.num_rows()).sum();
        let d_rows: usize = d_batches.iter().map(|b| b.num_rows()).sum();
        println!("single-node rows: {s_rows}, distributed rows: {d_rows}");
        compare_result_set(&Ok(d_batches), &Ok(s_batches))?;
        Ok(d_plan)
    }
}
