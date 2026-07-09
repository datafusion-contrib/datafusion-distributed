use crate::BroadcastExec;
use crate::distributed_planner::statistics::complexity::{Complexity, LinearComplexity};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec, SymmetricHashJoinExec,
};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use std::sync::Arc;

/// Calculates the memory cost for the provided node, without recursing into children.
pub(super) fn complexity_memory(node: &Arc<dyn ExecutionPlan>) -> Complexity {
    // NestedLoopJoinExec buffers the left/build side and streams the right/probe side. Its CPU
    // cost is O(left * right), but its retained memory is bounded by the build side plus small
    // output/bitmap buffers.
    // https://github.com/apache/datafusion/blob/branch-54/datafusion/physical-plan/src/joins/nested_loop_join.rs
    if node.is::<NestedLoopJoinExec>() {
        return Complexity::Linear(LinearComplexity::AllColumnsFromLeft);
    }

    // CrossJoinExec also loads the left/build side into memory once and combines it with the
    // streamed right side.
    // https://github.com/apache/datafusion/blob/branch-54/datafusion/physical-plan/src/joins/cross_join.rs
    if node.is::<CrossJoinExec>() {
        return Complexity::Linear(LinearComplexity::AllColumnsFromLeft);
    }

    // SortExec buffers input batches. For a full sort, DataFusion may need both input and sorted
    // output working space before spilling. A fetch-bearing SortExec is DataFusion's TopK; its
    // retained heap is capped by the fetch output, so use output statistics instead of input size.
    // https://github.com/apache/datafusion/blob/branch-54/datafusion/physical-plan/src/sorts/sort.rs
    if let Some(node) = node.downcast_ref::<SortExec>() {
        if node.fetch().is_some() {
            return Complexity::Linear(LinearComplexity::AllOutputColumns);
        }

        let input = Complexity::Linear(LinearComplexity::AllColumns);
        return input.clone().plus(input);
    }

    // HashJoinExec retains the left/build side in hash-table state. The right/probe side streams.
    // https://github.com/apache/datafusion/blob/branch-54/datafusion/physical-plan/src/joins/hash_join/exec.rs
    if node.is::<HashJoinExec>() {
        return Complexity::Linear(LinearComplexity::AllColumnsFromLeft);
    }

    // SortMergeJoinExec streams sorted inputs and only keeps bounded merge state.
    // https://github.com/apache/datafusion/blob/branch-54/datafusion/physical-plan/src/joins/sort_merge_join/exec.rs
    if node.is::<SortMergeJoinExec>() {
        return Complexity::Constant(0.);
    }

    // SymmetricHashJoinExec keeps hash tables for both inputs.
    // https://github.com/apache/datafusion/blob/branch-54/datafusion/physical-plan/src/joins/symmetric_hash_join.rs
    if node.is::<SymmetricHashJoinExec>() {
        return Complexity::Linear(LinearComplexity::AllColumnsFromLeft)
            .plus(Complexity::Linear(LinearComplexity::AllColumnsFromRight));
    }

    // Hash/group aggregation retains one accumulator state per group. Without a GROUP BY the
    // retained state is just the fixed set of aggregate accumulators.
    // https://github.com/apache/datafusion/blob/branch-54/datafusion/physical-plan/src/aggregates/mod.rs
    if let Some(agg) = node.downcast_ref::<AggregateExec>() {
        if agg.group_expr().is_true_no_grouping() {
            return Complexity::Constant(1.);
        }

        return Complexity::Linear(LinearComplexity::AllOutputColumns);
    }

    // WindowAggExec can retain full partitions for unbounded frames. BoundedWindowAggExec is only
    // selected when every window expression reports bounded memory, so keep that fixed-size.
    // https://github.com/apache/datafusion/blob/branch-54/datafusion/physical-plan/src/windows/window_agg_exec.rs
    if node.is::<WindowAggExec>() {
        return Complexity::Linear(LinearComplexity::AllColumns);
    }

    if node.is::<BoundedWindowAggExec>() {
        return Complexity::Constant(1.);
    }

    // SortPreservingMergeExec performs a streaming K-way merge.
    // https://github.com/apache/datafusion/blob/branch-54/datafusion/physical-plan/src/sorts/sort_preserving_merge.rs
    if node.is::<SortPreservingMergeExec>() {
        return Complexity::Constant(0.);
    }

    // BroadcastExec retains batches so several consumers can replay the same input partition.
    if node.is::<BroadcastExec>() {
        return Complexity::Linear(LinearComplexity::AllOutputColumns);
    }

    Complexity::Constant(0.)
}

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::distributed_planner::statistics::complexity_memory::complexity_memory;
    use crate::test_utils::plans::TestPlanBuilder;
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use std::cell::RefCell;
    use std::sync::Arc;

    #[tokio::test]
    async fn hash_join_buffers_build_side() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a JOIN weather b ON a."RainToday" = b."RainToday"
        "#,
            )
            .await;
        assert_snapshot!(plan_memory(plan), @r"
        M(left_Cols) | HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
         M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
         M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
    }

    #[tokio::test]
    async fn cross_and_nested_loop_join_buffer_build_side() {
        let cross = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT a."MinTemp", b."MaxTemp" FROM weather a CROSS JOIN weather b"#)
            .await;
        assert_snapshot!(plan_memory(cross), @r"
        M(left_Cols) | CrossJoinExec
         M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
         M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
        ");

        let nested = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a JOIN weather b ON a."MinTemp" > b."MaxTemp"
        "#,
            )
            .await;
        assert_snapshot!(plan_memory(nested), @r"
        M(left_Cols) | NestedLoopJoinExec: join_type=Inner, filter=MinTemp@0 > MaxTemp@1
         M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
         M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn sort_buffers_input() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT * FROM weather ORDER BY "WindGustDir""#)
            .await;
        assert_snapshot!(plan_memory(plan), @r"
        M((2*Cols)) | SortExec: expr=[WindGustDir@5 ASC NULLS LAST], preserve_partitioning=[false]
         M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, sort_order_for_reorder=[WindGustDir@5 ASC NULLS LAST]
        ");
    }

    #[tokio::test]
    async fn topk_sort_buffers_output() {
        let topk = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT * FROM weather ORDER BY "WindGustDir" LIMIT 10"#)
            .await;
        assert_snapshot!(plan_memory(topk), @r"
        M(out_Cols) | SortExec: TopK(fetch=10), expr=[WindGustDir@5 ASC NULLS LAST], preserve_partitioning=[false]
         M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[WindGustDir@5 ASC NULLS LAST]
        ");
    }

    #[tokio::test]
    async fn aggregate_memory_tracks_group_state() {
        let grouped = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT "RainToday", COUNT(*) FROM weather GROUP BY "RainToday""#)
            .await;
        assert_snapshot!(plan_memory(grouped), @r"
        M(0) | ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as count(*)]
         M(out_Cols) | AggregateExec: mode=Single, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");

        let no_grouping = TestPlanBuilder::new()
            .target_partitions(4)
            .physical_plan(r#"SELECT COUNT(*) FROM weather WHERE "MinTemp" > 5"#)
            .await;
        assert_snapshot!(plan_memory(no_grouping), @r"
        M(0) | ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
         M(1) | AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
          M(0) | CoalescePartitionsExec
           M(1) | AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
            M(0) | FilterExec: MinTemp@0 > 5, projection=[]
             M(0) | RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
              M(0) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
        ");
    }

    #[tokio::test]
    async fn window_memory_distinguishes_unbounded_and_bounded() {
        let unbounded = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r#"SELECT SUM("Rainfall") OVER (PARTITION BY "WindGustDir") FROM weather"#,
            )
            .await;
        assert_snapshot!(plan_memory(unbounded), @r#"
        M(0) | ProjectionExec: expr=[sum(weather.Rainfall) PARTITION BY [weather.WindGustDir] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@2 as sum(weather.Rainfall) PARTITION BY [weather.WindGustDir] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]
         M(Cols) | WindowAggExec: wdw=[sum(weather.Rainfall) PARTITION BY [weather.WindGustDir] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING: Ok(Field { name: "sum(weather.Rainfall) PARTITION BY [weather.WindGustDir] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
          M((2*Cols)) | SortExec: expr=[WindGustDir@1 ASC NULLS LAST], preserve_partitioning=[false]
           M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[Rainfall, WindGustDir], file_type=parquet, sort_order_for_reorder=[WindGustDir@1 ASC NULLS LAST]
        "#);

        let bounded = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r#"
        SELECT RANK() OVER (PARTITION BY "RainToday" ORDER BY "MaxTemp") FROM weather
        "#,
            )
            .await;
        assert_snapshot!(plan_memory(bounded), @r#"
        M(0) | ProjectionExec: expr=[rank() PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@2 as rank() PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]
         M(1) | BoundedWindowAggExec: wdw=[rank() PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Field { "rank() PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW": UInt64 }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
          M((2*Cols)) | SortExec: expr=[RainToday@1 ASC NULLS LAST, MaxTemp@0 ASC NULLS LAST], preserve_partitioning=[false]
           M(0) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000002.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000000.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, sort_order_for_reorder=[RainToday@1 ASC NULLS LAST, MaxTemp@0 ASC NULLS LAST]
        "#);
    }

    fn plan_memory(plan: Arc<dyn ExecutionPlan>) -> String {
        let mut display = String::new();
        let depth = RefCell::new(0);
        plan.transform_down_up(
            |plan| {
                let indent = " ".repeat(*depth.borrow());
                let node = displayable(plan.as_ref()).one_line().to_string();
                display += &format!(
                    "{indent}M({:?}) | {}\n",
                    complexity_memory(&plan),
                    node.trim_end()
                );
                *depth.borrow_mut() += 1;
                Ok(Transformed::no(plan))
            },
            |plan| {
                *depth.borrow_mut() -= 1;
                Ok(Transformed::no(plan))
            },
        )
        .expect("Cannot fail");
        display
    }
}
