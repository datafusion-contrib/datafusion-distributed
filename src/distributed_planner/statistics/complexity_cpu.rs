use crate::BroadcastExec;
use crate::distributed_planner::statistics::complexity::{Complexity, LinearComplexity};
use crate::execution_plans::ChildrenIsolatorUnionExec;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::JoinSide;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{Column, Literal};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec, SymmetricHashJoinExec,
};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use std::sync::Arc;

/// Calculates the CPU cost for the provided node, without recursing into children.
pub(super) fn complexity_cpu(node: &Arc<dyn ExecutionPlan>) -> Complexity {
    // NestedLoopJoinExec: O(n*m) - evaluates join condition for each pair of rows
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/nested_loop_join.rs
    if let Some(node) = node.downcast_ref::<NestedLoopJoinExec>() {
        // Assume we need to do read all input rows one by one.
        let n = Complexity::Linear(LinearComplexity::AllColumnsFromLeft);
        let m = Complexity::Linear(LinearComplexity::AllColumnsFromRight);
        let mut c = n.multiply(m);
        // The join condition is evaluated on every (left, right) pair. We can't express the
        // exact per-pair cost (it would be filter_cost * n * m), so we add the filter columns
        // as a lower-bound refinement; the O(n*m) materialization term above already dominates.
        if let Some(filter) = node.filter() {
            c = c.plus(join_filter_complexity(filter));
        }
        return c;
    }

    // CrossJoinExec: O(n*m) - produces Cartesian product of all row pairs
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/cross_join.rs
    if let Some(_node) = node.downcast_ref::<CrossJoinExec>() {
        // Assume we need to do read all input rows one by one.
        let n = Complexity::Linear(LinearComplexity::AllColumnsFromLeft);
        let m = Complexity::Linear(LinearComplexity::AllColumnsFromRight);
        return n.multiply(m);
    }

    // SortExec: O(n log n) - uses lexsort_to_indices, may spill to disk
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/sorts/sort.rs
    if let Some(node) = node.downcast_ref::<SortExec>() {
        // All the rows will need to be copied one by one.
        let mut n = Complexity::Linear(LinearComplexity::AllColumns);
        // The sort comparators read every sort key on every row, so even a plain column key costs
        // its bytes (a wide UTF8 key is far costlier to compare than an int).
        for expr in node.expr() {
            n = n.plus(hashed_or_sorted_key_complexity(&expr.expr))
        }
        return n.clone().log(n);
    }

    // HashJoinExec: hash table build (O(n)) + probe (O(m))
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/hash_join/exec.rs
    if let Some(join) = node.downcast_ref::<HashJoinExec>() {
        // Build side (left): concat_batches copies all data (2x read), plus hash table storage,
        // plus hashing left join keys.
        let mut c = Complexity::Linear(LinearComplexity::AllColumnsFromLeft)
            .plus(Complexity::Linear(LinearComplexity::AllColumnsFromLeft));
        for (left_key, _) in join.on() {
            c = c.plus(join_key_complexity(left_key, true));
        }
        // Probe side (right): read all columns + hash right join keys
        c = c.plus(Complexity::Linear(LinearComplexity::AllColumnsFromRight));
        for (_, right_key) in join.on() {
            c = c.plus(join_key_complexity(right_key, false));
        }
        // Optional join filter evaluated on candidate matches during the probe.
        if let Some(filter) = join.filter() {
            c = c.plus(join_filter_complexity(filter));
        }
        return c;
    }

    // SortMergeJoinExec: merge of sorted streams with comparisons
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/sort_merge_join/exec.rs
    // Unlike hash join, sort-merge doesn't buffer all data or build hash tables. It streams
    // through both sorted inputs with O(max_group_size) memory, using partial_cmp comparisons
    // (no hashing). Per-row cost is just key comparisons + optional filter evaluation.
    if let Some(node) = node.downcast_ref::<SortMergeJoinExec>() {
        let mut c: Option<Complexity> = None;
        // Left side: compare join keys during merge
        for (left_key, _) in node.on() {
            let key = join_key_complexity(left_key, true);
            c = Some(match c {
                Some(existing) => existing.plus(key),
                None => key,
            });
        }
        // Right side: compare join keys during merge
        for (_, right_key) in node.on() {
            let key = join_key_complexity(right_key, false);
            c = Some(match c {
                Some(existing) => existing.plus(key),
                None => key,
            });
        }
        // Optional join filter evaluated on matched pairs during the merge.
        if let Some(filter) = node.filter() {
            let f = join_filter_complexity(filter);
            c = Some(match c {
                Some(existing) => existing.plus(f),
                None => f,
            });
        }
        return c.unwrap_or(Complexity::Constant(1.));
    }

    // SymmetricHashJoinExec: streaming join with hash tables on both sides
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/symmetric_hash_join.rs
    // More expensive than HashJoinExec: both sides maintain hash tables, concat_batches
    // runs on every incoming batch (not once at end), plus pruning interval computation
    // and HashSet tracking for visited rows.
    if let Some(node) = node.downcast_ref::<SymmetricHashJoinExec>() {
        // Both sides: concat_batches on every batch (2x read) + hash table + hash keys
        let mut c = Complexity::Linear(LinearComplexity::AllColumnsFromLeft)
            .plus(Complexity::Linear(LinearComplexity::AllColumnsFromLeft));
        for (left_key, _) in node.on() {
            c = c.plus(join_key_complexity(left_key, true));
        }
        c = c
            .plus(Complexity::Linear(LinearComplexity::AllColumnsFromRight))
            .plus(Complexity::Linear(LinearComplexity::AllColumnsFromRight));
        for (_, right_key) in node.on() {
            c = c.plus(join_key_complexity(right_key, false));
        }
        // Optional join filter evaluated on matched pairs as batches stream in.
        if let Some(filter) = node.filter() {
            c = c.plus(join_filter_complexity(filter));
        }
        return c;
    }

    // Aggregation: hash group-by keys + accumulate aggregate inputs
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/aggregates/mod.rs
    if let Some(agg) = node.downcast_ref::<AggregateExec>() {
        // Base: read all input columns for accumulation
        let mut c = Complexity::Linear(LinearComplexity::AllColumns);
        // Additional: evaluate and hash group-by key expressions
        for (expr, _) in agg.group_expr().expr() {
            c = c.plus(hashed_or_sorted_key_complexity(expr));
        }
        // Per-aggregate filter expressions (e.g. COUNT(*) FILTER (WHERE ...))
        for filter in agg.filter_expr().iter().flatten() {
            c = c.plus(expression_complexity(filter));
        }
        return c;
    }

    // Window functions: buffer partitions, compute aggregates over windows
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/windows/window_agg_exec.rs
    if let Some(node) = node.downcast_ref::<WindowAggExec>() {
        // Read all input data + evaluate/hash partition key expressions
        let mut c = Complexity::Linear(LinearComplexity::AllColumns);
        for expr in node.partition_keys() {
            c = c.plus(hashed_or_sorted_key_complexity(&expr));
        }
        return c;
    }

    if let Some(node) = node.downcast_ref::<BoundedWindowAggExec>() {
        let mut c = Complexity::Linear(LinearComplexity::AllColumns);
        for expr in node.partition_keys() {
            c = c.plus(hashed_or_sorted_key_complexity(&expr));
        }
        return c;
    }

    // SortPreservingMergeExec: merges pre-sorted streams with comparisons
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/sorts/sort_preserving_merge.rs
    // K-way merge: O(N log K) comparisons on sort key expressions
    if let Some(node) = node.downcast_ref::<SortPreservingMergeExec>() {
        // need to copy all rows...
        let mut n = Complexity::Linear(LinearComplexity::AllColumns);
        // and compare the sort keys on all of them; a plain column key still costs its bytes.
        for expr in node.expr() {
            n = n.plus(hashed_or_sorted_key_complexity(&expr.expr))
        }
        return n;
    }

    // FilterExec: evaluates predicate expression per row
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/filter.rs
    // Cost depends on predicate complexity - LIKE/Regex operations are expensive
    if let Some(node) = node.downcast_ref::<FilterExec>() {
        // It needs to perform a copy operation just to the output rows...
        let n = Complexity::Linear(LinearComplexity::AllOutputColumns);
        // ...and predicate evaluation on all input rows.
        return n.plus(expression_complexity(node.predicate()));
    }

    // ProjectionExec: cost depends on whether it's simple columns or expressions
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/projection.rs
    if let Some(node) = node.downcast_ref::<ProjectionExec>() {
        let mut n: Option<Complexity> = None;
        for expr in node.expr() {
            n = if let Some(n) = n {
                Some(n.plus(expression_complexity(&expr.expr)))
            } else {
                Some(expression_complexity(&expr.expr))
            };
        }
        return n.unwrap_or(Complexity::Constant(1.));
    }

    // RepartitionExec copies rows into output partitions and may evaluate repartition keys.
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/repartition/mod.rs
    if let Some(node) = node.downcast_ref::<RepartitionExec>() {
        // It needs to copy all the data for chunking it to the different output partitions...
        let mut n = Complexity::Linear(LinearComplexity::AllColumns);
        // Hash/range partitioning reads the partition keys. A plain column key still costs its
        // bytes because the operator hashes or compares it.
        match node.partitioning() {
            Partitioning::Hash(expressions, _) => {
                for expr in expressions {
                    n = n.plus(hashed_or_sorted_key_complexity(expr))
                }
            }
            Partitioning::Range(range) => {
                for expr in range.ordering() {
                    n = n.plus(hashed_or_sorted_key_complexity(&expr.expr))
                }
            }
            Partitioning::RoundRobinBatch(_) => {}
            Partitioning::UnknownPartitioning(_) => {}
        };
        return n;
    }

    // DataSourceExec: Produces data, so assume that it's an O(N) operation over all the columns.
    if node.is::<DataSourceExec>() {
        return Complexity::Linear(LinearComplexity::AllOutputColumns);
    }

    // Limit: just counts rows and stops early.
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/limit.rs
    if node.is::<GlobalLimitExec>() || node.is::<LocalLimitExec>() {
        return Complexity::Constant(1.);
    }

    // CoalescePartitionsExec: receives batches from partitions, just passes through the record
    // batches in a zero copy manner.
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/coalesce_partitions.rs
    if node.is::<CoalescePartitionsExec>() {
        return Complexity::Constant(1.0);
    }

    // BroadcastExec: This node does not do any computation, does not even read the data.
    if node.is::<BroadcastExec>() {
        return Complexity::Constant(1.);
    }

    // UnionExec: combines multiple input streams, no processing
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/union.rs
    if node.is::<UnionExec>() || node.is::<ChildrenIsolatorUnionExec>() {
        return Complexity::Constant(1.);
    }

    // InterleaveExec: round-robin merging of inputs, no processing
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/union.rs
    if node.is::<InterleaveExec>() {
        return Complexity::Constant(1.);
    }

    // EmptyExec: produces no data
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/empty.rs
    if node.is::<EmptyExec>() {
        return Complexity::Constant(1.);
    }

    // For unknown node types, assume we have to do an O(N) operation over all the rows.
    Complexity::Linear(LinearComplexity::AllOutputColumns)
}

struct BytesPerRow {
    processed: Option<Complexity>,
    cols_read: Vec<usize>,
}

fn expression_complexity(expression: &Arc<dyn PhysicalExpr>) -> Complexity {
    _expression_complexity(expression)
        .processed
        .unwrap_or(Complexity::Constant(1.))
}

/// Computes the complexity of processing a join key expression, including the cost of
/// reading the leaf columns from the appropriate child (left or right).
/// Unlike `expression_complexity`, this accounts for the cost of hashing/comparing
/// simple column references (which have zero evaluation cost but real I/O cost).
fn join_key_complexity(expression: &Arc<dyn PhysicalExpr>, from_left: bool) -> Complexity {
    let bpr = _expression_complexity(expression);
    let mut result: Option<Complexity> = None;
    for col_idx in &bpr.cols_read {
        let linear = if from_left {
            LinearComplexity::ColumnFromLeft(*col_idx)
        } else {
            LinearComplexity::ColumnFromRight(*col_idx)
        };
        result = Some(match result {
            Some(r) => r.plus(Complexity::Linear(linear)),
            None => Complexity::Linear(linear),
        });
    }
    result.unwrap_or(Complexity::Constant(1.))
}

/// Computes the per-row processing cost of a join filter predicate.
///
/// A `JoinFilter` is evaluated against an intermediate batch whose columns are described by
/// `column_indices`: intermediate column `i` originates from the left or right child at some
/// original index. `expression_complexity` returns a `Complexity` whose `LinearComplexity::Column`
/// terms reference those intermediate indices, so we remap each of them back onto the
/// corresponding child column before the cost can be evaluated against child statistics.
fn join_filter_complexity(filter: &JoinFilter) -> Complexity {
    remap_filter_columns(
        expression_complexity(filter.expression()),
        filter.column_indices(),
    )
}

/// Rewrites a `Complexity` built from a join filter's intermediate schema so that every
/// `LinearComplexity::Column` term refers to the left/right child column it actually reads.
/// Columns belonging to neither side (the mark-join sentinel) carry no child bytes, so they
/// collapse to a constant.
fn remap_filter_columns(c: Complexity, column_indices: &[ColumnIndex]) -> Complexity {
    match c {
        Complexity::Constant(v) => Complexity::Constant(v),
        Complexity::Linear(LinearComplexity::Column(i)) => match column_indices.get(i) {
            Some(ColumnIndex {
                index,
                side: JoinSide::Left,
            }) => Complexity::Linear(LinearComplexity::ColumnFromLeft(*index)),
            Some(ColumnIndex {
                index,
                side: JoinSide::Right,
            }) => Complexity::Linear(LinearComplexity::ColumnFromRight(*index)),
            _ => Complexity::Constant(1.),
        },
        // `expression_complexity` only ever emits `Column` linear terms, but keep the rest
        // intact so the remapping stays total.
        Complexity::Linear(other) => Complexity::Linear(other),
        Complexity::Log(n, m) => {
            remap_filter_columns(*n, column_indices).log(remap_filter_columns(*m, column_indices))
        }
        Complexity::Plus(n, m) => {
            remap_filter_columns(*n, column_indices).plus(remap_filter_columns(*m, column_indices))
        }
        Complexity::Multiply(n, m) => remap_filter_columns(*n, column_indices)
            .multiply(remap_filter_columns(*m, column_indices)),
    }
}

/// Cost of using an expression as a hashing or comparison key.
///
/// Unlike `expression_complexity` (which only counts the CPU of *evaluating* a `PhysicalExpr`,
/// so a bare column passthrough is free), this charges the bytes of each underlying leaf column.
/// The hashing/comparison itself is performed by the operator — hash-table build, partition
/// hashing, sort comparators — not by any expression in the plan, and its cost scales with the
/// key's byte width. Use it for group-by keys, hash-partition keys and sort keys.
fn hashed_or_sorted_key_complexity(expression: &Arc<dyn PhysicalExpr>) -> Complexity {
    let bpr = _expression_complexity(expression);
    let mut result: Option<Complexity> = None;
    for col_idx in &bpr.cols_read {
        result = Some(match result {
            Some(r) => r.plus(Complexity::Linear(LinearComplexity::Column(*col_idx))),
            None => Complexity::Linear(LinearComplexity::Column(*col_idx)),
        });
    }
    result.unwrap_or(Complexity::Constant(1.))
}

fn _expression_complexity(expression: &Arc<dyn PhysicalExpr>) -> BytesPerRow {
    if let Some(col) = expression.downcast_ref::<Column>() {
        BytesPerRow {
            processed: None,
            cols_read: vec![col.index()],
        }
    } else if expression.is::<Literal>() {
        BytesPerRow {
            processed: None,
            cols_read: vec![],
        }
    } else {
        // Generic handler for all other expressions: CastExpr, TryCastExpr, CaseExpr,
        // InListExpr, IsNullExpr, IsNotNullExpr, NotExpr, NegativeExpr, LikeExpr,
        // ScalarFunctionExpr, AsyncFuncExpr, etc.
        let mut bytes_per_row = BytesPerRow {
            processed: None,
            cols_read: vec![],
        };
        // This operation processes the result of every child once. We model its per-row cost as
        // the sum of (1) the processing already incurred inside each child sub-expression and
        // (2) one linear pass over each leaf column feeding the child. A leaf column therefore
        // contributes once per operation sitting above it, i.e. its bytes are weighted by its
        // depth in the expression tree. Carrying (1) is what keeps nested operations
        // (e.g. the `+` in `(a + b) * c`) from being silently dropped.
        for child in expression.children() {
            let c = _expression_complexity(child);
            if let Some(child_processed) = c.processed {
                bytes_per_row.processed = Some(match bytes_per_row.processed.take() {
                    Some(processed) => processed.plus(child_processed),
                    None => child_processed,
                });
            }
            for col_read in &c.cols_read {
                bytes_per_row.processed = Some(match bytes_per_row.processed.take() {
                    Some(processed) => {
                        processed.plus(Complexity::Linear(LinearComplexity::Column(*col_read)))
                    }
                    None => Complexity::Linear(LinearComplexity::Column(*col_read)),
                });
            }
            bytes_per_row.cols_read.extend(&c.cols_read);
        }
        bytes_per_row
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::distributed_planner::statistics::complexity_cpu::complexity_cpu;
    use crate::test_utils::plans::TestPlanBuilder;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::common::{ScalarValue, SplitPoint};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, RangePartitioning};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, Partitioning, displayable};
    use std::cell::RefCell;
    use std::sync::Arc;
    /* schema for the "weather" table

     MinTemp [type=DOUBLE] [repetitiontype=OPTIONAL]
     MaxTemp [type=DOUBLE] [repetitiontype=OPTIONAL]
     Rainfall [type=DOUBLE] [repetitiontype=OPTIONAL]
     Evaporation [type=DOUBLE] [repetitiontype=OPTIONAL]
     Sunshine [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindGustDir [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindGustSpeed [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindDir9am [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindDir3pm [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindSpeed9am [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindSpeed3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Humidity9am [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Humidity3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Pressure9am [type=DOUBLE] [repetitiontype=OPTIONAL]
     Pressure3pm [type=DOUBLE] [repetitiontype=OPTIONAL]
     Cloud9am [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Cloud3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Temp9am [type=DOUBLE] [repetitiontype=OPTIONAL]
     Temp3pm [type=DOUBLE] [repetitiontype=OPTIONAL]
     RainToday [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     RISK_MM [type=DOUBLE] [repetitiontype=OPTIONAL]
     RainTomorrow [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
    */

    // DataSourceExec: produces data, modeled as O(N) over all output columns.
    #[tokio::test]
    async fn data_source_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT "MinTemp" FROM weather"#)
            .await;
        assert_snapshot!(plan_costs(plan), @"O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet");
    }

    // FilterExec: copies the output rows + evaluates the predicate over the input rows.
    #[tokio::test]
    async fn filter_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT * FROM weather WHERE "MinTemp" > 5"#)
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O((out_Cols+Col0)) | FilterExec: MinTemp@0 > 5
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
        ");
    }

    // ProjectionExec: cost is the sum of its expressions; plain column passthroughs are free.
    #[tokio::test]
    async fn projection_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT "MinTemp" + "MaxTemp" AS s FROM weather"#)
            .await;
        assert_snapshot!(plan_costs(plan), @"O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp@0 + MaxTemp@1 as s], file_type=parquet");
    }

    // AggregateExec: reads all input columns + hashes the group-by keys.
    #[tokio::test]
    async fn aggregate_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT "RainToday", COUNT(*) FROM weather GROUP BY "RainToday""#)
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(2) | ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as count(*)]
         O((Cols+Col0)) | AggregateExec: mode=Single, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
    }

    // SortExec: O(n log n) copy + sort-key evaluation.
    #[tokio::test]
    async fn sort_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT * FROM weather ORDER BY "WindGustDir""#)
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O((Cols+Col5)*Log((Cols+Col5))) | SortExec: expr=[WindGustDir@5 ASC NULLS LAST], preserve_partitioning=[false]
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, sort_order_for_reorder=[WindGustDir@5 ASC NULLS LAST]
        ");
    }

    // SortPreservingMergeExec: appears when several pre-sorted partitions are merged.
    #[tokio::test]
    async fn sort_preserving_merge_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(4)
            .physical_plan(r#"SELECT * FROM weather ORDER BY "WindGustDir""#)
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O((Cols+Col5)) | SortPreservingMergeExec: [WindGustDir@5 ASC NULLS LAST]
         O((Cols+Col5)*Log((Cols+Col5))) | SortExec: expr=[WindGustDir@5 ASC NULLS LAST], preserve_partitioning=[true]
          O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, sort_order_for_reorder=[WindGustDir@5 ASC NULLS LAST]
        ");
    }

    // RepartitionExec (Hash): copies all data + hashes the partition keys.
    #[tokio::test]
    async fn repartition_hash_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(4)
            .physical_plan(r#"SELECT "RainToday", COUNT(*) FROM weather GROUP BY "RainToday""#)
            .await;

        assert_snapshot!(plan_costs(plan), @r"
        O(2) | ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as count(*)]
         O((Cols+Col0)) | AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          O((Cols+Col0)) | RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
           O((Cols+Col0)) | AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
    }

    // HashJoinExec: build side (2x read + key hash) + probe side (read + key hash).
    #[tokio::test]
    async fn hash_join_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a JOIN weather b ON a."RainToday" = b."RainToday"
        "#,
            )
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(((2*left_Cols)+left_Col1+right_Cols+right_Col1)) | HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
    }

    // HashJoinExec with a residual filter: the equi-predicate becomes the hash join key while the
    // inequality (`a.MinTemp > b.MaxTemp`) becomes a JoinFilter over an intermediate schema, so
    // the cost must include the left/right columns the filter reads, not just the join keys.
    #[tokio::test]
    async fn hash_join_with_filter_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a
        JOIN weather b
          ON a."RainToday" = b."RainToday"
         AND a."MinTemp" > b."MaxTemp"
        "#,
            )
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(((2*left_Cols)+left_Col1+right_Cols+right_Col1+(left_Col0+right_Col0))) | HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], filter=MinTemp@0 > MaxTemp@1, projection=[MinTemp@0, MaxTemp@2]
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
    }

    // CrossJoinExec: O(n*m) Cartesian product over all columns of both sides.
    #[tokio::test]
    async fn cross_join_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT a."MinTemp", b."MaxTemp" FROM weather a CROSS JOIN weather b"#)
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O((left_Cols*right_Cols)) | CrossJoinExec
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
        ");
    }

    // NestedLoopJoinExec: produced when a join has no equi-key, only an inequality filter.
    #[tokio::test]
    async fn nested_loop_join_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a JOIN weather b ON a."MinTemp" > b."MaxTemp"
        "#,
            )
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(((left_Cols*right_Cols)+(left_Col0+right_Col0))) | NestedLoopJoinExec: join_type=Inner, filter=MinTemp@0 > MaxTemp@1
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
        ");
    }

    // SortMergeJoinExec: produced when hash joins are disabled; streams both sorted inputs.
    // Requires target_partitions > 1 + repartition_joins + !prefer_hash_join (see physical_planner).
    #[tokio::test]
    async fn sort_merge_join_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(4)
            .information_schema(true)
            .prefer_hash_joins(false)
            .physical_plan(
                r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a JOIN weather b ON a."RainToday" = b."RainToday"
        "#,
            )
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(2) | ProjectionExec: expr=[MinTemp@0 as MinTemp, MaxTemp@2 as MaxTemp]
         O((left_Col1+right_Col1)) | SortMergeJoinExec: join_type=Inner, on=[(RainToday@1, RainToday@1)]
          O((Cols+Col1)*Log((Cols+Col1))) | SortExec: expr=[RainToday@1 ASC], preserve_partitioning=[true]
           O((Cols+Col1)) | RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
            O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          O((Cols+Col1)*Log((Cols+Col1))) | SortExec: expr=[RainToday@1 ASC], preserve_partitioning=[true]
           O((Cols+Col1)) | RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
            O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet
        ");
    }

    // BoundedWindowAggExec: window function with an ORDER BY frame (RANK).
    #[tokio::test]
    async fn bounded_window_agg_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r#"
        SELECT RANK() OVER (PARTITION BY "RainToday" ORDER BY "MaxTemp") FROM weather
        "#,
            )
            .await;
        assert_snapshot!(plan_costs(plan), @r#"
        O(1) | ProjectionExec: expr=[rank() PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@2 as rank() PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]
         O((Cols+Col1)) | BoundedWindowAggExec: wdw=[rank() PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Field { "rank() PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW": UInt64 }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
          O((Cols+Col1+Col0)*Log((Cols+Col1+Col0))) | SortExec: expr=[RainToday@1 ASC NULLS LAST, MaxTemp@0 ASC NULLS LAST], preserve_partitioning=[false]
           O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000002.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000000.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, sort_order_for_reorder=[RainToday@1 ASC NULLS LAST, MaxTemp@0 ASC NULLS LAST]
        "#);
    }

    // WindowAggExec: window aggregate without an ORDER BY (unbounded frame).
    #[tokio::test]
    async fn window_agg_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r#"SELECT SUM("Rainfall") OVER (PARTITION BY "WindGustDir") FROM weather"#,
            )
            .await;
        assert_snapshot!(plan_costs(plan), @r#"
        O(1) | ProjectionExec: expr=[sum(weather.Rainfall) PARTITION BY [weather.WindGustDir] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@2 as sum(weather.Rainfall) PARTITION BY [weather.WindGustDir] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]
         O((Cols+Col1)) | WindowAggExec: wdw=[sum(weather.Rainfall) PARTITION BY [weather.WindGustDir] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING: Ok(Field { name: "sum(weather.Rainfall) PARTITION BY [weather.WindGustDir] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
          O((Cols+Col1)*Log((Cols+Col1))) | SortExec: expr=[WindGustDir@1 ASC NULLS LAST], preserve_partitioning=[false]
           O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[Rainfall, WindGustDir], file_type=parquet, sort_order_for_reorder=[WindGustDir@1 ASC NULLS LAST]
        "#);
    }

    // UnionExec: combines input streams with no per-row processing.
    #[tokio::test]
    async fn union_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r#"
        SELECT "MinTemp" AS t FROM weather
        UNION ALL
        SELECT "MaxTemp" AS t FROM weather
        "#,
            )
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(1) | UnionExec
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp@0 as t], file_type=parquet
         O(out_Cols) | DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp@1 as t], file_type=parquet
        ");
    }

    // AggregateExec with no GROUP BY + CoalescePartitionsExec: the filter prevents the planner from
    // answering COUNT(*) straight from parquet metadata, so a real partial aggregate runs per
    // partition and is merged through a CoalescePartitionsExec before the single final aggregate.
    #[tokio::test]
    async fn aggregate_no_group_by_and_coalesce_partitions() {
        let plan = TestPlanBuilder::new()
            .target_partitions(4)
            .physical_plan(r#"SELECT COUNT(*) FROM weather WHERE "MinTemp" > 5"#)
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(1) | ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
         O(Cols) | AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
          O(1) | CoalescePartitionsExec
           O(Cols) | AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
            O((out_Cols+Col0)) | FilterExec: MinTemp@0 > 5, projection=[]
             O(Cols) | RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
              O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
        ");
    }

    // GlobalLimitExec: an OFFSET can't be pushed down as a per-partition fetch, so a GlobalLimitExec
    // is materialized. (LocalLimitExec shares this exact cost branch but the DF53 planner prefers to
    // carry `fetch` on CoalescePartitionsExec rather than emit a separate LocalLimitExec node.)
    #[tokio::test]
    async fn global_limit_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(4)
            .physical_plan(r#"SELECT * FROM weather WHERE "MinTemp" > 5 LIMIT 10 OFFSET 5"#)
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(1) | GlobalLimitExec: skip=5, fetch=10
         O(1) | CoalescePartitionsExec: fetch=15
          O((out_Cols+Col0)) | FilterExec: MinTemp@0 > 5, fetch=15
           O(Cols) | RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
            O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
        ");
    }

    // EmptyExec: an always-false predicate collapses to an empty relation that produces no data.
    #[tokio::test]
    async fn empty_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT "MinTemp" FROM weather WHERE 1 = 0"#)
            .await;
        assert_snapshot!(plan_costs(plan), @"O(1) | EmptyExec");
    }

    // RoundRobin RepartitionExec: has no hash keys, so it takes the bare all-columns copy path.
    #[tokio::test]
    async fn round_robin_repartition_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(4)
            .physical_plan(r#"SELECT * FROM weather WHERE "MinTemp" > 5"#)
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O((out_Cols+Col0)) | FilterExec: MinTemp@0 > 5
         O(Cols) | RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
        ");
    }

    #[test]
    fn range_repartition_exec() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "range_key",
            DataType::Int64,
            false,
        )]));
        let input = Arc::new(EmptyExec::new(schema).with_partitions(2));
        let ordering = LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new(
            "range_key",
            0,
        )))])
        .expect("non-empty ordering");
        let partitioning = Partitioning::Range(
            RangePartitioning::try_new(
                ordering,
                vec![SplitPoint::new(vec![ScalarValue::Int64(Some(10))])],
            )
            .expect("valid range partitioning"),
        );
        let plan = Arc::new(RepartitionExec::try_new(input, partitioning).unwrap());

        assert_snapshot!(plan_costs(plan), @r"
        O((Cols+Col0)) | RepartitionExec: partitioning=Range([range_key@0 ASC], [(10)], 2), input_partitions=2
         O(1) | EmptyExec
        ");
    }

    // HashJoinExec in Partitioned mode: a distinct planner path from CollectLeft. The cost formula
    // is the same (build 2x read + key hash, probe read + key hash), now over hash-repartitioned
    // inputs rather than a collected left side.
    #[tokio::test]
    async fn partitioned_hash_join_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(4)
            .information_schema(true)
            // Zero the single-partition thresholds so the planner uses Partitioned mode (hash
            // repartition on both sides) instead of collecting the left side.
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .physical_plan(
                r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a JOIN weather b ON a."RainToday" = b."RainToday"
        "#,
            )
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(((2*left_Cols)+left_Col1+right_Cols+right_Col1)) | HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
         O((Cols+Col1)) | RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
          O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
         O((Cols+Col1)) | RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
          O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
    }

    // InterleaveExec: unioning two identically hash-partitioned aggregates lets the planner
    // interleave the partitions instead of concatenating streams.
    #[tokio::test]
    async fn interleave_exec() {
        let plan = TestPlanBuilder::new()
            .target_partitions(4)
            .physical_plan(
                r#"
        SELECT "RainToday" AS k, COUNT(*) AS c FROM weather GROUP BY "RainToday"
        UNION ALL
        SELECT "RainTomorrow" AS k, COUNT(*) AS c FROM weather GROUP BY "RainTomorrow"
        "#,
            )
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(1) | InterleaveExec
         O(2) | ProjectionExec: expr=[RainToday@0 as k, count(Int64(1))@1 as c]
          O((Cols+Col0)) | AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
           O((Cols+Col0)) | RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
            O((Cols+Col0)) | AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
             O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
         O(2) | ProjectionExec: expr=[RainTomorrow@0 as k, count(Int64(1))@1 as c]
          O((Cols+Col0)) | AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[count(Int64(1))]
           O((Cols+Col0)) | RepartitionExec: partitioning=Hash([RainTomorrow@0], 4), input_partitions=3
            O((Cols+Col0)) | AggregateExec: mode=Partial, gby=[RainTomorrow@0 as RainTomorrow], aggr=[count(Int64(1))]
             O(out_Cols) | DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainTomorrow], file_type=parquet
        ");
    }

    // Default fallback for unhandled nodes: `SELECT 1` plans a PlaceholderRowExec, which has no
    // dedicated branch and therefore takes the catch-all O(N)-over-output-columns estimate. This is
    // intentionally conservative; for a 1-row placeholder the output byte size is tiny anyway.
    #[tokio::test]
    async fn default_fallback_unhandled_node() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(r#"SELECT 1"#)
            .await;
        assert_snapshot!(plan_costs(plan), @r"
        O(1) | ProjectionExec: expr=[1 as Int64(1)]
         O(out_Cols) | PlaceholderRowExec
        ");
    }

    // NOTE: BroadcastExec and ChildrenIsolatorUnionExec are inserted only by the distributed
    // planner (not by plain DataFusion planning), and SymmetricHashJoinExec requires unbounded
    // streaming inputs — none are reachable from these parquet-backed queries.

    fn plan_costs(plan: Arc<dyn ExecutionPlan>) -> String {
        let mut display = String::new();
        let depth = RefCell::new(0);
        plan.transform_down_up(
            |plan| {
                let indent = " ".repeat(*depth.borrow());
                // `one_line()` renders just this node (with its full config), not its children.
                let node = displayable(plan.as_ref()).one_line().to_string();
                display += &format!(
                    "{indent}O({:?}) | {}\n",
                    complexity_cpu(&plan),
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
