use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, plan_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::InputOrderMode;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{
    BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec, SymmetricHashJoinExec,
};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use std::sync::Arc;

/// Default filter selectivity when predicate statistics are unknown.
///
/// Reference: Trino's FilterStatsCalculator.java:76
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/FilterStatsCalculator.java#L76
const UNKNOWN_FILTER_COEFFICIENT: f64 = 0.9;

/// Default selectivity for inequality comparisons (<, >, <=, >=) on overlapping ranges.
/// Assumes uniform distribution; on average 50% of pairs satisfy the inequality.
///
/// Reference: Trino's ComparisonStatsCalculator.java:39
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/ComparisonStatsCalculator.java#L39
const INEQUALITY_SELECTIVITY: f64 = 0.5;

/// Default selectivity for IS NULL predicates when null fraction is unknown.
/// Assumes most data is not null.
///
/// Reference: Trino's IsNullStatsCalculator.java (default nullsFraction assumption)
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/FilterStatsCalculator.java#L298
const IS_NULL_SELECTIVITY: f64 = 0.1;

/// Default selectivity for IS NOT NULL predicates (complement of IS NULL).
const IS_NOT_NULL_SELECTIVITY: f64 = 1.0 - IS_NULL_SELECTIVITY;

/// Default selectivity for LIKE patterns.
///
/// LIKE patterns typically match a small fraction of string values:
/// - Prefix patterns (`prefix%`) can vary widely in selectivity
/// - Suffix patterns (`%suffix`) are typically very selective
/// - Contains patterns (`%contains%`) are typically selective
///
/// Using 0.1 (10%) as a reasonable estimate. Trino uses UNKNOWN_FILTER_COEFFICIENT
/// (0.9) for LIKE, but this overestimates the match rate for most real-world patterns.
const LIKE_SELECTIVITY: f64 = 0.1;

/// Default NDV ratio for string columns when no statistics are available.
/// Strings often have repeated values (e.g., status codes, categories, names),
/// but can also have high cardinality (e.g., user IDs, emails).
///
/// Using 0.5 (50%) as a neutral estimate that works across different workloads.
///
/// Reference: This is a heuristic not directly from Trino, which relies on
/// statistics when available. When statistics are absent, this provides
/// a middle-ground estimate.
const DEFAULT_STRING_NDV_RATIO: f64 = 0.5;

/// Default NDV ratio for complex types (lists, maps, structs).
/// These are less likely to have many duplicates due to their structure.
const DEFAULT_COMPLEX_NDV_RATIO: f64 = 0.8;

/// Statistics for row count and NDV (Number of Distinct Values) per column.
///
/// This struct is used to propagate statistics through the execution plan tree,
/// following Trino's approach for statistics estimation.
///
/// Reference: Trino's PlanNodeStatsEstimate.java:38 and SymbolStatsEstimate.java
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/PlanNodeStatsEstimate.java#L38
#[derive(Clone, Debug)]
pub(crate) struct RowStats {
    /// Number of rows.
    pub count: usize,
    /// Number of distinct values per column.
    /// NDV is capped at row count (can't have more distinct values than rows).
    ndv: Vec<usize>,
}

impl RowStats {
    /// Creates a new RowsStats with the given count and NDV per column.
    fn new(count: usize, ndv: Vec<usize>) -> Self {
        // Cap NDV at row count
        let ndv = ndv.into_iter().map(|n| n.min(count)).collect();
        Self { count, ndv }
    }

    /// Returns RowsStats with zero rows.
    pub(crate) fn zero(num_columns: usize) -> Self {
        Self {
            count: 0,
            ndv: vec![0; num_columns],
        }
    }

    /// Applies a selectivity factor to both count and NDVs.
    ///
    /// Following Trino's approach: when rows are filtered, NDV scales proportionally.
    /// Reference: Trino's FilterStatsCalculator.java:74
    /// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/FilterStatsCalculator.java#L74
    fn apply_selectivity(&self, selectivity: f64) -> Self {
        let new_count = (self.count as f64 * selectivity).ceil() as usize;
        let new_ndv = self
            .ndv
            .iter()
            .map(|&n| {
                let scaled = (n as f64 * selectivity).ceil() as usize;
                // NDV can't exceed new row count
                scaled.min(new_count).max(1.min(new_count))
            })
            .collect();
        Self {
            count: new_count,
            ndv: new_ndv,
        }
    }

    /// Caps the row count at a limit (e.g., for LIMIT clauses).
    ///
    /// NDVs are also capped at the new row count.
    /// Reference: Trino's LimitStatsRule.java:41
    /// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/LimitStatsRule.java#L41
    fn cap_at(&self, limit: usize) -> Self {
        if self.count <= limit {
            return self.clone();
        }
        let new_count = limit;
        let new_ndv = self.ndv.iter().map(|&n| n.min(new_count)).collect();
        Self {
            count: new_count,
            ndv: new_ndv,
        }
    }

    /// Subtracts rows (e.g., for OFFSET/skip).
    fn subtract_rows(&self, skip: usize) -> Self {
        let new_count = self.count.saturating_sub(skip);
        if new_count == 0 {
            return Self::zero(self.ndv.len());
        }
        // Scale NDVs proportionally
        let ratio = new_count as f64 / self.count as f64;
        let new_ndv = self
            .ndv
            .iter()
            .map(|&n| ((n as f64 * ratio).ceil() as usize).min(new_count).max(1))
            .collect();
        Self {
            count: new_count,
            ndv: new_ndv,
        }
    }
}

/// Given a specific [ExecutionPlan], and the input rows per children, it returns how many rows
/// it's expected to return.
///
/// This implementation follows Trino's statistics estimation approach:
/// - Uses DataFusion's built-in statistics when available for leaf nodes
/// - For non-leaf nodes, applies operator-specific selectivity factors
///
/// Reference: Trino's StatsCalculator and various *StatsRule classes
/// https://github.com/trinodb/trino/tree/458/core/trino-main/src/main/java/io/trino/cost
pub(crate) fn calculate_row_stats(
    node: &Arc<dyn ExecutionPlan>,
    input_rows_per_children: &[RowStats],
) -> Result<RowStats, DataFusionError> {
    let name = node.name();
    let any = node.as_any();

    // === Leaf nodes: use statistics from the node itself ===

    if node.children().is_empty() {
        let stats = node.partition_statistics(None)?;
        let count = match stats.num_rows {
            Precision::Exact(n) => n,
            Precision::Inexact(n) => n,
            Precision::Absent => return plan_err!("Row statistics for {name} are absent"),
        };
        let field_count = node.schema().fields().len();
        let ndv = if stats.column_statistics.len() == field_count {
            stats
                .column_statistics
                .iter()
                .enumerate()
                .map(|(i, v)| ndv_from_column_stats(v, count, node.schema().field(i).data_type()))
                .collect()
        } else {
            vec![count; field_count]
        };
        return Ok(RowStats { count, ndv });
    }

    // === Non-leaf nodes: estimate based on operator semantics ===

    // Validate we have the expected number of input rows
    let num_children = node.children().len();
    if input_rows_per_children.len() != num_children {
        return plan_err!(
            "Expected {num_children} input row counts for {name}, got {}",
            input_rows_per_children.len()
        );
    }

    // --- Operators that preserve row count (pass-through) ---

    // ProjectionExec: row count unchanged, NDV mapped from input columns
    // Reference: Trino's ProjectStatsRule.java:48
    // https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/ProjectStatsRule.java#L48
    if let Some(proj) = any.downcast_ref::<ProjectionExec>() {
        let input = &input_rows_per_children[0];
        let output_ndv = compute_projection_ndv(proj, input);
        return Ok(RowStats::new(input.count, output_ndv));
    }

    // SortExec: row count unchanged unless it has a fetch limit (top-k)
    if let Some(sort) = any.downcast_ref::<SortExec>() {
        return Ok(match sort.fetch() {
            Some(fetch) => input_rows_per_children[0].cap_at(fetch),
            None => input_rows_per_children[0].clone(),
        });
    }

    // CoalesceBatchesExec: row count unchanged, just combines batches
    if any.is::<CoalesceBatchesExec>() {
        return Ok(input_rows_per_children[0].clone());
    }

    // CoalescePartitionsExec: row count unchanged, merges partitions
    if any.is::<CoalescePartitionsExec>() {
        return Ok(input_rows_per_children[0].clone());
    }

    // RepartitionExec: row count unchanged, redistributes data
    if any.is::<RepartitionExec>() {
        return Ok(input_rows_per_children[0].clone());
    }

    // SortPreservingMergeExec: row count unchanged unless it has a fetch limit (top-k)
    if let Some(spm) = any.downcast_ref::<SortPreservingMergeExec>() {
        return Ok(match spm.fetch() {
            Some(fetch) => input_rows_per_children[0].cap_at(fetch),
            None => input_rows_per_children[0].clone(),
        });
    }

    // PartialSortExec: row count unchanged unless it has a fetch limit (top-k)
    if let Some(partial_sort) = any.downcast_ref::<PartialSortExec>() {
        return Ok(match partial_sort.fetch() {
            Some(fetch) => input_rows_per_children[0].cap_at(fetch),
            None => input_rows_per_children[0].clone(),
        });
    }

    // Window functions preserve row count (add columns, don't filter)
    // NDV for new window columns = row count (each row gets a unique value)
    if any.is::<WindowAggExec>() || any.is::<BoundedWindowAggExec>() {
        let input = &input_rows_per_children[0];
        let output_cols = node.schema().fields().len();
        let input_cols = input.ndv.len();
        // Preserve input NDVs, new window columns get NDV = row count
        let mut output_ndv = input.ndv.clone();
        for _ in input_cols..output_cols {
            output_ndv.push(input.count);
        }
        return Ok(RowStats::new(input.count, output_ndv));
    }

    // --- Operators that reduce row count ---

    // FilterExec: applies selectivity factor based on predicate analysis
    // Reference: Trino's FilterStatsRule.java:26 and FilterStatsCalculator.java:76
    // https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/FilterStatsRule.java#L26
    if let Some(filter) = any.downcast_ref::<FilterExec>() {
        let input = &input_rows_per_children[0];
        let selectivity = estimate_predicate_selectivity(filter.predicate(), input);
        return Ok(input.apply_selectivity(selectivity));
    }

    // GlobalLimitExec: caps row count at limit
    // Reference: Trino's LimitStatsRule.java:41
    // https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/LimitStatsRule.java#L41
    if let Some(limit) = any.downcast_ref::<GlobalLimitExec>() {
        let skip = limit.skip();
        let fetch = limit.fetch();
        let after_skip = input_rows_per_children[0].subtract_rows(skip);
        return Ok(match fetch {
            Some(f) => after_skip.cap_at(f),
            None => after_skip,
        });
    }

    // LocalLimitExec: similar to GlobalLimitExec but per-partition
    if let Some(limit) = any.downcast_ref::<LocalLimitExec>() {
        return Ok(input_rows_per_children[0].cap_at(limit.fetch()));
    }

    // AggregateExec: reduces rows based on grouping
    // Reference: Trino's AggregationStatsRule.java:50
    // https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/AggregationStatsRule.java#L50
    if let Some(agg) = any.downcast_ref::<AggregateExec>() {
        return estimate_aggregation_stats(agg, &input_rows_per_children[0]);
    }

    // --- Join operators ---
    // Reference: Trino's JoinStatsRule.java:80 (doCalculate method)
    // https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/JoinStatsRule.java#L80

    // CrossJoinExec: Cartesian product
    // Reference: Trino's JoinStatsRule.java:369-378
    if any.is::<CrossJoinExec>() {
        return Ok(compute_cross_join_stats(
            &input_rows_per_children[0],
            &input_rows_per_children[1],
        ));
    }

    // HashJoinExec: equi-join with hash table
    if let Some(join) = any.downcast_ref::<HashJoinExec>() {
        return Ok(estimate_equi_join_stats(
            &input_rows_per_children[0],
            &input_rows_per_children[1],
            join.on(),
        ));
    }

    // SortMergeJoinExec: equi-join with sorted merge
    if let Some(join) = any.downcast_ref::<SortMergeJoinExec>() {
        return Ok(estimate_equi_join_stats(
            &input_rows_per_children[0],
            &input_rows_per_children[1],
            join.on(),
        ));
    }

    // SymmetricHashJoinExec: streaming equi-join
    if let Some(join) = any.downcast_ref::<SymmetricHashJoinExec>() {
        return Ok(estimate_equi_join_stats(
            &input_rows_per_children[0],
            &input_rows_per_children[1],
            join.on(),
        ));
    }

    // NestedLoopJoinExec: can have arbitrary join conditions
    if any.is::<NestedLoopJoinExec>() {
        // Conservative: assume some filtering but not as selective as equi-join
        let cross =
            compute_cross_join_stats(&input_rows_per_children[0], &input_rows_per_children[1]);
        return Ok(cross.apply_selectivity(INEQUALITY_SELECTIVITY));
    }

    // --- Operators that combine/increase row count ---

    // UnionExec: concatenates all inputs
    // Reference: Trino's UnionStatsRule.java
    if any.is::<UnionExec>() {
        return Ok(compute_union_stats(input_rows_per_children));
    }

    // InterleaveExec: round-robin merge of inputs (same as union for stats)
    if any.is::<InterleaveExec>() {
        return Ok(compute_union_stats(input_rows_per_children));
    }

    // --- Special leaf nodes (should have been handled above) ---

    if any.is::<EmptyExec>() {
        let num_cols = node.schema().fields().len();
        return Ok(RowStats::zero(num_cols));
    }

    if any.is::<DataSourceExec>() {
        // Should be handled by the leaf node case above
        return plan_err!("DataSourceExec should be handled as a leaf node");
    }

    // Default: pass through the first child's stats (conservative for unknown operators)
    Ok(input_rows_per_children
        .first()
        .cloned()
        .unwrap_or_else(|| RowStats::zero(node.schema().fields().len())))
}

/// Estimates NDV from column statistics, using multiple strategies:
///
/// 1. If distinct_count is available, use it directly
/// 2. If min/max bounds are available for discrete types, use range size (Trino's approach)
/// 3. Apply type-specific heuristics (e.g., strings often have repeated values)
/// 4. Fall back to type-based theoretical bounds
///
/// Reference: Trino's StatsNormalizer.java for range-based NDV bounds
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/StatsNormalizer.java
fn ndv_from_column_stats(
    column_statistics: &ColumnStatistics,
    row_count: usize,
    ty: &DataType,
) -> usize {
    // Strategy 1: Use distinct_count if available
    match column_statistics.distinct_count {
        Precision::Exact(v) => return v.min(row_count),
        Precision::Inexact(v) => return v.min(row_count),
        Precision::Absent => {}
    }

    // Strategy 2: For discrete types, try to compute NDV from min/max range
    // This follows Trino's approach in StatsNormalizer.maxDistinctValuesByLowHigh()
    if let Some(range_ndv) = ndv_from_min_max_range(column_statistics, ty) {
        return range_ndv.min(row_count).max(1);
    }

    // Strategy 3: Apply type-specific heuristics
    match ty {
        // Null type: only one possible value (null)
        DataType::Null => 1,

        // Boolean: exactly 2 possible values (true/false)
        DataType::Boolean => row_count.min(2),

        // Small integer types: cap at the number of possible values
        DataType::Int8 => row_count.min(256),
        DataType::UInt8 => row_count.min(256),
        DataType::Int16 => row_count.min(65_536),
        DataType::UInt16 => row_count.min(65_536),

        // Larger integer types: without min/max, assume high cardinality
        DataType::Int32 | DataType::Int64 | DataType::UInt32 | DataType::UInt64 => row_count,

        // Floating point: effectively infinite range
        DataType::Float16 | DataType::Float32 | DataType::Float64 => row_count,

        // Date: without min/max, could be anything
        DataType::Date32 | DataType::Date64 => row_count,

        // Timestamps: nanosecond precision, effectively unique per row
        DataType::Timestamp(_, _) => row_count,

        // Time32: limited to seconds/millis in a day
        DataType::Time32(_) => row_count.min(86_400),
        DataType::Time64(_) => row_count,

        // Duration and Interval: wide range
        DataType::Duration(_) | DataType::Interval(_) => row_count,

        // String types: often have repeated values (categories, names, status codes)
        // Use a heuristic ratio - strings are rarely all unique
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            ((row_count as f64 * DEFAULT_STRING_NDV_RATIO).ceil() as usize).max(1)
        }

        // Binary types: similar to strings but slightly higher cardinality expected
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => {
            ((row_count as f64 * DEFAULT_STRING_NDV_RATIO).ceil() as usize).max(1)
        }

        // Complex types: lists, maps, structs - less likely to have duplicates
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => {
            ((row_count as f64 * DEFAULT_COMPLEX_NDV_RATIO).ceil() as usize).max(1)
        }

        // Dictionary: the whole point is low cardinality, assume ~10% of rows
        DataType::Dictionary(_, _) => ((row_count as f64 * 0.1).ceil() as usize).max(1),

        // Decimal types: typically numeric IDs or amounts, high cardinality
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => row_count,
    }
}

/// Computes NDV from min/max range for discrete types.
///
/// For integer types: NDV ≤ max - min + 1
/// For dates: NDV ≤ days between max and min + 1
///
/// Reference: Trino's StatsNormalizer.maxDistinctValuesByLowHigh()
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/StatsNormalizer.java#L142
fn ndv_from_min_max_range(column_statistics: &ColumnStatistics, ty: &DataType) -> Option<usize> {
    use datafusion::common::ScalarValue;

    let min = match &column_statistics.min_value {
        Precision::Exact(v) | Precision::Inexact(v) => v,
        Precision::Absent => return None,
    };
    let max = match &column_statistics.max_value {
        Precision::Exact(v) | Precision::Inexact(v) => v,
        Precision::Absent => return None,
    };

    // Compute range size based on type
    match (ty, min, max) {
        // Signed integers
        (DataType::Int8, ScalarValue::Int8(Some(min)), ScalarValue::Int8(Some(max))) => {
            Some((*max as i64 - *min as i64 + 1) as usize)
        }
        (DataType::Int16, ScalarValue::Int16(Some(min)), ScalarValue::Int16(Some(max))) => {
            Some((*max as i64 - *min as i64 + 1) as usize)
        }
        (DataType::Int32, ScalarValue::Int32(Some(min)), ScalarValue::Int32(Some(max))) => {
            Some((*max as i64 - *min as i64 + 1) as usize)
        }
        (DataType::Int64, ScalarValue::Int64(Some(min)), ScalarValue::Int64(Some(max))) => max
            .checked_sub(*min)
            .and_then(|diff| diff.checked_add(1))
            .map(|v| v as usize),

        // Unsigned integers
        (DataType::UInt8, ScalarValue::UInt8(Some(min)), ScalarValue::UInt8(Some(max))) => {
            Some((*max as usize) - (*min as usize) + 1)
        }
        (DataType::UInt16, ScalarValue::UInt16(Some(min)), ScalarValue::UInt16(Some(max))) => {
            Some((*max as usize) - (*min as usize) + 1)
        }
        (DataType::UInt32, ScalarValue::UInt32(Some(min)), ScalarValue::UInt32(Some(max))) => {
            Some((*max as usize) - (*min as usize) + 1)
        }
        (DataType::UInt64, ScalarValue::UInt64(Some(min)), ScalarValue::UInt64(Some(max))) => max
            .checked_sub(*min)
            .and_then(|diff| diff.checked_add(1))
            .map(|v| v as usize),

        // Date32: days since epoch
        (DataType::Date32, ScalarValue::Date32(Some(min)), ScalarValue::Date32(Some(max))) => {
            Some((*max - *min + 1) as usize)
        }

        // Date64: milliseconds since epoch, convert to days
        (DataType::Date64, ScalarValue::Date64(Some(min)), ScalarValue::Date64(Some(max))) => {
            let days = (*max - *min) / (24 * 60 * 60 * 1000) + 1;
            Some(days as usize)
        }

        // Boolean: if we have min=false max=true, that's 2 values
        (DataType::Boolean, ScalarValue::Boolean(Some(min)), ScalarValue::Boolean(Some(max))) => {
            if min == max {
                Some(1)
            } else {
                Some(2)
            }
        }

        // For other types, we can't easily compute range
        _ => None,
    }
}

/// Computes NDV for projection output columns.
///
/// For simple column references, NDV is passed through from input.
/// For computed expressions, NDV is estimated as row count (worst case).
///
/// Reference: Trino's ProjectStatsRule.java:57-75 (doCalculate method)
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/ProjectStatsRule.java#L57-L75
fn compute_projection_ndv(proj: &ProjectionExec, input: &RowStats) -> Vec<usize> {
    use datafusion::physical_plan::expressions::Column;

    proj.expr()
        .iter()
        .map(|expr| {
            // If it's a simple column reference, use the input column's NDV
            if let Some(col) = expr.expr.as_any().downcast_ref::<Column>() {
                input.ndv.get(col.index()).copied().unwrap_or(input.count)
            } else {
                // For computed expressions, assume NDV = row count (conservative)
                input.count
            }
        })
        .collect()
}

/// Computes statistics for a cross join (Cartesian product).
///
/// Row count = left_rows * right_rows
/// NDV for each column is preserved from its source side.
///
/// Reference: Trino's JoinStatsRule.java:369-378
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/JoinStatsRule.java#L369-L378
fn compute_cross_join_stats(left: &RowStats, right: &RowStats) -> RowStats {
    let output_count = left.count.saturating_mul(right.count);

    // NDV for each column: preserved from source, but capped at output row count
    let mut output_ndv = Vec::with_capacity(left.ndv.len() + right.ndv.len());
    output_ndv.extend(left.ndv.iter().map(|&n| n.min(output_count)));
    output_ndv.extend(right.ndv.iter().map(|&n| n.min(output_count)));

    RowStats::new(output_count, output_ndv)
}

/// Estimates statistics for an equi-join using NDV.
///
/// Following Trino's approach:
///   selectivity = 1 / max(leftNDV, rightNDV) for each join key
///   outputRows = leftRows * rightRows * product(selectivity for each key)
///
/// NDV for join key columns becomes min(leftNDV, rightNDV).
/// NDV for other columns is scaled proportionally.
///
/// Reference: Trino's JoinStatsRule.java:134-146 and ComparisonStatsCalculator.java:171-207
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/JoinStatsRule.java#L134-L146
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/ComparisonStatsCalculator.java#L171-L207
fn estimate_equi_join_stats(
    left: &RowStats,
    right: &RowStats,
    join_keys: &[(PhysicalExprRef, PhysicalExprRef)],
) -> RowStats {
    use datafusion::physical_plan::expressions::Column;

    if left.count == 0 || right.count == 0 {
        return RowStats::zero(left.ndv.len() + right.ndv.len());
    }

    // Calculate selectivity based on join key NDVs
    let mut selectivity = 1.0;
    for (left_expr, right_expr) in join_keys {
        let left_ndv = left_expr
            .as_any()
            .downcast_ref::<Column>()
            .and_then(|c| left.ndv.get(c.index()).copied())
            .unwrap_or(left.count);
        let right_ndv = right_expr
            .as_any()
            .downcast_ref::<Column>()
            .and_then(|c| right.ndv.get(c.index()).copied())
            .unwrap_or(right.count);

        // Trino's formula: selectivity = 1 / max(leftNDV, rightNDV, 1)
        let max_ndv = left_ndv.max(right_ndv).max(1);
        selectivity *= 1.0 / max_ndv as f64;
    }

    let output_count = ((left.count as f64 * right.count as f64 * selectivity).ceil() as usize)
        .min(left.count.max(right.count)); // Can't exceed larger input for inner join

    // Compute output NDV for each column
    let mut output_ndv = Vec::with_capacity(left.ndv.len() + right.ndv.len());

    // Left columns: scale by output/left ratio, but join keys use min(left, right)
    let left_ratio = output_count as f64 / left.count.max(1) as f64;
    for (i, &ndv) in left.ndv.iter().enumerate() {
        let is_join_key = join_keys.iter().any(|(l, _)| {
            l.as_any()
                .downcast_ref::<Column>()
                .is_some_and(|c| c.index() == i)
        });
        let new_ndv = if is_join_key {
            // For join keys, NDV = min of both sides, capped at output
            let right_key_ndv = join_keys
                .iter()
                .find_map(|(l, r)| {
                    if l.as_any()
                        .downcast_ref::<Column>()
                        .is_some_and(|c| c.index() == i)
                    {
                        r.as_any()
                            .downcast_ref::<Column>()
                            .and_then(|c| right.ndv.get(c.index()).copied())
                    } else {
                        None
                    }
                })
                .unwrap_or(right.count);
            ndv.min(right_key_ndv).min(output_count)
        } else {
            ((ndv as f64 * left_ratio).ceil() as usize).min(output_count)
        };
        output_ndv.push(new_ndv.max(1));
    }

    // Right columns: scale by output/right ratio
    let right_ratio = output_count as f64 / right.count.max(1) as f64;
    for (i, &ndv) in right.ndv.iter().enumerate() {
        let is_join_key = join_keys.iter().any(|(_, r)| {
            r.as_any()
                .downcast_ref::<Column>()
                .is_some_and(|c| c.index() == i)
        });
        let new_ndv = if is_join_key {
            // Already handled on left side, use same logic
            let left_key_ndv = join_keys
                .iter()
                .find_map(|(l, r)| {
                    if r.as_any()
                        .downcast_ref::<Column>()
                        .is_some_and(|c| c.index() == i)
                    {
                        l.as_any()
                            .downcast_ref::<Column>()
                            .and_then(|c| left.ndv.get(c.index()).copied())
                    } else {
                        None
                    }
                })
                .unwrap_or(left.count);
            ndv.min(left_key_ndv).min(output_count)
        } else {
            ((ndv as f64 * right_ratio).ceil() as usize).min(output_count)
        };
        output_ndv.push(new_ndv.max(1));
    }

    RowStats::new(output_count, output_ndv)
}

/// Computes statistics for a UNION ALL.
///
/// Row count = sum of all input row counts
/// NDV for each column = sum of NDVs (conservative; could use max for UNION DISTINCT)
///
/// Reference: Trino's UnionStatsRule.java:46-63 (doCalculate method)
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/UnionStatsRule.java#L46-L63
fn compute_union_stats(inputs: &[RowStats]) -> RowStats {
    if inputs.is_empty() {
        return RowStats::zero(0);
    }

    let output_count: usize = inputs.iter().map(|s| s.count).sum();
    let num_cols = inputs[0].ndv.len();

    // Sum NDVs across inputs, capped at output row count
    let output_ndv: Vec<usize> = (0..num_cols)
        .map(|i| {
            inputs
                .iter()
                .map(|s| s.ndv.get(i).copied().unwrap_or(0))
                .sum::<usize>()
                .min(output_count)
        })
        .collect();

    RowStats::new(output_count, output_ndv)
}

/// Estimates statistics for aggregation.
///
/// Following Trino's approach:
/// - Scalar aggregate (no GROUP BY): returns 1 row, NDV = 1 for all columns
/// - GROUP BY: output rows = product of NDVs for group columns (capped at input rows)
///   NDV for group columns = their original NDV, aggregates get NDV = output rows
///
/// Reference: Trino's AggregationStatsRule.java:72-100
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/AggregationStatsRule.java#L72-L100
fn estimate_aggregation_stats(
    agg: &AggregateExec,
    input: &RowStats,
) -> Result<RowStats, DataFusionError> {
    use datafusion::physical_plan::expressions::Column;

    let group_by = agg.group_expr();
    let group_exprs = group_by.expr();
    let num_group_cols = group_exprs.len();
    let num_aggr_exprs = agg.aggr_expr().len();
    let output_cols = num_group_cols + num_aggr_exprs;

    // Partial aggregates in streaming mode don't reduce row count
    if matches!(agg.mode(), AggregateMode::Partial)
        && !matches!(agg.input_order_mode(), InputOrderMode::Linear)
    {
        // Pass through with adjusted column count
        let mut ndv = Vec::with_capacity(output_cols);
        for (expr, _name) in group_exprs {
            let col_ndv = expr
                .as_any()
                .downcast_ref::<Column>()
                .and_then(|c| input.ndv.get(c.index()).copied())
                .unwrap_or(input.count);
            ndv.push(col_ndv);
        }
        // Aggregate columns get NDV = row count (intermediate states)
        for _ in 0..num_aggr_exprs {
            ndv.push(input.count);
        }
        return Ok(RowStats::new(input.count, ndv));
    }

    // Scalar aggregate (no GROUP BY) produces exactly 1 row
    if num_group_cols == 0 && !group_by.has_grouping_set() {
        return Ok(RowStats::new(1, vec![1; output_cols]));
    }

    // Estimate output rows from product of group column NDVs
    // Reference: Trino's AggregationStatsRule.java:91-100
    let mut estimated_groups: usize = 1;
    let mut group_ndvs = Vec::with_capacity(num_group_cols);

    for (expr, _name) in group_exprs {
        let col_ndv = expr
            .as_any()
            .downcast_ref::<Column>()
            .and_then(|c| input.ndv.get(c.index()).copied())
            .unwrap_or(input.count);

        // Add 1 for potential NULL group
        let ndv_with_null = col_ndv.saturating_add(1);
        estimated_groups = estimated_groups.saturating_mul(ndv_with_null);
        group_ndvs.push(col_ndv);
    }

    // Cap at input row count
    let output_count = estimated_groups.min(input.count).max(1);

    // Build output NDV: group columns keep their NDV (capped), aggregates get NDV = output
    let mut output_ndv = Vec::with_capacity(output_cols);
    for ndv in group_ndvs {
        output_ndv.push(ndv.min(output_count));
    }
    for _ in 0..num_aggr_exprs {
        output_ndv.push(output_count); // Each aggregate value is unique per group
    }

    Ok(RowStats::new(output_count, output_ndv))
}

/// Estimates the selectivity of a filter predicate based on its structure.
///
/// This follows Trino's approach in FilterStatsCalculator:
/// - AND: multiply selectivities (assuming independence)
/// - OR: use inclusion-exclusion principle
/// - NOT: 1 - child selectivity
/// - Equality (=): 1/NDV of the column
/// - Inequality (<, >, <=, >=): 0.5 default
/// - IS NULL: 0.1 default
/// - IS NOT NULL: 0.9 default
/// - IN list: list_size / NDV
/// - Unknown predicates: 0.9 default (UNKNOWN_FILTER_COEFFICIENT)
///
/// Reference: Trino's FilterStatsCalculator.java
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/FilterStatsCalculator.java
fn estimate_predicate_selectivity(
    predicate: &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    input: &RowStats,
) -> f64 {
    let any = predicate.as_any();

    // BinaryExpr: handle AND, OR, and comparison operators
    if let Some(binary) = any.downcast_ref::<BinaryExpr>() {
        return estimate_binary_selectivity(binary, input);
    }

    // NotExpr: complement of child selectivity
    // Reference: Trino's FilterStatsCalculator.java:320-330
    if let Some(not) = any.downcast_ref::<NotExpr>() {
        let child_selectivity = estimate_predicate_selectivity(not.arg(), input);
        return (1.0 - child_selectivity).max(0.0);
    }

    // IsNullExpr: default null fraction
    // Reference: Trino's FilterStatsCalculator.java:298
    if any.is::<IsNullExpr>() {
        return IS_NULL_SELECTIVITY;
    }

    // IsNotNullExpr: complement of null fraction
    if any.is::<IsNotNullExpr>() {
        return IS_NOT_NULL_SELECTIVITY;
    }

    // InListExpr: selectivity based on list size and NDV
    // Reference: Trino's FilterStatsCalculator.java:263-280 (InPredicate handling)
    if let Some(in_list) = any.downcast_ref::<InListExpr>() {
        return estimate_in_list_selectivity(in_list, input);
    }

    // Literal: if it's a boolean literal, return 1.0 or 0.0
    if let Some(lit) = any.downcast_ref::<Literal>() {
        if let datafusion::common::ScalarValue::Boolean(Some(b)) = lit.value() {
            return if *b { 1.0 } else { 0.0 };
        }
    }

    // Unknown predicate type: use default coefficient
    UNKNOWN_FILTER_COEFFICIENT
}

/// Estimates selectivity for a binary expression.
///
/// Reference: Trino's FilterStatsCalculator.java and ComparisonStatsCalculator.java
fn estimate_binary_selectivity(binary: &BinaryExpr, input: &RowStats) -> f64 {
    let op = binary.op();
    let left = binary.left();
    let right = binary.right();

    match op {
        // AND: multiply selectivities (independence assumption)
        // Reference: Trino's FilterStatsCalculator.java:180 (visitLogicalExpression)
        Operator::And => {
            let left_sel = estimate_predicate_selectivity(left, input);
            let right_sel = estimate_predicate_selectivity(right, input);
            left_sel * right_sel
        }

        // OR: inclusion-exclusion principle: P(A or B) = P(A) + P(B) - P(A and B)
        // Reference: Trino's FilterStatsCalculator.java:213-222
        Operator::Or => {
            let left_sel = estimate_predicate_selectivity(left, input);
            let right_sel = estimate_predicate_selectivity(right, input);
            // Assuming independence: P(A and B) = P(A) * P(B)
            (left_sel + right_sel - left_sel * right_sel).min(1.0)
        }

        // Equality: selectivity = 1/max(leftNDV, rightNDV)
        // Reference: Trino's ComparisonStatsCalculator.java:171-207
        Operator::Eq => estimate_equality_selectivity(left, right, input),

        // NotEq: complement of equality
        Operator::NotEq => 1.0 - estimate_equality_selectivity(left, right, input),

        // Inequality comparisons: default to 0.5
        // Reference: Trino's ComparisonStatsCalculator.java:39
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => INEQUALITY_SELECTIVITY,

        // LIKE patterns: use dedicated selectivity based on TPC-H observations
        // Reference: Trino's FilterStatsCalculator.java:235-250
        // TPC-H shows LIKE '%BRASS' has ~4% selectivity, so 0.1 is reasonable
        Operator::LikeMatch | Operator::ILikeMatch => LIKE_SELECTIVITY,
        Operator::NotLikeMatch | Operator::NotILikeMatch => 1.0 - LIKE_SELECTIVITY,

        // IS DISTINCT FROM: similar to NotEq but handles NULLs
        Operator::IsDistinctFrom => 1.0 - estimate_equality_selectivity(left, right, input),
        Operator::IsNotDistinctFrom => estimate_equality_selectivity(left, right, input),

        // Regex: use default coefficient
        Operator::RegexMatch | Operator::RegexIMatch => UNKNOWN_FILTER_COEFFICIENT,
        Operator::RegexNotMatch | Operator::RegexNotIMatch => 1.0 - UNKNOWN_FILTER_COEFFICIENT,

        // Arithmetic and bitwise operators shouldn't appear as filter predicates,
        // but if they do, return the default
        _ => UNKNOWN_FILTER_COEFFICIENT,
    }
}

/// Estimates selectivity for an equality comparison.
///
/// If one side is a column and the other is a literal, selectivity = 1/NDV.
/// If both are columns, selectivity = 1/max(NDV1, NDV2).
///
/// Reference: Trino's ComparisonStatsCalculator.java:171-207
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/ComparisonStatsCalculator.java#L171-L207
fn estimate_equality_selectivity(
    left: &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    right: &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    input: &RowStats,
) -> f64 {
    let left_ndv = get_expression_ndv(left, input);
    let right_ndv = get_expression_ndv(right, input);

    // Selectivity = 1 / max(leftNDV, rightNDV, 1)
    let max_ndv = left_ndv.max(right_ndv).max(1);
    1.0 / max_ndv as f64
}

/// Gets the NDV for an expression.
///
/// Uses type-aware heuristics:
/// - Literals: NDV = 1
/// - Boolean-producing expressions (comparisons, IS NULL, NOT, etc.): NDV = 2
/// - Column references: uses stored NDV from input stats
/// - Other expressions: conservative estimate (row count)
///
/// Reference: Trino considers data types when estimating NDV
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/SymbolStatsEstimate.java
fn get_expression_ndv(
    expr: &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    input: &RowStats,
) -> usize {
    let any = expr.as_any();

    // Literal: single distinct value
    if any.is::<Literal>() {
        return 1;
    }

    // Boolean-producing expressions have NDV = 2 (true/false)
    // IS NULL / IS NOT NULL
    if any.is::<IsNullExpr>() || any.is::<IsNotNullExpr>() {
        return 2;
    }

    // NOT expression produces boolean
    if any.is::<NotExpr>() {
        return 2;
    }

    // IN list produces boolean
    if any.is::<InListExpr>() {
        return 2;
    }

    // BinaryExpr: check if it's a comparison/logical operator (produces boolean)
    if let Some(binary) = any.downcast_ref::<BinaryExpr>() {
        if is_boolean_operator(binary.op()) {
            return 2;
        }
        // For arithmetic operators, estimate based on operands
        // (could be more sophisticated, but row count is a safe upper bound)
        return input.count;
    }

    // Column reference: use column's NDV from stats
    if let Some(col) = any.downcast_ref::<Column>() {
        return input.ndv.get(col.index()).copied().unwrap_or(input.count);
    }

    // Other expressions: conservative estimate
    input.count
}

/// Returns true if the operator produces a boolean result.
fn is_boolean_operator(op: &Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::And
            | Operator::Or
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
            | Operator::RegexMatch
            | Operator::RegexIMatch
            | Operator::RegexNotMatch
            | Operator::RegexNotIMatch
            | Operator::LikeMatch
            | Operator::ILikeMatch
            | Operator::NotLikeMatch
            | Operator::NotILikeMatch
    )
}

/// Estimates selectivity for an IN list predicate.
///
/// Selectivity = list_size / NDV of the expression.
///
/// Reference: Trino's FilterStatsCalculator.java:263-280
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/FilterStatsCalculator.java#L263-L280
fn estimate_in_list_selectivity(in_list: &InListExpr, input: &RowStats) -> f64 {
    let expr = in_list.expr();
    let list = in_list.list();

    // If negated (NOT IN), compute complement
    let is_negated = in_list.negated();

    // Get NDV of the expression being checked
    let expr_ndv = get_expression_ndv(expr, input);

    // Count the number of values in the list
    let list_size = list.len();

    // Selectivity = list_size / NDV, capped at 1.0
    let selectivity = (list_size as f64 / expr_ndv.max(1) as f64).min(1.0);

    if is_negated {
        1.0 - selectivity
    } else {
        selectivity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rows_stats_new() {
        let stats = RowStats::new(100, vec![50, 200, 100]);
        assert_eq!(stats.count, 100);
        // NDV capped at row count
        assert_eq!(stats.ndv, vec![50, 100, 100]);
    }

    #[test]
    fn test_rows_stats_apply_selectivity() {
        let stats = RowStats::new(1000, vec![100, 500, 1000]);
        let filtered = stats.apply_selectivity(0.5);

        assert_eq!(filtered.count, 500);
        // NDVs scaled proportionally, capped at new row count
        assert_eq!(filtered.ndv[0], 50);
        assert_eq!(filtered.ndv[1], 250);
        assert_eq!(filtered.ndv[2], 500);
    }

    #[test]
    fn test_rows_stats_cap_at() {
        let stats = RowStats::new(1000, vec![100, 500, 1000]);
        let capped = stats.cap_at(200);

        assert_eq!(capped.count, 200);
        // NDVs capped at new row count
        assert_eq!(capped.ndv, vec![100, 200, 200]);
    }

    #[test]
    fn test_rows_stats_subtract_rows() {
        let stats = RowStats::new(1000, vec![100, 500, 1000]);
        let after_skip = stats.subtract_rows(500);

        assert_eq!(after_skip.count, 500);
        // NDVs scaled proportionally
        assert!(after_skip.ndv[0] <= 100);
        assert!(after_skip.ndv[1] <= 500);
    }

    #[test]
    fn test_cross_join_stats() {
        let left = RowStats::new(100, vec![10, 50]);
        let right = RowStats::new(200, vec![20, 100]);

        let result = compute_cross_join_stats(&left, &right);

        assert_eq!(result.count, 20000);
        assert_eq!(result.ndv.len(), 4);
        // Left columns preserved
        assert_eq!(result.ndv[0], 10);
        assert_eq!(result.ndv[1], 50);
        // Right columns preserved
        assert_eq!(result.ndv[2], 20);
        assert_eq!(result.ndv[3], 100);
    }

    #[test]
    fn test_union_stats() {
        let input1 = RowStats::new(100, vec![10, 50]);
        let input2 = RowStats::new(200, vec![20, 100]);

        let result = compute_union_stats(&[input1, input2]);

        assert_eq!(result.count, 300);
        // NDVs summed
        assert_eq!(result.ndv[0], 30);
        assert_eq!(result.ndv[1], 150);
    }

    #[test]
    fn test_constants() {
        // Verify constants match Trino's defaults where applicable
        assert!((UNKNOWN_FILTER_COEFFICIENT - 0.9).abs() < f64::EPSILON);
        assert!((INEQUALITY_SELECTIVITY - 0.5).abs() < f64::EPSILON);
        assert!((IS_NULL_SELECTIVITY - 0.1).abs() < f64::EPSILON);
        assert!((IS_NOT_NULL_SELECTIVITY - 0.9).abs() < f64::EPSILON);
        assert!((LIKE_SELECTIVITY - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_ndv_from_column_stats_no_stats() {
        // Test with absent statistics - falls back to type-based heuristics
        let absent_stats = ColumnStatistics::new_unknown();

        // Null type: always 1
        assert_eq!(
            ndv_from_column_stats(&absent_stats, 1000, &DataType::Null),
            1
        );

        // Boolean: capped at 2
        assert_eq!(
            ndv_from_column_stats(&absent_stats, 1000, &DataType::Boolean),
            2
        );
        assert_eq!(
            ndv_from_column_stats(&absent_stats, 1, &DataType::Boolean),
            1
        );

        // Int8/UInt8: capped at 256
        assert_eq!(
            ndv_from_column_stats(&absent_stats, 1000, &DataType::Int8),
            256
        );
        assert_eq!(
            ndv_from_column_stats(&absent_stats, 100, &DataType::Int8),
            100
        );

        // Int16/UInt16: capped at 65536
        assert_eq!(
            ndv_from_column_stats(&absent_stats, 100_000, &DataType::Int16),
            65_536
        );

        // Larger integers: use row count
        assert_eq!(
            ndv_from_column_stats(&absent_stats, 1000, &DataType::Int32),
            1000
        );

        // Strings: use heuristic ratio (50% - neutral estimate for mixed workloads)
        assert_eq!(
            ndv_from_column_stats(&absent_stats, 1000, &DataType::Utf8),
            500
        );

        // Dictionary: low cardinality assumption (10%)
        assert_eq!(
            ndv_from_column_stats(
                &absent_stats,
                1000,
                &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            ),
            100
        );

        // Time32: capped at 86400 (seconds in a day)
        assert_eq!(
            ndv_from_column_stats(
                &absent_stats,
                100_000,
                &DataType::Time32(arrow::datatypes::TimeUnit::Second)
            ),
            86_400
        );
    }

    #[test]
    fn test_ndv_from_column_stats_with_distinct_count() {
        // When distinct_count is available, use it directly
        let stats_with_ndv = ColumnStatistics {
            distinct_count: Precision::Exact(42),
            ..ColumnStatistics::new_unknown()
        };

        assert_eq!(
            ndv_from_column_stats(&stats_with_ndv, 1000, &DataType::Int32),
            42
        );
        assert_eq!(
            ndv_from_column_stats(&stats_with_ndv, 1000, &DataType::Utf8),
            42
        );
    }

    #[test]
    fn test_ndv_from_column_stats_with_min_max() {
        use datafusion::common::ScalarValue;

        // Int32 with min=10, max=19 -> range of 10 values
        let stats_with_range = ColumnStatistics {
            min_value: Precision::Exact(ScalarValue::Int32(Some(10))),
            max_value: Precision::Exact(ScalarValue::Int32(Some(19))),
            ..ColumnStatistics::new_unknown()
        };

        assert_eq!(
            ndv_from_column_stats(&stats_with_range, 1000, &DataType::Int32),
            10 // max - min + 1 = 19 - 10 + 1 = 10
        );

        // Boolean with min=false, max=true -> 2 values
        let bool_range = ColumnStatistics {
            min_value: Precision::Exact(ScalarValue::Boolean(Some(false))),
            max_value: Precision::Exact(ScalarValue::Boolean(Some(true))),
            ..ColumnStatistics::new_unknown()
        };

        assert_eq!(
            ndv_from_column_stats(&bool_range, 1000, &DataType::Boolean),
            2
        );

        // Boolean with min=true, max=true -> 1 value (all true)
        let bool_single = ColumnStatistics {
            min_value: Precision::Exact(ScalarValue::Boolean(Some(true))),
            max_value: Precision::Exact(ScalarValue::Boolean(Some(true))),
            ..ColumnStatistics::new_unknown()
        };

        assert_eq!(
            ndv_from_column_stats(&bool_single, 1000, &DataType::Boolean),
            1
        );

        // Date32: min=0 (1970-01-01), max=364 -> 365 days
        let date_range = ColumnStatistics {
            min_value: Precision::Exact(ScalarValue::Date32(Some(0))),
            max_value: Precision::Exact(ScalarValue::Date32(Some(364))),
            ..ColumnStatistics::new_unknown()
        };

        assert_eq!(
            ndv_from_column_stats(&date_range, 10000, &DataType::Date32),
            365
        );
    }

    #[test]
    fn test_equality_selectivity() {
        // Input with 1000 rows, column 0 has NDV=100, column 1 has NDV=10
        let input = RowStats::new(1000, vec![100, 10]);

        // Column reference with NDV=100 -> selectivity = 1/100
        let col0: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("a", 0));
        let ndv = get_expression_ndv(&col0, &input);
        assert_eq!(ndv, 100);

        // Column reference with NDV=10 -> selectivity = 1/10
        let col1: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("b", 1));
        let ndv = get_expression_ndv(&col1, &input);
        assert_eq!(ndv, 10);

        // Literal has NDV=1
        let lit: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Literal::new(
            datafusion::common::ScalarValue::Int32(Some(42)),
        ));
        let ndv = get_expression_ndv(&lit, &input);
        assert_eq!(ndv, 1);

        // Equality col0 = literal -> selectivity = 1/100
        let selectivity = estimate_equality_selectivity(&col0, &lit, &input);
        assert!((selectivity - 0.01).abs() < 0.001);

        // Equality col1 = literal -> selectivity = 1/10
        let selectivity = estimate_equality_selectivity(&col1, &lit, &input);
        assert!((selectivity - 0.1).abs() < 0.001);

        // Equality col0 = col1 -> selectivity = 1/max(100, 10) = 1/100
        let selectivity = estimate_equality_selectivity(&col0, &col1, &input);
        assert!((selectivity - 0.01).abs() < 0.001);
    }

    #[test]
    fn test_boolean_expression_ndv() {
        let input = RowStats::new(1000, vec![100, 50]);

        // Comparison expressions produce boolean (NDV = 2)
        let col0: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("a", 0));
        let lit: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Literal::new(
            datafusion::common::ScalarValue::Int32(Some(42)),
        ));
        let comparison: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            Arc::new(BinaryExpr::new(col0.clone(), Operator::Eq, lit));
        assert_eq!(get_expression_ndv(&comparison, &input), 2);

        // IS NULL produces boolean (NDV = 2)
        let is_null: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            Arc::new(IsNullExpr::new(col0.clone()));
        assert_eq!(get_expression_ndv(&is_null, &input), 2);

        // IS NOT NULL produces boolean (NDV = 2)
        let is_not_null: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            Arc::new(IsNotNullExpr::new(col0.clone()));
        assert_eq!(get_expression_ndv(&is_not_null, &input), 2);

        // NOT produces boolean (NDV = 2)
        let not_expr: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            Arc::new(NotExpr::new(comparison.clone()));
        assert_eq!(get_expression_ndv(&not_expr, &input), 2);

        // AND/OR produce boolean (NDV = 2)
        let col1: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("b", 1));
        let lit2: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Literal::new(
            datafusion::common::ScalarValue::Int32(Some(10)),
        ));
        let comparison2: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            Arc::new(BinaryExpr::new(col1, Operator::Lt, lit2));
        let and_expr: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            Arc::new(BinaryExpr::new(comparison, Operator::And, comparison2));
        assert_eq!(get_expression_ndv(&and_expr, &input), 2);
    }

    #[test]
    fn test_in_list_selectivity() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let input = RowStats::new(1000, vec![100]);

        // Create schema for the IN list expression
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

        // Create IN list: col0 IN (1, 2, 3, 4, 5)
        let col0: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("a", 0));
        let values: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> = (1..=5)
            .map(|i| {
                Arc::new(Literal::new(datafusion::common::ScalarValue::Int32(Some(
                    i,
                )))) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            })
            .collect();

        let in_list = InListExpr::try_new(col0, values, false, &schema).unwrap();

        // Selectivity = 5/100 = 0.05
        let selectivity = estimate_in_list_selectivity(&in_list, &input);
        assert!((selectivity - 0.05).abs() < 0.001);
    }

    #[test]
    fn test_binary_and_or_selectivity() {
        let input = RowStats::new(1000, vec![100, 10]);

        // col0 = 1 has selectivity 1/100 = 0.01
        let col0: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("a", 0));
        let lit1: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Literal::new(
            datafusion::common::ScalarValue::Int32(Some(1)),
        ));
        let eq1 = BinaryExpr::new(col0.clone(), Operator::Eq, lit1);

        // col1 = 1 has selectivity 1/10 = 0.1
        let col1: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("b", 1));
        let lit2: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Literal::new(
            datafusion::common::ScalarValue::Int32(Some(1)),
        ));
        let eq2 = BinaryExpr::new(col1.clone(), Operator::Eq, lit2);

        // AND: 0.01 * 0.1 = 0.001
        let and_expr = BinaryExpr::new(Arc::new(eq1.clone()), Operator::And, Arc::new(eq2.clone()));
        let sel = estimate_binary_selectivity(&and_expr, &input);
        assert!((sel - 0.001).abs() < 0.0001);

        // OR: 0.01 + 0.1 - 0.01*0.1 = 0.109
        let or_expr = BinaryExpr::new(Arc::new(eq1), Operator::Or, Arc::new(eq2));
        let sel = estimate_binary_selectivity(&or_expr, &input);
        assert!((sel - 0.109).abs() < 0.001);
    }

    #[test]
    fn test_inequality_selectivity() {
        let input = RowStats::new(1000, vec![100]);

        let col0: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("a", 0));
        let lit: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Literal::new(
            datafusion::common::ScalarValue::Int32(Some(50)),
        ));

        // All inequality operators should return 0.5
        for op in [Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq] {
            let expr = BinaryExpr::new(col0.clone(), op, lit.clone());
            let sel = estimate_binary_selectivity(&expr, &input);
            assert!((sel - INEQUALITY_SELECTIVITY).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_not_selectivity() {
        let input = RowStats::new(1000, vec![100]);

        // NOT (col0 = 1) has selectivity 1 - 1/100 = 0.99
        let col0: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("a", 0));
        let lit: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Literal::new(
            datafusion::common::ScalarValue::Int32(Some(1)),
        ));
        let eq_expr: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            Arc::new(BinaryExpr::new(col0, Operator::Eq, lit));
        let not_expr: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            Arc::new(NotExpr::new(eq_expr));

        let sel = estimate_predicate_selectivity(&not_expr, &input);
        assert!((sel - 0.99).abs() < 0.001);
    }

    #[test]
    fn test_is_null_selectivity() {
        let input = RowStats::new(1000, vec![100]);

        let col0: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("a", 0));

        // IS NULL
        let is_null: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            Arc::new(IsNullExpr::new(col0.clone()));
        let sel = estimate_predicate_selectivity(&is_null, &input);
        assert!((sel - IS_NULL_SELECTIVITY).abs() < f64::EPSILON);

        // IS NOT NULL
        let is_not_null: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            Arc::new(IsNotNullExpr::new(col0));
        let sel = estimate_predicate_selectivity(&is_not_null, &input);
        assert!((sel - IS_NOT_NULL_SELECTIVITY).abs() < f64::EPSILON);
    }

    #[test]
    fn test_boolean_literal_selectivity() {
        let input = RowStats::new(1000, vec![100]);

        // TRUE literal returns 1.0
        let true_lit: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Literal::new(
            datafusion::common::ScalarValue::Boolean(Some(true)),
        ));
        let sel = estimate_predicate_selectivity(&true_lit, &input);
        assert!((sel - 1.0).abs() < f64::EPSILON);

        // FALSE literal returns 0.0
        let false_lit: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Literal::new(
            datafusion::common::ScalarValue::Boolean(Some(false)),
        ));
        let sel = estimate_predicate_selectivity(&false_lit, &input);
        assert!((sel - 0.0).abs() < f64::EPSILON);
    }
}
