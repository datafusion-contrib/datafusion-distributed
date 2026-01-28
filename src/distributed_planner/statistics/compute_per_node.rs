use crate::BroadcastExec;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::filter::FilterExec;
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

/// Represents the relative CPU/compute cost of an execution plan node.
///
/// This is a granular classification focused on actual computational work,
/// not I/O or memory bandwidth. The levels are ordered from least to most
/// compute-intensive.
///
/// Note: This intentionally separates compute cost from I/O cost. For example,
/// TableScan is Zero compute cost because it's I/O-bound, not CPU-bound.
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ComputeCost {
    /// No compute - pure passthrough or I/O-bound operators.
    /// Examples: Union, Empty, Interleave, DataSourceExec (I/O-bound).
    Zero,

    /// Extra extra small - minimal bookkeeping only.
    /// Examples: Limit (just counting rows), CoalescePartitions (receiving batches).
    XXS,

    /// Extra small - simple memory operations, no per-row computation.
    /// Examples: CoalesceBatches (concat_batches), RoundRobin repartition.
    XS,

    /// Small - light per-row computation.
    /// Examples: Simple column projection, hash computation for repartition.
    S,

    /// Medium - moderate per-row computation.
    /// Examples: Filter with simple predicate, projection with expressions.
    M,

    /// Large - significant per-row computation or light accumulation.
    /// Examples: SortPreservingMerge (comparisons), simple aggregation (COUNT).
    L,

    /// Extra large - heavy accumulating operators.
    /// Examples: Sort (O(n log n)), HashJoin, complex aggregation.
    XL,

    /// Extra extra large - very heavy computation.
    /// Examples: NestedLoopJoin (O(n*m)), CrossJoin, aggregation with many
    /// group-by columns and complex aggregate functions.
    XXL,
}

impl ComputeCost {
    pub(crate) fn factor(&self) -> f64 {
        match self {
            ComputeCost::Zero => 0.0,
            ComputeCost::XXS => 0.3,
            ComputeCost::XS => 0.5,
            ComputeCost::S => 0.7,
            ComputeCost::M => 1.0,
            ComputeCost::L => 1.3,
            ComputeCost::XL => 1.6,
            ComputeCost::XXL => 2.0,
        }
    }
}

/// Returns the estimated [`ComputeCost`] for the given execution plan node.
///
/// Cost estimation is based on the actual computational work performed by each
/// DataFusion operator, intentionally separating compute cost from I/O cost.
///
/// Reference: DataFusion physical-plan implementations:
/// <https://github.com/apache/datafusion/tree/branch-52/datafusion/physical-plan/src>
pub(crate) fn calculate_compute_cost(node: &Arc<dyn ExecutionPlan>) -> ComputeCost {
    let any = node.as_any();

    // === XXL: Very heavy computation (O(n*m) or worse) ===

    // NestedLoopJoinExec: O(n*m) - evaluates join condition for each pair of rows
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/nested_loop_join.rs
    if any.is::<NestedLoopJoinExec>() {
        return ComputeCost::XXL;
    }

    // CrossJoinExec: O(n*m) - produces Cartesian product of all row pairs
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/cross_join.rs
    if any.is::<CrossJoinExec>() {
        return ComputeCost::XXL;
    }

    // === XL: Heavy accumulating operators (O(n log n) or hash-based) ===

    // SortExec: O(n log n) - uses lexsort_to_indices, may spill to disk
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/sorts/sort.rs
    if any.is::<SortExec>() {
        return ComputeCost::XL;
    }

    // HashJoinExec: hash table build (O(n)) + probe (O(m))
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/hash_join/exec.rs
    // Cost varies by number of join keys and whether there's a filter
    if let Some(join) = any.downcast_ref::<HashJoinExec>() {
        let num_keys = join.on().len();
        let has_filter = join.filter().is_some();
        return match (num_keys, has_filter) {
            (_, true) => ComputeCost::XXL, // Filter adds per-match evaluation
            (n, _) if n > 3 => ComputeCost::XXL, // Many keys = expensive hashing
            _ => ComputeCost::XL,
        };
    }

    // SortMergeJoinExec: merge of sorted streams with comparisons
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/sort_merge_join/exec.rs
    if any.is::<SortMergeJoinExec>() {
        return ComputeCost::XL;
    }

    // SymmetricHashJoinExec: streaming join with hash tables on both sides
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/symmetric_hash_join.rs
    if any.is::<SymmetricHashJoinExec>() {
        return ComputeCost::XL;
    }

    // Aggregation: cost varies by grouping complexity and aggregate functions
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/aggregates/mod.rs
    if let Some(agg) = any.downcast_ref::<AggregateExec>() {
        return compute_aggregation_cost(agg);
    }

    // Window functions: buffer partitions, compute aggregates over windows
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/windows/window_agg_exec.rs
    if any.is::<WindowAggExec>() || any.is::<BoundedWindowAggExec>() {
        return ComputeCost::XL;
    }

    // === L: Significant per-row computation ===

    // SortPreservingMergeExec: merges pre-sorted streams with comparisons
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/sorts/sort_preserving_merge.rs
    // Lighter than full sort but still does comparisons for each output row
    if any.is::<SortPreservingMergeExec>() {
        return ComputeCost::L;
    }

    // === M: Moderate per-row computation ===

    // FilterExec: evaluates predicate expression per row
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/filter.rs
    if any.is::<FilterExec>() {
        return ComputeCost::M;
    }

    // ProjectionExec: cost depends on whether it's simple columns or expressions
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/projection.rs
    if let Some(proj) = any.downcast_ref::<ProjectionExec>() {
        return compute_projection_cost(proj);
    }

    // === S: Light per-row computation ===

    // RepartitionExec with Hash: computes hash per row + take_arrays
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/repartition/mod.rs
    if let Some(repartition) = any.downcast_ref::<RepartitionExec>() {
        return match repartition.partitioning() {
            // Hash partitioning: create_hashes (line 575) + take_arrays (line 606)
            Partitioning::Hash(exprs, _) => {
                if exprs.len() > 2 {
                    ComputeCost::M // Many hash columns = more compute
                } else {
                    ComputeCost::S
                }
            }
            // RoundRobin: just distributes whole batches, no per-row work
            Partitioning::RoundRobinBatch(_) => ComputeCost::XS,
            // UnknownPartitioning: conservative estimate
            Partitioning::UnknownPartitioning(_) => ComputeCost::S,
        };
    }

    // === XS: Simple memory operations ===

    // CoalesceBatchesExec: concatenates batches via concat_batches (memory copy)
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/coalesce_batches.rs
    if any.is::<CoalesceBatchesExec>() {
        return ComputeCost::XS;
    }

    // === XXS: Minimal bookkeeping ===

    // Limit: just counts rows and stops early
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/limit.rs
    if any.is::<GlobalLimitExec>() || any.is::<LocalLimitExec>() {
        return ComputeCost::XXS;
    }

    // CoalescePartitionsExec: receives batches from partitions, no transformation
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/coalesce_partitions.rs
    if any.is::<CoalescePartitionsExec>() {
        return ComputeCost::XXS;
    }

    // === Zero: No compute (passthrough or I/O-bound) ===

    // BroadcastExec: This node does not do any computation, does not even read the data.
    if any.is::<BroadcastExec>() {
        return ComputeCost::Zero;
    }

    // DataSourceExec: I/O-bound, CPU just receives decoded data
    // The actual decoding is done by the storage layer
    if any.is::<DataSourceExec>() {
        return ComputeCost::Zero;
    }

    // UnionExec: combines multiple input streams, no processing
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/union.rs
    if any.is::<UnionExec>() {
        return ComputeCost::Zero;
    }

    // InterleaveExec: round-robin merging of inputs, no processing
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/union.rs
    if any.is::<InterleaveExec>() {
        return ComputeCost::Zero;
    }

    // EmptyExec: produces no data
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/empty.rs
    if any.is::<EmptyExec>() {
        return ComputeCost::Zero;
    }

    // For unknown node types, default to M as a conservative estimate.
    ComputeCost::M
}

/// Computes the cost for an aggregation based on its complexity.
///
/// Factors considered:
/// - Number of group-by columns (more = more hashing work)
/// - Number of aggregate expressions
/// - Whether it's a simple aggregation (e.g., COUNT(*)) vs complex
fn compute_aggregation_cost(agg: &AggregateExec) -> ComputeCost {
    let group_by = agg.group_expr();
    let num_group_cols = group_by.expr().len();
    let num_aggr_exprs = agg.aggr_expr().len();
    let has_grouping_set = group_by.has_grouping_set();

    // No grouping (e.g., SELECT COUNT(*) FROM t) - just accumulates values
    if num_group_cols == 0 && !has_grouping_set {
        return if num_aggr_exprs <= 2 {
            ComputeCost::L // Simple: COUNT(*), SUM(x)
        } else {
            ComputeCost::XL // Multiple aggregates
        };
    }

    // GROUPING SETS/CUBE/ROLLUP - very expensive, multiple groupings
    if has_grouping_set {
        return ComputeCost::XXL;
    }

    // Regular GROUP BY - cost based on number of group columns and aggregates
    let complexity = num_group_cols + num_aggr_exprs;
    match complexity {
        0..=2 => ComputeCost::L,  // Simple: GROUP BY a with COUNT(*)
        3..=5 => ComputeCost::XL, // Moderate: GROUP BY a, b with SUM, AVG
        _ => ComputeCost::XXL,    // Complex: many columns/aggregates
    }
}

/// Computes the cost for a projection based on expression complexity.
///
/// Simple column references are nearly free (just pointer manipulation),
/// while computed expressions require evaluation per row.
fn compute_projection_cost(proj: &ProjectionExec) -> ComputeCost {
    let exprs = proj.expr();

    // Check if all expressions are simple column references
    let all_columns = exprs.iter().all(|e| e.expr.as_any().is::<Column>());

    if all_columns {
        // Simple column projection - just reordering/selecting columns
        return ComputeCost::XS;
    }

    // Has computed expressions - cost depends on number of non-column exprs
    let num_computed = exprs
        .iter()
        .filter(|e| !e.expr.as_any().is::<Column>())
        .count();

    match num_computed {
        0 => ComputeCost::XS,    // All columns (shouldn't reach here)
        1..=2 => ComputeCost::S, // Few computed expressions
        3..=5 => ComputeCost::M, // Several computed expressions
        _ => ComputeCost::L,     // Many computed expressions
    }
}
