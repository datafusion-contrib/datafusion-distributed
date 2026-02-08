use super::calculate_bytes_per_row::calculate_bytes_returned_per_cell;
use crate::BroadcastExec;
use crate::distributed_planner::statistics::calculate_bytes_returned_per_row;
use crate::execution_plans::ChildrenIsolatorUnionExec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{Column, Literal};
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
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// The runtime big O complexity where N and M are input rows.
pub enum ComputeComplexity {
    Constant,
    N(usize),
    NLogN(usize),
    NPlusM(usize, usize),
    NM(usize, usize),
}

impl ComputeComplexity {
    /// Computes the total bytes processed given per-child row counts.
    pub(crate) fn cost(&self, rows_per_child: &[usize]) -> usize {
        match self {
            Self::Constant => 0,
            Self::N(n) => {
                let total_rows: usize = rows_per_child.iter().sum();
                n * total_rows
            }
            Self::NLogN(n) => {
                let total_rows: usize = rows_per_child.iter().sum();
                if total_rows <= 1 {
                    return 0;
                }
                n * total_rows * (total_rows.ilog2() as usize)
            }
            Self::NPlusM(n, m) => {
                let left = rows_per_child.first().copied().unwrap_or(0);
                let right = rows_per_child.get(1).copied().unwrap_or(0);
                n * left + m * right
            }
            Self::NM(n, m) => {
                let left = rows_per_child.first().copied().unwrap_or(0);
                let right = rows_per_child.get(1).copied().unwrap_or(0);
                n * m * left * right
            }
        }
    }
}

impl Debug for ComputeComplexity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant => write!(f, "O(1)"),
            Self::N(n) => write!(f, "O({n}*N)"),
            Self::NLogN(n) => write!(f, "O({n}*N*LogN)"),
            Self::NPlusM(n, m) => write!(f, "O({n}*N+{m}*M)"),
            Self::NM(n, m) => write!(f, "O({}*N*M)", n * m),
        }
    }
}

/// Calculates what's the cost, expressed as a number, per input row for each input children.
///
/// The Vec return has equal size to `node.children()`, and determines how many each input needs
/// to be processed
pub(crate) fn calculate_compute_complexity(node: &Arc<dyn ExecutionPlan>) -> ComputeComplexity {
    let any = node.as_any();

    // NestedLoopJoinExec: O(n*m) - evaluates join condition for each pair of rows
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/nested_loop_join.rs
    if let Some(node) = any.downcast_ref::<NestedLoopJoinExec>() {
        // Assume we need to do read all input rows one by one.
        let mut n = calculate_bytes_returned_per_row(&node.left().schema());
        let mut m = calculate_bytes_returned_per_row(&node.right().schema());
        if let Some(filter) = node.filter() {
            let filter = bytes_processed_per_row(filter.expression(), filter.schema());
            n += filter.processed;
            m += filter.processed;
        }
        return ComputeComplexity::NM(n, m);
    }

    // CrossJoinExec: O(n*m) - produces Cartesian product of all row pairs
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/cross_join.rs
    if let Some(node) = any.downcast_ref::<CrossJoinExec>() {
        // Assume we need to do read all input rows one by one.
        let n = calculate_bytes_returned_per_row(&node.left().schema());
        let m = calculate_bytes_returned_per_row(&node.right().schema());
        return ComputeComplexity::NM(n, m);
    }

    // SortExec: O(n log n) - uses lexsort_to_indices, may spill to disk
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/sorts/sort.rs
    if let Some(node) = any.downcast_ref::<SortExec>() {
        // All the rows will need to be copied one by one.
        let mut n = calculate_bytes_returned_per_row(&node.input().schema());
        // All the sort expressions need to be evaluated on every row.
        for expr in node.expr() {
            n += bytes_processed_per_row(&expr.expr, &node.input().schema()).processed;
        }
        return ComputeComplexity::NLogN(n);
    }

    // HashJoinExec: hash table build (O(n)) + probe (O(m))
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/hash_join/exec.rs
    if let Some(join) = any.downcast_ref::<HashJoinExec>() {
        let left_schema = join.left().schema();
        let right_schema = join.right().schema();
        // Build side: store all left rows in hash table + hash left join keys
        let mut n = calculate_bytes_returned_per_row(&left_schema);
        for (left_key, _) in join.on() {
            n += bytes_processed_per_row(left_key, &left_schema).processed;
        }
        // Probe side: hash right join keys + look up matches
        let mut m = calculate_bytes_returned_per_row(&right_schema);
        for (_, right_key) in join.on() {
            m += bytes_processed_per_row(right_key, &right_schema).processed;
        }
        // Filter adds per-match evaluation cost to both sides
        if let Some(filter) = join.filter() {
            let fc = bytes_processed_per_row(filter.expression(), filter.schema());
            n += fc.processed;
            m += fc.processed;
        }
        return ComputeComplexity::NPlusM(n, m);
    }

    // SortMergeJoinExec: merge of sorted streams with comparisons
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/sort_merge_join/exec.rs
    if let Some(node) = any.downcast_ref::<SortMergeJoinExec>() {
        let left_schema = node.left().schema();
        let right_schema = node.right().schema();
        // Left side: read all rows + compare join keys during merge
        let mut n = calculate_bytes_returned_per_row(&left_schema);
        for (left_key, _) in node.on() {
            n += bytes_processed_per_row(left_key, &left_schema).processed;
        }
        // Right side: read all rows + compare join keys during merge
        let mut m = calculate_bytes_returned_per_row(&right_schema);
        for (_, right_key) in node.on() {
            m += bytes_processed_per_row(right_key, &right_schema).processed;
        }
        if let Some(filter) = node.filter().as_ref() {
            let fc = bytes_processed_per_row(filter.expression(), filter.schema());
            n += fc.processed;
            m += fc.processed;
        }
        return ComputeComplexity::NPlusM(n, m);
    }

    // SymmetricHashJoinExec: streaming join with hash tables on both sides
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/symmetric_hash_join.rs
    if let Some(node) = any.downcast_ref::<SymmetricHashJoinExec>() {
        let left_schema = node.left().schema();
        let right_schema = node.right().schema();
        // Both sides build hash tables: store rows + hash join keys
        let mut n = calculate_bytes_returned_per_row(&left_schema);
        for (left_key, _) in node.on() {
            n += bytes_processed_per_row(left_key, &left_schema).processed;
        }
        let mut m = calculate_bytes_returned_per_row(&right_schema);
        for (_, right_key) in node.on() {
            m += bytes_processed_per_row(right_key, &right_schema).processed;
        }
        if let Some(filter) = node.filter() {
            let fc = bytes_processed_per_row(filter.expression(), filter.schema());
            n += fc.processed;
            m += fc.processed;
        }
        return ComputeComplexity::NPlusM(n, m);
    }

    // Aggregation: hash group-by keys + accumulate aggregate inputs
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/aggregates/mod.rs
    if let Some(agg) = any.downcast_ref::<AggregateExec>() {
        let input_schema = agg.input_schema();
        // Base: read all input columns for accumulation
        let mut n = calculate_bytes_returned_per_row(&input_schema);
        // Additional: evaluate and hash group-by key expressions
        for (expr, _) in agg.group_expr().expr() {
            n += bytes_processed_per_row(expr, &input_schema).processed;
        }
        // Per-aggregate filter expressions (e.g. COUNT(*) FILTER (WHERE ...))
        for filter in agg.filter_expr().iter().flatten() {
            n += bytes_processed_per_row(filter, &input_schema).processed;
        }
        return ComputeComplexity::N(n);
    }

    // Window functions: buffer partitions, compute aggregates over windows
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/windows/window_agg_exec.rs
    if let Some(node) = any.downcast_ref::<WindowAggExec>() {
        let input_schema = node.input().schema();
        // Read all input data + evaluate partition key expressions
        let mut n = calculate_bytes_returned_per_row(&input_schema);
        for expr in node.partition_keys() {
            n += bytes_processed_per_row(&expr, &input_schema).processed;
        }
        return ComputeComplexity::N(n);
    }

    if let Some(node) = any.downcast_ref::<BoundedWindowAggExec>() {
        let input_schema = node.input().schema();
        let mut n = calculate_bytes_returned_per_row(&input_schema);
        for expr in node.partition_keys() {
            n += bytes_processed_per_row(&expr, &input_schema).processed;
        }
        return ComputeComplexity::N(n);
    }

    // SortPreservingMergeExec: merges pre-sorted streams with comparisons
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/sorts/sort_preserving_merge.rs
    // K-way merge: O(N log K) comparisons on sort key expressions
    if let Some(node) = any.downcast_ref::<SortPreservingMergeExec>() {
        // need to copy all rows...
        let mut n = calculate_bytes_returned_per_row(&node.input().schema());
        // and evaluate the sort expressions on all of them
        for expr in node.expr() {
            n += bytes_processed_per_row(&expr.expr, &node.input().schema()).processed;
        }
        return ComputeComplexity::N(n);
    }

    // FilterExec: evaluates predicate expression per row
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/filter.rs
    // Cost depends on predicate complexity - LIKE/Regex operations are expensive
    if let Some(node) = any.downcast_ref::<FilterExec>() {
        // It needs to perform a copy operation just to the output rows...
        let mut n = calculate_bytes_returned_per_row(&node.input().schema());
        // ...and predicate evaluation on all input rows.
        n += bytes_processed_per_row(node.predicate(), &node.input().schema()).processed;
        return ComputeComplexity::N(n);
    }

    // ProjectionExec: cost depends on whether it's simple columns or expressions
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/projection.rs
    if let Some(node) = any.downcast_ref::<ProjectionExec>() {
        let mut n = 0;
        for expr in node.expr() {
            n += bytes_processed_per_row(&expr.expr, &node.input().schema()).processed;
        }
        return ComputeComplexity::N(n);
    }

    // RepartitionExec with Hash: computes hash per row + take_arrays
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/repartition/mod.rs
    if let Some(node) = any.downcast_ref::<RepartitionExec>() {
        // It needs to copy all the data for chunking it to the different output partitions...
        let mut n = calculate_bytes_returned_per_row(&node.schema());
        // And it might need to compute a hash per row based on the provided expressions.
        match node.partitioning() {
            // Hash partitioning: create_hashes (line 575) + take_arrays (line 606)
            Partitioning::Hash(exprs, _) => {
                for expr in exprs {
                    n += bytes_processed_per_row(expr, &node.input().schema()).processed
                }
            }
            // RoundRobin: just distributes whole batches, no per-row work
            Partitioning::RoundRobinBatch(_) => {}
            // UnknownPartitioning: conservative estimate
            Partitioning::UnknownPartitioning(_) => {}
        };
        return ComputeComplexity::N(n);
    }

    // DataSourceExec: Produces data, so assume that it's an O(N) operation where all the bytes
    // need to get processed.
    if any.is::<DataSourceExec>() {
        return ComputeComplexity::N(calculate_bytes_returned_per_row(&node.schema()));
    }

    // CoalesceBatchesExec: concatenates batches via concat_batches (memory copy). As it copies
    // data, it does an O(N) operation over all the input bytes.
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/coalesce_batches.rs
    if any.is::<CoalesceBatchesExec>() {
        return ComputeComplexity::N(calculate_bytes_returned_per_row(&node.schema()));
    }

    // Limit: just counts rows and stops early.
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/limit.rs
    if any.is::<GlobalLimitExec>() || any.is::<LocalLimitExec>() {
        return ComputeComplexity::Constant;
    }

    // CoalescePartitionsExec: receives batches from partitions, just passes through the record
    // batches in a zero copy manner.
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/coalesce_partitions.rs
    if any.is::<CoalescePartitionsExec>() {
        return ComputeComplexity::Constant;
    }

    // BroadcastExec: This node does not do any computation, does not even read the data.
    if any.is::<BroadcastExec>() {
        return ComputeComplexity::Constant;
    }

    // UnionExec: combines multiple input streams, no processing
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/union.rs
    if any.is::<UnionExec>() || any.is::<ChildrenIsolatorUnionExec>() {
        return ComputeComplexity::Constant;
    }

    // InterleaveExec: round-robin merging of inputs, no processing
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/union.rs
    if any.is::<InterleaveExec>() {
        return ComputeComplexity::Constant;
    }

    // EmptyExec: produces no data
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/empty.rs
    if any.is::<EmptyExec>() {
        return ComputeComplexity::Constant;
    }

    // For unknown node types, assume we have to do an O(N) operation over all the rows.
    ComputeComplexity::N(calculate_bytes_returned_per_row(&node.schema()))
}

#[derive(Default)]
struct BytesPerRow {
    processed: usize,
    returned: usize,
}

fn bytes_processed_per_row(expression: &Arc<dyn PhysicalExpr>, schema: &SchemaRef) -> BytesPerRow {
    let any = expression.as_any();

    if let Some(col) = any.downcast_ref::<Column>() {
        if col.index() < schema.fields().len() {
            return BytesPerRow {
                processed: 0,
                returned: calculate_bytes_returned_per_cell(schema.field(col.index()).data_type()),
            };
        }
        BytesPerRow::default()
    } else if any.is::<Literal>() {
        BytesPerRow::default()
    } else {
        // Generic handler for all other expressions: CastExpr, TryCastExpr, CaseExpr,
        // InListExpr, IsNullExpr, IsNotNullExpr, NotExpr, NegativeExpr, LikeExpr,
        // ScalarFunctionExpr, AsyncFuncExpr, etc.
        let children: Vec<BytesPerRow> = expression
            .children()
            .iter()
            .map(|child| bytes_processed_per_row(child, schema))
            .collect();

        let processed: usize = children.iter().map(|c| c.processed + c.returned).sum();

        let returned = match expression.return_field(schema.as_ref()) {
            Ok(field) => calculate_bytes_returned_per_cell(field.data_type()),
            Err(_) => children.first().map_or(0, |c| c.returned),
        };

        BytesPerRow {
            processed,
            returned,
        }
    }
}
