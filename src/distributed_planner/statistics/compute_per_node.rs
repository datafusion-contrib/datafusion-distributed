use crate::BroadcastExec;
use crate::execution_plans::ChildrenIsolatorUnionExec;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::Statistics;
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

#[derive(Clone)]
pub enum Complexity {
    /// Constant complexity
    Constant,
    /// Linear with a specific column from a specific child.
    Linear(LinearComplexity),
    /// NLogM
    Log(Box<Complexity>, Box<Complexity>),
    /// N+M
    Plus(Box<Complexity>, Box<Complexity>),
    /// N*M
    Multiply(Box<Complexity>, Box<Complexity>),
}

#[derive(Clone)]
pub enum LinearComplexity {
    /// Depends on linearly with the input column with the provided index
    Column(usize),
    /// Depends on linearly with the all the input columns
    AllColumns,
    /// Depends on linearly with the input column with the provided index from the left child
    ColumnFromLeft(usize),
    /// Depends on linearly with the all the input columns from the left child
    AllColumnsFromLeft,
    /// Depends on linearly with the input column with the provided index from the right child
    ColumnFromRight(usize),
    /// Depends on linearly with the all the input columns from the right child
    AllColumnsFromRight,
    /// Depends on linearly with the output column with the provided index
    OutputColumn(usize),
    /// Depends on linearly with the all the output columns
    AllOutputColumns,
}

impl Complexity {
    fn log(self, other: Self) -> Self {
        Self::Log(Box::new(self), Box::new(other))
    }

    fn plus(self, other: Self) -> Self {
        Self::Plus(Box::new(self), Box::new(other))
    }

    fn multiply(self, other: Self) -> Self {
        Self::Multiply(Box::new(self), Box::new(other))
    }

    /// Computes the total bytes processed given per-child row counts.
    pub(crate) fn cost(&self, output_stat: &Statistics, input_stats: &[&Statistics]) -> usize {
        // TODO: do not index directly, that can cause panics.
        match self {
            Self::Constant => 1,
            Self::Linear(linear) => match linear {
                LinearComplexity::Column(i) => {
                    let input_stats = input_stats[0];
                    let stats = &input_stats.column_statistics[*i];
                    *stats.byte_size.get_value().unwrap_or(&0)
                }
                LinearComplexity::AllColumns => {
                    let input_stats = input_stats[0];
                    *input_stats.total_byte_size.get_value().unwrap_or(&0)
                }
                LinearComplexity::ColumnFromLeft(i) => {
                    let input_stats = input_stats[0];
                    let stats = &input_stats.column_statistics[*i];

                    *stats.byte_size.get_value().unwrap_or(&0)
                }
                LinearComplexity::AllColumnsFromLeft => {
                    let input_stats = input_stats[0];
                    *input_stats.total_byte_size.get_value().unwrap_or(&0)
                }
                LinearComplexity::ColumnFromRight(i) => {
                    let input_stats = input_stats[1];
                    let stats = &input_stats.column_statistics[*i];
                    *stats.byte_size.get_value().unwrap_or(&0)
                }
                LinearComplexity::AllColumnsFromRight => {
                    let input_stats = input_stats[1];
                    *input_stats.total_byte_size.get_value().unwrap_or(&0)
                }
                LinearComplexity::OutputColumn(i) => {
                    let stats = &output_stat.column_statistics[*i];
                    *stats.byte_size.get_value().unwrap_or(&0)
                }
                LinearComplexity::AllOutputColumns => {
                    *output_stat.total_byte_size.get_value().unwrap_or(&0)
                }
            },
            Self::Log(n, m) => {
                n.cost(output_stat, input_stats) * m.cost(output_stat, input_stats).ilog2() as usize
            }
            Self::Plus(n, m) => n.cost(output_stat, input_stats) + m.cost(output_stat, input_stats),
            Self::Multiply(n, m) => {
                n.cost(output_stat, input_stats) * m.cost(output_stat, input_stats)
            }
        }
    }
}

impl Debug for Complexity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant => write!(f, "1"),
            Self::Linear(linear) => match linear {
                LinearComplexity::Column(i) => write!(f, "Col{i}"),
                LinearComplexity::AllColumns => write!(f, "Cols"),
                LinearComplexity::ColumnFromLeft(i) => write!(f, "left_Col{i}"),
                LinearComplexity::AllColumnsFromLeft => write!(f, "left_Cols"),
                LinearComplexity::ColumnFromRight(i) => write!(f, "right_Col{i}"),
                LinearComplexity::AllColumnsFromRight => write!(f, "right_Cols"),
                LinearComplexity::OutputColumn(i) => write!(f, "out_Col{i}"),
                LinearComplexity::AllOutputColumns => write!(f, "out_Cols"),
            },
            Self::Log(n, m) => write!(f, "{n:?}*Log({m:?})"),
            Self::Plus(n, m) => write!(f, "({n:?}+{m:?})"),
            Self::Multiply(n, m) => write!(f, "({n:?}*{m:?})"),
        }
    }
}

/// Calculates what's the cost, expressed as a number, per input row for each input children.
///
/// The Vec return has equal size to `node.children()`, and determines how many each input needs
/// to be processed
pub(crate) fn calculate_compute_complexity(node: &Arc<dyn ExecutionPlan>) -> Complexity {
    let any = node.as_any();

    // NestedLoopJoinExec: O(n*m) - evaluates join condition for each pair of rows
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/nested_loop_join.rs
    if let Some(node) = any.downcast_ref::<NestedLoopJoinExec>() {
        // Assume we need to do read all input rows one by one.
        let n = Complexity::Linear(LinearComplexity::AllColumnsFromLeft);
        let m = Complexity::Linear(LinearComplexity::AllColumnsFromRight);
        let c = n.multiply(m);
        if let Some(_filter) = node.filter() {
            // TODO: how can we take into account he filter here? we don't have column stats for
            //  that filter because it's an intermediate weird thing.
        }
        return c;
    }

    // CrossJoinExec: O(n*m) - produces Cartesian product of all row pairs
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/cross_join.rs
    if let Some(_node) = any.downcast_ref::<CrossJoinExec>() {
        // Assume we need to do read all input rows one by one.
        let n = Complexity::Linear(LinearComplexity::AllColumnsFromLeft);
        let m = Complexity::Linear(LinearComplexity::AllColumnsFromRight);
        return n.multiply(m);
    }

    // SortExec: O(n log n) - uses lexsort_to_indices, may spill to disk
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/sorts/sort.rs
    if let Some(node) = any.downcast_ref::<SortExec>() {
        // All the rows will need to be copied one by one.
        let mut n = Complexity::Linear(LinearComplexity::AllColumns);
        // All the sort expressions need to be evaluated on every row.
        for expr in node.expr() {
            n = n.plus(expression_complexity(&expr.expr))
        }
        return n.clone().log(n);
    }

    // HashJoinExec: hash table build (O(n)) + probe (O(m))
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/hash_join/exec.rs
    if let Some(join) = any.downcast_ref::<HashJoinExec>() {
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
        // TODO: filter cost (intermediate schema, hard to model with current LinearComplexity)
        return c;
    }

    // SortMergeJoinExec: merge of sorted streams with comparisons
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/sort_merge_join/exec.rs
    // Unlike hash join, sort-merge doesn't buffer all data or build hash tables. It streams
    // through both sorted inputs with O(max_group_size) memory, using partial_cmp comparisons
    // (no hashing). Per-row cost is just key comparisons + optional filter evaluation.
    if let Some(node) = any.downcast_ref::<SortMergeJoinExec>() {
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
        // TODO: filter cost
        return c.unwrap_or(Complexity::Constant);
    }

    // SymmetricHashJoinExec: streaming join with hash tables on both sides
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/joins/symmetric_hash_join.rs
    // More expensive than HashJoinExec: both sides maintain hash tables, concat_batches
    // runs on every incoming batch (not once at end), plus pruning interval computation
    // and HashSet tracking for visited rows.
    if let Some(node) = any.downcast_ref::<SymmetricHashJoinExec>() {
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
        // TODO: filter cost
        return c;
    }

    // Aggregation: hash group-by keys + accumulate aggregate inputs
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/aggregates/mod.rs
    if let Some(agg) = any.downcast_ref::<AggregateExec>() {
        // Base: read all input columns for accumulation
        let mut c = Complexity::Linear(LinearComplexity::AllColumns);
        // Additional: evaluate and hash group-by key expressions
        for (expr, _) in agg.group_expr().expr() {
            c = c.plus(key_expression_complexity(expr));
        }
        // Per-aggregate filter expressions (e.g. COUNT(*) FILTER (WHERE ...))
        for filter in agg.filter_expr().iter().flatten() {
            c = c.plus(expression_complexity(filter));
        }
        return c;
    }

    // Window functions: buffer partitions, compute aggregates over windows
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/windows/window_agg_exec.rs
    if let Some(node) = any.downcast_ref::<WindowAggExec>() {
        // Read all input data + evaluate/hash partition key expressions
        let mut c = Complexity::Linear(LinearComplexity::AllColumns);
        for expr in node.partition_keys() {
            c = c.plus(key_expression_complexity(&expr));
        }
        return c;
    }

    if let Some(node) = any.downcast_ref::<BoundedWindowAggExec>() {
        let mut c = Complexity::Linear(LinearComplexity::AllColumns);
        for expr in node.partition_keys() {
            c = c.plus(key_expression_complexity(&expr));
        }
        return c;
    }

    // SortPreservingMergeExec: merges pre-sorted streams with comparisons
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/sorts/sort_preserving_merge.rs
    // K-way merge: O(N log K) comparisons on sort key expressions
    if let Some(node) = any.downcast_ref::<SortPreservingMergeExec>() {
        // need to copy all rows...
        let mut n = Complexity::Linear(LinearComplexity::AllColumns);
        // and evaluate the sort expressions on all of them
        for expr in node.expr() {
            n = n.plus(expression_complexity(&expr.expr))
        }
        return n;
    }

    // FilterExec: evaluates predicate expression per row
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/filter.rs
    // Cost depends on predicate complexity - LIKE/Regex operations are expensive
    if let Some(node) = any.downcast_ref::<FilterExec>() {
        // It needs to perform a copy operation just to the output rows...
        let n = Complexity::Linear(LinearComplexity::AllOutputColumns);
        // ...and predicate evaluation on all input rows.
        return n.plus(expression_complexity(node.predicate()));
    }

    // ProjectionExec: cost depends on whether it's simple columns or expressions
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/projection.rs
    if let Some(node) = any.downcast_ref::<ProjectionExec>() {
        let mut n: Option<Complexity> = None;
        for expr in node.expr() {
            n = if let Some(n) = n {
                Some(n.plus(expression_complexity(&expr.expr)))
            } else {
                Some(expression_complexity(&expr.expr))
            };
        }
        return n.unwrap_or(Complexity::Constant);
    }

    // RepartitionExec with Hash: computes hash per row + take_arrays
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/repartition/mod.rs
    if let Some(node) = any.downcast_ref::<RepartitionExec>() {
        // It needs to copy all the data for chunking it to the different output partitions...
        let mut n = Complexity::Linear(LinearComplexity::AllColumns);
        // And it might need to compute a hash per row based on the provided expressions.
        match node.partitioning() {
            Partitioning::Hash(expressions, _) => {
                for expr in expressions {
                    n = n.plus(expression_complexity(expr))
                }
            }
            Partitioning::RoundRobinBatch(_) => {}
            Partitioning::UnknownPartitioning(_) => {}
        };
        return n;
    }

    // DataSourceExec: Produces data, so assume that it's an O(N) operation over all the columns.
    if any.is::<DataSourceExec>() {
        return Complexity::Linear(LinearComplexity::AllOutputColumns);
    }

    // CoalesceBatchesExec: concatenates batches via concat_batches (memory copy). As it copies
    // data, it does an O(N) operation over all the input bytes.
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/coalesce_batches.rs
    if any.is::<CoalesceBatchesExec>() {
        return Complexity::Linear(LinearComplexity::AllColumns);
    }

    // Limit: just counts rows and stops early.
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/limit.rs
    if any.is::<GlobalLimitExec>() || any.is::<LocalLimitExec>() {
        return Complexity::Constant;
    }

    // CoalescePartitionsExec: receives batches from partitions, just passes through the record
    // batches in a zero copy manner.
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/coalesce_partitions.rs
    if any.is::<CoalescePartitionsExec>() {
        return Complexity::Constant;
    }

    // BroadcastExec: This node does not do any computation, does not even read the data.
    if any.is::<BroadcastExec>() {
        return Complexity::Constant;
    }

    // UnionExec: combines multiple input streams, no processing
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/union.rs
    if any.is::<UnionExec>() || any.is::<ChildrenIsolatorUnionExec>() {
        return Complexity::Constant;
    }

    // InterleaveExec: round-robin merging of inputs, no processing
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/union.rs
    if any.is::<InterleaveExec>() {
        return Complexity::Constant;
    }

    // EmptyExec: produces no data
    // https://github.com/apache/datafusion/blob/branch-52/datafusion/physical-plan/src/empty.rs
    if any.is::<EmptyExec>() {
        return Complexity::Constant;
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
        .unwrap_or(Complexity::Constant)
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
    result.unwrap_or(Complexity::Constant)
}

/// Like `expression_complexity`, but includes the cost of reading leaf columns.
/// Use for operations that hash/compare expression results (aggregation group-by keys,
/// window partition keys) where even simple column references have processing cost.
fn key_expression_complexity(expression: &Arc<dyn PhysicalExpr>) -> Complexity {
    let bpr = _expression_complexity(expression);
    let mut result: Option<Complexity> = None;
    for col_idx in &bpr.cols_read {
        result = Some(match result {
            Some(r) => r.plus(Complexity::Linear(LinearComplexity::Column(*col_idx))),
            None => Complexity::Linear(LinearComplexity::Column(*col_idx)),
        });
    }
    result.unwrap_or(Complexity::Constant)
}

fn _expression_complexity(expression: &Arc<dyn PhysicalExpr>) -> BytesPerRow {
    let any = expression.as_any();

    if let Some(col) = any.downcast_ref::<Column>() {
        BytesPerRow {
            processed: None,
            cols_read: vec![col.index()],
        }
    } else if any.is::<Literal>() {
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
        // TODO: I'm not sure if this is right
        for child in expression.children() {
            let c = _expression_complexity(child);
            bytes_per_row.cols_read.extend(&c.cols_read);
            // Linear processing per column:
            for col_read in &c.cols_read {
                bytes_per_row.processed = if let Some(processed) = bytes_per_row.processed {
                    Some(processed.plus(Complexity::Linear(LinearComplexity::Column(*col_read))))
                } else {
                    Some(Complexity::Linear(LinearComplexity::Column(*col_read)))
                }
            }
        }
        bytes_per_row
    }
}
