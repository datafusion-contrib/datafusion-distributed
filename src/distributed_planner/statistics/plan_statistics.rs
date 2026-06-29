use crate::distributed_planner::statistics::default_bytes_for_datatype::default_bytes_for_datatype;
use datafusion::common::stats::Precision;
use datafusion::common::{Statistics, not_impl_err, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use delegate::delegate;
use itertools::Itertools;
use std::fmt::Formatter;
use std::sync::Arc;

/// Ratio applied to the total number of rows for calculating the fallback value for NDVs.
///
/// If NDVs are absent for a specific column, this ratio kicks in and is applied to the estimated
/// number of rows for calculating the final NDV value.
const FALLBACK_NDV_RATIO: f64 = 0.5;

/// Uses upstream DataFusion stats system with some small overrides.
pub(super) fn plan_statistics(
    node: &Arc<dyn ExecutionPlan>,
    children_stats: &[Arc<Statistics>],
) -> Result<Arc<Statistics>> {
    let mut stats = partition_statistics_with_children_override(node, None, children_stats)?;

    // If rows are absent, but the children declares rows, be conservative and assume that the node
    // is not going to reduce cardinality and that the row count stays the same.
    if matches!(stats.num_rows, Precision::Absent)
        && let Some(child_rows) = children_stats
            .iter()
            .flat_map(|v| v.num_rows.get_value())
            .sum1::<usize>()
    {
        stats.num_rows = Precision::Inexact(child_rows)
    }

    let schema = node.schema();

    for (i, col_stats) in &mut stats.column_statistics.iter_mut().enumerate() {
        let Some(rows) = stats.num_rows.get_value() else {
            break;
        };

        // If a column's NDV is absent, fall back to a fraction of the row count
        if matches!(col_stats.distinct_count, Precision::Absent) {
            let fallback_ndv = ((*rows as f64) * FALLBACK_NDV_RATIO) as usize;
            col_stats.distinct_count = Precision::Inexact(fallback_ndv);
        }

        // If the per-column byte size stats are not present, estimate the byte size based on the
        // data type and the row count.
        let Some(dt) = schema.fields.get(i).map(|v| v.data_type()) else {
            return plan_err!("Field with index {i} not present in schema: {schema:?}");
        };

        // If it turns out that we do not have `byte_size` stats, but we do have an estimated number
        // of rows, do a best-effort in trying to infer the byte size for each column.
        if matches!(col_stats.byte_size, Precision::Absent) {
            col_stats.byte_size =
                Precision::Inexact(default_bytes_for_datatype(dt).saturating_mul(*rows))
        }
    }

    // If bytes are absent, let's just infer them based on the schema and the
    // number of rows.
    if matches!(stats.total_byte_size, Precision::Absent) {
        let mut total_byte_size: usize = 0;
        for col_stats in &stats.column_statistics {
            total_byte_size =
                total_byte_size.saturating_add(*col_stats.byte_size.get_value().unwrap_or(&0));
        }
        stats.total_byte_size = Precision::Inexact(total_byte_size);
    }

    Ok(Arc::new(stats))
}

// FIXME: because of limitations the the statistics API on DataFusion, we need to resource to
//  this sketchy way of overriding child statistics, as we cannot just provide our own.
//  If we don't do this:
//   1. we cannot tell nodes to compute statistics based on the ones we provide.
//   2. we recompute statistics unnecessarily across the plan
//  This is tracked by https://github.com/apache/datafusion/issues/20184 upstream, and until
//  that one is solved, we need to resource to this wrapper.
fn partition_statistics_with_children_override(
    node: &Arc<dyn ExecutionPlan>,
    partition: Option<usize>,
    child_stats: &[Arc<Statistics>],
) -> Result<Statistics> {
    // DataFusion stats system is not very mature yet. This override layer brings in changes
    // that might not have already been released or informed overrides.
    let statistics_wrapped_children = child_stats
        .iter()
        .zip(node.children())
        .map(|(stats, child)| StatisticsWrapper {
            inner: Arc::clone(child),
            stats: Arc::clone(stats),
        })
        .map(|v| Arc::new(v) as _)
        .collect();

    let stats = Arc::clone(node)
        .with_new_children(statistics_wrapped_children)?
        .partition_statistics(partition)?;

    Ok(stats.as_ref().clone())
}

#[derive(Debug)]
struct StatisticsWrapper {
    stats: Arc<Statistics>,
    inner: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for StatisticsWrapper {
    delegate! {
        to self.inner {
            fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result;
        }
    }
}

impl ExecutionPlan for StatisticsWrapper {
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        if partition.is_some() {
            return plan_err!("StatisticsWrapper not prepared for partition-specific stats");
        }
        Ok(Arc::clone(&self.stats))
    }

    delegate! {
        to self.inner {
            fn name(&self) -> &str;
            fn properties(&self) -> &Arc<PlanProperties>;
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn repartitioned(&self, _target_partitions: usize, _config: &ConfigOptions) -> Result<Option<Arc<dyn ExecutionPlan>>>;
            fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream>;
            fn supports_limit_pushdown(&self) -> bool;
            fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>>;
            fn fetch(&self) -> Option<usize>;
            fn cardinality_effect(&self) -> CardinalityEffect;
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("with_new_children not implemented")
    }
}
