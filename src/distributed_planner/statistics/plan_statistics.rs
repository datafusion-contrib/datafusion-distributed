use crate::distributed_planner::statistics::default_bytes_for_datatype::default_bytes_for_datatype;
use datafusion::common::stats::Precision;
use datafusion::common::{Statistics, plan_err};
use datafusion::error::Result;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs};
use itertools::Itertools;
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
    let mut stats = node
        .statistics_from_inputs(children_stats, &StatisticsArgs::new())?
        .as_ref()
        .clone();

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
