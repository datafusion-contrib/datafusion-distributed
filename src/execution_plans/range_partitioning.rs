use crate::DistributedTaskContext;
use crate::common::TreeNodeExt;
use crate::execution_plans::{DistributedLeafExec, NetworkCoalesceExec, NetworkShuffleExec};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Result, plan_err};
use datafusion::physical_expr::{Partitioning, RangePartitioning};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::ops::Range;
use std::sync::Arc;

/// A contiguous slice of the original partitions, with optional empty slots before it.
pub(crate) struct RangeTask {
    pub partitioning: RangePartitioning,
    pub input: Range<usize>,
    pub leading_padding: usize,
}

/// Divide whole partitions between tasks, keeping the boundary between tasks as bounds.
/// All tasks have the same number of slots, including empty padding partitions.
pub(crate) fn split_range(range: &RangePartitioning, tasks: usize) -> Result<Vec<RangeTask>> {
    if tasks == 0 {
        return plan_err!("Cannot split range partitioning into zero tasks");
    }
    let partitions = range.partition_count();
    let width = partitions.div_ceil(tasks);
    (0..tasks)
        .map(|task| {
            let (start, len) = if tasks > partitions {
                // Empty tasks precede the final partition, so its unbounded end stays last.
                let start = task.min(partitions - 1);
                (
                    start,
                    usize::from(task < partitions - 1 || task == tasks - 1),
                )
            } else {
                let base = partitions / tasks;
                let extra = partitions % tasks;
                (
                    task * base + task.min(extra),
                    base + usize::from(task < extra),
                )
            };
            let end = start + len;
            let start_bound = if start == 0 {
                range.start_bound().cloned()
            } else {
                Some(range.split_points()[start - 1].clone())
            };
            let end_bound = if end == partitions {
                range.end_bound().cloned()
            } else if end == 0 {
                start_bound.clone()
            } else {
                Some(range.split_points()[end - 1].clone())
            };
            if len == 0 {
                let Some(bound) = start_bound else {
                    return plan_err!(
                        "Cannot pad an unbounded single-partition range without a boundary"
                    );
                };
                return Ok(RangeTask {
                    partitioning: RangePartitioning::try_new(
                        range.ordering().clone(),
                        vec![bound.clone(); width - 1],
                    )?
                    .try_with_bounds(Some(bound.clone()), Some(bound))?,
                    input: start..end,
                    leading_padding: 0,
                });
            }
            let slice = RangePartitioning::try_new(
                range.ordering().clone(),
                range.split_points()[start..end - 1].to_vec(),
            )?
            .try_with_bounds(start_bound, end_bound)?;
            let (partitioning, leading_padding) = pad_range(&slice, width)?;
            Ok(RangeTask {
                partitioning,
                input: start..end,
                leading_padding,
            })
        })
        .collect()
}

/// Pad at a finite outer bound so empty slots have zero-width ranges. Padding at
/// the start of the last task avoids inventing an infinity-valued split point.
pub(crate) fn pad_range(
    range: &RangePartitioning,
    width: usize,
) -> Result<(RangePartitioning, usize)> {
    let padding = width.saturating_sub(range.partition_count());
    if padding == 0 {
        return Ok((range.clone(), 0));
    }
    let mut splits = range.split_points().to_vec();
    let leading = if let Some(end) = range.end_bound() {
        splits.extend(std::iter::repeat_n(end.clone(), padding));
        0
    } else if let Some(start) = range.start_bound() {
        splits.splice(0..0, std::iter::repeat_n(start.clone(), padding));
        padding
    } else {
        return plan_err!("Cannot pad a range without a finite outer bound");
    };
    Ok((
        RangePartitioning::try_new(range.ordering().clone(), splits)?
            .try_with_bounds(range.start_bound().cloned(), range.end_bound().cloned())?,
        leading,
    ))
}

/// Concatenation preserves range partitioning only for ordered, adjacent ranges.
pub(crate) fn concat_ranges(ranges: &[RangePartitioning]) -> Result<Option<RangePartitioning>> {
    let Some(first) = ranges.first() else {
        return Ok(None);
    };
    let mut splits = first.split_points().to_vec();
    let mut previous = first;
    for range in &ranges[1..] {
        if range.ordering() != first.ordering()
            || previous.end_bound().is_none()
            || previous.end_bound() != range.start_bound()
        {
            return Ok(None);
        }
        splits.extend(previous.end_bound().cloned());
        splits.extend_from_slice(range.split_points());
        previous = range;
    }
    Ok(Some(
        RangePartitioning::try_new(first.ordering().clone(), splits)?
            .try_with_bounds(first.start_bound().cloned(), previous.end_bound().cloned())?,
    ))
}

/// Capture task-specific output ranges before an adaptive stage loses its local plan.
pub(crate) fn task_ranges(
    plan: &Arc<dyn ExecutionPlan>,
    tasks: usize,
) -> Result<Option<Vec<RangePartitioning>>> {
    if !matches!(plan.output_partitioning(), Partitioning::Range(_)) {
        return Ok(None);
    }
    (0..tasks)
        .map(|task_index| {
            let ctx = DistributedTaskContext {
                task_index,
                task_count: tasks,
            };
            let plan = Arc::clone(plan)
                .transform_down_with_dt_ctx(ctx, |plan, ctx| {
                    if let Some(leaf) = plan.downcast_ref::<DistributedLeafExec>() {
                        return Ok(Transformed::yes(leaf.to_task_specialized(ctx.task_index)));
                    }
                    if let Some(coalesce) = plan.downcast_ref::<NetworkCoalesceExec>() {
                        return Ok(Transformed::yes(Arc::new(
                            coalesce.to_task_specialized(ctx.task_index)?,
                        )));
                    }
                    if let Some(shuffle) = plan.downcast_ref::<NetworkShuffleExec>() {
                        return Ok(Transformed::yes(Arc::new(
                            shuffle.to_task_specialized(ctx)?,
                        )));
                    }
                    Ok(Transformed::no(plan))
                })?
                .data;
            Ok(match plan.output_partitioning() {
                Partitioning::Range(range) => Some(range.clone()),
                _ => None,
            })
        })
        .collect::<Result<Option<Vec<_>>>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::{ScalarValue, SplitPoint};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};

    #[test]
    fn coalesce_ranges_with_two_padding_partitions() -> Result<()> {
        let point = |value| SplitPoint::new(vec![ScalarValue::Int32(Some(value))]);
        let range = RangePartitioning::try_new(
            LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new(
                "key", 0,
            )))])
            .unwrap(),
            (1..10).map(point).collect(),
        )?;
        let tasks = split_range(&range, 5)?
            .into_iter()
            .map(|task| task.partitioning)
            .collect::<Vec<_>>();

        // Coalesce five two-partition producers into groups of three and two tasks.
        let left = concat_ranges(&tasks[..3])?.unwrap();
        let right = concat_ranges(&tasks[3..])?.unwrap();
        let (right, leading) = pad_range(&right, left.partition_count())?;
        assert_eq!(leading, 2);
        assert_eq!(right.start_bound(), Some(&point(6)));
        assert_eq!(
            right.split_points(),
            &[point(6), point(6), point(7), point(8), point(9)]
        );

        let combined = concat_ranges(&[left, right])?.unwrap();
        assert_eq!(combined.partition_count(), 12);
        assert_eq!(combined.start_bound(), None);
        assert_eq!(combined.end_bound(), None);
        assert_eq!(
            combined.split_points(),
            &(1..=6)
                .chain([6, 6])
                .chain(7..10)
                .map(point)
                .collect::<Vec<_>>()
        );

        // Overlapping or reversed task ranges cannot be advertised as one range layout.
        assert!(concat_ranges(&[tasks[1].clone(), tasks[0].clone()])?.is_none());
        assert!(concat_ranges(&[tasks[0].clone(), tasks[0].clone()])?.is_none());
        Ok(())
    }
}
