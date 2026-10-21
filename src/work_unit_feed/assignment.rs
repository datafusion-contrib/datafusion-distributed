use std::cmp::Reverse;
use std::collections::BinaryHeap;

use datafusion::common::{Result, config_err};

/// Assigns indivisible work to tasks using largest-first, least-loaded placement.
///
/// Each input index occurs exactly once in the returned per-task lists. Costs must use
/// the same units; they are scheduling estimates, not memory reservations. Equal costs
/// are ordered by input index, so callers should supply a stable input order when they
/// require stable assignments. Empty tasks are allowed. This does not split work units
/// or choose workers.
///
/// ```
/// # use datafusion_distributed::greedy_work_unit_assignment;
/// let tasks = greedy_work_unit_assignment(&[100, 1, 2, 3], 2)?;
/// assert_eq!(tasks, vec![vec![0], vec![3, 2, 1]]);
/// # Ok::<(), datafusion::error::DataFusionError>(())
/// ```
pub fn greedy_work_unit_assignment(costs: &[u64], task_count: usize) -> Result<Vec<Vec<usize>>> {
    if task_count == 0 {
        return config_err!("Work assignment requires at least one task");
    }
    let mut order: Vec<usize> = (0..costs.len()).collect();
    order.sort_unstable_by_key(|&i| (Reverse(costs[i]), i));
    let mut tasks = vec![Vec::new(); task_count];
    // Break load ties by item count, then task index. This distributes zero-cost work too.
    let mut loads: BinaryHeap<_> = (0..task_count)
        .map(|task| Reverse((0_u128, 0_usize, task)))
        .collect();
    for index in order {
        let mut least_loaded = loads.peek_mut().expect("nonzero task count");
        let Reverse((load, count, task)) = &mut *least_loaded;
        tasks[*task].push(index);
        *load += u128::from(costs[index]);
        *count += 1;
    }
    Ok(tasks)
}

#[cfg(test)]
mod tests {
    use super::greedy_work_unit_assignment;
    use datafusion::common::Result;

    #[test]
    fn assignment_covers_skew_zero_costs_empty_tasks_and_large_costs() -> Result<()> {
        for costs in [vec![100, 1, 2, 3], vec![0; 7], vec![], vec![u64::MAX; 4]] {
            for task_count in [1, 2, 10] {
                let tasks = greedy_work_unit_assignment(&costs, task_count)?;
                assert_eq!(tasks.len(), task_count);
                let mut indices: Vec<_> = tasks.into_iter().flatten().collect();
                indices.sort_unstable();
                assert_eq!(indices, (0..costs.len()).collect::<Vec<_>>());
            }
        }
        assert_eq!(
            greedy_work_unit_assignment(&[0; 4], 2)?,
            vec![vec![0, 2], vec![1, 3]]
        );
        assert!(greedy_work_unit_assignment(&[1], 0).is_err());
        Ok(())
    }
}
