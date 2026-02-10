use crate::DistributedTaskContext;
use crate::distributed_planner::plan_annotator::AnnotatedPlan;
use datafusion::common::{Result, internal_err};

/// Given a list of children with a different compute cost each, and a restriction about the maximum
/// tasks in which they are allowed to run, it assigns tasks counts to them so that the following
/// conditions are met:
///     1. The amount of tasks used for each child is proportional to their cost.
///     2. The restriction about the maximum tasks in which a child can run is respected.
///        (e.g., a child with high cost and a "maximum 1 task" restriction runs in 1 task)
///     3. The children are distributed across tasks so that the total compute cost running within
///        each task is as evenly distributed as possible.
///
/// For example, given these inputs:
///     `children`: [{cost: 1, max: None}, {cost: 1, max: None}, {cost: 1, max: None}]
///     `task_count`: 3
/// It returns the following output:
///     `[[(0, 0/1)], [(1, 0/1)], [(2, 0/1)]]`
/// That means that:
/// - Task 0 will execute task 0 from child 0
/// - Task 1 will execute task 0 from child 1
/// - Task 2 will execute task 0 from child 2
///
/// If there is one child with a very high cost, preference is given to it to run distributed, even
/// if that implies squeezing two cheap nodes in 1 task in order to give room to the expensive one.
///     `children`: [{cost: 4, max: None}, {cost: 1, max: None}, {cost: 1, max: None}]
///     `task_count`: 3
/// It returns the following output:
///     `[[(0, 0/2)], [(0, 1/2)], [(1, 0/1), (2, 0/1)]]`
/// That means that:
/// - Task 0 will execute task 0 from child 0 (the expensive one)
/// - Task 1 will execute task 1 from child 0 (the expensive one)
/// - Task 2 will execute task 0 from child 1 and task 0 from child 2
///
/// If one child declares that it can only run in a specific number of tasks at most, this needs
/// to be respected:
///     `children`: [{cost: 4, max: Some(1)}, {cost: 1, max: None}, {cost: 1, max: None}]
///     `task_count`: 3
/// It returns the following output:
///     `[[(0, 0/1)], [(1, 0/1)], [(2, 0/1)]]`
/// - Task 0 will execute task 0 from child 0. This is expensive, but nothing we can do about it.
/// - Task 1 will execute task 0 from child 1
/// - Task 2 will execute task 0 from child 2
///
/// The function looks pretty much like the solution of a LeetCode problem, so it's not super
/// easy to understand just be looking at the code. The best way to get a grasp of what its doing
/// is by looking at the tests.
pub(super) fn children_isolator_union_split(
    children: &[impl CostChild],
    task_count_budget: usize,
) -> Result<
    // Task idx. This Vec will have `task_count_budget` length.
    Vec<
        // For this task, the child indexes and DistributedTaskContext that should be executed.
        Vec<(
            /* Child index */ usize,
            /* Distributed task ctx for the child */ DistributedTaskContext,
        )>,
    >,
> {
    if task_count_budget == 0 {
        return internal_err!(
            "ChildrenIsolatorUnionExec had a task count {task_count_budget}. This is a bug in the distributed planning logic, please report it"
        );
    }

    if children.is_empty() {
        return Ok(vec![vec![]; task_count_budget]);
    }

    // ==================== Phase 1: Calculate subtask count for each child ====================
    // Goal: distribute task_count_budget subtasks among children proportionally to their cost,
    // respecting max_tasks constraints and ensuring at least 1 subtask per child.

    let costs: Vec<usize> = children.iter().map(|c| c.cost()).collect();
    let max_tasks: Vec<Option<usize>> = children.iter().map(|c| c.max_tasks()).collect();
    let total_cost: usize = costs.iter().sum();

    // Calculate ideal subtask count for each child based on cost proportion
    let avg_budget_per_task = if total_cost > 0 {
        total_cost as f64 / task_count_budget as f64
    } else {
        1.0
    };

    let mut child_subtask_counts: Vec<usize> = costs
        .iter()
        .enumerate()
        .map(|(i, &cost)| {
            // Ideal subtasks proportional to cost
            let ideal = if total_cost > 0 {
                (cost as f64 / avg_budget_per_task).ceil() as usize
            } else {
                1
            };
            // At least 1 subtask per child
            let count = ideal.max(1);
            // Apply max_tasks constraint
            max_tasks[i].map_or(count, |max| count.min(max.max(1)))
        })
        .collect();

    // Adjust total subtasks to match budget (or get as close as possible)
    let mut total_subtasks: usize = child_subtask_counts.iter().sum();

    // Trim if over budget: reduce from children with the highest subtask count
    // Tiebreaker: prefer reducing lower-cost children (keep expensive ones parallel)
    while total_subtasks > task_count_budget {
        let mut best_idx = None;
        let mut best_count = 1; // Can't reduce below 1

        for (i, &count) in child_subtask_counts.iter().enumerate() {
            // Find child with the highest count (above 1), tiebreak by lower cost
            if count > best_count {
                best_idx = Some(i);
                best_count = count;
            }
        }

        if let Some(idx) = best_idx {
            child_subtask_counts[idx] -= 1;
            total_subtasks -= 1;
        } else {
            break; // All children at minimum (1), can't reduce further
        }
    }

    // Expand if under budget: add to children with the highest cost that can expand
    while total_subtasks < task_count_budget {
        let mut best_idx = None;
        let mut best_cost = 0;

        for (i, &cost) in costs.iter().enumerate() {
            let current = child_subtask_counts[i];
            let can_expand = max_tasks[i].is_none_or(|max| current < max);
            if can_expand && cost > best_cost {
                best_idx = Some(i);
                best_cost = cost;
            }
        }

        if let Some(idx) = best_idx {
            child_subtask_counts[idx] += 1;
            total_subtasks += 1;
        } else {
            break; // All children at max, can't expand further
        }
    }

    // ==================== Phase 2: Pack subtasks into output tasks ====================
    // Distribute child subtasks evenly across output tasks by count.
    // Process children in order for deterministic output.

    let total_subtasks: usize = child_subtask_counts.iter().sum();
    let base_per_task = total_subtasks / task_count_budget;
    let mut extra = total_subtasks % task_count_budget;

    let mut result = vec![vec![]; task_count_budget];
    let mut task_idx = 0;
    let mut current_count = 0;
    let mut task_capacity = base_per_task + if extra > 0 { 1 } else { 0 };

    for (child_idx, &subtask_count) in child_subtask_counts.iter().enumerate() {
        for subtask_i in 0..subtask_count {
            result[task_idx].push((
                child_idx,
                DistributedTaskContext {
                    task_index: subtask_i,
                    task_count: subtask_count,
                },
            ));
            current_count += 1;

            // Move to next output task if capacity reached (unless we're at the last task)
            if current_count >= task_capacity && task_idx < task_count_budget - 1 {
                task_idx += 1;
                current_count = 0;
                extra = extra.saturating_sub(1);
                task_capacity = base_per_task + if extra > 0 { 1 } else { 0 };
            }
        }
    }

    Ok(result)
}

/// private trait just for testability purposes.
pub(super) trait CostChild {
    fn cost(&self) -> usize;
    fn max_tasks(&self) -> Option<usize>;
}

impl CostChild for AnnotatedPlan {
    fn cost(&self) -> usize {
        self.cost_aggregated_until_network_boundary()
    }

    fn max_tasks(&self) -> Option<usize> {
        self.max_task_count_restriction
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn children_split_all_1_task() -> Result<()> {
        assert_eq!(
            children_isolator_union_split(&[(1, None), (1, None), (1, None)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))]
            ]
        );
        assert_eq!(
            children_isolator_union_split(&[(1, None), (1, None), (1, None)], 2)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1))], vec![(2, ctx(0, 1))]]
        );
        assert_eq!(
            children_isolator_union_split(&[(1, None), (1, None), (1, None)], 1)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1)), (2, ctx(0, 1))]]
        );
        Ok(())
    }

    #[test]
    fn children_isolator_union_split_different_tasks() -> Result<()> {
        assert_eq!(
            children_isolator_union_split(&[(1, None), (2, None), (3, None)], 6)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
                vec![(2, ctx(0, 3))],
                vec![(2, ctx(1, 3))],
                vec![(2, ctx(2, 3))]
            ]
        );
        assert_eq!(
            children_isolator_union_split(&[(1, None), (2, None), (3, None)], 5)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );
        assert_eq!(
            children_isolator_union_split(&[(1, None), (2, None), (3, None)], 4)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );
        assert_eq!(
            children_isolator_union_split(&[(1, None), (2, None), (3, None)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))],
            ]
        );
        assert_eq!(
            children_isolator_union_split(&[(1, None), (2, None), (3, None)], 2)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1))], vec![(2, ctx(0, 1))]]
        );
        assert_eq!(
            children_isolator_union_split(&[(1, None), (2, None), (3, None)], 1)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1)), (2, ctx(0, 1))]]
        );
        Ok(())
    }

    /// Tests the doc example: expensive child with max_tasks=1 restriction.
    /// Even though child 0 has cost=4 (would ideally get 2 tasks), it's capped at 1.
    #[test]
    fn expensive_child_with_max_tasks_restriction() -> Result<()> {
        // From doc: [{cost: 4, max: Some(1)}, {cost: 1, max: None}, {cost: 1, max: None}]
        // With budget=3, child 0 is forced to 1 task despite high cost.
        assert_eq!(
            children_isolator_union_split(&[(4, Some(1)), (1, None), (1, None)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))]
            ]
        );
        Ok(())
    }

    /// Tests that max_tasks caps expansion when budget exceeds what children can use.
    #[test]
    fn max_tasks_prevents_over_expansion() -> Result<()> {
        // All children capped at 1 task each, but budget is 5.
        // Should result in 3 used tasks with 2 empty ones.
        assert_eq!(
            children_isolator_union_split(&[(10, Some(1)), (10, Some(1)), (10, Some(1))], 5)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))],
                vec![],
                vec![]
            ]
        );
        Ok(())
    }

    #[test]
    fn unbounded_child_absorbs_budget() -> Result<()> {
        // Children 0,1,2 are capped at 1 task each. Child 3 is expensive and unbounded.
        // With budget=6, child 3 should expand to fill the remaining 3 tasks.
        assert_eq!(
            children_isolator_union_split(
                &[(1, Some(1)), (1, Some(1)), (1, Some(1)), (10, None)],
                6
            )?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))],
                vec![(3, ctx(0, 3))],
                vec![(3, ctx(1, 3))],
                vec![(3, ctx(2, 3))],
            ]
        );
        Ok(())
    }

    /// NOTE(gabotechs): This test proves how this approach has some limitations.
    ///
    /// One could argue that the correct thing to do here is to actually try to spread
    /// the last child across tasks, but this algorithm doesn't.
    #[test]
    fn unbounded_expensive_child_just_uses_1_task() -> Result<()> {
        assert_eq!(
            children_isolator_union_split(
                &[(1, Some(1)), (1, Some(1)), (1, Some(1)), (10, None)],
                4
            )?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))],
                vec![(3, ctx(0, 1))],
            ]
        );
        Ok(())
    }

    #[test]
    fn many_free_children_does_not_prevent_expensive_one_from_being_distributed() -> Result<()> {
        assert_eq!(
            children_isolator_union_split(&[(0, None), (0, None), (0, None), (10, None)], 2)?,
            vec![
                vec![(0, ctx(0, 1)), (1, ctx(0, 1))],
                vec![(2, ctx(0, 1)), (3, ctx(0, 1))],
            ]
        );
        Ok(())
    }

    fn ctx(task_index: usize, task_count: usize) -> DistributedTaskContext {
        DistributedTaskContext {
            task_index,
            task_count,
        }
    }

    impl CostChild for (usize, Option<usize>) {
        fn cost(&self) -> usize {
            self.0
        }

        fn max_tasks(&self) -> Option<usize> {
            self.1
        }
    }
}
