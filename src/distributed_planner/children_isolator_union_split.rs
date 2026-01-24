use crate::DistributedTaskContext;
use datafusion::common::{DataFusionError, internal_err};

/// Given a list of children with a different number of tasks assigned each, it redistributes them
/// and re-assign tasks numbers to them so that they fit in the provided `task_count`.
///
/// For example, given these inputs:
///     `task_count_per_children`: [1, 1, 1]
///     `task_count`: 3
/// It returns the following output:
///     `[[(0, 0/1)], [(1, 0/1)], [(2, 0/1)]]`
/// That means that:
/// - Task 0 will execute task 0 from child 0
/// - Task 1 will execute task 0 from child 1
/// - Task 2 will execute task 0 from child 2
///
/// Things can get more complicated if the sum of all the tasks from the children is greater than
/// the `task_count` passed:
///
/// For example, given these inputs:
///     `task_count_per_children`: [2, 1, 1]
///     `task_count`: 3
/// It returns the following output:
///     `[[(0, 0/1)], [(1, 0/1)], [(2, 0/1)]]`
/// As we have 3 tasks available for executing 3 children with a total sum of 2+1+1=4 tasks, we
/// need to tell that first child to be executed in 1 task.
///
/// In this other case:
///     `task_count_per_children`: [2, 3, 1]
///     `task_count`: 5
/// It returns the following output:
///     `[[(0, 0/2)], [(0, 1/2)], [(1, 0/2)], [(1, 1/2)], [(2, 0/1)]]`
/// Note how in this case there are 2+3+1=6 tasks to be executed, but we only have 5 tasks
/// available. We need to trim a task from somewhere, and the only candidates are child 0 and 1.
/// As child 1 is the one with the greatest task count (3), we prefer to trim the task from there,
/// as that's what will yield the most even distribution of tasks given the 5 tasks budget.
///
/// Going to the other extreme, given this input:
///     `task_count_per_children`: [1, 1, 1, 1, 1]
///     `task_count`: 2
/// It returns the following output:
///     `[[(0, 0/1), (1, 0/1), (2, 0/1)] , [(3, 0/1), (4, 0/1)]]`
/// In this case, we have very few `task_count` available, so we cannot afford to execute one
/// child per task. We need to group children so that one task executes several of them, and the
/// other tasks execute the several other remaining.
///
/// The function looks pretty much like the solution of a LeetCode problem, so it's not super
/// easy to understand just be looking at the code. The best way to get a grasp of what its doing
/// is by looking at the tests.
pub(super) fn children_isolator_union_split(
    mut task_count_per_children: Vec<usize>,
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
    DataFusionError,
> {
    let total_children_tasks = task_count_per_children.iter().sum::<usize>();
    if task_count_budget > total_children_tasks {
        return internal_err!(
            "ChildrenIsolatorUnionExec had a task count {task_count_budget}, which is greater than the sum of child task counts {total_children_tasks}. This is a bug in the distributed planning logic, please report it"
        );
    } else if task_count_budget == 0 {
        return internal_err!(
            "ChildrenIsolatorUnionExec had a task count {task_count_budget}. This is a bug in the distributed planning logic, please report it"
        );
    }

    let mut tasks_to_trim = total_children_tasks - task_count_budget;
    while tasks_to_trim > 0 {
        let mut max_child_task_count_idx = 0;
        let mut max_child_task_count_value = 1;
        for (i, child_task_count) in task_count_per_children.iter().enumerate() {
            if child_task_count > &max_child_task_count_value {
                max_child_task_count_idx = i;
                max_child_task_count_value = *child_task_count;
            }
        }
        if max_child_task_count_value == 1 {
            break;
        }
        task_count_per_children[max_child_task_count_idx] -= 1;
        tasks_to_trim -= 1;
    }

    let total_child_tasks: usize = task_count_per_children.iter().sum();
    let base_per_task = total_child_tasks / task_count_budget;
    let mut extra = total_child_tasks % task_count_budget;

    let mut result = vec![vec![]; task_count_budget];
    let mut task_idx = 0;
    let mut current_task_count = 0;
    let mut current_task_capacity = base_per_task;
    if extra > 0 {
        extra -= 1;
        current_task_capacity += 1
    }

    for (child_idx, &child_task_count) in task_count_per_children.iter().enumerate() {
        for task_i in 0..child_task_count {
            result[task_idx].push((
                child_idx,
                DistributedTaskContext {
                    task_index: task_i,
                    task_count: child_task_count,
                },
            ));
            current_task_count += 1;

            if current_task_count >= current_task_capacity && task_idx < task_count_budget - 1 {
                task_idx += 1;
                current_task_count = 0;
                current_task_capacity = base_per_task;
                if extra > 0 {
                    extra -= 1;
                    current_task_capacity += 1
                }
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn children_split_all_1_task() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            children_isolator_union_split(vec![1, 1, 1], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))]
            ]
        );
        assert_eq!(
            children_isolator_union_split(vec![1, 1, 1], 2)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1))], vec![(2, ctx(0, 1))]]
        );
        assert_eq!(
            children_isolator_union_split(vec![1, 1, 1], 1)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1)), (2, ctx(0, 1))]]
        );
        Ok(())
    }

    #[test]
    fn children_isolator_union_split_different_tasks() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            children_isolator_union_split(vec![1, 2, 3], 6)?,
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
            children_isolator_union_split(vec![1, 2, 3], 5)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );
        assert_eq!(
            children_isolator_union_split(vec![1, 2, 3], 4)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );
        assert_eq!(
            children_isolator_union_split(vec![1, 2, 3], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))],
            ]
        );
        assert_eq!(
            children_isolator_union_split(vec![1, 2, 3], 2)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1))], vec![(2, ctx(0, 1))]]
        );
        assert_eq!(
            children_isolator_union_split(vec![1, 2, 3], 1)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1)), (2, ctx(0, 1))]]
        );
        Ok(())
    }

    fn ctx(task_index: usize, task_count: usize) -> DistributedTaskContext {
        DistributedTaskContext {
            task_index,
            task_count,
        }
    }
}
