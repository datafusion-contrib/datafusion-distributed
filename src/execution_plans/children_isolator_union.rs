use crate::DistributedTaskContext;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

/// Distributed version of the vanilla [UnionExec] node that is capable of spreading the execution
/// of its children across multiple distributed tasks.
///
/// Without [ChildrenIsolatorUnionExec], distributing a normal [UnionExec] implies scaling up
/// in partitions all the child leaf nodes and executing them all in all the assigned tasks,
/// passing a [DistributedTaskContext] so that each child knows how to distribute its work.
///
/// With [ChildrenIsolatorUnionExec], its children are isolated per task, meaning that each
/// child will potentially be executed as if it was running in a single-node setup, and
/// [ChildrenIsolatorUnionExec] will figure out which children to execute depending on the
/// [DistributedTaskContext].
///
/// It's easy to think about this node in the case that the task count is equal to the number
/// of children. However, it gets a bit more complicated in case there are fewer tasks than children,
/// or more tasks than children.
///
/// ## Case when task_count == 3 and children.len() == 3
///
/// ```text
/// ┌─────────────────────────────┐┌─────────────────────────────┐┌─────────────────────────────┐
/// │           Task 1            ││           Task 2            ││           Task 3            │
/// │┌───────────────────────────┐││┌───────────────────────────┐││┌───────────────────────────┐│
/// ││ ChildrenIsolatorUnionExec ││││ ChildrenIsolatorUnionExec ││││ ChildrenIsolatorUnionExec ││
/// │└───▲─────────▲─────────▲───┘││└───▲─────────▲─────────▲───┘││└───▲─────────▲─────────▲───┘│
/// │    │                        ││              │              ││                        │    │
/// │┌───┴───┐ ┌  ─│ ─   ┌  ─│ ─  ││┌  ─│ ─   ┌───┴───┐ ┌  ─│ ─  ││┌  ─│ ─   ┌  ─│ ─   ┌───┴───┐│
/// ││Child 1│  Child 2│  Child 3│││ Child 1│ │Child 2│  Child 3│││ Child 1│  Child 2│ │Child 3││
/// │└───────┘ └  ─  ─   └  ─  ─  ││└  ─  ─   └───────┘ └  ─  ─  ││└  ─  ─   └  ─  ─   └───────┘│
/// └─────────────────────────────┘└─────────────────────────────┘└─────────────────────────────┘
/// ```
///
/// ## Case when task_count == 2 and children.len() == 3
///
/// ```text
/// ┌─────────────────────────────┐┌─────────────────────────────┐
/// │           Task 1            ││           Task 2            │
/// │┌───────────────────────────┐││┌───────────────────────────┐│
/// ││ ChildrenIsolatorUnionExec ││││ ChildrenIsolatorUnionExec ││
/// │└───▲─────────▲─────────▲───┘││└───▲─────────▲─────────▲───┘│
/// │    │         │              ││                        │    │
/// │┌───┴───┐ ┌───┴───┐ ┌  ─│ ─  ││┌  ─│ ─   ┌ ─ ┴ ─ ┐ ┌───┴───┐│
/// ││Child 1│ │Child 2│  Child 3│││ Child 1│  Child 2  │Child 3││
/// │└───────┘ └───────┘ └  ─  ─  ││└  ─  ─   └ ─ ─ ─ ┘ └───────┘│
/// └─────────────────────────────┘└─────────────────────────────┘
///```
///
/// ## Case when task_count == 4 and children.len() == 3
///
/// ```text
/// ┌─────────────────────────────┐┌─────────────────────────────┐┌─────────────────────────────┐┌─────────────────────────────┐
/// │           Task 1            ││           Task 2            ││           Task 3            ││           Task 4            │
/// │┌───────────────────────────┐││┌───────────────────────────┐││┌───────────────────────────┐││┌───────────────────────────┐│
/// ││ ChildrenIsolatorUnionExec ││││ ChildrenIsolatorUnionExec ││││ ChildrenIsolatorUnionExec ││││ ChildrenIsolatorUnionExec ││
/// │└───▲─────────▲─────────▲───┘││└───▲─────────▲─────────▲───┘││└───▲─────────▲─────────▲───┘││└───▲─────────▲─────────▲───┘│
/// │    │                        ││    │                        ││              │              ││                        │    │
/// │┌───┴───┐ ┌  ─│ ─   ┌  ─│ ─  ││┌───┴───┐ ┌  ─│ ─   ┌  ─│ ─  ││┌  ─│ ─   ┌───┴───┐ ┌  ─│ ─  ││┌  ─│ ─   ┌  ─│ ─   ┌───┴───┐│
/// ││Child 1│  Child 2│  Child 3││││Child 1│  Child 2│  Child 3│││ Child 1│ │Child 2│  Child 3│││ Child 1│  Child 2│ │Child 3││
/// ││ (1/2) │ └  ─  ─   └  ─  ─  │││ (2/2) │ └  ─  ─   └  ─  ─  ││└  ─  ─   └───────┘ └  ─  ─  ││└  ─  ─   └  ─  ─   └───────┘│
/// │└───────┘                    ││└───────┘                    ││                             ││                             │
/// └─────────────────────────────┘└─────────────────────────────┘└─────────────────────────────┘└─────────────────────────────┘
/// ```
#[derive(Debug, Clone)]
pub struct ChildrenIsolatorUnionExec {
    pub(crate) properties: PlanProperties,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) children: Vec<Arc<dyn ExecutionPlan>>,
    pub(crate) task_idx_map: Vec<
        /* outer distributed task idx */
        Vec<(
            /* child index */ usize,
            /* inner distributed task ctx for the isolated child*/ DistributedTaskContext,
        )>,
    >,
}

impl ChildrenIsolatorUnionExec {
    pub(crate) fn from_children_and_task_counts(
        children: impl IntoIterator<Item = Arc<dyn ExecutionPlan>>,
        children_task_count: impl IntoIterator<Item = usize>,
        task_count: usize,
    ) -> Result<Self, DataFusionError> {
        let children = children.into_iter().collect_vec();
        let task_count_per_children = children_task_count.into_iter().collect_vec();

        if children.len() != task_count_per_children.len() {
            return internal_err!(
                "ChildrenIsolatorUnionExec received {} children but a vec of {} positions for those children. This is a bug in the distributed planning logic, please report it",
                children.len(),
                task_count_per_children.len()
            );
        }

        let task_idx_map = split_children(task_count_per_children, task_count)?;

        // Because different children might return a different number of partitions, and we might
        // execute a different number of children in different tasks, the reality is that this node,
        // depending on which task index is running, it will have a different number of partitions.
        //
        // We want to hide that to the outside and just advertise as many partitions as the task
        // that will handle the greatest number of partitions, and just return empty streams for
        // remainder partitions in tasks that will execute fewer partitions.
        let mut partition_counts = vec![0; task_idx_map.len()];
        for (t, children_in_task) in task_idx_map.iter().enumerate() {
            for (child_idx, _) in children_in_task {
                partition_counts[t] += children[*child_idx].output_partitioning().partition_count();
            }
        }
        let Some(partition_count) = partition_counts.iter().max() else {
            return internal_err!(
                "ChildrenIsolatorUnionExec built an empty task_idx_map. This is a bug in the distributed planning logic, please report it"
            );
        };

        // It's not supper efficient to build a UnionExec just to get the properties out, but the
        // other solution is to copy-paste a bunch of code from upstream for computing the properties
        // of a union, so we prefer to just reuse it like this.
        let mut properties = UnionExec::try_new(children.clone())?.properties().clone();
        properties.partitioning = Partitioning::UnknownPartitioning(*partition_count);
        Ok(Self {
            properties,
            metrics: ExecutionPlanMetricsSet::default(),
            children,
            task_idx_map,
        })
    }
}

impl DisplayAs for ChildrenIsolatorUnionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DistributedUnionExec:")?;
                for (task_i, children_in_task) in self.task_idx_map.iter().enumerate() {
                    write!(f, " t{task_i}:[")?;
                    for (i, (child_idx, child_task_ctx)) in children_in_task.iter().enumerate() {
                        if child_task_ctx.task_count > 1 {
                            write!(
                                f,
                                "c{child_idx}({}/{})",
                                child_task_ctx.task_index, child_task_ctx.task_count
                            )?;
                        } else {
                            write!(f, "c{child_idx}")?;
                        }
                        if i < children_in_task.len() - 1 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, "]")?;
                }

                Ok(())
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for ChildrenIsolatorUnionExec {
    fn name(&self) -> &str {
        "ChildrenIsolatorUnionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != self.children.len() {
            return plan_err!(
                "Number of children must match the original plan, have {} but expected {}",
                children.len(),
                self.children.len()
            );
        }
        let mut clone = self.as_ref().clone();
        clone.children = children;
        Ok(Arc::new(clone))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        mut partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let d_ctx = DistributedTaskContext::from_ctx(&context);

        let children = self.task_idx_map[d_ctx.task_index].clone();

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer(); // record on drop

        for (child_idx, child_task_ctx) in children {
            let Some(input) = self.children.get(child_idx) else {
                return internal_err!("Could not find child with index {child_idx}");
            };
            // Calculate whether a partition belongs to the current partition
            if partition < input.output_partitioning().partition_count() {
                // We need to intercept the DistributedTaskContext and insert a modified one that
                // tells the child that is running in "isolation" (see the beginning of this file
                // for a longer explanation)
                let context = Arc::new(TaskContext::new(
                    context.task_id(),
                    context.session_id(),
                    context
                        .session_config()
                        .clone()
                        .with_extension(Arc::new(child_task_ctx)),
                    context.scalar_functions().clone(),
                    context.aggregate_functions().clone(),
                    context.window_functions().clone(),
                    context.runtime_env(),
                ));

                let stream = input.execute(partition, context)?;

                return Ok(Box::pin(ObservedStream::new(
                    stream,
                    baseline_metrics,
                    None,
                )));
            } else {
                partition -= input.output_partitioning().partition_count();
            }
        }

        Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())))
    }
}

// Struct copied from https://github.com/apache/datafusion/blob/2c3566ce856bf7c87508567119bc3834f007e94b/datafusion/physical-plan/src/stream.rs#L506-L506
// It's what allows a UnionExec to have metrics.
pub(crate) struct ObservedStream {
    inner: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    fetch: Option<usize>,
    produced: usize,
}

impl ObservedStream {
    pub fn new(
        inner: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        fetch: Option<usize>,
    ) -> Self {
        Self {
            inner,
            baseline_metrics,
            fetch,
            produced: 0,
        }
    }

    fn limit_reached(
        &mut self,
        poll: Poll<Option<datafusion::common::Result<RecordBatch>>>,
    ) -> Poll<Option<datafusion::common::Result<RecordBatch>>> {
        let Some(fetch) = self.fetch else { return poll };

        if self.produced >= fetch {
            return Poll::Ready(None);
        }

        if let Poll::Ready(Some(Ok(batch))) = &poll {
            if self.produced + batch.num_rows() > fetch {
                let batch = batch.slice(0, fetch.saturating_sub(self.produced));
                self.produced += batch.num_rows();
                return Poll::Ready(Some(Ok(batch)));
            };
            self.produced += batch.num_rows()
        }
        poll
    }
}

impl RecordBatchStream for ObservedStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for ObservedStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut poll = self.inner.poll_next_unpin(cx);
        if self.fetch.is_some() {
            poll = self.limit_reached(poll);
        }
        self.baseline_metrics.record_poll(poll)
    }
}

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
fn split_children(
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
            split_children(vec![1, 1, 1], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))]
            ]
        );
        assert_eq!(
            split_children(vec![1, 1, 1], 2)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1))], vec![(2, ctx(0, 1))]]
        );
        assert_eq!(
            split_children(vec![1, 1, 1], 1)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1)), (2, ctx(0, 1))]]
        );
        Ok(())
    }

    #[test]
    fn split_children_different_tasks() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            split_children(vec![1, 2, 3], 6)?,
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
            split_children(vec![1, 2, 3], 5)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );
        assert_eq!(
            split_children(vec![1, 2, 3], 4)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );
        assert_eq!(
            split_children(vec![1, 2, 3], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))],
            ]
        );
        assert_eq!(
            split_children(vec![1, 2, 3], 2)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1))], vec![(2, ctx(0, 1))]]
        );
        assert_eq!(
            split_children(vec![1, 2, 3], 1)?,
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
