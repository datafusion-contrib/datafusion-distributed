use crate::common::task_ctx_with_extension;
use crate::events::TaskCountRestriction;
use crate::{DistributedTaskContext, TaskCountAnnotation};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use std::cmp::Ordering;
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
    pub(crate) properties: Arc<PlanProperties>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) children: Vec<Arc<dyn ExecutionPlan>>,
    /// The per-child annotations used to build the `task_idx_map`. Stored so
    /// `with_new_children` can preserve task allocation across plan rewrites.
    pub(crate) child_annotations: Vec<TaskCountAnnotation>,
    pub(crate) task_idx_map: Vec<
        /* outer distributed task idx */
        Vec<(
            /* child index */ usize,
            /* inner distributed task ctx for the isolated child*/ DistributedTaskContext,
        )>,
    >,
}

impl ChildrenIsolatorUnionExec {
    /// Creates a single-node union placeholder with every child assigned to the default task.
    pub(crate) fn new_single_task(
        children: impl IntoIterator<Item = Arc<dyn ExecutionPlan>>,
    ) -> Result<Self, DataFusionError> {
        let children = children.into_iter().collect_vec();
        let child_count = children.len();
        Self::from_children_and_annotations(
            children,
            vec![TaskCountAnnotation::soft(1.0); child_count],
            1,
        )
    }

    pub(crate) fn from_children_and_annotations(
        children: impl IntoIterator<Item = Arc<dyn ExecutionPlan>>,
        child_annotations: impl IntoIterator<Item = TaskCountAnnotation>,
        task_count: usize,
    ) -> Result<Self, DataFusionError> {
        let children = children.into_iter().collect_vec();
        let child_annotations = child_annotations.into_iter().collect_vec();

        if children.len() != child_annotations.len() {
            return internal_err!(
                "ChildrenIsolatorUnionExec received {} children but {} task count annotations. This is a bug in the distributed planning logic, please report it",
                children.len(),
                child_annotations.len()
            );
        }

        let task_idx_map = split_children(&child_annotations, task_count)?;

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
        let mut properties = UnionExec::try_new(children.clone())?
            .properties()
            .as_ref()
            .clone();
        properties.partitioning = Partitioning::UnknownPartitioning(*partition_count);
        Ok(Self {
            properties: Arc::new(properties),
            metrics: ExecutionPlanMetricsSet::default(),
            children,
            child_annotations,
            task_idx_map,
        })
    }

    pub(crate) fn child_task_counts(&self) -> Vec<usize> {
        // Preserve the task assignment in task_idx_map and allow child plans to be
        // replaced and properties to be recomputed from these new children.
        let mut counts = vec![0; self.children.len()];
        for children_in_task in &self.task_idx_map {
            for (child_idx, child_task_ctx) in children_in_task {
                counts[*child_idx] = counts[*child_idx].max(child_task_ctx.task_count);
            }
        }
        counts
    }

    /// Trims out all the children that are going to be ignored based on the provided
    /// task index. These children are replaced by [EmptyExec] as placeholders.
    pub(crate) fn to_task_specialized(&self, task_i: usize) -> Self {
        let mut children_to_keep = vec![];
        for (child_i, _) in &self.task_idx_map[task_i] {
            children_to_keep.push(*child_i);
        }
        let new_children = self
            .children
            .iter()
            .enumerate()
            .map(
                |(child_i, plan)| match children_to_keep.contains(&child_i) {
                    true => Arc::clone(plan),
                    false => Arc::new(
                        EmptyExec::new(plan.schema())
                            .with_partitions(plan.output_partitioning().partition_count()),
                    ) as Arc<dyn ExecutionPlan>,
                },
            )
            .collect_vec();
        Self {
            children: new_children,
            properties: self.properties.clone(),
            metrics: self.metrics.clone(),
            child_annotations: self.child_annotations.clone(),
            task_idx_map: self.task_idx_map.clone(),
        }
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

    fn properties(&self) -> &Arc<PlanProperties> {
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
        Ok(Arc::new(Self::from_children_and_annotations(
            children,
            self.child_annotations.clone(),
            self.task_idx_map.len(),
        )?))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::common::Result<TreeNodeRecursion>,
    ) -> datafusion::common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
                let context = Arc::new(task_ctx_with_extension(context.as_ref(), child_task_ctx));

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

/// Given per-child [`TaskCountAnnotation`]s and a `task_count_budget`, distribute task slots
/// proportional to loads, then impose each child's exact count or minimum. When a light
/// exact child shares a slot, a flexible child can still use the full task budget.
///
/// The tables at each step follow one example through to its final task map. Columns always
/// identify children (`cN`). In the placement tables, rows identify outer union tasks (`tN`),
/// and a cell `i/n` assigns that child task index i and task count n. `-` means no assignment
/// or no applicable constraint.
fn split_children(
    children: &[TaskCountAnnotation],
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
    // Step 1: validate the budget and each child's constraints.
    //
    // Running example:
    //
    // task_count_budget = 4
    //
    // +------------------+---------+---------+---------+---------+
    // |                  | c0      | c1      | c2      | c3      |
    // +------------------+---------+---------+---------+---------+
    // | load             | 4       | 2       | 0       | 0       |
    // | exact count      | 1       | -       | 1       | -       |
    // | minimum count    | -       | 1       | -       | -       |
    // +------------------+---------+---------+---------+---------+
    //
    // Budget > 0; loads are finite and >= 0; each exact count is in 1..=budget.
    // Each minimum must fit the budget; a child has either an exact count or a minimum.
    // Counts need not SUM to <= budget: different children can share an outer slot.
    if task_count_budget == 0 {
        return internal_err!(
            "ChildrenIsolatorUnionExec had a task count {task_count_budget}. This is a bug in the distributed planning logic, please report it"
        );
    }
    if children.is_empty() {
        return internal_err!(
            "ChildrenIsolatorUnionExec built with no children. This is a bug in the distributed planning logic, please report it"
        );
    }
    for (i, annotation) in children.iter().enumerate() {
        match annotation.restriction {
            TaskCountRestriction::Exact(exact) if exact.get() > task_count_budget => {
                return plan_err!(
                    "ChildrenIsolatorUnionExec child {i} requires {exact} tasks, but the union has only {task_count_budget}"
                );
            }
            TaskCountRestriction::Min(minimum) if minimum.get() > task_count_budget => {
                return plan_err!(
                    "ChildrenIsolatorUnionExec child {i} requires at least {minimum} tasks, but the union has only {task_count_budget}"
                );
            }
            _ => {}
        }
        if annotation.soft < 0.0 {
            return plan_err!(
                "ChildrenIsolatorUnionExec child {i} has a negative load of {}, which is invalid.",
                annotation.soft
            );
        }
        if !annotation.soft.is_finite() {
            return plan_err!(
                "ChildrenIsolatorUnionExec child {i} has a non-finite load of {}, which is invalid.",
                annotation.soft
            );
        }
    }

    // Step 2: turn loads into fractional shares of the full budget.
    //
    // +------------------+---------+---------+---------+---------+
    // |                  | c0      | c1      | c2      | c3      |
    // +------------------+---------+---------+---------+---------+
    // | load             | 4       | 2       | 0       | 0       |
    // | fractional share | 2.667   | 1.333   | 0       | 0       |
    // +------------------+---------+---------+---------+---------+
    //
    // Total load = 6. Multiply each load by budget / total = 4/6.
    // Fractional values in these tables are shown to three decimal places.
    //
    // Exact counts do not reserve slots yet, so trivial exact children do not take a flexible
    // sibling's share. If ALL loads are zero, split max(budget - sum(exact), 0) evenly
    // among flexible children instead; exact children receive their exact counts in step 4.
    let exact_total = children
        .iter()
        .filter_map(|child| match child.restriction {
            TaskCountRestriction::Exact(exact) => Some(exact.get()),
            _ => None,
        })
        .fold(0usize, usize::saturating_add);
    let total_weight: f64 = children.iter().map(|child| child.soft).sum();
    let child_count = children.len();
    let flexible_count = children
        .iter()
        .filter(|child| !matches!(child.restriction, TaskCountRestriction::Exact(_)))
        .count();

    let unrounded_child_task_counts: Vec<f64> = if total_weight > 0.0 {
        children
            .iter()
            .map(|child| task_count_budget as f64 * child.soft / total_weight)
            .collect()
    } else if flexible_count > 0 {
        let flexible_budget = task_count_budget.saturating_sub(exact_total);
        children
            .iter()
            .map(|child| {
                if matches!(child.restriction, TaskCountRestriction::Exact(_)) {
                    0.0
                } else {
                    flexible_budget as f64 / flexible_count as f64
                }
            })
            .collect()
    } else {
        vec![0.0; child_count]
    };

    // Step 3: round shares using the largest remainders (lower child index breaks ties).
    //
    // +------------------+---------+---------+---------+---------+
    // |                  | c0      | c1      | c2      | c3      |
    // +------------------+---------+---------+---------+---------+
    // | fractional share | 2.667   | 1.333   | 0       | 0       |
    // | floor            | 2       | 1       | 0       | 0       |
    // | remainder        | 0.667   | 0.333   | 0       | 0       |
    // | rounded count    | 3       | 1       | 0       | 0       |
    // +------------------+---------+---------+---------+---------+
    //
    // Floors sum to 3, leaving one slot. c0 has the largest remainder and receives it.
    //
    // Finish rounding BEFORE imposing exact counts: a nearly zero-load exact child must not
    // consume a slot that should round up to a flexible child. With all-zero loads,
    // defer leftover slots until step 5, after imposing exact counts.
    let mut child_task_counts = unrounded_child_task_counts
        .iter()
        .map(|x| x.floor() as usize)
        .collect::<Vec<_>>();

    let mut order: Vec<usize> = (0..child_count).collect();
    order.sort_by(|&a, &b| {
        let ra = unrounded_child_task_counts[a] - unrounded_child_task_counts[a].floor();
        let rb = unrounded_child_task_counts[b] - unrounded_child_task_counts[b].floor();
        rb.partial_cmp(&ra)
            .unwrap_or(Ordering::Equal)
            .then(a.cmp(&b))
    });
    if total_weight > 0.0 {
        let mut unallocated = task_count_budget.saturating_sub(child_task_counts.iter().sum());
        for &idx in &order {
            if unallocated == 0 {
                break;
            }
            child_task_counts[idx] += 1;
            unallocated -= 1;
        }
    }

    // Step 4: impose each child's exact count or minimum.
    //
    // +------------------+---------+---------+---------+---------+
    // |                  | c0      | c1      | c2      | c3      |
    // +------------------+---------+---------+---------+---------+
    // | rounded count    | 3       | 1       | 0       | 0       |
    // | exact count      | 1       | -       | 1       | -       |
    // | minimum count    | -       | 1       | -       | -       |
    // | imposed count    | 1       | 1       | 1       | 0       |
    // +------------------+---------+---------+---------+---------+
    //
    // c0 drops from 3 to 1; c2 rises from 0 to 1. c1 already meets its minimum.
    // The total is now 3. A minimum can raise a share, but does not cap future growth.
    //
    // This can reduce OR increase the total. If it exceeds the budget, children will share
    // outer slots in step 6; their individual exact counts are never reduced to make them fit.
    for (task_count, annotation) in child_task_counts.iter_mut().zip(children.iter()) {
        *task_count = match annotation.restriction {
            TaskCountRestriction::None => *task_count,
            TaskCountRestriction::Exact(exact) => exact.get(),
            TaskCountRestriction::Min(min) => (*task_count).max(min.get()),
        };
    }

    // Step 5: fill unused slots with the flexible child furthest below its fractional share.
    //
    // +------------------+---------+---------+---------+---------+
    // |                  | c0      | c1      | c2      | c3      |
    // +------------------+---------+---------+---------+---------+
    // | imposed count    | 1       | 1       | 1       | 0       |
    // | eligible deficit | -       | 0.333   | -       | 0       |
    // | final count      | 1       | 2       | 1       | 0       |
    // +------------------+---------+---------+---------+---------+
    //
    // One slot remains. c1 has the largest eligible deficit (share - count) and receives it.
    //
    // Recompute deficits after each assignment, breaking ties by lower child index. Never
    // change exact counts here. If all children are exact, surplus outer slots remain empty.
    let allocated_task_counts: usize = child_task_counts.iter().sum();
    let mut unallocated_task_counts = task_count_budget.saturating_sub(allocated_task_counts);
    while unallocated_task_counts > 0 {
        let Some(idx) = (0..child_count)
            .filter(|&idx| !matches!(children[idx].restriction, TaskCountRestriction::Exact(_)))
            .max_by(|&a, &b| {
                let a_deficit = unrounded_child_task_counts[a] - child_task_counts[a] as f64;
                let b_deficit = unrounded_child_task_counts[b] - child_task_counts[b] as f64;
                a_deficit
                    .partial_cmp(&b_deficit)
                    .unwrap_or(Ordering::Equal)
                    .then(b.cmp(&a))
            })
        else {
            // Every child has an exact count; leftover budget becomes empty slots.
            break;
        };
        child_task_counts[idx] += 1;
        unallocated_task_counts -= 1;
    }

    // Step 6: place child task contexts into consecutive outer slots, wrapping at the budget.
    //
    // +------------------+---------+---------+---------+---------+
    // |                  | c0      | c1      | c2      | c3      |
    // +------------------+---------+---------+---------+---------+
    // | final count      | 1       | 2       | 1       | 0       |
    // +------------------+---------+---------+---------+---------+
    // | t0               | 0/1     | -       | -       | -       |
    // | t1               | -       | 0/2     | -       | -       |
    // | t2               | -       | 1/2     | -       | -       |
    // | t3               | -       | -       | 0/1     | -       |
    // +------------------+---------+---------+---------+---------+
    //
    // Each occupied cell is a child context (task index / task count).
    // Sharing example with budget=2: after placing c0 in t0 and t1, c1 wraps back to t0.
    //
    // +------------------+---------+---------+
    // |                  | c0      | c1      |
    // +------------------+---------+---------+
    // | load             | 2       | 0       |
    // | exact count      | -       | 1       |
    // | final count      | 2       | 1       |
    // +------------------+---------+---------+
    // | t0               | 0/2     | 0/1     |
    // | t1               | 1/2     | -       |
    // +------------------+---------+---------+
    //
    // Each child's count fits the budget, so it cannot occur twice in the same outer slot.
    let mut result = vec![vec![]; task_count_budget];
    let mut task_idx = 0;
    for (child_idx, &task_count) in child_task_counts.iter().enumerate() {
        for task_i in 0..task_count {
            result[task_idx % task_count_budget].push((
                child_idx,
                DistributedTaskContext {
                    task_index: task_i,
                    task_count,
                },
            ));
            task_idx += 1;
        }
    }

    // Step 7: run every zero-allocation child once, sharing already occupied slots.
    //
    // +------------------+---------+---------+---------+---------+
    // |                  | c0      | c1      | c2      | c3      |
    // +------------------+---------+---------+---------+---------+
    // | t0               | 0/1     | -       | -       | 0/1     |
    // | t1               | -       | 0/2     | -       | -       |
    // | t2               | -       | 1/2     | -       | -       |
    // | t3               | -       | -       | 0/1     | -       |
    // +------------------+---------+---------+---------+---------+
    //
    // c3 received zero slots, so add it to t0 with context 0/1 to produce its rows.
    //
    // Additional zero-allocation children go to t1, t2, ... round-robin, each with context
    // (0/1). This preserves every child's output without taking slots from heavier children.
    if task_idx > 0 {
        let mut zero_alloc_i = 0usize;
        for (child_idx, &task_count) in child_task_counts.iter().enumerate() {
            if task_count != 0 {
                continue;
            }
            let slot = zero_alloc_i % task_idx.min(task_count_budget);
            result[slot].push((
                child_idx,
                DistributedTaskContext {
                    task_index: 0,
                    task_count: 1,
                },
            ));
            zero_alloc_i += 1;
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    #[test]
    fn children_split_all_1_task() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            split_children(&[load(1.0), load(1.0), load(1.0)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))]
            ]
        );
        assert_eq!(
            split_children(&[load(1.0), load(1.0), load(1.0)], 2)?,
            // Floor = [0,0,0]. The remainder pass gives one slot each to c0 and c1 (tiebreak
            // by lower index); c2 rounds to zero and is distributed round-robin: slot 0 % 2 = 0.
            vec![vec![(0, ctx(0, 1)), (2, ctx(0, 1))], vec![(1, ctx(0, 1))]]
        );
        assert_eq!(
            split_children(&[load(1.0), load(1.0), load(1.0)], 1)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1)), (2, ctx(0, 1))]]
        );
        Ok(())
    }

    #[test]
    fn split_children_different_tasks() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            split_children(&[load(1.0), load(2.0), load(3.0)], 6)?,
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
            split_children(&[load(1.0), load(2.0), load(3.0)], 5)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );
        assert_eq!(
            split_children(&[load(1.0), load(2.0), load(3.0)], 4)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );
        assert_eq!(
            split_children(&[load(1.0), load(2.0), load(3.0)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))],
            ]
        );
        assert_eq!(
            split_children(&[load(1.0), load(2.0), load(3.0)], 2)?,
            // Floor = [0, 0, 1] (only c2's share is ≥ 1). Remainder of 1 goes to c1 (highest
            // fractional remainder). c0 rounds to zero and is distributed round-robin: slot 0 % 2 = 0.
            vec![vec![(1, ctx(0, 1)), (0, ctx(0, 1))], vec![(2, ctx(0, 1))]]
        );
        assert_eq!(
            split_children(&[load(1.0), load(2.0), load(3.0)], 1)?,
            // Only c2 (the highest weight) wins the single slot via the remainder pass; c0
            // and c1 pack onto it.
            vec![vec![(2, ctx(0, 1)), (0, ctx(0, 1)), (1, ctx(0, 1))]]
        );
        Ok(())
    }

    /// Regression test for a production planner bug: the budget can legitimately exceed the
    /// sum of children weights (when a sibling subtree in the same stage drives the stage
    /// budget up). The CIU redistributes the surplus proportionally rather than rejecting it.
    #[test]
    fn split_children_budget_exceeds_children_weight_sum() -> Result<(), Box<dyn std::error::Error>>
    {
        // weights=[1,1], budget=3 → fractional shares of 1.5 each; lower-index child wins the
        // tiebreak and absorbs the surplus, getting 2 task slots; the other gets 1.
        assert_eq!(
            split_children(&[load(1.0), load(1.0)], 3)?,
            vec![
                vec![(0, ctx(0, 2))],
                vec![(0, ctx(1, 2))],
                vec![(1, ctx(0, 1))],
            ]
        );
        // weights=[1,1], budget=5 → fractional shares of 2.5 each; tiebreak gives the extra to
        // the lower-index child.
        assert_eq!(
            split_children(&[load(1.0), load(1.0)], 5)?,
            vec![
                vec![(0, ctx(0, 3))],
                vec![(0, ctx(1, 3))],
                vec![(0, ctx(2, 3))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
            ]
        );
        // weights=[1,2], budget=4 → shares of 4/3≈1.33 and 8/3≈2.67; floors are [1,2] with one
        // leftover, awarded to the larger-remainder child (idx 1).
        assert_eq!(
            split_children(&[load(1.0), load(2.0)], 4)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 3))],
                vec![(1, ctx(1, 3))],
                vec![(1, ctx(2, 3))],
            ]
        );
        Ok(())
    }

    /// A child whose proportional share rounds down to zero doesn't steal a slot from heavier
    /// children — instead it's packed into the last occupied task slot, so its data still
    /// gets produced without disturbing the proportional layout for the heavy children.
    #[test]
    fn split_children_packs_zero_share_children_into_last_slot()
    -> Result<(), Box<dyn std::error::Error>> {
        // weights=[10, 1, 1], budget=3 → child 0 wins the budget (2.5 → 3 via largest-remainder);
        // children 1 and 2 round down to 0 and are distributed round-robin: c1 → slot 0, c2 → slot 1.
        assert_eq!(
            split_children(&[load(10.0), load(1.0), load(1.0)], 3)?,
            vec![
                vec![(0, ctx(0, 3)), (1, ctx(0, 1))],
                vec![(0, ctx(1, 3)), (2, ctx(0, 1))],
                vec![(0, ctx(2, 3))],
            ]
        );
        Ok(())
    }

    /// Exact counts are reserved before the remaining slots are shared by flexible children.
    #[test]
    fn split_children_respects_exact_counts() -> Result<(), Box<dyn std::error::Error>> {
        // Two children both require 1. Budget 3 → can only hand out 2 (one per child),
        // the third slot stays empty.
        assert_eq!(
            split_children(&[exact(1), exact(1)], 3)?,
            vec![vec![(0, ctx(0, 1))], vec![(1, ctx(0, 1))], vec![]]
        );

        // Tiny loads do not force exact-one children into separate union tasks.
        assert_eq!(
            split_children(
                &[TaskCountAnnotation::exact(NonZeroUsize::MIN, 0.0001); 3],
                1
            )?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1)), (2, ctx(0, 1))]]
        );

        // One exact at 1, one unconstrained with weight 1. Budget 3 → c1 absorbs
        // the surplus and ends up running in 2 tasks.
        assert_eq!(
            split_children(&[exact(1), load(1.0)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
            ]
        );

        // c0 requires 2; unconstrained siblings divide the remaining four slots.
        assert_eq!(
            split_children(&[exact(2), load(1.0), load(1.0)], 6)?,
            vec![
                vec![(0, ctx(0, 2))],
                vec![(0, ctx(1, 2))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );

        // All children exact, and budget matches the sum — no surplus, no empty
        // slots.
        assert_eq!(
            split_children(&[exact(2), exact(1)], 3)?,
            vec![
                vec![(0, ctx(0, 2))],
                vec![(0, ctx(1, 2))],
                vec![(1, ctx(0, 1))],
            ]
        );

        // A trivial exact child shares a slot so a flexible sibling can use the full budget.
        assert_eq!(
            split_children(
                &[
                    load(2.0),
                    TaskCountAnnotation::exact(NonZeroUsize::MIN, 0.0)
                ],
                2
            )?,
            vec![vec![(0, ctx(0, 2)), (1, ctx(0, 1))], vec![(0, ctx(1, 2))],]
        );

        // A large load lets the sibling share a slot without reducing c0's exact count.
        assert_eq!(
            split_children(&[exact(2), load(10.0)], 3)?,
            vec![
                vec![(0, ctx(0, 2)), (1, ctx(1, 2))],
                vec![(0, ctx(1, 2))],
                vec![(1, ctx(0, 2))],
            ]
        );

        // Both children keep their exact counts when their combined count exceeds the budget.
        assert_eq!(
            split_children(&[exact(2), exact(2)], 3)?,
            vec![
                vec![(0, ctx(0, 2)), (1, ctx(1, 2))],
                vec![(0, ctx(1, 2))],
                vec![(1, ctx(0, 2))],
            ]
        );
        Ok(())
    }

    /// All-zero weights are valid: the budget is split evenly across children.
    #[test]
    fn split_children_all_zero_weights_splits_evenly() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            split_children(&[load(0.0), load(0.0), load(0.0)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))],
            ]
        );
        Ok(())
    }

    #[test]
    fn split_children_minimums_allow_sharing_and_growth() -> Result<(), DataFusionError> {
        let minimum = TaskCountAnnotation::min(NonZeroUsize::new(2).unwrap(), 0.0);
        assert_eq!(
            split_children(&[minimum, load(4.0)], 4)?,
            vec![
                vec![(0, ctx(0, 2)), (1, ctx(2, 4))],
                vec![(0, ctx(1, 2)), (1, ctx(3, 4))],
                vec![(1, ctx(0, 4))],
                vec![(1, ctx(1, 4))],
            ]
        );
        // A minimum does not prevent a child from using more tasks.
        assert_eq!(
            split_children(
                &[
                    TaskCountAnnotation::min(NonZeroUsize::new(2).unwrap(), 4.0),
                    load(2.0)
                ],
                6,
            )?,
            vec![
                vec![(0, ctx(0, 4))],
                vec![(0, ctx(1, 4))],
                vec![(0, ctx(2, 4))],
                vec![(0, ctx(3, 4))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
            ]
        );
        Ok(())
    }

    #[test]
    fn split_children_minimums_must_fit_budget() {
        let err = split_children(
            &[TaskCountAnnotation::min(NonZeroUsize::new(2).unwrap(), 1.0)],
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("requires at least 2 tasks"));
    }

    /// Negative and non-finite weights are rejected upfront.
    #[test]
    fn split_children_rejects_negative_weight() {
        let err = split_children(&[load(1.0), load(-1.0), load(1.0)], 3).unwrap_err();
        assert!(
            err.to_string().contains("negative"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn split_children_rejects_nan_weight() {
        let err = split_children(&[load(f64::NAN), load(1.0)], 2).unwrap_err();
        assert!(
            err.to_string().contains("non-finite"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn split_children_rejects_infinite_weight() {
        let err = split_children(&[load(1.0), load(f64::INFINITY)], 2).unwrap_err();
        assert!(
            err.to_string().contains("non-finite"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn split_children_rejects_exact_count_above_budget() {
        let err = split_children(&[exact(3), load(1.0)], 2).unwrap_err();
        assert!(
            err.to_string().contains("requires 3 tasks"),
            "unexpected error: {err}"
        );
    }

    fn ctx(task_index: usize, task_count: usize) -> DistributedTaskContext {
        DistributedTaskContext {
            task_index,
            task_count,
        }
    }

    /// Shorthand for a load estimate — keeps the unit tests readable.
    fn load(w: f64) -> TaskCountAnnotation {
        TaskCountAnnotation::soft(w)
    }

    /// Shorthand for an exact task count — keeps the unit tests readable.
    fn exact(n: usize) -> TaskCountAnnotation {
        TaskCountAnnotation::exact(
            NonZeroUsize::new(n).expect("exact count must be nonzero"),
            n,
        )
    }
}
