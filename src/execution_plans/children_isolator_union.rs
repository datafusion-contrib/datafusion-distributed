use crate::DistributedTaskContext;
use crate::common::task_ctx_with_extension;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::empty::EmptyExec;
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
    pub(crate) properties: Arc<PlanProperties>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) children: Vec<Arc<dyn ExecutionPlan>>,
    /// The original per-child weights (and their optional hard caps) used to build the
    /// `task_idx_map`. Stored so `with_new_children` can re-run the allocator with the same
    /// inputs and preserve `Maximum(N)` caps across plan rewrites.
    pub(crate) child_weights: Vec<ChildWeight>,
    pub(crate) task_idx_map: Vec<
        /* outer distributed task idx */
        Vec<(
            /* child index */ usize,
            /* inner distributed task ctx for the isolated child*/ DistributedTaskContext,
        )>,
    >,
}

/// Per-child allocation hint passed to [`ChildrenIsolatorUnionExec::from_children_and_weights`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChildWeight {
    /// Weight relative to other children. The higher the weight vs other children, the more tasks
    /// will be allocated to it.
    pub(crate) weight: f64,
    /// Maximum task count cap for this child. While allocating tasks for this child, it cannot
    /// exceed the specified `max` no matter its `weight`
    pub(crate) max: Option<usize>,
}

impl ChildWeight {
    /// Convenience: a child with relative weight `w` and no cap.
    pub fn desired(w: f64) -> Self {
        Self {
            weight: w,
            max: None,
        }
    }

    /// Convenience: a child whose relative weight equals its hard cap `n`.
    pub fn maximum(n: usize) -> Self {
        Self {
            weight: n as f64,
            max: Some(n),
        }
    }
}

impl ChildrenIsolatorUnionExec {
    pub(crate) fn from_children_and_weights(
        children: impl IntoIterator<Item = Arc<dyn ExecutionPlan>>,
        children_weights: impl IntoIterator<Item = ChildWeight>,
        task_count: usize,
    ) -> Result<Self, DataFusionError> {
        let children = children.into_iter().collect_vec();
        let weights = children_weights.into_iter().collect_vec();

        if children.len() != weights.len() {
            return internal_err!(
                "ChildrenIsolatorUnionExec received {} children but a vec of {} weights for those children. This is a bug in the distributed planning logic, please report it",
                children.len(),
                weights.len()
            );
        }

        let task_idx_map = split_children(&weights, task_count)?;

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
            child_weights: weights,
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
                    false => Arc::new(EmptyExec::new(plan.schema())),
                },
            )
            .collect_vec();
        Self {
            children: new_children,
            properties: self.properties.clone(),
            metrics: self.metrics.clone(),
            child_weights: self.child_weights.clone(),
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

    fn as_any(&self) -> &dyn Any {
        self
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
        Ok(Arc::new(Self::from_children_and_weights(
            children,
            self.child_weights.clone(),
            self.task_idx_map.len(),
        )?))
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

/// Given a per-child [`ChildWeight`] slice and a `task_count_budget`, distribute the budget
/// across children proportional to their weights, honoring any per-child caps.
///
/// ## Examples (read alongside the unit tests for the full picture):
///
/// ```text
///   weights: [w(1), w(1), w(1)], budget: 3  →  one task slot per child
///       [[(0, 0/1)], [(1, 0/1)], [(2, 0/1)]]
///
///   weights: [w(1), w(2), w(3)], budget: 6  →  weights match the budget exactly
///       [[(0, 0/1)], [(1, 0/2)], [(1, 1/2)], [(2, 0/3)], [(2, 1/3)], [(2, 2/3)]]
///
///   weights: [w(1), w(1)], budget: 3  →  more budget than the total weight — the heavier-
///       share child (after tiebreak) covers two slots:
///       [[(0, 0/2)], [(0, 1/2)], [(1, 0/1)]]
///
///   weights: [Maximum(1), Maximum(1)], budget: 3  →  both children are capped at 1; the
///       third slot stays empty:
///       [[(0, 0/1)], [(1, 0/1)], []]
///
///   weights: [Maximum(1), w(1)], budget: 3  →  the capped child gets exactly 1 slot, the
///       uncapped sibling absorbs the surplus:
///       [[(0, 0/1)], [(1, 0/2)], [(1, 1/2)]]
///
///   weights: [w(10), w(1), w(1)], budget: 3  →  child 0's proportional share is 2.5
///       (rounds up to 3 via the largest-remainder pass); children 1 and 2 round down to 0
///       and are distributed round-robin across occupied slots instead of stealing one from child 0:
///       [[(0, 0/3), (1, 0/1)], [(0, 1/3), (2, 0/1)], [(0, 2/3)]]
/// ```
fn split_children(
    children: &[ChildWeight],
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
    for (i, weight) in children.iter().enumerate() {
        if weight.max == Some(0) {
            return plan_err!(
                "ChildrenIsolatorUnionExec child {i} has a max task count of 0, which is invalid"
            );
        }
        if weight.weight < 0.0 {
            return plan_err!(
                "ChildrenIsolatorUnionExec child {i} has a negative desired wait of {}, which is invalid.",
                weight.weight
            );
        }
        if !weight.weight.is_finite() {
            return plan_err!(
                "ChildrenIsolatorUnionExec child {i} has a non-finite desired wait of {}, which is invalid.",
                weight.weight
            );
        }
    }

    // Two running examples (A and B) traced through every step below.
    // A: [w(10), w(1), w(1)], budget=3   (heavy child dominates)
    // B: [max(1), w(1)],      budget=3   (cap kicks in during remainder pass)
    let child_weights: Vec<f64> = children.iter().map(|w| w.weight).collect();
    let total_weight: f64 = child_weights.iter().sum(); // A→12.0  B→2.0
    let child_count = children.len();

    // Ideal share per child = budget * w_i / Σw.
    // A: [2.5, 0.25, 0.25]
    // B: [1.5, 1.5]
    let unrounded_child_task_counts: Vec<f64> = if total_weight > 0.0 {
        child_weights
            .iter()
            .map(|w| task_count_budget as f64 * w / total_weight)
            .collect()
    } else {
        // All weights zero → even split.
        vec![task_count_budget as f64 / child_count as f64; child_count]
    };

    // Floor each share, then clamp by any per-child cap.
    // A: floor:[2,0,0] cap:unchanged
    // B: floor:[1,1]   cap:c0        min(1,1)=1 → unchanged
    let mut child_task_counts = unrounded_child_task_counts
        .iter()
        .map(|x| x.floor() as usize)
        .collect::<Vec<_>>();
    for (task_count, child_weight) in child_task_counts.iter_mut().zip(children.iter()) {
        if let Some(max) = child_weight.max {
            *task_count = (*task_count).min(max);
        }
    }

    // Hare largest-remainder: give the remaining slots to children with the biggest
    // fractional parts, skipping any already at their cap.
    // A: Σfloors=2, unallocated=1; remainders=[0.5,0.25,0.25] → c0 wins → [3,0,0]
    // B: Σfloors=2, unallocated=1; remainders=[0.5,0.5] → c0 tied-wins but capped → c1 wins → [1,2]
    let allocated_task_counts: usize = child_task_counts.iter().sum();
    let mut unallocated_task_counts = task_count_budget.saturating_sub(allocated_task_counts);
    if unallocated_task_counts > 0 {
        let mut order: Vec<usize> = (0..child_count).collect();
        // Sort descending by fractional part; lower index breaks ties deterministically.
        order.sort_by(|&a, &b| {
            let ra = unrounded_child_task_counts[a] - unrounded_child_task_counts[a].floor();
            let rb = unrounded_child_task_counts[b] - unrounded_child_task_counts[b].floor();
            rb.partial_cmp(&ra)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.cmp(&b))
        });
        while unallocated_task_counts > 0 {
            let mut made_progress = false;
            for &idx in &order {
                if unallocated_task_counts == 0 {
                    break;
                }
                if let Some(max) = children[idx].max
                    && child_task_counts[idx] >= max
                {
                    continue;
                }
                child_task_counts[idx] += 1;
                unallocated_task_counts -= 1;
                made_progress = true;
            }
            if !made_progress {
                // All remaining children are at their cap; leftover budget becomes empty slots.
                break;
            }
        }
    }

    // Lay out each child's alloc in consecutive result slots.
    // A: c0×3 → slots 0=(0,0/3), 1=(0,1/3), 2=(0,2/3); c1,c2 alloc=0 → skipped; task_idx=3
    // B: c0×1 → slot  0=(0,0/1); c1×2 → slots 1=(1,0/2), 2=(1,1/2);       task_idx=3
    let mut result = vec![vec![]; task_count_budget];
    let mut task_idx = 0;
    for (child_idx, &task_count) in child_task_counts.iter().enumerate() {
        for task_i in 0..task_count {
            result[task_idx].push((
                child_idx,
                DistributedTaskContext {
                    task_index: task_i,
                    task_count,
                },
            ));
            task_idx += 1;
        }
    }

    // Distribute zero-alloc children round-robin across occupied slots so their data still
    // gets produced without overpacking a single slot.
    // A: c1 → slot 0, c2 → slot 1: [(0,0/3),(1,0/1)], [(0,1/3),(2,0/1)], [(0,2/3)]
    // B: no zero-alloc children → result unchanged
    if task_idx > 0 {
        let mut zero_alloc_i = 0usize;
        for (child_idx, &task_count) in child_task_counts.iter().enumerate() {
            if task_count != 0 {
                continue;
            }
            let slot = zero_alloc_i % task_idx;
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

    #[test]
    fn children_split_all_1_task() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            split_children(&[des(1.0), des(1.0), des(1.0)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))]
            ]
        );
        assert_eq!(
            split_children(&[des(1.0), des(1.0), des(1.0)], 2)?,
            // Floor = [0,0,0]. The remainder pass gives one slot each to c0 and c1 (tiebreak
            // by lower index); c2 rounds to zero and is distributed round-robin: slot 0 % 2 = 0.
            vec![vec![(0, ctx(0, 1)), (2, ctx(0, 1))], vec![(1, ctx(0, 1))]]
        );
        assert_eq!(
            split_children(&[des(1.0), des(1.0), des(1.0)], 1)?,
            vec![vec![(0, ctx(0, 1)), (1, ctx(0, 1)), (2, ctx(0, 1))]]
        );
        Ok(())
    }

    #[test]
    fn split_children_different_tasks() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            split_children(&[des(1.0), des(2.0), des(3.0)], 6)?,
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
            split_children(&[des(1.0), des(2.0), des(3.0)], 5)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );
        assert_eq!(
            split_children(&[des(1.0), des(2.0), des(3.0)], 4)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );
        assert_eq!(
            split_children(&[des(1.0), des(2.0), des(3.0)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))],
            ]
        );
        assert_eq!(
            split_children(&[des(1.0), des(2.0), des(3.0)], 2)?,
            // Floor = [0, 0, 1] (only c2's share is ≥ 1). Remainder of 1 goes to c1 (highest
            // fractional remainder). c0 rounds to zero and is distributed round-robin: slot 0 % 2 = 0.
            vec![vec![(1, ctx(0, 1)), (0, ctx(0, 1))], vec![(2, ctx(0, 1))]]
        );
        assert_eq!(
            split_children(&[des(1.0), des(2.0), des(3.0)], 1)?,
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
            split_children(&[des(1.0), des(1.0)], 3)?,
            vec![
                vec![(0, ctx(0, 2))],
                vec![(0, ctx(1, 2))],
                vec![(1, ctx(0, 1))],
            ]
        );
        // weights=[1,1], budget=5 → fractional shares of 2.5 each; tiebreak gives the extra to
        // the lower-index child.
        assert_eq!(
            split_children(&[des(1.0), des(1.0)], 5)?,
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
            split_children(&[des(1.0), des(2.0)], 4)?,
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
            split_children(&[des(10.0), des(1.0), des(1.0)], 3)?,
            vec![
                vec![(0, ctx(0, 3)), (1, ctx(0, 1))],
                vec![(0, ctx(1, 3)), (2, ctx(0, 1))],
                vec![(0, ctx(2, 3))],
            ]
        );
        Ok(())
    }

    /// Hard cap (`Maximum(N)`) is honored: a capped child never receives more slots than its
    /// cap, even if its proportional share would be larger. Excess budget is redistributed to
    /// uncapped siblings; if every child is capped, the surplus slots stay empty.
    #[test]
    fn split_children_respects_maximum_caps() -> Result<(), Box<dyn std::error::Error>> {
        // Two children both capped at 1. Budget 3 → can only hand out 2 (one per child),
        // the third slot stays empty.
        assert_eq!(
            split_children(&[max(1), max(1)], 3)?,
            vec![vec![(0, ctx(0, 1))], vec![(1, ctx(0, 1))], vec![]]
        );

        // One capped at 1, one uncapped with weight 1. Budget 3 → c0 stuck at 1, c1 absorbs
        // the surplus and ends up running in 2 tasks.
        assert_eq!(
            split_children(&[max(1), des(1.0)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
            ]
        );

        // Three children: c0 capped at 2, c1 and c2 uncapped with weight 1 each. Budget 6 →
        // c0's proportional share would be 3 but it caps at 2; the saved slot goes to c1
        // (lower-index tiebreak among the uncapped siblings).
        assert_eq!(
            split_children(&[max(2), des(1.0), des(1.0)], 6)?,
            vec![
                vec![(0, ctx(0, 2))],
                vec![(0, ctx(1, 2))],
                vec![(1, ctx(0, 2))],
                vec![(1, ctx(1, 2))],
                vec![(2, ctx(0, 2))],
                vec![(2, ctx(1, 2))],
            ]
        );

        // All children capped, but budget matches the cap sum exactly — no surplus, no empty
        // slots.
        assert_eq!(
            split_children(&[max(2), max(1)], 3)?,
            vec![
                vec![(0, ctx(0, 2))],
                vec![(0, ctx(1, 2))],
                vec![(1, ctx(0, 1))],
            ]
        );
        Ok(())
    }

    /// All-zero weights are valid: the budget is split evenly across children.
    #[test]
    fn split_children_all_zero_weights_splits_evenly() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            split_children(&[des(0.0), des(0.0), des(0.0)], 3)?,
            vec![
                vec![(0, ctx(0, 1))],
                vec![(1, ctx(0, 1))],
                vec![(2, ctx(0, 1))],
            ]
        );
        Ok(())
    }

    /// Negative and non-finite weights are rejected upfront.
    #[test]
    fn split_children_rejects_negative_weight() {
        let err = split_children(&[des(1.0), des(-1.0), des(1.0)], 3).unwrap_err();
        assert!(
            err.to_string().contains("negative"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn split_children_rejects_nan_weight() {
        let err = split_children(&[des(f64::NAN), des(1.0)], 2).unwrap_err();
        assert!(
            err.to_string().contains("non-finite"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn split_children_rejects_infinite_weight() {
        let err = split_children(&[des(1.0), des(f64::INFINITY)], 2).unwrap_err();
        assert!(
            err.to_string().contains("non-finite"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn split_children_rejects_zero_max() {
        let err = split_children(
            &[
                des(1.0),
                ChildWeight {
                    weight: 1.0,
                    max: Some(0),
                },
                des(1.0),
            ],
            3,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("max task count of 0"),
            "unexpected error: {err}"
        );
    }

    fn ctx(task_index: usize, task_count: usize) -> DistributedTaskContext {
        DistributedTaskContext {
            task_index,
            task_count,
        }
    }

    /// Shorthand for `ChildWeight::desired(w)` — keeps the unit tests readable.
    fn des(w: f64) -> ChildWeight {
        ChildWeight::desired(w)
    }

    /// Shorthand for `ChildWeight::maximum(n)` — keeps the unit tests readable.
    fn max(n: usize) -> ChildWeight {
        ChildWeight::maximum(n)
    }
}
