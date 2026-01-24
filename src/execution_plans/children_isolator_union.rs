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
        children: Vec<Arc<dyn ExecutionPlan>>,
        task_idx_map: Vec<
            /* outer distributed task idx */
            Vec<(
                /* child index */ usize,
                /* inner distributed task ctx for the isolated child*/
                DistributedTaskContext,
            )>,
        >,
    ) -> Result<Self, DataFusionError> {
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
