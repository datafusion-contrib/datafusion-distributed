use crate::common::require_one_child;
use crate::distributed_planner::NetworkBoundary;
use crate::execution_plans::common::scale_partitioning_props;
use crate::stage::{LocalStage, Stage};
use crate::worker::WorkerConnectionPool;
use crate::{DistributedTaskContext, ExchangeAssignment, SlotReadPlan};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{exec_err, internal_datafusion_err, not_impl_err, plan_err};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, PlanProperties,
    internal_err,
};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use uuid::Uuid;

/// [ExecutionPlan] that coalesces partitions from multiple tasks into a one or more task without
/// performing any repartition, and maintaining the same partitioning scheme.
///
/// This is the equivalent of a [CoalescePartitionsExec] but coalescing tasks across the network
/// between distributed stages.
///
/// ```text
///                                ┌───────────────────────────┐                                   ■
///                                │    NetworkCoalesceExec    │                                   │
///                                │         (task 1)          │                                   │
///                                └┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┘                                Stage N+1
///                                 │1││2││3││4││5││6││7││8││9│                                    │
///                                 └─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘                                    │
///                                 ▲  ▲  ▲   ▲  ▲  ▲   ▲  ▲  ▲                                    ■
///   ┌──┬──┬───────────────────────┴──┴──┘   │  │  │   └──┴──┴──────────────────────┬──┬──┐
///   │  │  │                                 │  │  │                                │  │  │       ■
///  ┌─┐┌─┐┌─┐                               ┌─┐┌─┐┌─┐                              ┌─┐┌─┐┌─┐      │
///  │1││2││3│                               │4││5││6│                              │7││8││9│      │
/// ┌┴─┴┴─┴┴─┴──────────────────┐  ┌─────────┴─┴┴─┴┴─┴─────────┐ ┌──────────────────┴─┴┴─┴┴─┴┐  Stage N
/// │  Arc<dyn ExecutionPlan>   │  │  Arc<dyn ExecutionPlan>   │ │  Arc<dyn ExecutionPlan>   │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// The communication between two stages across a [NetworkCoalesceExec] has two implications:
///
/// - Stage N+1 may have one or more tasks. Each consumer task reads a contiguous group of upstream
///   tasks from Stage N.
/// - Output partitioning for Stage N+1 is sized based on the maximum upstream-group size. When
///   groups are uneven, consumer tasks with smaller groups return empty streams for the “extra”
///   partitions.
/// ```text
///                    ┌───────────────────────────┐        ┌───────────────────────────┐          ■
///                    │    NetworkCoalesceExec    │        │    NetworkCoalesceExec    │          │
///                    │         (task 1)          │        │         (task 2)          │          │
///                    └┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬─────────┘        └┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬─────────┘       Stage N+1
///                     │1││2││3││4││5││6│                   │7││8││9││_││_││_│                    │
///                     └─┘└─┘└─┘└─┘└─┘└─┘                   └─┘└─┘└─┘└─┘└─┘└─┘                    │
///                      ▲  ▲  ▲  ▲  ▲  ▲                     ▲  ▲  ▲                              ■
///   ┌──┬──┬────────────┴──┴──┘  └──┴──┴─────┬──┬──┐         └──┴──┴────────────────┬──┬──┐
///   │  │  │                                 │  │  │                                │  │  │       ■
///  ┌─┐┌─┐┌─┐                               ┌─┐┌─┐┌─┐                              ┌─┐┌─┐┌─┐      │
///  │1││2││3│                               │4││5││6│                              │7││8││9│      │
/// ┌┴─┴┴─┴┴─┴──────────────────┐  ┌─────────┴─┴┴─┴┴─┴─────────┐ ┌──────────────────┴─┴┴─┴┴─┴┐  Stage N
/// │  Arc<dyn ExecutionPlan>   │  │  Arc<dyn ExecutionPlan>   │ │  Arc<dyn ExecutionPlan>   │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// This node has two variants.
/// 1. Pending: acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///    using Arrow Flight.
#[derive(Debug, Clone)]
pub struct NetworkCoalesceExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: Arc<PlanProperties>,
    pub(crate) input_stage: Stage,
    pub(crate) assignment: Option<Arc<ExchangeAssignment>>,
    pub(crate) worker_connections: WorkerConnectionPool,
}

impl NetworkCoalesceExec {
    /// Does nothing, but it's here for explicitly stating that this network boundary does not
    /// need to mutate the input plan in other to account for more consumer tasks.
    pub(crate) fn scale_input(
        plan: Arc<dyn ExecutionPlan>,
        _consumer_partitions: usize,
        _consumer_task_count: usize,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        Ok(Transformed::no(plan))
    }

    pub(crate) fn from_stage(input_stage: LocalStage, consumer_tasks: usize) -> Self {
        // Each output task coalesces a group of input tasks. We size the output partition count
        // per output task based on the maximum group size, returning empty streams for tasks with
        // smaller groups.
        let max_input_task_count = input_stage.tasks.div_ceil(consumer_tasks).max(1);
        let props =
            scale_partitioning_props(input_stage.plan.properties(), |p| p * max_input_task_count);

        Self {
            properties: props,
            worker_connections: WorkerConnectionPool::new(0),
            input_stage: Stage::Local(input_stage),
            assignment: None,
        }
    }

    pub(crate) fn with_exchange_assignment(&self, assignment: Arc<ExchangeAssignment>) -> Self {
        let mut this = self.clone();
        this.properties = scale_partitioning_props(&this.properties, |_| {
            assignment.max_partition_count_per_consumer()
        });
        this.assignment = Some(assignment);
        this
    }

    fn exchange_assignment(&self) -> Result<&ExchangeAssignment> {
        self.assignment.as_deref().ok_or_else(|| {
            internal_datafusion_err!("NetworkCoalesceExec is missing exchange assignment")
        })
    }

    /// Creates a new [NetworkCoalesceExec] fed by the provided `input` plan.
    ///
    /// The `input` plan will be remotely executed in `producer_tasks` tasks, while the
    /// [NetworkCoalesceExec] will be executed in `consumer_tasks` tasks in the stage above.
    ///
    /// Typically, this node should be placed right after nodes that coalesce all the input
    /// partitions into one, for example:
    /// - [CoalescePartitionsExec]
    /// - [SortPreservingMergeExec]
    ///
    /// ## Warning
    ///
    /// The caller must ensure that the provided `consumer_tasks` count matches the `producer_tasks`
    /// of the network boundary immediately above.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        producer_tasks: usize,
        consumer_tasks: usize,
    ) -> Result<Self> {
        if consumer_tasks == 0 {
            return plan_err!("The `consumer_tasks` input of a NetworkCoalesceExec must not be 0");
        }
        Ok(Self::from_stage(
            LocalStage {
                // At this point, query_id and num are just placeholders that will be filled by
                // prepare_network_boundaries.rs. Users are not expected to provide valid values for
                // these two parameters.
                query_id: Uuid::nil(),
                num: 0,
                plan: input,
                tasks: producer_tasks,
            },
            consumer_tasks,
        ))
    }
}

impl NetworkBoundary for NetworkCoalesceExec {
    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }

    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.clone();
        self_clone.properties = scale_partitioning_props(self_clone.properties(), |p| {
            p * input_stage.task_count() / self_clone.input_stage.task_count().max(1)
        });
        self_clone.worker_connections = WorkerConnectionPool::new(input_stage.task_count());
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }
}

impl DisplayAs for NetworkCoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_tasks = self.input_stage.task_count();
        let partitions = self.properties.partitioning.partition_count();
        let stage = self.input_stage.num();
        write!(
            f,
            "[Stage {stage}] => NetworkCoalesceExec: output_partitions={partitions}, input_tasks={input_tasks}",
        )
    }
}

impl ExecutionPlan for NetworkCoalesceExec {
    fn name(&self) -> &str {
        "NetworkCoalesceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.input_stage.local_plan() {
            Some(v) => vec![v],
            None => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.as_ref().clone();
        match &mut self_clone.input_stage {
            Stage::Local(local) => {
                local.plan = require_one_child(children)?;
            }
            Stage::Remote(_) => not_impl_err!("NetworkBoundary cannot accept children")?,
        }
        Ok(Arc::new(self_clone))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let remote_stage = match &self.input_stage {
            Stage::Local(local) => return local.execute(partition, context),
            Stage::Remote(remote_stage) => remote_stage,
        };

        let task_context = DistributedTaskContext::from_ctx(&context);
        if task_context.task_index >= task_context.task_count {
            return exec_err!(
                "NetworkCoalesceExec invalid task context: task_index={} >= task_count={}",
                task_context.task_index,
                task_context.task_count
            );
        }

        let assignment = self.exchange_assignment()?;
        if task_context.task_count != assignment.consumer_task_count() {
            return exec_err!(
                "NetworkCoalesceExec task count {} does not match exchange assignment consumer task count {}",
                task_context.task_count,
                assignment.consumer_task_count()
            );
        }
        if partition >= assignment.max_partition_count_per_consumer() {
            return exec_err!(
                "NetworkCoalesceExec partition {} >= assignment partition count {}",
                partition,
                assignment.max_partition_count_per_consumer()
            );
        }

        let Some(read_plan) = assignment.resolve_slot(task_context.task_index, partition) else {
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())));
        };
        let SlotReadPlan::Single {
            producer_task,
            producer_partition,
        } = read_plan
        else {
            return internal_err!("NetworkCoalesceExec expected single-task exchange slot");
        };
        let Some(partition_range) =
            assignment.partition_range_for_consumer(task_context.task_index)
        else {
            return exec_err!(
                "NetworkCoalesceExec cannot resolve partition range for task {}",
                task_context.task_index
            );
        };

        let worker_connection = self.worker_connections.get_or_init_worker_connection(
            remote_stage,
            partition_range,
            producer_task,
            &context,
        )?;

        let stream = worker_connection.stream_partition(producer_partition, |_meta| {})?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.worker_connections.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::empty::EmptyExec;

    #[derive(Clone, Copy)]
    struct Case {
        name: &'static str,
        input_tasks: usize,
        consumer_tasks: usize,
    }

    fn expected_groups(input_tasks: usize, consumer_tasks: usize) -> Vec<(usize, usize)> {
        assert!(consumer_tasks > 0, "consumer_tasks must be non-zero");

        let base_tasks_per_group = input_tasks / consumer_tasks;
        let groups_with_extra_task = input_tasks % consumer_tasks;
        let mut groups = Vec::with_capacity(consumer_tasks);
        let mut start_task = 0;

        for task_index in 0..consumer_tasks {
            let len = base_tasks_per_group + usize::from(task_index < groups_with_extra_task);
            groups.push((start_task, len));
            start_task += len;
        }

        groups
    }

    fn assert_case(case: Case) -> Result<()> {
        // Child plan used only for properties/schema (we won't reach network codepaths).
        let child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let child_partitions = child.properties().partitioning.partition_count();

        let exec = NetworkCoalesceExec::try_new(
            Arc::clone(&child),
            case.input_tasks,
            case.consumer_tasks,
        )?;

        // Output partitions are sized by the maximum group size.
        let max_group_size = case.input_tasks.div_ceil(case.consumer_tasks).max(1);
        assert_eq!(
            exec.properties().partitioning.partition_count(),
            child_partitions * max_group_size
        );

        let groups = expected_groups(case.input_tasks, case.consumer_tasks);
        assert_eq!(groups.len(), case.consumer_tasks);

        let mut seen = vec![false; case.input_tasks];
        let mut expected_start = 0;
        let mut padding_slots = 0;

        for (index, (start, len)) in groups.into_iter().enumerate() {
            assert_eq!(
                start, expected_start,
                "case {} group {} should be contiguous",
                case.name, index
            );
            assert!(
                start + len <= case.input_tasks,
                "case {} group {} exceeds input task count",
                case.name,
                index
            );

            for (offset, seen_task) in seen.iter_mut().skip(start).take(len).enumerate() {
                let task = start + offset;
                assert!(
                    !*seen_task,
                    "case {} input task {} appears twice",
                    case.name, task
                );
                *seen_task = true;
            }

            expected_start = start + len;
            padding_slots += max_group_size - len;
        }

        assert_eq!(
            expected_start, case.input_tasks,
            "case {} groups should cover all input tasks",
            case.name
        );
        assert!(
            seen.iter().all(|v| *v),
            "case {} missing at least one input task",
            case.name
        );

        let total_slots = case.consumer_tasks * max_group_size;
        let total_padding = total_slots - case.input_tasks;
        assert_eq!(
            padding_slots, total_padding,
            "case {} padding slots mismatch",
            case.name
        );

        Ok(())
    }

    const ONE_TO_MANY_INPUT: usize = 1;
    const ONE_TO_MANY_OUTPUT: usize = 3;
    const MANY_TO_ONE_INPUT: usize = 4;
    const MANY_TO_ONE_OUTPUT: usize = 1;
    const MANY_TO_FEWER_INPUT: usize = 5;
    const MANY_TO_FEWER_OUTPUT: usize = 2;
    const FEWER_TO_MANY_INPUT: usize = 2;
    const FEWER_TO_MANY_OUTPUT: usize = 5;

    #[test]
    fn validates_partition_coverage_one_to_many() -> Result<()> {
        assert_case(Case {
            name: "1_to_n",
            input_tasks: ONE_TO_MANY_INPUT,
            consumer_tasks: ONE_TO_MANY_OUTPUT,
        })
    }

    #[test]
    fn validates_partition_coverage_many_to_one() -> Result<()> {
        assert_case(Case {
            name: "n_to_1",
            input_tasks: MANY_TO_ONE_INPUT,
            consumer_tasks: MANY_TO_ONE_OUTPUT,
        })
    }

    #[test]
    fn validates_partition_coverage_many_to_fewer() -> Result<()> {
        assert_case(Case {
            name: "n_to_m_n_gt_m",
            input_tasks: MANY_TO_FEWER_INPUT,
            consumer_tasks: MANY_TO_FEWER_OUTPUT,
        })
    }

    #[test]
    fn validates_partition_coverage_fewer_to_many() -> Result<()> {
        assert_case(Case {
            name: "m_to_n_n_gt_m",
            input_tasks: FEWER_TO_MANY_INPUT,
            consumer_tasks: FEWER_TO_MANY_OUTPUT,
        })
    }
}
