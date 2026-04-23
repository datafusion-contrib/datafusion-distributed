use crate::common::require_one_child;
use crate::distributed_planner::{ExchangeLayout, NetworkBoundary, SlotReadPlan};
use crate::execution_plans::common::map_partitioning_props;
use crate::stage::Stage;
use crate::worker::WorkerConnectionPool;
use crate::{DistributedTaskContext, ExecutionTask};
use datafusion::common::{exec_err, plan_err};
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
    pub(crate) worker_connections: WorkerConnectionPool,
    pub(crate) layout: Arc<ExchangeLayout>,
}

impl NetworkCoalesceExec {
    /// Builds a new [NetworkCoalesceExec] in "Pending" state.
    ///
    /// Typically, this node should be placed right after nodes that coalesce all the input
    /// partitions into one, for example:
    /// - [CoalescePartitionsExec]
    /// - [SortPreservingMergeExec]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        num: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Self> {
        if task_count == 0 {
            return plan_err!("NetworkCoalesceExec cannot be executed with task_count=0");
        }

        let input_partition_count = input.properties().partitioning.partition_count();
        let layout =
            ExchangeLayout::try_coalesce(input_task_count, task_count, input_partition_count)?;
        let max_input_task_count = layout.max_input_task_count_per_consumer().unwrap_or(1);
        Ok(Self {
            properties: map_partitioning_props(input.properties(), |p| p * max_input_task_count),
            input_stage: Stage {
                query_id,
                num,
                plan: Some(input),
                tasks: vec![ExecutionTask { url: None }; input_task_count],
            },
            worker_connections: WorkerConnectionPool::new(input_task_count),
            layout,
        })
    }

    /// The planner-facing layout for this exchange.
    pub fn layout(&self) -> &Arc<ExchangeLayout> {
        &self.layout
    }
}

impl NetworkBoundary for NetworkCoalesceExec {
    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }

    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.clone();
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }
}

impl DisplayAs for NetworkCoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_tasks = self.input_stage.tasks.len();
        let partitions = self.properties.partitioning.partition_count();
        let stage = self.input_stage.num;
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
        match &self.input_stage.plan {
            Some(v) => vec![v],
            None => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = require_one_child(children)?;
        Ok(Arc::new(Self::try_new(
            child,
            self.input_stage.query_id,
            self.input_stage.num,
            self.layout.consumer_task_count(),
            self.layout.producer_task_count(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task_context = DistributedTaskContext::from_ctx(&context);
        let resolver = self.layout.resolver();
        if task_context.task_count != resolver.consumer_task_count() {
            return exec_err!(
                "NetworkCoalesceExec expected task_count={} from layout resolver, got {}",
                resolver.consumer_task_count(),
                task_context.task_count
            );
        }
        if task_context.task_index >= resolver.consumer_task_count() {
            return exec_err!(
                "NetworkCoalesceExec invalid task context: task_index={} >= consumer_tasks={}",
                task_context.task_index,
                resolver.consumer_task_count()
            );
        }

        let Some(SlotReadPlan::Single {
            producer_task: target_task,
            producer_partition: target_partition,
        }) = resolver.resolve_slot(task_context.task_index, partition)
        else {
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())));
        };

        let producer_tasks = resolver.producer_task_range(task_context.task_index);
        if !producer_tasks.contains(&target_task) {
            return internal_err!(
                "NetworkCoalesceExec derived target_task={} outside resolver range {:?}",
                target_task,
                producer_tasks
            );
        }

        let worker_connection = self.worker_connections.get_or_init_worker_connection(
            &self.input_stage,
            0..resolver.partitions_per_producer_task(),
            target_task,
            &context,
        )?;

        let stream = worker_connection.stream_partition(target_partition, |_meta| {})?;

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
        const STAGE_NUM: usize = 1;

        // Child plan used only for properties/schema (we won't reach network codepaths).
        let child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let child_partitions = child.properties().partitioning.partition_count();

        let exec = NetworkCoalesceExec::try_new(
            Arc::clone(&child),
            Uuid::nil(),
            STAGE_NUM,
            case.consumer_tasks,
            case.input_tasks,
        )?;
        let layout = exec.layout();
        let resolver = layout.resolver();
        let partitions_per_task = resolver.partitions_per_producer_task();

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
                resolver.producer_task_range(index),
                start..start + len,
                "case {} producer-task ownership mismatch for consumer {}",
                case.name,
                index
            );
            assert_eq!(
                layout.consumer_partition_range(index),
                &(start * partitions_per_task..(start + len) * partitions_per_task),
                "case {} logical-slot ownership mismatch for consumer {}",
                case.name,
                index
            );
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
