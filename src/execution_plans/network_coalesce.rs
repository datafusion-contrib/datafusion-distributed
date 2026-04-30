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

/// Network boundary that gathers partitions from upstream tasks without repartitioning rows.
///
/// This is the distributed equivalent of
/// [CoalescePartitionsExec](datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec).
/// The [ExchangeLayout] assigns each consumer task a contiguous group of producer tasks to read.
/// Each output slot maps to exactly one `(producer_task, partition)` pair
/// ([`SlotReadPlan::Single`]) since there is no cross-task fanout.
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
        let layout = &self.layout;
        if task_context.task_count != layout.consumer_task_count() {
            return exec_err!(
                "NetworkCoalesceExec expected task_count={} from layout, got {}",
                layout.consumer_task_count(),
                task_context.task_count
            );
        }
        if task_context.task_index >= layout.consumer_task_count() {
            return exec_err!(
                "NetworkCoalesceExec invalid task context: task_index={} >= consumer_tasks={}",
                task_context.task_index,
                layout.consumer_task_count()
            );
        }

        let Some(SlotReadPlan::Single {
            producer_task: target_task,
            producer_partition: target_partition,
        }) = layout.resolve_slot(task_context.task_index, partition)
        else {
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())));
        };

        let producer_tasks = layout.producer_task_range(task_context.task_index);
        if !producer_tasks.contains(&target_task) {
            return internal_err!(
                "NetworkCoalesceExec derived target_task={} outside layout range {:?}",
                target_task,
                producer_tasks
            );
        }

        let worker_connection = self.worker_connections.get_or_init_worker_connection(
            &self.input_stage,
            0..layout.partitions_per_producer_task(),
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

    #[test]
    fn try_new_wires_coalesce_layout() -> Result<()> {
        let child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let child_partitions = child.properties().partitioning.partition_count();

        let exec = NetworkCoalesceExec::try_new(Arc::clone(&child), Uuid::nil(), 1, 2, 5)?;
        let layout = &exec.layout;

        assert_eq!(
            exec.properties().partitioning.partition_count(),
            child_partitions * 3
        );
        assert_eq!(layout.producer_task_count(), 5);
        assert_eq!(layout.consumer_task_count(), 2);
        assert_eq!(layout.producer_task_range(0), 0..3);
        assert_eq!(layout.producer_task_range(1), 3..5);
        assert_eq!(layout.consumer_partition_range(0), &(0..3));
        assert_eq!(layout.consumer_partition_range(1), &(3..5));
        assert_eq!(
            layout.resolve_slot(1, 1),
            Some(SlotReadPlan::Single {
                producer_task: 4,
                producer_partition: 0,
            })
        );
        assert_eq!(layout.resolve_slot(1, 2), None);

        Ok(())
    }
}
