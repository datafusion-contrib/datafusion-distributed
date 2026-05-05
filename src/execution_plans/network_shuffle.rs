use crate::common::require_one_child;
use crate::distributed_planner::{ExchangeLayout, SlotReadPlan};
use crate::execution_plans::common::with_partition_count_props;
use crate::stage::Stage;
use crate::worker::WorkerConnectionPool;
use crate::{DistributedTaskContext, ExecutionTask, NetworkBoundary};
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties,
};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use uuid::Uuid;

/// Network boundary that reads hash-partitioned data from an upstream distributed stage.
///
/// The planner scales the upstream `RepartitionExec` to produce exactly `consumer_task_count`
/// logical hash partitions (one per consumer task). Each consumer task owns one of those
/// partitions and reads it from every producer task ([`SlotReadPlan::Fanout`]). Local
/// parallelism above this boundary is provided by [crate::LocalExchangeSplitExec], not by this node.
///
/// ```text
///               ┌─┐                                 ┌─┐                                 ┌─┐
///               │1│                                 │1│                                 │1│
/// ┌─────────────┴─┴─────────────┐     ┌─────────────┴─┴─────────────┐     ┌─────────────┴─┴─────────────┐
/// │                             │     │                             │     │                             │
/// │     NetworkShuffleExec      │     │     NetworkShuffleExec      │ ... │     NetworkShuffleExec      │
/// │          (Task 1)           │     │          (Task 2)           │     │          (Task M)           │
/// │                             │     │                             │     │                             │
/// └─────────────┬─┬─────────────┘     └─────────────┬─┬─────────────┘     └─────────────┬─┬─────────────┘
///               │1│                                 │2│                                 │M│
///               └▲┘                                 └▲┘                                 └▲┘
///                │                                   │                                   │
///                │                                   │                                   │
///                │                                                                       │
///                │                                                                       │
///                │                                                                       │
///                │                                                                       │
///                └───────────────────────────────┐       ┌───────────────────────────────┘
///                                                │       │
///                                               ┌─┐ ... ┌─┐
///                                               │1│     │M│
///                                     ┌─────────┴─┴─────┴─┴─────────┐
///                                     │                             │
///                                     │       RepartitionExec       │
///                                     │          (Task 1)           │
///                                     │                             │
///                                     └─────────┬─┬─────┬─┬─────────┘
///                                               │1│     │N│
///                                               └─┘ ... └─┘
/// ```
///
/// Each consumer task in Stage N+1 gathers its one assigned partition from all producer tasks
/// in Stage N.
#[derive(Debug, Clone)]
pub struct NetworkShuffleExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: Arc<PlanProperties>,
    pub(crate) input_stage: Stage,
    pub(crate) worker_connections: WorkerConnectionPool,
    pub(crate) layout: Arc<ExchangeLayout>,
}

impl NetworkShuffleExec {
    /// Builds a new [NetworkShuffleExec] in "Pending" state.
    ///
    /// The `input` must be hash-partitioned (typically a [RepartitionExec] with
    /// [Partitioning::Hash]). The producer-side hash partition count is preserved as the logical
    /// downstream partition space, and consumer tasks each own a contiguous subset of those
    /// logical partitions.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        num: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Self, DataFusionError> {
        let producer_partitioning = input.output_partitioning().clone();
        let Partitioning::Hash(_, _) = producer_partitioning else {
            return plan_err!("NetworkShuffleExec input must be hash partitioned");
        };

        let consumer_tasks = task_count;
        let producer_tasks = input_task_count;
        let layout =
            ExchangeLayout::try_shuffle(producer_partitioning, producer_tasks, consumer_tasks)?;
        let properties = with_partition_count_props(
            input.properties(),
            layout.max_partition_count_per_consumer(),
        );

        Ok(Self {
            input_stage: Stage {
                query_id,
                num,
                plan: Some(input),
                tasks: vec![ExecutionTask { url: None }; input_task_count],
            },
            worker_connections: WorkerConnectionPool::new(input_task_count),
            properties,
            layout,
        })
    }
}

impl NetworkBoundary for NetworkShuffleExec {
    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }

    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.clone();
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }
}

impl DisplayAs for NetworkShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_tasks = self.input_stage.tasks.len();
        let partitions = self.properties.partitioning.partition_count();
        let stage = self.input_stage.num;
        write!(
            f,
            "[Stage {stage}] => NetworkShuffleExec: output_partitions={partitions}, input_tasks={input_tasks}",
        )
    }
}

impl ExecutionPlan for NetworkShuffleExec {
    fn name(&self) -> &str {
        "NetworkShuffleExec"
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
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
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
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let task_context = DistributedTaskContext::from_ctx(&context);
        let layout = &self.layout;
        if task_context.task_count != layout.consumer_task_count() {
            return exec_err!(
                "NetworkShuffleExec expected task_count={} from layout, got {}",
                layout.consumer_task_count(),
                task_context.task_count
            );
        }
        if task_context.task_index >= layout.consumer_task_count() {
            return exec_err!(
                "NetworkShuffleExec task_index={} is out of range for layout with {} consumer tasks",
                task_context.task_index,
                layout.consumer_task_count()
            );
        }
        let Some(SlotReadPlan::Fanout {
            producer_tasks,
            producer_partition: target_partition,
        }) = layout.resolve_slot(task_context.task_index, partition)
        else {
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())));
        };

        let target_partition_range = layout
            .consumer_partition_range(task_context.task_index)
            .clone();
        let mut streams = Vec::with_capacity(producer_tasks.len());
        for input_task_index in producer_tasks {
            let worker_connection = self.worker_connections.get_or_init_worker_connection(
                &self.input_stage,
                target_partition_range.clone(),
                input_task_index,
                &context,
            )?;

            let stream = worker_connection.stream_partition(target_partition, |_meta| {})?;
            streams.push(stream);
        }

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::select_all(streams),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.worker_connections.metrics.clone_inner())
    }
}
