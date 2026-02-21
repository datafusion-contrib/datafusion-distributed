use crate::DistributedTaskContext;
use crate::common::require_one_child;
use crate::distributed_planner::NetworkBoundary;
use crate::flight_service::WorkerConnectionPool;
use crate::metrics::proto::MetricsSetProto;
use crate::protobuf::{AppMetadata, StageKey};
use crate::stage::Stage;
use dashmap::DashMap;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use uuid::Uuid;

/// Network boundary for broadcasting data to all consumer tasks.
///
/// This operator works with [BroadcastExec] which scales up partitions so each
/// consumer task fetches a unique set of partition numbers. Each partition request
/// is sent to all stage tasks because PartitionIsolatorExec maps the same logical
/// partition to different actual data on each task.
///
/// Here are some examples of how [NetworkBroadcastExec] distributes data:
///
/// # 1 to many
///
/// ```text
/// ┌────────────────────────┐                        ┌────────────────────────┐           ■
/// │  NetworkBroadcastExec  │                        │  NetworkBroadcastExec  │           │
/// │        (task 1)        │           ...          │        (task M)        │           │
/// │                        │                        │                        │        Stage N
/// │    Populates Caches    │                        │    Populates Caches    │           │
/// └────────┬─┬┬─┬┬─┬───────┘                        └────────┬─┬┬─┬┬─┬───────┘           │
///          │0││1││2│                                         │0││1││2│                   │
///          └▲┘└▲┘└▲┘                                         └▲┘└▲┘└▲┘                   ■
///           │  │  │                                           │  │  │
///           │  │  │                                           │  │  │
///           │  │  │                                           │  │  │
///           │  │  └─────────────┐          ┌──────────────────┘  │  │
///           │  └─────────────┐  │          │     ┌───────────────┘  │
///           └─────────────┐  │  │          │     │    ┌─────────────┘
///                         │  │  │          │     │    │
///                        ┌┴┐┌┴┐┌┴┐ ... ┌───┴┐┌───┴┐┌──┴─┐
///                        │1││2││3│     │NM-3││NM-2││NM-1│                                ■
///                       ┌┴─┴┴─┴┴─┴─────┴────┴┴────┴┴────┴─┐                              │
///                       │          BroadcastExec          │                              │
///                       │        ┌───────────────┐        │                          Stage N-1
///                       │        │  Batch Cache  │        │                              │
///                       │        │  ┌─┐ ┌─┐ ┌─┐  │        │                              │
///                       │        │  │0│ │1│ │2│  │        │                              │
///                       │        │  └─┘ └─┘ └─┘  │        │                              │
///                       │        └───────────────┘        │                              │
///                       └───────────┬─┬─┬─┬─┬─┬───────────┘                              │
///                                   │0│ │1│ │2│                                          │
///                                   └▲┘ └▲┘ └▲┘                                          ■
///                                    │   │   │
///                                    │   │   │
///                                    │   │   │
///                                   ┌┴┐ ┌┴┐ ┌┴┐                                          ■
///                                   │0│ │1│ │2│                                          │
///                            ┌──────┴─┴─┴─┴─┴─┴──────┐                               Stage N-2
///                            │Arc<dyn ExecutionPlan> │                                   │
///                            │       (task 1)        │                                   │
///                            └───────────────────────┘                                   ■
/// ```
///
/// # Many to many
///
/// ```text
///    ┌────────────────────────┐                        ┌────────────────────────┐          ■
///    │  NetworkBroadcastExec  │                        │  NetworkBroadcastExec  │          │
///    │        (task 1)        │                        │        (task M)        │          │
///    │                        │           ...          │                        │       Stage N
///    │    Populates Caches    │                        │       Cache Hits       │          │
///    └────────┬─┬┬─┬┬─┬───────┘                        └────────┬─┬┬─┬┬─┬───────┘          │
///             │0││1││2│                                         │0││1││2│                  │
///             └▲┘└▲┘└▲┘                                         └▲┘└▲┘└▲┘                  ■
///              │  │  │                                           │  │  │
///   ┌──────────┴──┼──┼────────────────────────────────┐          │  │  │
///   │  ┌──────────┴──┼────────────────────────────────┼──┐       │  │  │
///   │  │  ┌──────────┴────────────────────────────────┼──┼──┐    │  │  │
///   │  │  │                                           │  │  │    │  │  │
///   │  │  │         ┌─────────────────────────────────┼──┼──┼────┴──┼─┐│
///   │  │  │         │     ┌───────────────────────────┼──┼──┼───────┴─┼┼─────┐
///   │  │  │         │     │     ┌─────────────────────┼──┼──┼─────────┼┴─────┼────┐
///   │  │  │         │     │     │                     │  │  │         │      │    │
///  ┌┴┐┌┴┐┌┴┐ ... ┌──┴─┐┌──┴─┐┌──┴─┐                  ┌┴┐┌┴┐┌┴┐ ... ┌──┴─┐┌───┴┐┌──┴─┐      ■
///  │0││1││2│     │3M-3││3M-2││3M-1│                  │0││1││2│     │3M-3││3M-2││3M-1│      │
/// ┌┴─┴┴─┴┴─┴─────┴────┴┴────┴┴────┴┐                ┌┴─┴┴─┴┴─┴─────┴────┴┴────┴┴────┴┐     │
/// │         BroadcastExec          │                │         BroadcastExec          │     │
/// │        ┌───────────────┐       │                │        ┌───────────────┐       │     │
/// │        │  Batch Cache  │       │                │        │  Batch Cache  │       │     │
/// │        │  ┌─┐ ┌─┐ ┌─┐  │       │      ...       │        │  ┌─┐ ┌─┐ ┌─┐  │       │ Stage N-1
/// │        │  │0│ │1│ │2│  │       │                │        │  │0│ │1│ │2│  │       │     │
/// │        │  └─┘ └─┘ └─┘  │       │                │        │  └─┘ └─┘ └─┘  │       │     │
/// │        └───────────────┘       │                │        └───────────────┘       │     │
/// └───────────┬─┬─┬─┬─┬─┬──────────┘                └───────────┬─┬─┬─┬─┬─┬──────────┘     │
///             │0│ │1│ │2│                                       │0│ │1│ │2│                │
///             └▲┘ └▲┘ └▲┘                                       └▲┘ └▲┘ └▲┘                ■
///              │   │   │                                         │   │   │
///              │   │   │                                         │   │   │
///              │   │   │                                         │   │   │
///             ┌┴┐ ┌┴┐ ┌┴┐                                       ┌┴┐ ┌┴┐ ┌┴┐                ■
///             │0│ │1│ │2│                                       │0│ │1│ │2│                │
///      ┌──────┴─┴─┴─┴─┴─┴──────┐                         ┌──────┴─┴─┴─┴─┴─┴──────┐     Stage N-2
///      │Arc<dyn ExecutionPlan> │          ...            │Arc<dyn ExecutionPlan> │         │
///      │       (task 1)        │                         │       (task N)        │         │
///      └───────────────────────┘                         └───────────────────────┘         ■
/// ```
///
/// Notice in this diagram that each [NetworkBroadcastExec] sends a request to fetch data from each
/// [BroadcastExec] in the stage below per partition. This is because each [BroadcastExec] has its
/// own cache which contains partial results for the partition. It is the [NetworkBroadcastExec]'s
/// job to merge these partial partitions to then broadcast complete data to the consumers.
#[derive(Debug, Clone)]
pub struct NetworkBroadcastExec {
    pub(crate) properties: PlanProperties,
    pub(crate) input_stage: Stage,
    pub(crate) worker_connections: WorkerConnectionPool,
    pub(crate) metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
}

impl NetworkBroadcastExec {
    /// Creates a [NetworkBroadcastExec].
    ///
    /// Extracts its child, a BroadcastExec, and creates a new BroadcastExec with
    /// the correct consumer_task_count.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_num: usize,
        consumer_task_count: usize,
        input_task_count: usize,
    ) -> Result<Self, DataFusionError> {
        let Some(broadcast) = input.as_any().downcast_ref::<super::BroadcastExec>() else {
            return Err(internal_datafusion_err!(
                "NetworkBroadcastExec requires a BroadcastExec input, found: {}",
                input.name()
            ));
        };

        let child = require_one_child(broadcast.children())?;
        let input_partition_count = child.properties().partitioning.partition_count();
        let broadcast_exec: Arc<dyn ExecutionPlan> =
            Arc::new(super::BroadcastExec::new(child, consumer_task_count));

        let properties = broadcast_exec
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(input_partition_count));

        let input_stage = Stage::new(query_id, stage_num, broadcast_exec, input_task_count);

        Ok(Self {
            properties,
            input_stage,
            worker_connections: WorkerConnectionPool::new(input_task_count),
            metrics_collection: Default::default(),
        })
    }
}

impl NetworkBoundary for NetworkBroadcastExec {
    fn with_input_stage(
        &self,
        input_stage: Stage,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut self_clone = self.clone();
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }

    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }
}

impl DisplayAs for NetworkBroadcastExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_tasks = self.input_stage.tasks.len();
        let stage = self.input_stage.num;
        let consumer_partitions = self.properties.partitioning.partition_count();
        let stage_partitions = self
            .input_stage
            .plan
            .as_ref()
            .map(|p| p.properties().partitioning.partition_count())
            .unwrap_or(0);
        write!(
            f,
            "[Stage {stage}] => NetworkBroadcastExec: partitions_per_consumer={consumer_partitions}, stage_partitions={stage_partitions}, input_tasks={input_tasks}",
        )
    }
}

impl ExecutionPlan for NetworkBroadcastExec {
    fn name(&self) -> &str {
        "NetworkBroadcastExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.input_stage.plan {
            Some(plan) => vec![plan],
            None => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut self_clone = self.as_ref().clone();
        self_clone.input_stage.plan = Some(require_one_child(children)?);
        Ok(Arc::new(self_clone))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let task_context = DistributedTaskContext::from_ctx(&context);
        let off = self.properties.partitioning.partition_count() * task_context.task_index;
        let mut streams = Vec::with_capacity(self.input_stage.tasks.len());

        for input_task_index in 0..self.input_stage.tasks.len() {
            let worker_connection = self.worker_connections.get_or_init_worker_connection(
                &self.input_stage,
                off..(off + self.properties.partitioning.partition_count()),
                input_task_index,
                &context,
            )?;

            let metrics_collection = Arc::clone(&self.metrics_collection);
            let stream = worker_connection.stream_partition(off + partition, move |meta| {
                if let Some(AppMetadata::MetricsCollection(m)) = meta.content {
                    for task_metrics in m.tasks {
                        if let Some(stage_key) = task_metrics.stage_key {
                            metrics_collection.insert(stage_key, task_metrics.metrics);
                        };
                    }
                }
            })?;
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
