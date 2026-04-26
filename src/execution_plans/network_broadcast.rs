use crate::common::require_one_child;
use crate::distributed_planner::NetworkBoundary;
use crate::stage::{LocalStage, Stage};
use crate::worker::WorkerConnectionPool;
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::TaskKey;
use crate::worker::generated::worker::flight_app_metadata;
use crate::{BroadcastExec, DistributedTaskContext};
use dashmap::DashMap;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Result, not_impl_err};
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
    pub(crate) properties: Arc<PlanProperties>,
    pub(crate) input_stage: Stage,
    pub(crate) worker_connections: WorkerConnectionPool,
    pub(crate) metrics_collection: Arc<DashMap<TaskKey, Vec<pb::MetricsSet>>>,
}

impl NetworkBroadcastExec {
    pub(crate) fn scale_input(
        plan: Arc<dyn ExecutionPlan>,
        _consumer_partitions: usize,
        consumer_task_count: usize,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(broadcast) = plan.as_any().downcast_ref::<BroadcastExec>() else {
            return Ok(Transformed::no(plan));
        };

        Ok(Transformed::yes(Arc::new(BroadcastExec::new(
            require_one_child(broadcast.children())?,
            consumer_task_count,
        ))))
    }

    /// Creates a [NetworkBroadcastExec].
    ///
    /// Extracts its child, a BroadcastExec, and creates a new BroadcastExec with
    /// the correct consumer_task_count.
    pub fn new(stage: LocalStage) -> Self {
        let input_partition_count = stage.plan.properties().partitioning.partition_count();
        let properties = Arc::new(
            PlanProperties::clone(stage.plan.properties())
                .with_partitioning(Partitioning::UnknownPartitioning(input_partition_count)),
        );

        Self {
            properties,
            worker_connections: WorkerConnectionPool::new(0),
            input_stage: Stage::Local(stage),
            metrics_collection: Default::default(),
        }
    }
}

impl NetworkBoundary for NetworkBroadcastExec {
    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.clone();
        self_clone.worker_connections = WorkerConnectionPool::new(input_stage.task_count());
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }

    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }
}

impl DisplayAs for NetworkBroadcastExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_tasks = self.input_stage.task_count();
        let stage = self.input_stage.num();
        let consumer_partitions = self.properties.partitioning.partition_count();
        let stage_partitions = self
            .input_stage
            .local_plan()
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

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.input_stage.local_plan() {
            Some(plan) => vec![plan],
            None => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
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
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let remote_stage = match &self.input_stage {
            Stage::Local(local) => return local.execute(partition, context),
            Stage::Remote(remote_stage) => remote_stage,
        };

        let task_context = DistributedTaskContext::from_ctx(&context);
        let off = self.properties.partitioning.partition_count() * task_context.task_index;
        let mut streams = Vec::with_capacity(self.input_stage.task_count());

        for input_task_index in 0..self.input_stage.task_count() {
            let worker_connection = self.worker_connections.get_or_init_worker_connection(
                remote_stage,
                off..(off + self.properties.partitioning.partition_count()),
                input_task_index,
                &context,
            )?;

            let metrics_collection = Arc::clone(&self.metrics_collection);
            let stream = worker_connection.stream_partition(off + partition, move |meta| {
                if let Some(flight_app_metadata::Content::MetricsCollection(m)) = meta.content {
                    for task_metrics in m.tasks {
                        if let Some(task_key) = task_metrics.task_key {
                            metrics_collection.insert(task_key, task_metrics.metrics);
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
