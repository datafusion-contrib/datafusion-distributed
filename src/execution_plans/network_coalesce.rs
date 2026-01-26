use crate::common::require_one_child;
use crate::distributed_planner::NetworkBoundary;
use crate::execution_plans::common::scale_partitioning_props;
use crate::flight_service::WorkerConnectionPool;
use crate::metrics::proto::MetricsSetProto;
use crate::protobuf::{AppMetadata, StageKey};
use crate::stage::{MaybeEncodedPlan, Stage};
use crate::{DistributedTaskContext, ExecutionTask};
use dashmap::DashMap;
use datafusion::common::{exec_err, plan_err};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use uuid::Uuid;

/// [ExecutionPlan] that coalesces partitions from multiple tasks into a single task without
/// performing any repartition, and maintaining the same partitioning scheme.
///
/// This is the equivalent of a [CoalescePartitionsExec] but coalescing tasks across the network
/// into one.
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
/// - Stage N+1 must have exactly 1 task. The distributed planner ensures this is true.
/// - The number of partitions in the single task of Stage N+1 is equal to the total number of
///   partitions across all tasks in Stage N (e.g. (1,2,3,4,5,6,7,8,9) = (1,2,3)+(4,5,6)+(7,8,9) )
///
/// This node has two variants.
/// 1. Pending: acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///    using Arrow Flight.
#[derive(Debug, Clone)]
pub struct NetworkCoalesceExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: PlanProperties,
    pub(crate) input_stage: Stage,
    pub(crate) worker_connections: WorkerConnectionPool,
    /// metrics_collection is used to collect metrics from child tasks. It is initially
    /// instantiated as an empty [DashMap] (see `try_decode` in `distributed_codec.rs`).
    /// Metrics are populated here via [NetworkCoalesceExec::execute].
    ///
    /// An instance may receive metrics for 0 to N child tasks, where N is the number of tasks in
    /// the stage it is reading from. This is because, by convention, the Worker sends metrics for
    /// a task to the last NetworkCoalesceExec to read from it, which may or may not be this
    /// instance.
    pub(crate) metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
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
        if task_count > 1 {
            return plan_err!(
                "NetworkCoalesceExec cannot be executed in more than one task, {task_count} were passed."
            );
        }
        Ok(Self {
            properties: scale_partitioning_props(input.properties(), |p| p * input_task_count),
            input_stage: Stage {
                query_id,
                num,
                plan: MaybeEncodedPlan::Decoded(input),
                tasks: vec![ExecutionTask { url: None }; input_task_count],
            },
            worker_connections: WorkerConnectionPool::new(input_task_count),
            metrics_collection: Default::default(),
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

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.input_stage.plan {
            MaybeEncodedPlan::Decoded(v) => vec![v],
            MaybeEncodedPlan::Encoded(_) => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.as_ref().clone();
        self_clone.input_stage.plan = MaybeEncodedPlan::Decoded(require_one_child(children)?);
        Ok(Arc::new(self_clone))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task_context = DistributedTaskContext::from_ctx(&context);
        if task_context.task_index > 0 {
            return exec_err!("NetworkCoalesceExec cannot be executed in more than one task");
        }

        let partitions_per_task =
            self.properties().partitioning.partition_count() / self.input_stage.tasks.len();

        let target_task = partition / partitions_per_task;
        let target_partition = partition % partitions_per_task;

        let worker_connection = self.worker_connections.get_or_init_worker_connection(
            &self.input_stage,
            0..partitions_per_task,
            target_task,
            &context,
        )?;

        let metrics_collection = Arc::clone(&self.metrics_collection);

        let stream = worker_connection.stream_partition(target_partition, move |meta| {
            if let Some(AppMetadata::MetricsCollection(m)) = meta.content {
                for task_metrics in m.tasks {
                    if let Some(stage_key) = task_metrics.stage_key {
                        metrics_collection.insert(stage_key, task_metrics.metrics);
                    };
                }
            }
        })?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.worker_connections.metrics.clone_inner())
    }
}
