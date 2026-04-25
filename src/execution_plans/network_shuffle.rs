use crate::common::require_one_child;
use crate::execution_plans::common::scale_partitioning;
use crate::stage::{LocalStage, Stage};
use crate::worker::WorkerConnectionPool;
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::TaskKey;
use crate::worker::generated::worker::flight_app_metadata;
use crate::{DistributedTaskContext, NetworkBoundary};
use dashmap::DashMap;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, not_impl_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

/// [ExecutionPlan] implementation that shuffles data across the network in a distributed context.
///
/// The easiest way of thinking about this node is as a plan [RepartitionExec] node that is
/// capable of fanning out the different produced partitions to different tasks.
/// This allows redistributing data across different tasks in different stages, so that different
/// physical machines can make progress on different non-overlapping sets of data.
///
/// This node allows fanning out of data from N tasks to M tasks, with N and M being arbitrary non-zero
/// positive numbers. Here are some examples of how data can be shuffled in different scenarios:
///
/// # 1 to many
///
/// ```text
/// ┌───────────────────────────┐  ┌───────────────────────────┐ ┌───────────────────────────┐     ■
/// │    NetworkShuffleExec     │  │    NetworkShuffleExec     │ │    NetworkShuffleExec     │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └┬─┬┬─┬┬─┬──────────────────┘  └─────────┬─┬┬─┬┬─┬─────────┘ └──────────────────┬─┬┬─┬┬─┬┘  Stage N+1
///  │1││2││3│                               │4││5││6│                              │7││8││9│      │
///  └─┘└─┘└─┘                               └─┘└─┘└─┘                              └─┘└─┘└─┘      │
///   ▲  ▲  ▲                                 ▲  ▲  ▲                                ▲  ▲  ▲       ■
///   └──┴──┴────────────────────────┬──┬──┐  │  │  │  ┌──┬──┬───────────────────────┴──┴──┘
///                                  │  │  │  │  │  │  │  │  │                                     ■
///                                 ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐                                    │
///                                 │1││2││3││4││5││6││7││8││9│                                    │
///                                ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐                                Stage N
///                                │      RepartitionExec      │                                   │
///                                │         (task 1)          │                                   │
///                                └───────────────────────────┘                                   ■
/// ```
///
/// # many to 1
///
/// ```text
///                                ┌───────────────────────────┐                                   ■
///                                │    NetworkShuffleExec     │                                   │
///                                │         (task 1)          │                                   │
///                                └┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┘                                Stage N+1
///                                 │1││2││3││4││5││6││7││8││9│                                    │
///                                 └─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘                                    │
///                                 ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲                                    ■
///   ┌──┬──┬──┬──┬──┬──┬──┬──┬─────┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴────┬──┬──┬──┬──┬──┬──┬──┬──┐
///   │  │  │  │  │  │  │  │  │      │  │  │  │  │  │  │  │  │     │  │  │  │  │  │  │  │  │       ■
///  ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐    ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐   ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐      │
///  │1││2││3││4││5││6││7││8││9│    │1││2││3││4││5││6││7││8││9│   │1││2││3││4││5││6││7││8││9│      │
/// ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐  ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐ ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐  Stage N
/// │      RepartitionExec      │  │      RepartitionExec      │ │      RepartitionExec      │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// # many to many
///
/// ```text
///                    ┌───────────────────────────┐  ┌───────────────────────────┐                ■
///                    │    NetworkShuffleExec     │  │    NetworkShuffleExec     │                │
///                    │         (task 1)          │  │         (task 2)          │                │
///                    └┬─┬┬─┬┬─┬┬─┬───────────────┘  └───────────────┬─┬┬─┬┬─┬┬─┬┘             Stage N+1
///                     │1││2││3││4│                                  │5││6││7││8│                 │
///                     └─┘└─┘└─┘└─┘                                  └─┘└─┘└─┘└─┘                 │
///                     ▲▲▲▲▲▲▲▲▲▲▲▲                                  ▲▲▲▲▲▲▲▲▲▲▲▲                 ■
///     ┌──┬──┬──┬──┬──┬┴┴┼┴┴┼┴┴┴┴┴┴───┬──┬──┬──┬──┬──┬──┬──┬────────┬┴┴┼┴┴┼┴┴┼┴┴┼──┬──┬──┐
///     │  │  │  │  │  │  │  │         │  │  │  │  │  │  │  │        │  │  │  │  │  │  │  │        ■
///    ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐       ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐      ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐       │
///    │1││2││3││4││5││6││7││8│       │1││2││3││4││5││6││7││8│      │1││2││3││4││5││6││7││8│       │
/// ┌──┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴─┐  ┌──┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴─┐ ┌──┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴─┐  Stage N
/// │      RepartitionExec      │  │      RepartitionExec      │ │      RepartitionExec      │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// The communication between two stages across a [NetworkShuffleExec] has two implications:
///
/// - Each task in Stage N+1 gathers data from all tasks in Stage N
/// - The total number of partitions across all tasks in Stage N+1 is equal to the
///   number of partitions in a single task in Stage N. (e.g. (1,2,3,4)+(5,6,7,8) = (1,2,3,4,5,6,7,8) )
///
/// This node has two variants.
/// 1. Pending: acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///    using Arrow Flight.
#[derive(Debug, Clone)]
pub struct NetworkShuffleExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: Arc<PlanProperties>,
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
    pub(crate) metrics_collection: Arc<DashMap<TaskKey, Vec<pb::MetricsSet>>>,
}

impl NetworkShuffleExec {
    pub fn prepare_input_for_consumer_tasks(
        plan: Arc<dyn ExecutionPlan>,
        consumer_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let transformed = plan.transform_down(|plan| {
            if let Some(r_exe) = plan.as_any().downcast_ref::<RepartitionExec>() {
                // Scale the input RepartitionExec to account for all the tasks to which it will
                // need to fan data out.
                let scaled = Arc::new(RepartitionExec::try_new(
                    require_one_child(r_exe.children())?,
                    scale_partitioning(r_exe.partitioning(), |p| p * consumer_task_count),
                )?);
                Ok(Transformed::new(scaled, true, TreeNodeRecursion::Stop))
            } else if matches!(plan.output_partitioning(), Partitioning::Hash(_, _)) {
                // This might be a passthrough node from the input plan.
                // This is fine, we can let the node be here.
                Ok(Transformed::no(plan))
            } else {
                plan_err!(
                    "NetworkShuffleExec input must be hash partitioned, but {} is not",
                    plan.name()
                )
            }
        })?;

        Ok(transformed.data)
    }

    /// Builds a new [NetworkShuffleExec] in "Pending" state.
    ///
    /// Typically, the `input` to this
    /// node is a [RepartitionExec] with a [Partitioning::Hash] partition scheme.
    pub fn try_new(mut input: LocalStage, task_count: usize) -> Result<Self, DataFusionError> {
        if !matches!(input.plan.output_partitioning(), Partitioning::Hash(_, _)) {
            return plan_err!("NetworkShuffleExec input must be hash partitioned");
        }
        let properties = input.plan.properties().clone();
        input.plan = Self::prepare_input_for_consumer_tasks(input.plan, task_count)?;

        Ok(Self {
            properties,
            worker_connections: WorkerConnectionPool::new(0),
            input_stage: Stage::Local(input),
            metrics_collection: Default::default(),
        })
    }
}

impl NetworkBoundary for NetworkShuffleExec {
    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }

    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.clone();
        self_clone.worker_connections = WorkerConnectionPool::new(input_stage.task_count());
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }
}

impl DisplayAs for NetworkShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_tasks = self.input_stage.task_count();
        let partitions = self.properties.partitioning.partition_count();
        let stage = self.input_stage.num();
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
        match &self.input_stage.local_plan() {
            Some(v) => vec![v],
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

        let mut streams = Vec::with_capacity(remote_stage.workers.len());
        for input_task_index in 0..remote_stage.workers.len() {
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
