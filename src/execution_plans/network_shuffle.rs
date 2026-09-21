use crate::common::require_one_child;
use crate::distributed_planner::ProducerHead;
use crate::execution_plans::common::{salted_partitioning, scale_partitioning};
use crate::stage::{LocalStage, Stage};
use crate::worker::WorkerConnectionPool;
use crate::{DistributedTaskContext, MaybeEncoded, NetworkBoundary};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, not_impl_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, PlanProperties,
    Statistics, StatisticsArgs,
};
use std::fmt::Formatter;
use std::sync::Arc;
use uuid::Uuid;

pub const PRODUCER_SALT_DEFAULT: u64 = 0x517cc1b727220a95;

/// Routing strategy between producer and consumer stages.
#[derive(Debug, Clone)]
pub enum ShuffleMode {
    /// Hash(key, consumer_task_count × consumer_partition_count): consumer reads global partitions
    Direct,
    /// Hash(key+salt, consumer_task_count): each consumer re-partitions locally into consumer_partition_count.
    Salted { salt: u64 },
}

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
/// - When input streams carry an output ordering, each partition sort-merges incoming
///   streams from upstream tasks to preserve that ordering across tasks.
///
/// This node has two variants.
/// 1. Pending: acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///    using Arrow Flight.
#[derive(Debug, Clone)]
pub struct NetworkShuffleExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: Arc<PlanProperties>,
    /// the consumer's hash partitioning; in Salted mode `properties` advertises UnknownPartitioning
    /// since salting breaks hash guarantees, so we keep the original partitioning here
    pub(crate) consumer_partitioning: Partitioning,
    pub(crate) input_stage: Stage,
    pub(crate) worker_connections: WorkerConnectionPool,
    pub(crate) mode: ShuffleMode,
}

impl NetworkShuffleExec {
    /// Computes the properties advertised by this [NetworkShuffleExec].
    ///
    /// When `input_task_count > 1`, partition-local equivalence constants from individual
    /// upstream tasks cannot be assumed to hold across tasks and are cleared.
    /// Output ordering is preserved across tasks because [Self::execute] sort-merges incoming
    /// worker streams when sort expressions are present.
    ///
    /// When `input_task_count <= 1`, all batches are received from a single upstream task stream,
    /// so the upstream equivalence properties and constants are preserved as-is.
    pub(crate) fn compute_properties(
        input_properties: &Arc<PlanProperties>,
        input_task_count: usize,
    ) -> Arc<PlanProperties> {
        if input_task_count > 1 {
            let mut eq_properties = input_properties.eq_properties.clone();
            eq_properties.clear_per_partition_constants();
            Arc::new(PlanProperties::new(
                eq_properties,
                input_properties.partitioning.clone(),
                input_properties.emission_type,
                input_properties.boundedness,
            ))
        } else {
            Arc::clone(input_properties)
        }
    }

    pub(crate) fn from_stage(
        input_stage: Stage,
        input_properties: Arc<PlanProperties>,
        output_partitions: usize,
        mode: ShuffleMode,
    ) -> Self {
        let consumer_partitioning = input_properties.partitioning.clone();
        let advertised_partitioning = match &mode {
            ShuffleMode::Direct => consumer_partitioning.clone(),
            ShuffleMode::Salted { .. } => Partitioning::UnknownPartitioning(output_partitions),
        };
        let properties = Arc::new(PlanProperties::new(
            input_properties.equivalence_properties().clone(),
            advertised_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            properties,
            consumer_partitioning,
            worker_connections: WorkerConnectionPool::new(input_stage.task_count()),
            input_stage,
            mode,
        }
    }

    /// Creates a new [NetworkShuffleExec] fed by the provided [RepartitionExec]. The input plan
    /// will be executed in a remote worker in `producer_tasks` number of tasks.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, producer_tasks: usize) -> Result<Self> {
        let Some(r_exec) = input.downcast_ref::<RepartitionExec>() else {
            return plan_err!("The input of a NetworkShuffleExec can only be a RepartitionExec");
        };
        if !matches!(r_exec.partitioning(), Partitioning::Hash(_, _)) {
            return plan_err!("The input of a NetworkShuffleExec must be hash partitioned");
        }

        let input_properties = Arc::clone(input.properties());
        Ok(Self::from_stage(
            Stage::Local(LocalStage {
                // At this point, query_id and num are just placeholders that will be filled by
                // prepare_network_boundaries.rs. Users are not expected to provide valid values for
                // these two parameters.
                query_id: Uuid::nil(),
                num: 0,
                plan: input,
                tasks: producer_tasks,
                metrics_set: Default::default(),
            }),
            input_properties,
            1,
            ShuffleMode::Salted {
                salt: PRODUCER_SALT_DEFAULT,
            },
        ))
    }
}

impl NetworkBoundary for NetworkShuffleExec {
    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }

    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn NetworkBoundary>> {
        let mut self_clone = self.clone();
        self_clone.worker_connections = WorkerConnectionPool::new(input_stage.task_count());
        self_clone.properties =
            Self::compute_properties(&self.properties, input_stage.task_count());
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }

    fn producer_head(&self, consumer_task_count: usize) -> Result<ProducerHead> {
        let partitioning = match &self.mode {
            ShuffleMode::Direct => {
                scale_partitioning(&self.consumer_partitioning, |n| n * consumer_task_count)?
            }
            ShuffleMode::Salted { salt } => {
                salted_partitioning(&self.consumer_partitioning, *salt, consumer_task_count)?
            }
        };
        Ok(ProducerHead::RepartitionExec {
            partitioning: MaybeEncoded::Decoded(partitioning),
        })
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
        )?;
        // Only display sort expressions when multiple input tasks require sort-merging.
        if let Some(ordering) = self.properties.output_ordering()
            && !ordering.is_empty()
            && input_tasks > 1
        {
            write!(f, ", sort_exprs=[{ordering}]")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for NetworkShuffleExec {
    fn name(&self) -> &str {
        "NetworkShuffleExec"
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

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
            Stage::Remote(_) => {
                if !children.is_empty() {
                    not_impl_err!("NetworkBoundary cannot accept children")?
                }
            }
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
        let task_index = task_context.task_index;
        let producer_task_count = remote_stage.workers.len();

        let schema = self.schema();
        let mut streams: Vec<SendableRecordBatchStream> = if matches!(self.mode, ShuffleMode::Direct) {
            // Read global partition task_index*partition_count+p from all producer tasks.
            // All partitions for this consumer task share the same range key so the connection
            // pool returns the same cached stream group for every partition call.
            let partition_count = self.properties.partitioning.partition_count();
            let off = task_index * partition_count;
            let global_partition = off + partition;
            let mut streams = Vec::with_capacity(producer_task_count);
            for input_task_index in 0..producer_task_count {
                let stream = self.worker_connections.execute(
                    remote_stage,
                    off..(off + partition_count),
                    input_task_index,
                    global_partition,
                    self.producer_head(task_context.task_count)?,
                    &context,
                )?;
                streams.push(
                    Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream))
                        as SendableRecordBatchStream,
                );
            }
            streams
        } else {
            // Salted: one producer per output partition.
            let stream = self.worker_connections.execute(
                remote_stage,
                task_index..task_index + 1,
                partition,
                task_index,
                self.producer_head(task_context.task_count)?,
                &context,
            )?;
            vec![Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream))
                as SendableRecordBatchStream]
        };

        if streams.is_empty() {
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())));
        }
        // When there is only one input task stream, no merging or interleaving is needed.
        if streams.len() == 1 {
            return Ok(streams.pop().unwrap());
        }

        if let Some(ordering) = self.properties.output_ordering()
            && !ordering.is_empty()
        {
            let reservation = MemoryConsumer::new(format!("NetworkShuffleExec[{partition}]"))
                .register(&context.runtime_env().memory_pool);
            let batch_size = context.session_config().batch_size();
            // StreamingMergeBuilder requires BaselineMetrics (panics if not provided).
            // Pass an isolated metrics set to avoid double-counting into worker_connections.metrics.
            let metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), partition);
            StreamingMergeBuilder::new()
                .with_streams(streams)
                .with_schema(self.schema())
                .with_expressions(ordering)
                .with_metrics(metrics)
                .with_batch_size(batch_size)
                .with_reservation(reservation)
                .build()
        } else {
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                futures::stream::select_all(streams),
            )))
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.worker_connections.metrics.clone_inner())
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.input_stage.partition_statistics(
            args.partition(),
            self.properties.partitioning.partition_count(),
            self.schema(),
        )
    }
}
