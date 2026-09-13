use crate::common::require_one_child;
use crate::distributed_planner::ProducerHead;
use crate::execution_plans::common::{salted_partitioning, scale_partitioning};
use crate::stage::{LocalStage, RemoteStage, Stage};
use crate::worker::WorkerConnectionPool;
use crate::{DistributedTaskContext, MaybeEncoded, NetworkBoundary};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{HashMap, Result, internal_datafusion_err, not_impl_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics, StatisticsArgs,
};
use futures::future::{BoxFuture, Shared};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryFutureExt};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Indexed by output partition. Each slot holds a stream taken at most once; `Mutex<Option>`
/// enforces the single-take contract under concurrent `execute()` calls.
type CachedPartitionStreams =
    Arc<Vec<Mutex<Option<BoxStream<'static, Result<RecordBatch, DataFusionError>>>>>>;

/// A future shared across all partitions so streams are opened exactly once, even under concurrent `execute()` calls.
type SharedCachedStreamsFuture =
    Shared<BoxFuture<'static, Result<CachedPartitionStreams, Arc<DataFusionError>>>>;

/// Per-execution stream cache. Keyed by `TaskContext` pointer so each `execute_task`
/// invocation (including coordinator retries) gets its own independent set of streams.
type ExecutionStreamCache = HashMap<usize, SharedCachedStreamsFuture>;

/// Presents N streams as N partitions for `RepartitionExec` input.
struct MultiStreamExec {
    streams: Arc<Vec<Mutex<Option<SendableRecordBatchStream>>>>,
    properties: Arc<PlanProperties>,
}

impl MultiStreamExec {
    fn new(streams: Vec<SendableRecordBatchStream>, schema: SchemaRef) -> Self {
        let n = streams.len();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            streams: Arc::new(streams.into_iter().map(|s| Mutex::new(Some(s))).collect()),
            properties,
        }
    }
}

impl Debug for MultiStreamExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiStreamExec")
    }
}

impl DisplayAs for MultiStreamExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MultiStreamExec")
    }
}

impl ExecutionPlan for MultiStreamExec {
    fn name(&self) -> &str {
        "MultiStreamExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    /// Returns the stream for `partition`, opened lazily on first poll.
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.streams
            .get(partition)
            .ok_or_else(|| {
                internal_datafusion_err!("MultiStreamExec: no stream for partition {partition}")
            })?
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "MultiStreamExec: stream for partition {partition} already consumed"
                )
            })
    }
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
///
/// This node has two variants.
/// 1. Pending: acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///    using Arrow Flight.
///
/// Below this consumer-task count, each consumer requests only its own partition slice directly
/// from each producer with no local re-hash. A local re-hash adds overhead that outweighs the
/// benefit at low fanout.
const SINGLE_STREAM_TASK_THRESHOLD_DEFAULT: usize = 3;
const PRODUCER_SALT_DEFAULT: u64 = 0x517cc1b727220a95;

pub struct NetworkShuffleExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: Arc<PlanProperties>,
    pub(crate) input_stage: Stage,
    pub(crate) worker_connections: WorkerConnectionPool,
    /// Cached partition streams for the single-stream path, keyed by `TaskContext` pointer.
    output_streams: Arc<Mutex<ExecutionStreamCache>>,
    pub(crate) producer_salt: u64,
    pub(crate) single_stream_threshold: usize,
}

impl NetworkShuffleExec {
    /// Opens one Arrow Flight stream per producer and merges them into per-partition output
    /// streams, returning a shared future so concurrent `execute()` calls share the same work.
    fn init_partition_streams(
        remote_stage: RemoteStage,
        worker_connections: WorkerConnectionPool,
        task_index: usize,
        producer_head: ProducerHead,
        schema: SchemaRef,
        partitioning: Partitioning,
        ctx: Arc<TaskContext>,
    ) -> SharedCachedStreamsFuture {
        let init_task = SpawnedTask::spawn(async move {
            let mut producer_streams = Vec::with_capacity(remote_stage.workers.len());
            for producer_task_index in 0..remote_stage.workers.len() {
                let wc = worker_connections.clone();
                let rs = remote_stage.clone();
                let ph = producer_head.clone();
                let ctx2 = Arc::clone(&ctx);
                producer_streams.push(Box::pin(RecordBatchStreamAdapter::new(
                    schema.clone(),
                    async move {
                        wc.open_single_stream(&rs, producer_task_index, task_index, ph, &ctx2)
                            .await
                    }
                    .try_flatten_stream(),
                )) as SendableRecordBatchStream);
            }

            let local_repartition = RepartitionExec::try_new(
                Arc::new(MultiStreamExec::new(producer_streams, schema)),
                partitioning,
            )?;
            let out_partitions = local_repartition.partitioning().partition_count();

            let partition_streams = (0..out_partitions)
                .map(|partition_index| -> Result<_> {
                    Ok(Mutex::new(Some(
                        local_repartition
                            .execute(partition_index, Arc::clone(&ctx))?
                            .boxed(),
                    )))
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Arc::new(partition_streams))
        });

        async move {
            match init_task.await {
                Ok(Ok(partition_streams)) => Ok(partition_streams),
                Ok(Err(e)) => Err(Arc::new(e)),
                Err(e) => Err(Arc::new(internal_datafusion_err!(
                    "JoinError in NetworkShuffleExec stream init: {e}"
                ))),
            }
        }
        .boxed()
        .shared()
    }

    pub(crate) fn from_stage(input_stage: Stage, input_properties: Arc<PlanProperties>) -> Self {
        Self::from_stage_with_producer_salt(
            input_stage,
            input_properties,
            PRODUCER_SALT_DEFAULT,
            SINGLE_STREAM_TASK_THRESHOLD_DEFAULT,
        )
    }

    pub(crate) fn from_stage_with_producer_salt(
        input_stage: Stage,
        input_properties: Arc<PlanProperties>,
        producer_salt: u64,
        single_stream_threshold: usize,
    ) -> Self {
        Self {
            properties: input_properties,
            worker_connections: WorkerConnectionPool::new(input_stage.task_count()),
            input_stage,
            output_streams: Arc::new(Mutex::new(HashMap::new())),
            producer_salt,
            single_stream_threshold,
        }
    }

    pub fn with_threshold(self, threshold: usize) -> Self {
        Self {
            single_stream_threshold: threshold,
            ..self
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
        self_clone.input_stage = input_stage;
        self_clone.output_streams = Default::default();
        Ok(Arc::new(self_clone))
    }

    fn producer_head(&self, consumer_task_count: usize) -> Result<ProducerHead> {
        let partitioning = if consumer_task_count < self.single_stream_threshold {
            let total = self.properties.partitioning.partition_count() * consumer_task_count;
            scale_partitioning(&self.properties.partitioning, |_| total)?
        } else {
            salted_partitioning(
                &self.properties.partitioning,
                self.producer_salt,
                consumer_task_count,
            )?
        };
        Ok(ProducerHead::RepartitionExec {
            partitioning: MaybeEncoded::Decoded(partitioning),
        })
    }
}

impl Debug for NetworkShuffleExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkShuffleExec")
            .field("properties", &self.properties)
            .field("input_stage", &self.input_stage)
            .finish()
    }
}

impl Clone for NetworkShuffleExec {
    fn clone(&self) -> Self {
        Self {
            properties: Arc::clone(&self.properties),
            input_stage: self.input_stage.clone(),
            worker_connections: self.worker_connections.clone(),
            output_streams: Arc::new(Mutex::new(HashMap::new())),
            producer_salt: self.producer_salt,
            single_stream_threshold: self.single_stream_threshold,
        }
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
        let out_partitions = self.properties.partitioning.partition_count();

        // Fallback: with few consumer tasks the N×M fan-out is small enough to be acceptable.
        if task_context.task_count < self.single_stream_threshold {
            let off = out_partitions * task_context.task_index;
            let total = out_partitions * task_context.task_count;
            let producer_head = ProducerHead::RepartitionExec {
                partitioning: MaybeEncoded::Decoded(scale_partitioning(
                    &self.properties.partitioning,
                    |_| total,
                )?),
            };
            let mut streams = Vec::with_capacity(remote_stage.workers.len());
            for input_task_index in 0..remote_stage.workers.len() {
                streams.push(self.worker_connections.execute(
                    remote_stage,
                    off..(off + out_partitions),
                    input_task_index,
                    off + partition,
                    producer_head.clone(),
                    &context,
                )?);
            }
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                futures::stream::select_all(streams),
            )));
        }

        let schema = self.schema();
        let partitioning = self.properties.partitioning.clone();
        let remote_stage = remote_stage.clone();
        let worker_connections = self.worker_connections.clone();
        let task_index = task_context.task_index;
        let producer_head = self.producer_head(task_context.task_count)?;
        let ctx = Arc::clone(&context);

        let ctx_ptr = Arc::as_ptr(&context) as usize;
        let cached_streams = {
            let mut guard = self.output_streams.lock().unwrap();
            guard
                .entry(ctx_ptr)
                .or_insert_with(|| {
                    Self::init_partition_streams(
                        remote_stage,
                        worker_connections,
                        task_index,
                        producer_head,
                        schema,
                        partitioning,
                        ctx,
                    )
                })
                .clone()
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            async move {
                let partition_streams = cached_streams.await.map_err(DataFusionError::Shared)?;
                let slot = partition_streams.get(partition).ok_or_else(|| {
                    internal_datafusion_err!(
                        "NetworkShuffleExec: no cached stream for partition {partition}"
                    )
                })?;
                slot.lock().unwrap().take().ok_or_else(|| {
                    internal_datafusion_err!(
                        "NetworkShuffleExec: stream for partition {partition} already consumed"
                    )
                })
            }
            .try_flatten_stream(),
        )))
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
            self.properties.output_partitioning().partition_count(),
            self.schema(),
        )
    }
}
