use crate::common::require_one_child;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::internal_datafusion_err;
use datafusion::common::runtime::SpawnedTask;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::StreamExt;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::BroadcastStream;

/// [ExecutionPlan] that scales up partitions for network broadcasting.
///
/// This plan takes N input partitions and exposes N*M output partitions,
/// where M is the number of consumer tasks. Each virtual partition `i`
/// returns the cached result of input partition `i % N`.
///
/// This allows each consumer task to fetch a unique set of partition numbers,
/// the virtual partitions, while all receiving the same data via the actual partitions.
/// This structure maintains the invariant that each partition is executed exactly
/// once by the framework.
///
/// Broadcast is used in a 1 to many context, like this:
/// ```text
/// ┌────────────────────────┐      ┌────────────────────────┐                         ┌────────────────────────┐     ■
/// │  NetworkBroadcastExec  │      │  NetworkBroadcastExec  │           ...           │  NetworkBroadcastExec  │     │
/// │        (task 1)        │      │        (task 2)        │                         │        (task M)        │ Stage N+1
/// └┬─┬─────┬───┬───────────┘      └───────┬─┬─────┬────┬───┘                         └─────┬──────┬─────┬────┬┘     │
///  │0│     │N-1│                          │N│     │2N-1│                                   │(M-1)N│     │MN-1│      │
///  └▲┘ ... └▲──┘                          └▲┘ ... └──▲─┘                                   └───▲──┘ ... └──▲─┘      ■
///   │       │     Populates                │         │                                         │           │
///   │       └────Cache Index ───┐     Cache Hit   Cache Hit                    ┌──Cache Hit────┘           │
///   │                N-1        │      Index 0    Index N-1                    │                           │
///   └────Populates ─────┐       │          │         │                         │            ┌───Cache Hit──┘
///      Cache Index 0    │       │          │         │                         │            │
///                      ┌┴┐ ... ┌┴──┐      ┌┴┐ ... ┌──┴─┐        ...        ┌───┴──┐ ... ┌───┴┐                      ■
///                      │0│     │N-1│      │N│     │2N-1│                   │(M-1)N│     │MN-1│                      │
///                     ┌┴─┴─────┴───┴──────┴─┴─────┴────┴───────────────────┴──────┴─────┴────┴┐                     │
///                     │                             BroadcastExec                             │                     │
///                     │                     ┌───────────────────────────┐                     │                     │
///                     │                     │        Batch Cache        │                     │                  Stage N
///                     │                     │┌─────────┐     ┌─────────┐│                     │                     │
///                     │                     ││ index 0 │ ... │index N-1││                     │                     │
///                     │                     │└─────────┘     └─────────┘│                     │                     │
///                     │                     └───────────────────────────┘                     │                     │
///                     └───────────────────────────┬─┬──────────┬───┬──────────────────────────┘                     ■
///                                                 │0│          │N-1│
///                                                 └▲┘    ...   └─▲─┘
///                                                  │             │
///                                               ┌──┘             └──┐
///                                               │                   │                                               ■
///                                              ┌┴┐       ...     ┌──┴┐                                              │
///                                              │0│               │N-1│                                          Stage N-1
///                                             ┌┴─┴───────────────┴───┴┐                                             │
///                                             │Arc<dyn ExecutionPlan> │                                             │
///                                             └───────────────────────┘                                             ■
/// ```
///
/// Notice that the first consumer task, [NetworkBroadcastExec] task 1, triggers the execution of
/// the operator below the [BroadCastExec] and populates each cache index with the respective
/// partition. Subsequent consumer tasks, rather than executing the same partitions, read the
/// data from the cache for each partition.
type BroadcastReceiver = Receiver<Result<RecordBatch, Arc<DataFusionError>>>;

/// For each real partition, stores the spawned task and pre-created receivers for all consumers.
struct PartitionState {
    task: Arc<SpawnedTask<()>>,
    receivers: Vec<Option<BroadcastReceiver>>,
}

impl std::fmt::Debug for PartitionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionState")
            .field(
                "receivers_remaining",
                &self.receivers.iter().filter(|r| r.is_some()).count(),
            )
            .finish()
    }
}

#[derive(Debug)]
pub struct BroadcastExec {
    input: Arc<dyn ExecutionPlan>,
    consumer_task_count: usize,
    properties: PlanProperties,
    /// For each input partition, stores the spawned task and pre-created receivers.
    tasks: Vec<OnceLock<Result<Mutex<PartitionState>, Arc<DataFusionError>>>>,
}

impl BroadcastExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, consumer_task_count: usize) -> Self {
        let input_partition_count = input.properties().partitioning.partition_count();
        let output_partition_count = input_partition_count * consumer_task_count;

        let properties = input
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(output_partition_count));

        Self {
            input,
            consumer_task_count,
            properties,
            tasks: (0..input_partition_count)
                .map(|_| OnceLock::new())
                .collect(),
        }
    }

    pub fn input_partition_count(&self) -> usize {
        self.input.properties().partitioning.partition_count()
    }

    pub fn consumer_task_count(&self) -> usize {
        self.consumer_task_count
    }
}

impl DisplayAs for BroadcastExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_partition_count = self.input_partition_count();
        write!(
            f,
            "BroadcastExec: input_partitions={}, consumer_tasks={}, output_partitions={}",
            input_partition_count,
            self.consumer_task_count,
            input_partition_count * self.consumer_task_count
        )
    }
}

impl ExecutionPlan for BroadcastExec {
    fn name(&self) -> &str {
        "BroadcastExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            require_one_child(children)?,
            self.consumer_task_count,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let real_partition = partition % self.input_partition_count();
        let consumer_index = partition / self.input_partition_count();
        let schema = self.schema();

        // Initialize the broadcast channel, receivers, and spawned task for this real partition
        let result = self.tasks[real_partition].get_or_init(|| {
            let (tx, _) = tokio::sync::broadcast::channel(10000);

            // Pre-create all receivers before starting the task
            let receivers: Vec<Option<BroadcastReceiver>> = (0..self.consumer_task_count)
                .map(|_| Some(tx.subscribe()))
                .collect();

            let mut stream = self.input.execute(real_partition, context)?;

            let task = Arc::new(SpawnedTask::spawn(async move {
                while let Some(msg) = stream.next().await {
                    if tx.send(msg.map_err(Arc::new)).is_err() {
                        return; // All receivers dropped
                    }
                }
            }));

            Ok(Mutex::new(PartitionState { task, receivers }))
        });

        let state = match result {
            Ok(s) => s,
            Err(err) => return Err(DataFusionError::Shared(Arc::clone(err))),
        };

        // Take the receiver for this consumer
        let (rx, task) = {
            let mut guard = state.lock().unwrap();
            let rx = guard.receivers[consumer_index].take().ok_or_else(|| {
                internal_datafusion_err!("Partition {partition} was already executed")
            })?;
            (rx, Arc::clone(&guard.task))
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            BroadcastStream::new(rx).map(move |msg| {
                let _ = &task; // keep task alive
                match msg {
                    Ok(inner) => inner.map_err(DataFusionError::Shared),
                    Err(_recv_err) => Err(DataFusionError::Internal(
                        "Broadcast channel lagged".to_string(),
                    )),
                }
            }),
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
