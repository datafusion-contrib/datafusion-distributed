use crate::common::require_one_child;
use dashmap::DashMap;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
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
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;

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
#[derive(Debug)]
pub struct BroadcastExec {
    input: Arc<dyn ExecutionPlan>,
    consumer_task_count: usize,
    properties: PlanProperties,
    tasks: Vec<OnceLock<Result<Arc<SpawnedTask<()>>, Arc<DataFusionError>>>>,
    txs: DashMap<usize, UnboundedSender<Result<RecordBatch, Arc<DataFusionError>>>>,
    rxs: DashMap<usize, UnboundedReceiver<Result<RecordBatch, Arc<DataFusionError>>>>,
}

impl BroadcastExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, consumer_task_count: usize) -> Self {
        let input_partition_count = input.properties().partitioning.partition_count();
        let output_partition_count = input_partition_count * consumer_task_count;

        let properties = input
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(output_partition_count));

        let txs = DashMap::with_capacity(output_partition_count);
        let rxs = DashMap::with_capacity(output_partition_count);
        for i in 0..output_partition_count {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            rxs.insert(i, rx);
            txs.insert(i, tx);
        }

        Self {
            input,
            consumer_task_count,
            properties,
            tasks: vec![OnceLock::new(); input_partition_count],
            txs,
            rxs,
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
        let schema = self.schema();
        let Some((_, rx)) = self.rxs.remove(&partition) else {
            return exec_err!("Partition {partition} was already executed");
        };

        let task = self.tasks[real_partition].get_or_init(|| {
            let mut txs = Vec::with_capacity(self.consumer_task_count);
            for off in 0..self.consumer_task_count {
                let p = real_partition + off * self.input_partition_count();
                let Some((_, tx)) = self.txs.remove(&p) else {
                    return exec_err!("Partition {p} is already being produced").map_err(Arc::new);
                };
                txs.push(tx);
            }
            let input = Arc::clone(&self.input);
            let ctx = Arc::clone(&context);
            Ok(Arc::new(SpawnedTask::spawn(async move {
                let mut stream = match input.execute(real_partition, ctx) {
                    Ok(s) => s,
                    Err(e) => {
                        let err = Arc::new(e);
                        for tx in &txs {
                            let _ = tx.send(Err(Arc::clone(&err)));
                        }
                        return;
                    }
                };
                while let Some(msg) = stream.next().await {
                    let msg = msg.map_err(Arc::new);
                    let mut all_closed = true;
                    for tx in &txs {
                        if tx.send(msg.clone()).is_ok() {
                            all_closed = false;
                        }
                    }
                    if all_closed || msg.is_err() {
                        return; // All consumers cancelled
                    }
                }
            })))
        });
        let task = match task {
            Ok(task) => Arc::clone(task),
            Err(err) => return Err(DataFusionError::Shared(Arc::clone(err))),
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            UnboundedReceiverStream::new(rx).map(move |msg| {
                let _ = &task; // keep the task alive as long as this stream is alive.
                msg.map_err(DataFusionError::Shared)
            }),
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
