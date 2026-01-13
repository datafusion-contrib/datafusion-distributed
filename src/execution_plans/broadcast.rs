use crate::common::require_one_child;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tokio::sync::OnceCell;

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
/// the operator below the [BroadCastExec] and populates each cache index with the repective
/// partition. Subsequent consumer tasks, rather than executing the same partitions, read the
/// data from the cache for each partition.
#[derive(Debug)]
pub struct BroadcastExec {
    input: Arc<dyn ExecutionPlan>,
    consumer_task_count: usize,
    properties: PlanProperties,
    cached_batches: Vec<Arc<OnceCell<Arc<Vec<RecordBatch>>>>>,
}

impl BroadcastExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, consumer_task_count: usize) -> Self {
        let input_partition_count = input.properties().partitioning.partition_count();
        let output_partition_count = input_partition_count * consumer_task_count;

        let properties = input
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(output_partition_count));

        let cached_batches = (0..input_partition_count)
            .map(|_| Arc::new(OnceCell::new()))
            .collect();

        Self {
            input,
            consumer_task_count,
            properties,
            cached_batches,
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
        let cache = Arc::clone(&self.cached_batches[real_partition]);
        let input = Arc::clone(&self.input);
        let schema = self.schema();

        // TODO: Stream batches as they're produced instead of collect-then-emit. Currently we
        // wait for all batches before consumers receive any. Streaming would allow overlapping
        // production with network transfer.
        //
        // Challenges: late subscribers must replay from buffer since tokio::sync::broadcast drops old messages,
        // need proper error propagation to all consumers and backpressure handling.
        let stream = futures::stream::once(async move {
            let batches = cache
                .get_or_try_init(|| async {
                    let stream = input.execute(real_partition, context)?;
                    let batches: Vec<RecordBatch> =
                        futures::TryStreamExt::try_collect(stream).await?;
                    Ok::<_, datafusion::error::DataFusionError>(Arc::new(batches))
                })
                .await?;
            let batches = Arc::clone(batches);
            let batches_vec: Vec<RecordBatch> = batches.iter().cloned().collect();
            Ok::<_, datafusion::error::DataFusionError>(stream::iter(
                batches_vec.into_iter().map(Ok),
            ))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::TryStreamExt::try_flatten(stream),
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
