use crate::common::require_one_child;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::future::{BoxFuture, FutureExt, Shared, ready};
use futures::stream;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;

type CacheResult = Arc<Result<Arc<Vec<RecordBatch>>, DataFusionError>>;
type CacheFuture = Shared<BoxFuture<'static, CacheResult>>;
type CacheHandle = Arc<OnceCell<CacheFuture>>;

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
    cached_batches: Vec<CacheHandle>,
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
            .collect::<Vec<CacheHandle>>();

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
            // Initialized the cache immediately with a shared future. Actual execution runs on a
            // background tasks that will not be cancelled in the event that the consumer task is
            // cancelled (like a TopK). This way, any consumer that reuses the same results will
            // still see fetch from the cache even if the the first consumer was cancelled.
            let cached = cache
                .get_or_init(|| {
                    let input = Arc::clone(&input);
                    let context = Arc::clone(&context);
                    let handle: JoinHandle<Result<Arc<Vec<RecordBatch>>, DataFusionError>> =
                        tokio::spawn(async move {
                            let stream = input.execute(real_partition, context)?;
                            let batches: Vec<RecordBatch> =
                                futures::TryStreamExt::try_collect(stream).await?;
                            Ok(Arc::new(batches))
                        });
                    let cache_future: CacheFuture = async move {
                        match handle.await {
                            Ok(result) => Arc::new(result),
                            Err(err) => Arc::new(Err(DataFusionError::Execution(format!(
                                "BroadcastExec cache task failed: {err}"
                            )))),
                        }
                    }
                    .boxed()
                    .shared();
                    ready(cache_future)
                })
                .await
                .clone();

            let result = cached.await;
            let batches: Arc<Vec<RecordBatch>> = match result.as_ref() {
                Ok(batches) => Arc::clone(batches),
                Err(err) => return Err(DataFusionError::Execution(err.to_string())),
            };

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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::Statistics;
    use datafusion::error::DataFusionError;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    fn assert_int32_batch_values(batch: &RecordBatch, expected: &[i32]) {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        assert_eq!(values.len(), expected.len());
        for (idx, expected_value) in expected.iter().enumerate() {
            assert_eq!(values.value(idx), *expected_value);
        }
    }

    #[tokio::test]
    async fn broadcast_exec_reuses_cache_for_virtual_partitions() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let counts = Arc::new(vec![AtomicUsize::new(0)]);
        let input = Arc::new(CountingExec::new(
            Arc::clone(&schema),
            1,
            Arc::clone(&counts),
        ));
        let broadcast = Arc::new(BroadcastExec::new(input, 2));

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        let batches0 =
            datafusion::physical_plan::common::collect(broadcast.execute(0, task_ctx.clone())?)
                .await?;
        let batches1 =
            datafusion::physical_plan::common::collect(broadcast.execute(1, task_ctx)?).await?;

        // Only exxecutes the partition once, second batch is read from the cache
        assert_eq!(counts[0].load(Ordering::SeqCst), 1);
        assert_eq!(batches0.len(), 1);
        assert_eq!(batches1.len(), 1);
        assert_eq!(batches0[0].num_rows(), 1);
        assert_eq!(batches1[0].num_rows(), 1);
        assert_int32_batch_values(&batches0[0], &[0]);
        assert_int32_batch_values(&batches1[0], &[0]);

        Ok(())
    }

    #[tokio::test]
    async fn broadcast_exec_maps_partitions_by_modulo() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let counts = Arc::new(vec![AtomicUsize::new(0), AtomicUsize::new(0)]);
        let input = Arc::new(CountingExec::new(
            Arc::clone(&schema),
            2,
            Arc::clone(&counts),
        ));
        let broadcast = Arc::new(BroadcastExec::new(input, 2));

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        // Should map to real partition 0
        let batches0 =
            datafusion::physical_plan::common::collect(broadcast.execute(0, task_ctx.clone())?)
                .await?;
        // Should map to real partition 1
        let batches1 =
            datafusion::physical_plan::common::collect(broadcast.execute(1, task_ctx.clone())?)
                .await?;
        // Should map to real partition 0
        let batches2 =
            datafusion::physical_plan::common::collect(broadcast.execute(2, task_ctx.clone())?)
                .await?;
        // Should map to real partition 1
        let batches3 =
            datafusion::physical_plan::common::collect(broadcast.execute(3, task_ctx)?).await?;

        assert_eq!(counts[0].load(Ordering::SeqCst), 1);
        assert_eq!(counts[1].load(Ordering::SeqCst), 1);

        assert_eq!(batches0.len(), 1);
        assert_eq!(batches1.len(), 1);
        assert_eq!(batches2.len(), 1);
        assert_eq!(batches3.len(), 1);
        assert_int32_batch_values(&batches0[0], &[0]);
        assert_int32_batch_values(&batches1[0], &[1]);
        assert_int32_batch_values(&batches2[0], &[0]);
        assert_int32_batch_values(&batches3[0], &[1]);

        Ok(())
    }

    #[tokio::test]
    async fn broadcast_exec_cache_survives_cancellation() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let execute_count = Arc::new(AtomicUsize::new(0));
        let start_notify = Arc::new(Notify::new());
        let permit_open = Arc::new(AtomicBool::new(false));
        let permit_notify = Arc::new(Notify::new());

        let input = Arc::new(BlockingExec::new(
            Arc::clone(&schema),
            1,
            Arc::clone(&execute_count),
            Arc::clone(&start_notify),
            Arc::clone(&permit_open),
            Arc::clone(&permit_notify),
        ));

        // Has two consumers that will execute the same real partition
        let broadcast = Arc::new(BroadcastExec::new(input, 2));

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        let mut stream1 = broadcast.execute(0, task_ctx.clone())?;
        let handle = tokio::spawn(async move { stream1.next().await });

        // Execute should not finish until we set permit_open to true but the consumer count should
        // increment.
        timeout(Duration::from_secs(5), start_notify.notified())
            .await
            .expect("execute did not start");
        assert_eq!(execute_count.load(Ordering::SeqCst), 1);

        // Cancel this consumer (simulates a cancellation like a TopK)
        handle.abort();
        let _ = handle.await;

        // Execute with a different virtual partition but maps to same real partition and allow
        // full execution
        let stream2 = broadcast.execute(1, task_ctx)?;
        permit_open.store(true, Ordering::SeqCst);
        permit_notify.notify_waiters();

        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(stream2).await?;
        assert_eq!(batches.len(), 1);
        assert_int32_batch_values(&batches[0], &[1, 2, 3]);

        // Partition should only be executed a single time, second stream should've pull from
        // cache
        assert_eq!(execute_count.load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[derive(Debug)]
    struct CountingExec {
        schema: SchemaRef,
        // Should never be > 1 since partitions should not be rexecuted
        execute_counts: Arc<Vec<AtomicUsize>>,
        properties: PlanProperties,
    }

    impl CountingExec {
        fn new(
            schema: SchemaRef,
            partitions: usize,
            execute_counts: Arc<Vec<AtomicUsize>>,
        ) -> Self {
            let properties = PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                schema,
                execute_counts,
                properties,
            }
        }
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "CountingExec")
                }
                DisplayFormatType::TreeRender => write!(f, ""),
            }
        }
    }

    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &str {
            "CountingExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.execute_counts[partition].fetch_add(1, Ordering::SeqCst);

            let batch = RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(Int32Array::from(vec![partition as i32]))],
            )?;
            let stream = futures::stream::iter(vec![Ok(batch)]);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream,
            )))
        }

        fn statistics(&self) -> Result<Statistics> {
            Ok(Statistics::new_unknown(&self.schema))
        }

        fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
            Ok(Statistics::new_unknown(&self.schema))
        }
    }

    #[derive(Debug)]
    struct BlockingExec {
        schema: SchemaRef,
        partitions: usize,
        // Should never be > 1 since partitions should not be rexecuted
        execute_count: Arc<AtomicUsize>,
        // Informs the test that we have started blocking
        start_notify: Arc<Notify>,
        // If false, the stream will wait
        permit_open: Arc<AtomicBool>,
        // When permit_open flips to true, this is called and allows stream to continue.
        permit_notify: Arc<Notify>,
        properties: PlanProperties,
    }

    impl BlockingExec {
        fn new(
            schema: SchemaRef,
            partitions: usize,
            execute_count: Arc<AtomicUsize>,
            start_notify: Arc<Notify>,
            permit_open: Arc<AtomicBool>,
            permit_notify: Arc<Notify>,
        ) -> Self {
            let properties = PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                schema,
                partitions,
                execute_count,
                start_notify,
                permit_open,
                permit_notify,
                properties,
            }
        }
    }

    impl DisplayAs for BlockingExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "BlockingExec")
                }
                DisplayFormatType::TreeRender => write!(f, ""),
            }
        }
    }

    impl ExecutionPlan for BlockingExec {
        fn name(&self) -> &str {
            "BlockingExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            assert!(partition < self.partitions);
            self.execute_count.fetch_add(1, Ordering::SeqCst);
            // Notify we started executing the BlockingExec to we can cancel consumer.
            self.start_notify.notify_waiters();

            let schema = Arc::clone(&self.schema);
            let stream_schema = Arc::clone(&schema);
            let permit_open = Arc::clone(&self.permit_open);
            let permit_notify = Arc::clone(&self.permit_notify);

            let stream = futures::stream::once(async move {
                while !permit_open.load(Ordering::SeqCst) {
                    permit_notify.notified().await;
                }
                let batch = RecordBatch::try_new(
                    Arc::clone(&stream_schema),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                )?;
                Ok::<_, DataFusionError>(batch)
            });

            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }

        fn statistics(&self) -> Result<Statistics> {
            Ok(Statistics::new_unknown(&self.schema))
        }

        fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
            Ok(Statistics::new_unknown(&self.schema))
        }
    }
}
