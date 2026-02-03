use crate::common::{on_drop_stream, require_one_child};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::SpawnedTask;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt, stream};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::sync::Notify;

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
    queue_entries: Vec<OnceLock<Arc<BroadcastQueueEntry>>>,
}

impl BroadcastExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, consumer_task_count: usize) -> Self {
        let input_partition_count = input.properties().partitioning.partition_count();
        let output_partition_count = input_partition_count * consumer_task_count;

        let properties = input
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(output_partition_count));

        let queue_entries = (0..input_partition_count)
            .map(|_| OnceLock::new())
            .collect();

        Self {
            input,
            consumer_task_count,
            properties,
            queue_entries,
        }
    }

    pub fn input_partition_count(&self) -> usize {
        self.input.properties().partitioning.partition_count()
    }

    pub fn consumer_task_count(&self) -> usize {
        self.consumer_task_count
    }

    /// Gets or initializes the broadcast queue entry for the given partition.
    fn get_or_init_entry(
        &self,
        partition: usize,
        ctx: &Arc<TaskContext>,
    ) -> Arc<BroadcastQueueEntry> {
        let expected_consumers = self.consumer_task_count;
        Arc::clone(self.queue_entries[partition].get_or_init(|| {
            let consumer = MemoryConsumer::new("BroadcastExec");
            let reservation = consumer.register(ctx.memory_pool());
            Arc::new(BroadcastQueueEntry::new(reservation, expected_consumers))
        }))
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
        let entry = self.get_or_init_entry(real_partition, &context);
        let schema = self.schema();

        entry.start_if_needed(Arc::clone(&self.input), real_partition, context)?;
        let stream = entry.stream();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// A single entry in the broadcast queue, holding results for one "real" partition.
/// This entry is produced by one task and consumed by multiple consumer tasks.
struct BroadcastQueueEntry {
    /// Shared state for producer and consumers.
    state: Mutex<BroadcastQueueState>,
    /// Notifies consumers when state changes.
    notify: Notify,
    /// Whether execution for this partition has begun.
    started: AtomicBool,
    /// Number of consumers that have completed.
    completed_consumers: AtomicUsize,
    /// Expected number of consumers.
    expected_consumers: usize,
}

impl std::fmt::Debug for BroadcastQueueEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastQueueEntry")
            .field("state", &self.state)
            .field("started", &self.started.load(Ordering::SeqCst))
            .field(
                "completed_consumers",
                &self.completed_consumers.load(Ordering::SeqCst),
            )
            .field("expected_consumers", &self.expected_consumers)
            .finish()
    }
}

/// Shared state for a broadcast queue entry.
struct BroadcastQueueState {
    /// Buffered [RecordBatch]es produced thus far.
    buffer: Vec<RecordBatch>,
    /// Whether execution for this partition has completed.
    done: bool,
    /// Error from the producer (if any).
    error: Option<Arc<DataFusionError>>,
    /// Producer task handle.
    task: Option<SpawnedTask<()>>,
    /// Memory reservation tracking buffer size.
    reservation: MemoryReservation,
}

impl std::fmt::Debug for BroadcastQueueState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastQueueState")
            .field("buffer_len", &self.buffer.len())
            .field("done", &self.done)
            .field("error", &self.error)
            .field("task", &self.task.is_some())
            .field("reservation_size", &self.reservation.size())
            .finish()
    }
}

/// Result from reading at a buffer index.
struct ReadResult {
    batch: Option<RecordBatch>,
    done: bool,
    error: Option<Arc<DataFusionError>>,
}

impl BroadcastQueueEntry {
    fn new(reservation: MemoryReservation, expected_consumers: usize) -> Self {
        let state = Mutex::new(BroadcastQueueState {
            buffer: Vec::new(),
            done: false,
            error: None,
            task: None,
            reservation,
        });

        Self {
            state,
            notify: Notify::new(),
            started: AtomicBool::new(false),
            completed_consumers: AtomicUsize::new(0),
            expected_consumers,
        }
    }

    /// Performs a modification to the internal state via the passed closure in a lock-safe manner.
    fn with_state<T>(
        &self,
        f: impl FnOnce(&mut BroadcastQueueState) -> T,
    ) -> Result<T, DataFusionError> {
        let mut guard = self.state.lock().map_err(|e| {
            DataFusionError::Execution(format!("broadcast queue lock poisoned: {e}"))
        })?;
        Ok(f(&mut guard))
    }

    /// Pushes a [RecordBatch] onto the buffer and notifies all consumers.
    fn push_batch(&self, batch: RecordBatch) -> Result<(), DataFusionError> {
        self.with_state(|s| {
            s.reservation.grow(batch.get_array_memory_size());
            s.buffer.push(batch);
        })?;
        self.notify.notify_waiters();
        Ok(())
    }

    /// Sets field `done` to true and notifies all consumers.
    fn finish_ok(&self) -> Result<(), DataFusionError> {
        self.with_state(|s| s.done = true)?;
        self.notify.notify_waiters();
        self.try_release_buffer();
        Ok(())
    }

    /// Sets field `err` to the passed error, `done` to true and notifies all consumers.
    fn finish_err(&self, err: Arc<DataFusionError>) -> Result<(), DataFusionError> {
        self.with_state(|s| {
            s.error = Some(err);
            s.done = true;
        })?;
        self.notify.notify_waiters();
        self.try_release_buffer();
        Ok(())
    }

    /// Attempts to release the buffer memory if both production is complete and all expected
    /// consumers have finished.
    fn try_release_buffer(&self) {
        let completed = self.completed_consumers.load(Ordering::SeqCst);
        if completed == self.expected_consumers {
            let _ = self.with_state(|s| {
                if s.done && !s.buffer.is_empty() {
                    let freed: usize = s.buffer.iter().map(|b| b.get_array_memory_size()).sum();
                    s.buffer.clear();
                    s.reservation.shrink(freed);
                }
            });
        }
    }

    /// Tries to start executing an input operator and populate the buffer.
    fn start_if_needed(
        self: &Arc<Self>,
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<(), DataFusionError> {
        if self
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }

        // Use Weak to avoid a ref-cycle (BroadcastQueueEntry -> task -> BroadcastQueueEntry)
        // and allow cleanup when all consumers drop.
        let entry = Arc::downgrade(self);
        let task = SpawnedTask::spawn(async move {
            let result: Result<(), DataFusionError> = async {
                let mut stream = input.execute(partition, ctx)?;
                while let Some(next) = stream.next().await {
                    let batch = next?;
                    let Some(entry) = Weak::upgrade(&entry) else {
                        return Ok(());
                    };
                    entry.push_batch(batch)?;
                }
                if let Some(entry) = Weak::upgrade(&entry) {
                    entry.finish_ok()?;
                }
                Ok(())
            }
            .await;

            if let Err(err) = result
                && let Some(entry) = Weak::upgrade(&entry)
            {
                let _ = entry.finish_err(Arc::new(err));
            }
        });

        // Store task handle to keep it alive (SpawnedTask aborts on drop)
        self.with_state(|s| s.task = Some(task))?;

        Ok(())
    }

    /// Reads the batch at the given index, or returns done/error status.
    fn read_at_index(&self, idx: usize) -> Result<ReadResult, DataFusionError> {
        self.with_state(|s| {
            if idx < s.buffer.len() {
                ReadResult {
                    batch: Some(s.buffer[idx].clone()),
                    done: false,
                    error: None,
                }
            } else {
                ReadResult {
                    batch: None,
                    done: s.done,
                    error: s.error.clone(),
                }
            }
        })
    }

    /// Called when a consumer stream is dropped.
    fn on_consumer_done(&self) {
        self.completed_consumers.fetch_add(1, Ordering::SeqCst);
        self.try_release_buffer();
    }

    /// Creates a stream of [RecordBatch]es.
    /// Will replay any batches already buffered and wait for more batches while the producer is
    /// running. Each consumer has its own index into the shared buffer and waits on `notify`
    /// when no new batches are available yet.
    fn stream(self: Arc<Self>) -> impl Stream<Item = Result<RecordBatch, DataFusionError>> {
        enum StreamState {
            Active {
                entry: Arc<BroadcastQueueEntry>,
                idx: usize,
            },
            Done,
        }

        let entry_for_drop = Arc::clone(&self);

        let stream = stream::unfold(
            StreamState::Active {
                entry: self,
                idx: 0,
            },
            move |state| async move {
                match state {
                    StreamState::Done => None,
                    StreamState::Active { entry, mut idx } => loop {
                        let entry_for_notify = Arc::clone(&entry);
                        let notified = entry_for_notify.notify.notified();

                        let result = match entry.read_at_index(idx) {
                            Ok(r) => r,
                            Err(err) => return Some((Err(err), StreamState::Done)),
                        };

                        if let Some(batch) = result.batch {
                            idx += 1;
                            return Some((Ok(batch), StreamState::Active { entry, idx }));
                        }

                        if result.done {
                            if let Some(err) = result.error {
                                return Some((
                                    Err(DataFusionError::Shared(err)),
                                    StreamState::Done,
                                ));
                            }
                            return None;
                        }

                        notified.await;
                    },
                }
            },
        );

        on_drop_stream(stream, move || entry_for_drop.on_consumer_done())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::mock_exec::MockExec;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::time::{Duration, sleep, timeout};

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
    async fn broadcast_exec_reuses_queue_for_virtual_partitions() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let counts = Arc::new(vec![AtomicUsize::new(0)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![0]))],
        )?;
        let input = Arc::new(
            MockExec::new_partitioned(vec![vec![Ok(batch)]], Arc::clone(&schema))
                .with_execute_counts(Arc::clone(&counts)),
        );
        let broadcast = Arc::new(BroadcastExec::new(input, 2));

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        let batches0 =
            datafusion::physical_plan::common::collect(broadcast.execute(0, task_ctx.clone())?)
                .await?;
        let batches1 =
            datafusion::physical_plan::common::collect(broadcast.execute(1, task_ctx)?).await?;

        // Only executes the partition once, second batch is read from the queue
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
        let batch0 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![0]))],
        )?;
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )?;
        let input = Arc::new(
            MockExec::new_partitioned(
                vec![vec![Ok(batch0)], vec![Ok(batch1)]],
                Arc::clone(&schema),
            )
            .with_execute_counts(Arc::clone(&counts)),
        );
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
    async fn broadcast_exec_queue_survives_cancellation() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let execute_counts = Arc::new(vec![AtomicUsize::new(0)]);
        let start_notify = Arc::new(Notify::new());
        let permit_open = Arc::new(AtomicBool::new(false));
        let permit_notify = Arc::new(Notify::new());

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let input = Arc::new(
            MockExec::new_partitioned(vec![vec![Ok(batch)]], Arc::clone(&schema))
                .with_execute_counts(Arc::clone(&execute_counts))
                .with_start_notify(Arc::clone(&start_notify))
                .with_gate(Arc::clone(&permit_open), Arc::clone(&permit_notify)),
        );

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
        assert_eq!(execute_counts[0].load(Ordering::SeqCst), 1);

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

        // Partition should only be executed a single time, second stream should've pulled from
        // queue
        assert_eq!(execute_counts[0].load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[tokio::test]
    async fn broadcast_exec_continues_after_consumer_cancel() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batches = vec![
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![0]))],
            )?),
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1]))],
            )?),
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![2]))],
            )?),
        ];
        let input = Arc::new(
            MockExec::new_partitioned(vec![batches], Arc::clone(&schema))
                .with_delay_between_batches(Duration::from_millis(10)),
        );
        let broadcast = Arc::new(BroadcastExec::new(input, 2));

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        let mut stream1 = broadcast.execute(0, task_ctx.clone())?;
        let stream2 = broadcast.execute(1, task_ctx)?;

        let first = stream1.next().await.transpose()?.expect("first batch");
        assert_int32_batch_values(&first, &[0]);
        drop(stream1);

        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(stream2).await?;
        assert_eq!(batches.len(), 3);
        assert_int32_batch_values(&batches[0], &[0]);
        assert_int32_batch_values(&batches[1], &[1]);
        assert_int32_batch_values(&batches[2], &[2]);

        Ok(())
    }

    #[tokio::test]
    async fn broadcast_exec_replay_for_late_consumer() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batches = vec![
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![0]))],
            )?),
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1]))],
            )?),
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![2]))],
            )?),
        ];
        let input = Arc::new(
            MockExec::new_partitioned(vec![batches], Arc::clone(&schema))
                .with_delay_between_batches(Duration::from_millis(10)),
        );
        let broadcast = Arc::new(BroadcastExec::new(input, 2));

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        let mut stream0 = broadcast.execute(0, task_ctx.clone())?;
        let batch0 = stream0.next().await.transpose()?.expect("batch 0");
        assert_int32_batch_values(&batch0, &[0]);
        let batch1 = stream0.next().await.transpose()?.expect("batch 1");
        assert_int32_batch_values(&batch1, &[1]);

        // Late consumer joins after producer has already emitted some batches.
        sleep(Duration::from_millis(5)).await;
        let stream1 = broadcast.execute(1, task_ctx)?;
        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(stream1).await?;
        assert_eq!(batches.len(), 3);
        assert_int32_batch_values(&batches[0], &[0]);
        assert_int32_batch_values(&batches[1], &[1]);
        assert_int32_batch_values(&batches[2], &[2]);

        Ok(())
    }
}
