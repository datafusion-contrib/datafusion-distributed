use crate::common::{OnceLockResult, require_one_child};
use crossbeam_queue::SegQueue;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, internal_err,
};
use futures::{Stream, StreamExt};
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use tokio_stream::wrappers::WatchStream;

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
    properties: Arc<PlanProperties>,
    queues: Vec<OnceLockResult<StreamAndTask>>,
}

type StreamAndTask = (SegQueue<SendableRecordBatchStream>, Arc<SpawnedTask<()>>);

impl BroadcastExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, consumer_task_count: usize) -> Self {
        let input_partition_count = input.properties().partitioning.partition_count();
        let output_partition_count = input_partition_count * consumer_task_count;

        let properties = <PlanProperties as Clone>::clone(&input.properties().clone())
            .with_partitioning(Partitioning::UnknownPartitioning(output_partition_count));

        let queues = (0..input_partition_count)
            .map(|_| OnceLock::new())
            .collect();

        Self {
            input,
            consumer_task_count,
            properties: Arc::new(properties),
            queues,
        }
    }

    pub fn input_partition_count(&self) -> usize {
        self.input.properties().partitioning.partition_count()
    }

    pub fn consumer_task_count(&self) -> usize {
        self.consumer_task_count
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
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

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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

        let input = Arc::clone(&self.input);

        let queue_or_err = self.queues[real_partition].get_or_init(|| {
            let queue = BroadcastQueue::new(self.consumer_task_count);
            let consumers = SegQueue::new();
            for _ in 0..self.consumer_task_count {
                consumers.push(Box::pin(RecordBatchStreamAdapter::new(
                    self.schema(),
                    queue.new_consumer().map(|msg| match msg {
                        Ok((batch, _reservation)) => Ok(batch),
                        Err(e) => Err(DataFusionError::Shared(e)),
                    }),
                )) as SendableRecordBatchStream);
            }

            let pool = Arc::clone(context.memory_pool());
            let mut stream = input.execute(real_partition, context).map_err(Arc::new)?;
            let task = SpawnedTask::spawn(async move {
                let mem_consumer = MemoryConsumer::new(format!("BroadcastExec[{real_partition}]"));

                while let Some(msg) = stream.next().await {
                    match msg {
                        Ok(record_batch) => {
                            let reservation = mem_consumer.clone_with_new_id().register(&pool);
                            reservation.grow(record_batch.get_array_memory_size());
                            queue.push(Ok((record_batch, Arc::new(reservation))));
                        }
                        Err(err) => {
                            queue.push(Err(Arc::new(err)));
                            break;
                        }
                    }
                }
            });

            Ok::<_, Arc<DataFusionError>>((consumers, Arc::new(task)))
        });
        let (consumer, task) = match queue_or_err {
            Ok((consumers, task)) => (consumers.pop(), Arc::clone(task)),
            Err(err) => return Err(DataFusionError::Shared(Arc::clone(err))),
        };
        let Some(consumer) = consumer else {
            return internal_err!("Too many consumers for real partition {real_partition}");
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            consumer.inspect(move |_| {
                let _ = &task;
            }),
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

#[derive(Debug)]
struct Entry<T> {
    value: T,
    remaining_readers: usize,
}

#[derive(Debug, Clone, Copy)]
struct BroadcastState {
    tail_sequence: usize,
    closed: bool,
}

#[derive(Debug)]
struct QueueState<T> {
    entries: VecDeque<Entry<T>>,
    base_sequence: usize,
    tail_sequence: usize,
    active_readers: usize,
}

#[derive(Debug)]
struct BroadcastQueue<T: Clone> {
    state: Arc<Mutex<QueueState<T>>>,
    notify: tokio::sync::watch::Sender<BroadcastState>,
}

impl<T: Clone> BroadcastQueue<T> {
    fn new(expected_readers: usize) -> Self {
        let (notify, _rx) = tokio::sync::watch::channel(BroadcastState {
            tail_sequence: 0,
            closed: false,
        });
        Self {
            state: Arc::new(Mutex::new(QueueState {
                entries: VecDeque::new(),
                base_sequence: 0,
                tail_sequence: 0,
                active_readers: expected_readers,
            })),
            notify,
        }
    }

    fn new_consumer(&self) -> BroadcastConsumer<T> {
        let rx = self.notify.subscribe();
        let state = *rx.borrow();
        BroadcastConsumer {
            next_sequence: 0,
            state: Arc::clone(&self.state),
            notify: WatchStream::new(rx),
            notification: state,
            registered: true,
        }
    }

    fn push(&self, value: T) {
        let tail_sequence = {
            let mut queue_state = self.state.lock().unwrap();

            // Once every consumer has been dropped, no future consumer can execute this
            // queue, so don't add values that cannot be read.
            if queue_state.active_readers == 0 {
                return;
            }

            let tail_sequence = queue_state.tail_sequence;
            let remaining_readers = queue_state.active_readers;
            queue_state.entries.push_back(Entry {
                value,
                remaining_readers,
            });
            queue_state.tail_sequence += 1;
            tail_sequence + 1
        };

        let mut broadcast_state = *self.notify.borrow();
        broadcast_state.tail_sequence = tail_sequence;
        let _ = self.notify.send(broadcast_state);
    }

    #[cfg(test)]
    fn buffered_len(&self) -> usize {
        self.state.lock().unwrap().entries.len()
    }
}

impl<T: Clone> Drop for BroadcastQueue<T> {
    fn drop(&mut self) {
        let mut state = *self.notify.borrow();
        state.closed = true;
        let _ = self.notify.send(state);
    }
}

/// A consumer stream that reads from the broadcast queue.
struct BroadcastConsumer<T: Clone> {
    next_sequence: usize,
    state: Arc<Mutex<QueueState<T>>>,
    notify: WatchStream<BroadcastState>,
    notification: BroadcastState,
    registered: bool,
}

impl<T: Clone> Stream for BroadcastConsumer<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let value = {
                let mut queue_state = self.state.lock().unwrap();
                if self.next_sequence < queue_state.tail_sequence {
                    let offset = self
                        .next_sequence
                        .checked_sub(queue_state.base_sequence)
                        .expect("broadcast consumer fell behind evicted entries");
                    let entry = queue_state
                        .entries
                        .get_mut(offset)
                        .expect("broadcast entry sequence was not retained");
                    debug_assert!(entry.remaining_readers > 0);
                    let value = entry.value.clone();
                    entry.remaining_readers -= 1;

                    while queue_state
                        .entries
                        .front()
                        .is_some_and(|entry| entry.remaining_readers == 0)
                    {
                        queue_state.entries.pop_front();
                        queue_state.base_sequence += 1;
                    }

                    Some(value)
                } else {
                    None
                }
            };

            if let Some(value) = value {
                self.next_sequence += 1;
                return Poll::Ready(Some(value));
            }

            if self.notification.closed {
                return Poll::Ready(None);
            }

            match Pin::new(&mut self.notify).poll_next(cx) {
                Poll::Ready(Some(state)) => {
                    self.notification = state;
                }
                Poll::Ready(None) => {
                    self.notification.closed = true;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T: Clone> Drop for BroadcastConsumer<T> {
    fn drop(&mut self) {
        if !self.registered {
            return;
        }

        let mut state = self.state.lock().unwrap();
        debug_assert!(self.next_sequence >= state.base_sequence);
        let offset = self.next_sequence.saturating_sub(state.base_sequence);
        for entry in state.entries.iter_mut().skip(offset) {
            entry.remaining_readers = entry.remaining_readers.saturating_sub(1);
        }
        state.active_readers = state.active_readers.saturating_sub(1);

        while state
            .entries
            .front()
            .is_some_and(|entry| entry.remaining_readers == 0)
        {
            state.entries.pop_front();
            state.base_sequence += 1;
        }
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
    use tokio::time::{Duration, sleep};

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
    async fn broadcast_queue_evicts_consumed_prefix() {
        let queue = BroadcastQueue::new(2);
        let mut consumer0 = queue.new_consumer();
        let mut consumer1 = queue.new_consumer();

        queue.push(10);
        queue.push(20);
        assert_eq!(queue.buffered_len(), 2);

        assert_eq!(consumer0.next().await, Some(10));
        assert_eq!(queue.buffered_len(), 2);
        assert_eq!(consumer1.next().await, Some(10));
        assert_eq!(queue.buffered_len(), 1);

        assert_eq!(consumer0.next().await, Some(20));
        assert_eq!(queue.buffered_len(), 1);
        assert_eq!(consumer1.next().await, Some(20));
        assert_eq!(queue.buffered_len(), 0);
    }

    #[tokio::test]
    async fn broadcast_queue_drop_releases_unread_entries() {
        let queue = BroadcastQueue::new(2);
        let mut consumer0 = queue.new_consumer();
        let mut consumer1 = queue.new_consumer();

        queue.push(10);
        queue.push(20);
        assert_eq!(consumer0.next().await, Some(10));
        drop(consumer0);

        // The dropped consumer must no longer pin the unread suffix, including entries produced
        // after cancellation.
        queue.push(30);
        assert_eq!(consumer1.next().await, Some(10));
        assert_eq!(consumer1.next().await, Some(20));
        assert_eq!(consumer1.next().await, Some(30));
        assert_eq!(queue.buffered_len(), 0);
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
        let permit_open = Arc::new(AtomicBool::new(false));
        let permit_notify = Arc::new(Notify::new());

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let input = Arc::new(
            MockExec::new_partitioned(vec![vec![Ok(batch)]], Arc::clone(&schema))
                .with_execute_counts(Arc::clone(&execute_counts))
                .with_gate(Arc::clone(&permit_open), Arc::clone(&permit_notify)),
        );

        // Has two consumers that will execute the same real partition
        let broadcast = Arc::new(BroadcastExec::new(input, 2));

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        // Execute is called synchronously, so execute_counts should increment immediately
        let mut stream1 = broadcast.execute(0, task_ctx.clone())?;
        assert_eq!(execute_counts[0].load(Ordering::SeqCst), 1);

        #[allow(clippy::disallowed_methods)]
        let handle = tokio::spawn(async move { stream1.next().await });

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
