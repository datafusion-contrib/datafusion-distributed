use crate::common::{OnceLockResult, require_one_child};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
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
use tokio_util::sync::CancellationToken;

const RECLAIM_INTERVAL: usize = 8;

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

type BroadcastMessage =
    std::result::Result<(RecordBatch, Arc<MemoryReservation>), Arc<DataFusionError>>;
type StreamAndTask = (BroadcastReaders<BroadcastMessage>, Arc<SpawnedTask<()>>);

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

impl Drop for BroadcastExec {
    fn drop(&mut self) {
        // The last plan reference is gone, so its unclaimed virtual partitions cannot
        // execute. Active streams may still hold their producer task and queue alive.
        for queue in &self.queues {
            if let Some(Ok((readers, _task))) = queue.get() {
                readers.release_pending();
            }
        }
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
        let input_partition_count = self.input_partition_count();
        let real_partition = partition % input_partition_count;
        let consumer_task = partition / input_partition_count;

        let input = Arc::clone(&self.input);

        let queue_or_err = self.queues[real_partition].get_or_init(|| {
            let queue = BroadcastQueue::new(self.consumer_task_count);
            let readers = queue.readers();

            let pool = Arc::clone(context.memory_pool());
            let mut stream = input.execute(real_partition, context).map_err(Arc::new)?;
            let cancel = queue.shared.cancel.clone();
            let task = SpawnedTask::spawn(async move {
                let mem_consumer = MemoryConsumer::new(format!("BroadcastExec[{real_partition}]"));

                loop {
                    let msg = tokio::select! {
                        _ = cancel.cancelled() => break,
                        msg = stream.next() => msg,
                    };
                    let Some(msg) = msg else { break };
                    match msg {
                        Ok(record_batch) => {
                            let reservation = mem_consumer.clone_with_new_id().register(&pool);
                            reservation.grow(record_batch.get_array_memory_size());
                            if !queue.push(Ok((record_batch, Arc::new(reservation)))) {
                                // If there are no remaining readers, short-circuit.
                                break;
                            }
                        }
                        Err(err) => {
                            let _ = queue.push(Err(Arc::new(err)));
                            break;
                        }
                    }
                }
            });

            Ok::<_, Arc<DataFusionError>>((readers, Arc::new(task)))
        });
        let (consumer, task) = match queue_or_err {
            Ok((readers, task)) => (readers.claim(consumer_task)?, Arc::clone(task)),
            Err(err) => return Err(DataFusionError::Shared(Arc::clone(err))),
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            consumer
                .map(|msg| match msg {
                    Ok((batch, _reservation)) => Ok(batch),
                    Err(e) => Err(DataFusionError::Shared(e)),
                })
                .inspect(move |_| {
                    let _ = &task;
                }),
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Represents the queue for a single real partition, which multiple virtual partitions may share.
/// Assume we have 4 consumer tasks each represented as a reader, this could create a situation as
/// such:
///
/// ```text
///
///              base_sequence             tail_sequence
///                    │                         │
///                    ▼                         ▼
///            ┌ ─ ─┌────┬────┬────┐     ┌────┬────┐
///  entries:    e0 │ e1 │ e2 │ e3 │ ... │eN-1│ eN │
///            └ ─ ─└────┴────┴────┘     └────┴────┘
///                         ▲                    ▲
///                  ┌──────┘   ┌────────────────┴────┐
///                  │          │                     │
///            ┌──────────┬──────────┬──────────┬──────────┐
///  readers:  │   r0:    │   r1:    │   r2:    │   r3:    │
///            │Reading(2)│Reading(N)│ Released │Reading(N)│
///            └──────────┴──────────┴──────────┴──────────┘
/// ```
///
/// The `base_sequence` represents the first retained entry while `tail_sequence` indicates the next
/// append position. In this example, entry `e0` is shown using a dashed line because it has already
/// been evicted and `base_sequence` now points at `e1`. Every retaining reader has advanced past
/// `e1`, so `e1` is reclaimable but remains temporarily retained until an append reaches the next
/// `RECLAIM_INTERVAL` boundary. At that boundary, the queue removes entries before the smallest
/// `next_sequence` (the most lagging retaining reader, `r0` here) and advances `base_sequence`:
///
/// ```text
///                   base_sequence             tail_sequence
///                         │                         │
///                         ▼                         ▼
///            ┌ ─ ─┌ ─ ─┌────┬────┐     ┌────┬────┬────┐
///  entries:    e0 │ e1 │ e2 │ e3 │ ... │eN-1│ eN │eN+1│
///            └ ─ ─└ ─ ─└────┴────┘     └────┴────┴────┘
///                         ▲                    ▲    ▲
///                  ┌──────┘   ┌────────────────┘    │
///                  │          │                     │
///            ┌──────────┬──────────┬──────────┬────────────┐
///  readers:  │   r0:    │   r1:    │   r2:    │    r3:     │
///            │Reading(2)│Reading(N)│ Released │Reading(N+1)│
///            └──────────┴──────────┴──────────┴────────────┘
/// ```
#[derive(Debug)]
struct QueueState<T> {
    entries: VecDeque<T>,
    base_sequence: usize,
    tail_sequence: usize,
    readers: Box<[ReaderSlot]>,
    retaining_readers: usize,
    closed: bool,
}

#[derive(Debug)]
enum ReaderSlot {
    Pending,
    Reading { next_sequence: usize },
    Released,
}

/// Shared state and signals for one real input partition.
///
/// The producer task owns the input stream and queue handle while every consumer stream shares the
/// same queue state but has its own reader cursor and notification receiver. There are two flows
/// this is responsible for: queue updates and cancellation.
///
/// ## Queue Push Flow:
///
/// ```text
///                                                                                  ┌───────────────────reads──────────────────┐
///                                                                                  │  ┌─────────(each acquire lock)─────────┐ │
///                                                                                  │  │ ┌─────────────────────────────────┐ │ │
///                                                                                  │  │ │                                 │ │ │
///                                                                                  │  │ │             ┌────────────────┐  │ │ │
///                                   ┌─────────────────────────┐                    │  │ │             │                │  │ │ │
///                                   │BroadcastShared          │                    │  │ │       ┌────▶│   Consumer 0   │──┘ │ │
///                                   │ ┌─────────────────────┐◀┼────────────────────┘  │ │       │     │                │    │ │
///                           ┌───────┼▶│  Mutex(QueueState)  │◀┼───────────────────────┘ │       │     └────────────────┘    │ │
///                         push      │ └─────────────────────┘◀┼─────────────────────────┘       │                           │ │
///                    (acquires lock)│            │notify      │                                 │                           │ │
/// ┌────────────────┐        │       │            ▼            │                                 │     ┌────────────────┐    │ │
/// │                │        │       │ ┌─────────────────────┐ │   notify   ┌───────────────┐    │     │                │    │ │
/// │    Producer    │────────┘       │ │       Sender        │ ├──signals──▶│ Watch Channel │─notify──▶│   Consumer 1   │────┘ │
/// │                │                │ └─────────────────────┘ │            └───────────────┘ signal   │                │      │
/// └────────────────┘                │ ┌─────────────────────┐ │                                 │     └────────────────┘      │
///                                   │ │  CancellationToken  │ │                                 │                             │
///                                   │ └─────────────────────┘ │                                 │           ...               │
///                                   └─────────────────────────┘                                 │                             │
///                                                                                               │     ┌────────────────┐      │
///                                                                                               │     │                │      │
///                                                                                               └────▶│   Consumer N   │──────┘
///                                                                                                     │                │
///                                                                                                     └────────────────┘
/// ```
///
/// A consumer stream has access to the shared `Arc`, but only `queue_state` is locked so its
/// `poll_next` holds that mutex while it reads an entry. The producer follows the same rule,
/// it appends under the mutex, then sends the notification after unlocking.
///
/// ## Cancellation flow:
///
/// ```text
/// ┌────────────────┐
/// │                │───────────────────────┐                      ┌─────────────────────────┐
/// │   Consumer 0   │─────────────┐         │                      │BroadcastShared          │       ┌────────────close ───────┐
/// │                │◀───────┐    │         │    release reader    │ ┌─────────────────────┐ │       │       (acquires lock)   │
/// └────────────────┘        │    │         └────(acquires lock)───┼▶│  Mutex(QueueState)  │◀┼───────┘                         │
///                           │    │                                │ └─────────────────────┘ │             ┌────────────────┐  │
///                           │    │                                │            │ notify on  │             │                │  │
/// ┌────────────────┐        │    │                                │            ▼   close    │     ┌──────▶│    Producer    │──┘
/// │                │        │    │   ┌───────────────┐    notify  │ ┌─────────────────────┐ │     │       │                │
/// │   Consumer 1   │◀────notify──┼───│ Watch Channel │◀──signals──┼─│       Sender        │ │  cancel     └────────────────┘
/// │                │     signal  │   └───────────────┘            │ └─────────────────────┘ │  signal
/// └────────────────┘        │    │                                │ ┌─────────────────────┐ │     │
///                           │    └────────────cancel──────────────┼▶│  CancellationToken  │─┼─────┘
///       ...                 │                                     │ └─────────────────────┘ │
///                           │                                     └─────────────────────────┘
/// ┌────────────────┐        │
/// │                │        │
/// │   Consumer N   │◀───────┘
/// │                │
/// └────────────────┘
/// ```
///
/// In this case, the consumer initiates the action by mutating the queue state to release itself.
/// Only in the case that the last reader has dropped the consumer will also set the
/// `CancellationToken` to tell the producer to close the queue.
///
/// Also, a consumer stream may outlive the `BroadcastExec`. In this case, the plan's `Drop` releases
/// only still `Pending` readers while active `BroadcastConsumer`s keep the shared state alive, and
/// keeps the producer task alive until those streams finish or are dropped.
#[derive(Debug)]
struct BroadcastShared<T: Clone> {
    queue_state: Mutex<QueueState<T>>,
    notify: tokio::sync::watch::Sender<()>,
    cancel: CancellationToken,
}

#[derive(Debug)]
struct BroadcastQueue<T: Clone> {
    shared: Arc<BroadcastShared<T>>,
}

#[derive(Debug)]
struct BroadcastReaders<T: Clone> {
    shared: Arc<BroadcastShared<T>>,
}

impl<T: Clone> BroadcastQueue<T> {
    fn new(expected_readers: usize) -> Self {
        let (notify, _rx) = tokio::sync::watch::channel(());
        Self {
            shared: Arc::new(BroadcastShared {
                queue_state: Mutex::new(QueueState {
                    entries: VecDeque::new(),
                    base_sequence: 0,
                    tail_sequence: 0,
                    readers: (0..expected_readers).map(|_| ReaderSlot::Pending).collect(),
                    retaining_readers: expected_readers,
                    closed: false,
                }),
                notify,
                cancel: CancellationToken::new(),
            }),
        }
    }

    fn readers(&self) -> BroadcastReaders<T> {
        BroadcastReaders {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Appends a value to the entry queue and increments `tail_sequence`. Every `RECLAIM_INTERVAL`
    /// calls, this checks for reclaimable entries in the queue.
    ///
    /// This method will not append the value and returns `false` if no retaining readers remain.
    fn push(&self, value: T) -> bool {
        let reclaimed = {
            let mut queue_state = self.shared.queue_state.lock().unwrap();

            if queue_state.retaining_readers == 0 {
                return false;
            }

            queue_state.entries.push_back(value);
            queue_state.tail_sequence += 1;
            if queue_state.tail_sequence.is_multiple_of(RECLAIM_INTERVAL) {
                Self::reclaim_processed_entries(&mut queue_state)
            } else {
                Vec::new()
            }
        };

        drop(reclaimed);

        self.shared.notify.send_replace(());
        true
    }

    /// Frees all entries in the queue that have been processed and updates `base_sequence` to point
    /// at the first non-freeable position.
    fn reclaim_processed_entries(queue_state: &mut QueueState<T>) -> Vec<T> {
        let minimum_sequence = queue_state
            .readers
            .iter()
            .filter_map(|reader| match reader {
                ReaderSlot::Pending => Some(0),
                ReaderSlot::Reading { next_sequence } => Some(*next_sequence),
                ReaderSlot::Released => None,
            })
            .min()
            .unwrap_or(queue_state.tail_sequence);

        let reclaim_count = minimum_sequence - queue_state.base_sequence;
        queue_state.base_sequence = minimum_sequence;
        queue_state.entries.drain(..reclaim_count).collect()
    }
}

impl<T: Clone> Drop for BroadcastQueue<T> {
    fn drop(&mut self) {
        {
            let mut state = self.shared.queue_state.lock().unwrap();
            state.closed = true;
        }
        self.shared.notify.send_replace(());
    }
}

impl<T: Clone> BroadcastReaders<T> {
    /// Creates a new `BroadcastConsumer` for a given consumer task and claims its reader slot,
    /// starting at sequence zero.
    ///
    /// Returns an error if the consumer task is out of range or if the same reader is claimed more
    /// than once.
    fn claim(&self, consumer_task: usize) -> Result<BroadcastConsumer<T>> {
        let rx = self.shared.notify.subscribe();
        let mut state = self.shared.queue_state.lock().unwrap();
        let Some(reader) = state.readers.get_mut(consumer_task) else {
            return internal_err!("broadcast consumer {consumer_task} is out of range");
        };
        match reader {
            ReaderSlot::Pending => *reader = ReaderSlot::Reading { next_sequence: 0 },
            ReaderSlot::Reading { .. } | ReaderSlot::Released => {
                return internal_err!("broadcast consumer {consumer_task} cannot execute twice");
            }
        }
        Ok(BroadcastConsumer {
            consumer_id: consumer_task,
            shared: Arc::clone(&self.shared),
            notify: WatchStream::new(rx),
        })
    }

    /// Sets all `ReaderSlot::Pending` readers to `ReaderSlot::Released`. This also cleans up newly
    /// freeable entries and cancels the producer if all readers are released.
    fn release_pending(&self) {
        let (no_readers_remain, reclaimed) = {
            let mut state = self.shared.queue_state.lock().unwrap();
            let mut released = 0;
            for reader in &mut state.readers {
                if matches!(reader, ReaderSlot::Pending) {
                    *reader = ReaderSlot::Released;
                    released += 1;
                }
            }
            state.retaining_readers -= released;
            let reclaimed = BroadcastQueue::<T>::reclaim_processed_entries(&mut state);
            (state.retaining_readers == 0, reclaimed)
        };

        drop(reclaimed);

        if no_readers_remain {
            self.shared.cancel.cancel();
        }
    }
}

/// A consumer stream that reads from the broadcast queue.
struct BroadcastConsumer<T: Clone> {
    consumer_id: usize,
    shared: Arc<BroadcastShared<T>>,
    notify: WatchStream<()>,
}

impl<T: Clone> Stream for BroadcastConsumer<T> {
    type Item = T;

    /// Poll the next value from the stream reading from the shared entry queue.
    ///
    /// TODO: Profile lock contention and inspect if a lock free implementation has better
    /// performance.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let (value, closed) = {
                let mut state = self.shared.queue_state.lock().unwrap();
                let next_sequence = match state.readers[self.consumer_id] {
                    ReaderSlot::Reading { next_sequence } => next_sequence,
                    ReaderSlot::Released => return Poll::Ready(None),
                    ReaderSlot::Pending => unreachable!("an unclaimed consumer was polled"),
                };
                if next_sequence < state.tail_sequence {
                    let offset = next_sequence
                        .checked_sub(state.base_sequence)
                        .expect("broadcast consumer fell behind evicted entries");
                    let value = state
                        .entries
                        .get(offset)
                        .expect("broadcast entry sequence was not retained")
                        .clone();
                    state.readers[self.consumer_id] = ReaderSlot::Reading {
                        next_sequence: next_sequence + 1,
                    };
                    (Some(value), false)
                } else {
                    (None, state.closed)
                }
            };

            if let Some(value) = value {
                return Poll::Ready(Some(value));
            }

            if closed {
                self.shared.release(self.consumer_id);
                return Poll::Ready(None);
            }

            match Pin::new(&mut self.notify).poll_next(cx) {
                Poll::Ready(Some(_)) => continue,
                Poll::Ready(None) => {
                    self.shared.release(self.consumer_id);
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T: Clone> BroadcastShared<T> {
    fn release(&self, consumer_id: usize) {
        let (no_readers_remain, reclaimed) = {
            let mut state = self.queue_state.lock().unwrap();
            if matches!(state.readers[consumer_id], ReaderSlot::Released) {
                return;
            }
            state.readers[consumer_id] = ReaderSlot::Released;
            state.retaining_readers -= 1;
            let reclaimed = BroadcastQueue::<T>::reclaim_processed_entries(&mut state);
            (state.retaining_readers == 0, reclaimed)
        };

        drop(reclaimed);

        if no_readers_remain {
            self.cancel.cancel();
        }
    }
}

impl<T: Clone> Drop for BroadcastConsumer<T> {
    fn drop(&mut self) {
        self.shared.release(self.consumer_id);
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

    fn buffered_len<T: Clone>(queue: &BroadcastQueue<T>) -> usize {
        queue.shared.queue_state.lock().unwrap().entries.len()
    }

    fn sequence_bounds<T: Clone>(queue: &BroadcastQueue<T>) -> (usize, usize) {
        let state = queue.shared.queue_state.lock().unwrap();
        (state.base_sequence, state.tail_sequence)
    }

    #[tokio::test]
    async fn broadcast_queue_evicts_consumed_prefix() {
        let queue = BroadcastQueue::new(2);
        let mut consumer0 = queue.readers().claim(0).expect("consumer 0 registration");
        let mut consumer1 = queue.readers().claim(1).expect("consumer 1 registration");

        queue.push(10);
        queue.push(20);
        assert_eq!(buffered_len(&queue), 2);

        assert_eq!(consumer0.next().await, Some(10));
        assert_eq!(buffered_len(&queue), 2);
        assert_eq!(consumer1.next().await, Some(10));
        assert_eq!(sequence_bounds(&queue), (0, 2));

        assert_eq!(consumer0.next().await, Some(20));
        assert_eq!(buffered_len(&queue), 2);
        assert_eq!(consumer1.next().await, Some(20));
        assert_eq!(buffered_len(&queue), 2);

        // The eighth append observes that consumers have advanced and removes the consumed prefix.
        for value in [30, 40, 50, 60, 70, 80] {
            queue.push(value);
        }
        assert_eq!(sequence_bounds(&queue), (2, 8));
        assert_eq!(buffered_len(&queue), 6);

        drop(consumer0);
        drop(consumer1);
        assert_eq!(buffered_len(&queue), 0);
    }

    #[tokio::test]
    async fn broadcast_queue_drop_releases_unread_entries() {
        let queue = BroadcastQueue::new(2);
        let mut consumer0 = queue.readers().claim(0).expect("consumer 0 registration");
        let mut consumer1 = queue.readers().claim(1).expect("consumer 1 registration");

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
        drop(consumer1);
        assert_eq!(buffered_len(&queue), 0);
    }

    #[tokio::test]
    async fn broadcast_queue_releases_reader_at_eof() {
        let queue = BroadcastQueue::new(1);
        let shared = Arc::clone(&queue.shared);
        let mut consumer = queue.readers().claim(0).expect("consumer registration");

        assert!(queue.push(10));
        assert_eq!(consumer.next().await, Some(10));
        drop(queue);

        assert_eq!(consumer.next().await, None);
        assert_eq!(shared.queue_state.lock().unwrap().entries.len(), 0);
        assert!(shared.cancel.is_cancelled());
        assert_eq!(consumer.next().await, None);
    }

    #[tokio::test]
    async fn broadcast_queue_cancels_when_all_readers_drop() {
        let queue = BroadcastQueue::new(2);
        let reader0 = queue.readers().claim(0).expect("consumer 0 registration");
        let reader1 = queue.readers().claim(1).expect("consumer 1 registration");

        assert!(queue.push(10));
        drop(reader0);
        assert!(!queue.shared.cancel.is_cancelled());
        drop(reader1);
        assert!(queue.shared.cancel.is_cancelled());
        assert_eq!(buffered_len(&queue), 0);
        assert!(!queue.push(20));
    }

    #[tokio::test]
    async fn broadcast_exec_releases_pending_readers_when_plan_drops() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input = Arc::new(MockExec::new_partitioned(vec![vec![]], Arc::clone(&schema)));
        let broadcast = BroadcastExec::new(input, 2);
        let task_ctx = SessionContext::new().task_ctx();

        let stream = broadcast.execute(0, task_ctx)?;
        let shared = Arc::clone(
            &broadcast.queues[0]
                .get()
                .expect("initialized queue")
                .as_ref()
                .expect("queue initialization")
                .0
                .shared,
        );

        drop(broadcast);
        assert_eq!(shared.queue_state.lock().unwrap().retaining_readers, 1);
        drop(stream);
        assert_eq!(shared.queue_state.lock().unwrap().retaining_readers, 0);
        assert!(shared.cancel.is_cancelled());

        Ok(())
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
    async fn broadcast_exec_claims_virtual_partition_once() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input = Arc::new(MockExec::new_partitioned(vec![vec![]], Arc::clone(&schema)));
        let broadcast = BroadcastExec::new(input, 2);
        let task_ctx = SessionContext::new().task_ctx();

        let _stream = broadcast.execute(0, Arc::clone(&task_ctx))?;
        let err = broadcast
            .execute(0, task_ctx)
            .err()
            .expect("duplicate execute");
        assert!(err.to_string().contains("consumer 0 cannot execute twice"));

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
