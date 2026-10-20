use crate::common::on_drop_stream;
use crate::distributed_planner::ProducerHead;
use crate::passthrough_headers::get_passthrough_headers;
use crate::stage::RemoteStage;
use crate::{ExecuteTaskRequest, LocalWorkerContext, TaskKey, get_distributed_channel_resolver};
use dashmap::DashMap;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricBuilder;
use futures::future::{BoxFuture, Shared};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::{Arc, Mutex};

/// Manages connections to remote workers.
/// - Handles a range of partitions at a time in other to give the chance to [crate::WorkerChannel]
///   implementations to batch RecordBatch streams up, avoiding the overhead of multiple small
///   streams over an IO interface.
/// - Short circuits to a local in-memory connection if the remote worker that should be reached
///   happens to be the same one issuing the request.
/// - Lazily initializes connections per remote worker and requested partition range.
pub(crate) struct WorkerConnectionPool {
    lazy_stream_groups: Vec<DashMap<Range<usize>, StreamGroup>>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
}

/// Slots exist before asynchronous connection setup, so dropping a consumer can cancel
/// its receiver both before and after the worker returns the partition streams.
struct StreamGroup {
    slots: Vec<Arc<Mutex<StreamSlot>>>,
    ready: SharedBoxFuture<()>,
}

#[derive(Default)]
struct StreamSlot {
    stream: Option<BoxStream<'static, Result<RecordBatch>>>,
    claimed: bool,
    closed: bool,
}

/// Just some boilerplate for a shared future.
type SharedBoxFuture<T> = Shared<BoxFuture<'static, Result<T, Arc<DataFusionError>>>>;

impl WorkerConnectionPool {
    /// Builds a new [WorkerConnectionPool] with as many empty slots for worker stream entries as
    /// the provided `input_tasks`.
    pub(crate) fn new(input_tasks: usize) -> Self {
        Self {
            lazy_stream_groups: (0..input_tasks).map(|_| Default::default()).collect(),
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }

    pub(crate) fn execute(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        target_partition: usize,
        producer_head: ProducerHead,
        ctx: &Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let Some(target_url) = input_stage.workers.get(target_task).cloned() else {
            internal_err!("input_stage.workers[{target_task}] out of range.")?
        };
        let task_key = TaskKey {
            query_id: input_stage.query_id,
            stage_id: input_stage.num,
            task_number: target_task,
        };
        let bdr = || MetricBuilder::new(&self.metrics);
        let output_bytes = bdr().output_bytes(target_partition);
        let output_rows = bdr().output_rows(target_partition);
        let local_connections_used = bdr().global_counter("local_connections_used");

        // Otherwise, we need to reach the remote worker through the `WorkerChannel`. Unlike local
        // connections, these remote connections span a range of partitions so that `WorkerChannel`
        // implementations have the option of batch them.
        let Some(worker_connection) = self.lazy_stream_groups.get(target_task) else {
            return internal_err!(
                "WorkerConnections: Task index {target_task} not found, only have {} tasks",
                self.lazy_stream_groups.len()
            );
        };

        let entry = worker_connection.entry(target_partitions.clone());

        let group = entry.or_insert_with(|| {
            let slots: Vec<_> = target_partitions
                .clone()
                .map(|_| Arc::new(Mutex::new(StreamSlot::default())))
                .collect();
            let task_slots = slots.clone();
            let metrics = self.metrics.clone();
            let ctx = Arc::clone(ctx);

            // The relevant entry from `task_data_entries` needs to be eagerly retrieved, it cannot be
            // left for until someone decides to start polling the returned `BoxStream`, otherwise,
            // there's risk that the entry is evicted by Moka's TTL, and by the time the returned stream
            // is polled, the entry might not be there.
            //
            // Note that this does not start polling the returned streams, it just instantiates them.
            let streams_task = SpawnedTask::spawn(async move {
                let request = ExecuteTaskRequest {
                    task_key,
                    target_partition_start: target_partitions.start,
                    target_partition_end: target_partitions.end,
                    producer_head,
                };
                let mut client = match LocalWorkerContext::from_ctx(&ctx) {
                    Some(lw) if lw.self_url == target_url => {
                        local_connections_used.add(1);
                        Ok(lw.to_worker_channel())
                    }
                    _ => {
                        let ch_resolver = get_distributed_channel_resolver(ctx.as_ref());
                        ch_resolver.get_worker_client_for_url(&target_url).await
                    }
                }?;
                let headers = get_passthrough_headers(ctx.session_config());
                let streams = client.execute_task(headers, request, metrics, &ctx).await?;
                if streams.len() != task_slots.len() {
                    return internal_err!(
                        "Worker returned {} streams for {} partitions",
                        streams.len(),
                        task_slots.len()
                    );
                }
                for (slot, stream) in task_slots.iter().zip(streams) {
                    let mut slot = slot.lock().unwrap();
                    if !slot.closed {
                        slot.stream = Some(stream);
                    }
                }
                Ok(())
            });

            let ready = async move {
                match streams_task.await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(Arc::new(e)),
                    Err(e) => Err(Arc::new(internal_datafusion_err!(
                        "JoinError instantiating streams {e}"
                    ))),
                }
            }
            .boxed()
            .shared();
            StreamGroup { slots, ready }
        });

        let slot = target_partition
            .checked_sub(target_partitions.start)
            .and_then(|index| group.slots.get(index))
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "WorkerConnections has no stream for partition {target_partition}"
                )
            })?;
        {
            let mut slot = slot.lock().unwrap();
            if slot.claimed {
                return internal_err!(
                    "WorkerConnections stream for partition {target_partition} was already claimed"
                );
            }
            slot.claimed = true;
        }
        let slot = Arc::clone(slot);
        let drop_slot = Arc::clone(&slot);
        let ready = group.ready.clone();
        let stream = async move {
            ready.await.map_err(DataFusionError::Shared)?;
            slot.lock().unwrap().stream.take().ok_or_else(|| {
                internal_datafusion_err!(
                    "WorkerConnections has no stream for partition {target_partition}"
                )
            })
        }
        .try_flatten_stream()
        .inspect_ok(move |batch| {
            output_bytes.add(logical_record_batch_size(batch));
            output_rows.add(batch.num_rows());
        });
        // Install cleanup eagerly, so it also runs if the stream is never polled.
        Ok(on_drop_stream(stream, move || {
            let stream = {
                let mut slot = drop_slot.lock().unwrap();
                slot.closed = true;
                slot.stream.take()
            };
            drop(stream);
        })
        .boxed())
    }
}

/// Returns the logical size of a batch's slices, excluding unused backing-buffer capacity.
fn logical_record_batch_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| column.to_data().get_slice_memory_size().unwrap_or(0))
        .sum()
}

impl Debug for WorkerConnectionPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConnections")
            .field("num_connections", &self.lazy_stream_groups.len())
            .finish()
    }
}

impl Clone for WorkerConnectionPool {
    fn clone(&self) -> Self {
        Self::new(self.lazy_stream_groups.len())
    }
}

#[cfg(all(test, feature = "grpc"))]
mod tests {
    use super::*;
    use crate::grpc::DefaultChannelResolver;
    use crate::worker::test_utils::worker_handles::TcpWorkerHandle;
    use crate::{DistributedConfig, DistributedExt};
    use datafusion::arrow::array::Int32Array;
    use datafusion::prelude::SessionContext;
    use std::time::Duration;
    use test_case::test_case;
    use uuid::Uuid;

    enum DropWhen {
        BeforeSetup,
        AfterSetup,
        AfterBuffering,
    }

    #[test_case(DropWhen::BeforeSetup; "before_setup")]
    #[test_case(DropWhen::AfterSetup; "after_setup")]
    #[test_case(DropWhen::AfterBuffering; "after_buffering")]
    #[tokio::test]
    async fn abandoned_probe_does_not_block_sibling(when: DropWhen) -> Result<()> {
        let fixture = Fixture::new().await?;
        let idle = fixture.stream(0)?;
        let mut active = Box::pin(fixture.stream(1)?.try_collect::<Vec<_>>());
        match when {
            DropWhen::BeforeSetup => {}
            DropWhen::AfterSetup => fixture.wait_for_setup().await?,
            DropWhen::AfterBuffering => {
                assert!(
                    tokio::time::timeout(Duration::from_secs(1), &mut active)
                        .await
                        .is_err()
                );
                assert!(fixture.ctx.memory_pool().reserved() >= 128);
            }
        }
        // Models an inner join dropping an untouched probe after finding an empty build.
        drop(idle);
        let batches = tokio::time::timeout(Duration::from_secs(5), active)
            .await
            .expect("abandoned probe must not block its sibling")?;
        assert_values(&batches);
        // Keep the connection pool alive: cancellation must not depend on dropping the pool.
        assert_eq!(fixture.ctx.memory_pool().reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn live_unpolled_partition_preserves_backpressure_and_data() -> Result<()> {
        let fixture = Fixture::new().await?;
        let idle = fixture.stream(0)?;
        let mut active = Box::pin(fixture.stream(1)?.try_collect::<Vec<_>>());
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut active)
                .await
                .is_err()
        );
        assert!(fixture.ctx.memory_pool().reserved() >= 128);
        let (idle, active) = tokio::time::timeout(Duration::from_secs(5), async {
            futures::try_join!(idle.try_collect::<Vec<_>>(), active)
        })
        .await
        .expect("consuming both partitions should release backpressure")?;
        assert_values(&idle);
        assert_values(&active);
        assert_eq!(fixture.ctx.memory_pool().reserved(), 0);
        Ok(())
    }

    fn assert_values(batches: &[RecordBatch]) {
        let values: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(values, (0..1024).collect::<Vec<_>>());
    }

    struct Fixture {
        pool: WorkerConnectionPool,
        ctx: Arc<TaskContext>,
        stage: RemoteStage,
        _worker: TcpWorkerHandle,
    }

    impl Fixture {
        async fn new() -> Result<Self> {
            let batch = RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int32Array::from_iter_values(0..1024)) as _,
            )])?;
            let worker = TcpWorkerHandle::spawn(
                0,
                batch.schema(),
                vec![vec![batch.clone()], vec![batch]],
                None,
            )
            .await?;
            let query_id = Uuid::new_v4();
            worker.register_plan(query_id).await?;
            let ctx = SessionContext::new()
                .with_distributed_option_extension(DistributedConfig::default())
                .with_distributed_channel_resolver(DefaultChannelResolver::default())
                .with_distributed_worker_connection_buffer_budget_bytes(128)?
                .task_ctx();
            let stage = RemoteStage {
                query_id,
                num: 0,
                workers: vec![worker.url.clone()],
                runtime_stats: None,
                dynamic_filter_anchors: vec![],
            };
            Ok(Self {
                pool: WorkerConnectionPool::new(1),
                ctx,
                stage,
                _worker: worker,
            })
        }

        fn stream(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
            self.pool.execute(
                &self.stage,
                0..2,
                0,
                partition,
                ProducerHead::None,
                &self.ctx,
            )
        }

        async fn wait_for_setup(&self) -> Result<()> {
            let ready = self.pool.lazy_stream_groups[0]
                .get(&(0..2))
                .unwrap()
                .ready
                .clone();
            ready.await.map_err(DataFusionError::Shared)
        }
    }
}
