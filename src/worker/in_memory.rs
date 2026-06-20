//! An in-process [WorkerTransport]: every "worker" is the current process, so plans are delivered
//! with a function call and partitions are read straight from the local task registry. It is the
//! default transport when the `flight` feature is off, and the reference implementation for the
//! transport extension points: it goes through the same plan encode/decode, session building,
//! work-unit feed, and metrics delivery as a remote transport, just without a wire underneath.

use crate::common::{deserialize_uuid, serialize_uuid};
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::coordinator::{CoordinatorToWorkerMetrics, encode_task_plan};
use crate::distributed_planner::ProducerHead;
use crate::networking::set_distributed_worker_transport;
use crate::passthrough_headers::get_passthrough_headers;
use crate::stage::RemoteStage;
use crate::work_unit_feed::{collect_task_work_unit_feeds, set_received_time, set_sent_time};
use crate::worker::WorkerSessionBuilder;
use crate::worker::generated::worker as pb;
use crate::worker::impl_execute_task::execute_local_task;
use crate::worker::transport::{
    WorkerConnection, WorkerDispatch, WorkerDispatchRequest, WorkerTransport,
};
use crate::worker::worker_service::{TaskDataEntries, Worker};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricBuilder;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// [WorkerTransport] that hosts its workers in the current process.
///
/// All tasks share one [Worker] (one task registry, one session builder): task keys carry the
/// query, stage, and task number, so a single registry isolates them. The worker resolver's URLs
/// only size the stages; nothing is dialed.
///
/// With the `flight` feature off this is the default transport, so distributed plans run out of the
/// box without the Flight stack. Embedders with their own comms register a custom transport.
#[derive(Clone, Default)]
pub struct InMemoryWorkerTransport {
    worker: Worker,
}

impl InMemoryWorkerTransport {
    /// Builds the transport around an existing [Worker], sharing its task registry, session
    /// builder, and runtime environment.
    pub fn new(worker: Worker) -> Self {
        Self { worker }
    }

    /// Builds the transport with a custom [WorkerSessionBuilder], the same customization hook a
    /// remote worker offers.
    pub fn from_session_builder(
        session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    ) -> Self {
        Self {
            worker: Worker::from_session_builder(session_builder),
        }
    }

    /// Builds the transport from a custom [WorkerSessionBuilder], then runs `configure_worker` over
    /// the hosted [Worker], so a test can install worker-level hooks the session builder can't reach
    /// (e.g. `add_on_plan_hook`).
    pub fn from_configured_worker(
        session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
        configure_worker: impl FnOnce(Worker) -> Worker,
    ) -> Self {
        Self {
            worker: configure_worker(Worker::from_session_builder(session_builder)),
        }
    }

    /// The in-process [Worker] backing this transport.
    pub fn worker(&self) -> &Worker {
        &self.worker
    }
}

impl WorkerTransport for InMemoryWorkerTransport {
    fn open(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        producer_head: ProducerHead,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn WorkerConnection>> {
        Ok(Box::new(InMemoryWorkerConnection::init(
            input_stage,
            target_partitions,
            target_task,
            producer_head,
            Arc::clone(&self.worker.task_data_entries),
            ctx,
            metrics,
        )?))
    }

    fn dispatcher(&self) -> Box<dyn WorkerDispatch> {
        Box::new(InMemoryWorkerDispatcher {
            worker: self.worker.clone(),
            metrics: OnceLock::new(),
        })
    }
}

/// Per-query plan-delivery state for the in-memory transport. As with Flight, the plan-send
/// metrics and the query start timestamp live for the whole query, not per stage.
struct InMemoryWorkerDispatcher {
    worker: Worker,
    metrics: OnceLock<CoordinatorToWorkerMetrics>,
}

/// Delivery is [Worker::set_task_plan] called directly: the plan still round-trips through the
/// session's codec stack, so each task executes its own decoded instance (plan nodes carry
/// per-execution state, sharing one tree between tasks is not sound) and codec gaps surface the
/// same way they would over a wire.
impl WorkerDispatch for InMemoryWorkerDispatcher {
    fn dispatch(&self, request: WorkerDispatchRequest<'_>) -> Result<()> {
        let WorkerDispatchRequest {
            stage,
            routed_urls,
            task_ctx,
            metrics,
            metrics_store,
            join_set,
            ..
        } = request;
        let metrics = self
            .metrics
            .get_or_init(|| CoordinatorToWorkerMetrics::new(metrics))
            .clone();

        let mut headers = get_config_extension_propagation_headers(task_ctx.session_config())?;
        headers.extend(get_passthrough_headers(task_ctx.session_config()));

        for (task_i, url) in routed_urls.iter().enumerate() {
            let encoded =
                encode_task_plan(&stage.plan, task_i, stage.tasks, task_ctx.session_config())?;
            let plan_size = encoded.plan_proto.len();

            let task_key = pb::TaskKey {
                query_id: serialize_uuid(&stage.query_id),
                stage_id: stage.num as u64,
                task_number: task_i as u64,
            };
            let set_plan = pb::SetPlanRequest {
                plan_proto: encoded.plan_proto,
                task_count: stage.tasks as u64,
                task_key: Some(task_key.clone()),
                work_unit_feed_declarations: encoded.feed_declarations,
                target_worker_url: url.to_string(),
                query_start_time_ns: metrics.instantiation_time,
            };

            // Collected before spawning so the providers see the same eager `feed()` timing as
            // they do under Flight.
            let feed_streams =
                collect_task_work_unit_feeds(&stage.plan, task_ctx, task_i, stage.tasks)?;

            let worker = self.worker.clone();
            let transport = InMemoryWorkerTransport::new(self.worker.clone());
            let headers = headers.clone();
            let metrics = metrics.clone();
            let metrics_store = metrics_store.cloned();
            join_set.spawn(async move {
                let start = Instant::now();
                let outcome = worker
                    .set_task_plan(set_plan, headers, move |mut cfg| {
                        // Child-stage reads inside the decoded worker plan consult the worker
                        // session for a transport; they must land on this same task registry.
                        set_distributed_worker_transport(&mut cfg, transport);
                        Ok(cfg)
                    })
                    .await?;
                metrics.plan_send_latency.record(&start);
                metrics.plan_bytes_sent.add_bytes(plan_size);

                // Detached like Flight's metrics collection: the receiver resolves only once every
                // partition finished or was dropped, and a task whose partitions are never opened
                // must not stall query completion.
                let metrics_rx = outcome.metrics_rx;
                #[allow(clippy::disallowed_methods)]
                tokio::spawn(async move {
                    if let (Ok(task_metrics), Some(store)) = (metrics_rx.await, metrics_store) {
                        store.insert(task_key, task_metrics);
                    }
                });

                // Pump the work-unit feeds straight into the worker-side channels. Both hop stamps
                // are set here; the latency metrics read as (near) zero, which is what an in-process
                // hop is.
                let senders = Arc::new(outcome.work_unit_senders);
                let mut pumps = vec![];
                for mut stream in feed_streams {
                    let senders = Arc::clone(&senders);
                    pumps.push(async move {
                        while let Some(unit) = stream.next().await {
                            let mut unit = unit?;
                            set_sent_time(&mut unit);
                            set_received_time(&mut unit);
                            let Ok(id) = deserialize_uuid(&unit.id) else {
                                continue;
                            };
                            let Some(tx) = senders.get(&(id, unit.partition as usize)) else {
                                continue;
                            };
                            if tx.send(Ok(unit)).is_err() {
                                break; // channel closed
                            }
                        }
                        Ok::<_, DataFusionError>(())
                    });
                }
                futures::future::try_join_all(pumps).await?;
                Ok(())
            });
        }
        Ok(())
    }
}

/// [WorkerConnection] over the local task registry: builds the per-partition record batch streams
/// straight from [execute_local_task], with no encoding in between.
pub(crate) struct InMemoryWorkerConnection {
    partition_start: usize,
    local_streams: Vec<Mutex<Option<BoxStream<'static, Result<RecordBatch>>>>>,
    /// Drives the single `execute_local_task` call that produces every partition stream. Held so it
    /// lives as long as the connection (the pool keeps it for the query), not aborted early.
    _driver: SpawnedTask<()>,
}

impl InMemoryWorkerConnection {
    pub(crate) fn init(
        input_stage: &RemoteStage,
        target_partition_range: Range<usize>,
        target_task: usize,
        producer_head: ProducerHead,
        task_data_entries: Arc<TaskDataEntries>,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        MetricBuilder::new(metrics)
            .global_counter("local_connections_used")
            .add(1);

        let task_key = pb::TaskKey {
            query_id: serialize_uuid(&input_stage.query_id),
            stage_id: input_stage.num as u64,
            task_number: target_task as u64,
        };
        let producer_head = producer_head.to_proto(ctx)?;

        let partition_start = target_partition_range.start;
        let n_partitions = target_partition_range.len();

        // Execute the whole partition range in ONE `execute_local_task` call, then drive each
        // partition stream from a dedicated pump into a buffer the consumer reads from. This
        // decouples the worker plan's execution from when (or whether) the consumer pulls, exactly
        // as Flight gets from its gRPC stream. Letting the consumer drive the worker plan directly
        // (its `SortPreservingMergeExec` interleaving the polls of a partitioned `HashJoinExec`) can
        // otherwise leave some partitions empty.
        let request = pb::ExecuteTaskRequest {
            task_key: Some(task_key),
            target_partition_start: target_partition_range.start as u64,
            target_partition_end: target_partition_range.end as u64,
            producer_head: Some(producer_head),
        };

        let mut senders = Vec::with_capacity(n_partitions);
        let mut local_streams = Vec::with_capacity(n_partitions);
        for _ in 0..n_partitions {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<RecordBatch>>();
            senders.push(tx);
            local_streams.push(Mutex::new(Some(UnboundedReceiverStream::new(rx).boxed())));
        }

        let driver = SpawnedTask::spawn(async move {
            match execute_local_task(&task_data_entries, request).await {
                Ok((streams, _)) => {
                    let pumps = senders
                        .into_iter()
                        .zip(streams)
                        .map(|(tx, mut stream)| async move {
                            while let Some(item) = stream.next().await {
                                if tx.send(item).is_err() {
                                    break; // consumer dropped this partition
                                }
                            }
                        })
                        .collect::<Vec<_>>();
                    futures::future::join_all(pumps).await;
                }
                Err(err) => {
                    let err = Arc::new(err);
                    for tx in senders {
                        let _ = tx.send(Err(DataFusionError::Shared(Arc::clone(&err))));
                    }
                }
            }
        });

        Ok(Self {
            partition_start,
            local_streams,
            _driver: driver,
        })
    }
}

impl WorkerConnection for InMemoryWorkerConnection {
    fn execute(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let Some(relative_i) = partition.checked_sub(self.partition_start) else {
            return internal_err!(
                "InMemoryWorkerConnection received an invalid partition {partition}, the starting partition is {}",
                self.partition_start
            );
        };
        let Some(slot) = self.local_streams.get(relative_i) else {
            return internal_err!(
                "InMemoryWorkerConnection has no stream for partition {partition}. Was it already consumed?"
            );
        };
        slot.lock().unwrap().take().ok_or_else(|| {
            internal_datafusion_err!(
                "InMemoryWorkerConnection stream for partition {partition} was already consumed"
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::test_utils::session_context::register_temp_parquet_table;
    use crate::{DistributedExt, SessionStateBuilderExt, display_plan_ascii};
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch as ArrowRecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;

    const QUERY: &str = "SELECT tag, count(*) AS c, sum(val) AS s FROM t GROUP BY tag ORDER BY tag";

    fn sample_batch() -> ArrowRecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let tags: Vec<String> = (0..100).map(|i| format!("tag{}", i % 7)).collect();
        let vals: Vec<i32> = (0..100).collect();
        ArrowRecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(tags)),
                Arc::new(Int32Array::from(vals)),
            ],
        )
        .unwrap()
    }

    fn distributed_ctx(transport: Option<InMemoryWorkerTransport>) -> SessionContext {
        let mut builder = SessionStateBuilder::new()
            .with_default_features()
            .with_distributed_planner()
            .with_distributed_task_estimator(2)
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(3));
        if let Some(transport) = transport {
            builder = builder.with_distributed_worker_transport(transport);
        }
        let mut state = builder.build();
        state.config_mut().options_mut().execution.target_partitions = 3;
        SessionContext::from(state)
    }

    async fn run(ctx: &SessionContext) -> Result<(String, String)> {
        let plan = ctx.sql(QUERY).await?.create_physical_plan().await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        let batches: Vec<_> = execute_stream(plan, ctx.task_ctx())?.try_collect().await?;
        Ok((display, pretty_format_batches(&batches)?.to_string()))
    }

    #[tokio::test]
    async fn distributed_query_matches_single_node() -> Result<()> {
        let ctx = distributed_ctx(Some(InMemoryWorkerTransport::default()));
        let path =
            register_temp_parquet_table("t", sample_batch().schema(), vec![sample_batch()], &ctx)
                .await?;

        let (display, distributed) = run(&ctx).await?;
        assert!(
            display.contains("NetworkShuffleExec"),
            "the query did not distribute:\n{display}"
        );

        let single = SessionContext::default();
        single
            .register_parquet("t", path.to_string_lossy().as_ref(), Default::default())
            .await?;
        let (_, expected) = run(&single).await?;

        assert_eq!(distributed, expected);
        Ok(())
    }

    // With `flight` compiled out, no transport is registered here: the query must run through the
    // process-wide default, which is the in-memory transport.
    #[cfg(not(feature = "flight"))]
    #[tokio::test]
    async fn no_flight_default_runs_distributed_queries() -> Result<()> {
        let ctx = distributed_ctx(None);
        register_temp_parquet_table("t", sample_batch().schema(), vec![sample_batch()], &ctx)
            .await?;

        let (display, results) = run(&ctx).await?;
        assert!(
            display.contains("NetworkShuffleExec"),
            "the query did not distribute:\n{display}"
        );
        assert!(results.contains("tag0"));
        Ok(())
    }
}
