use crate::common::{TreeNodeExt, now_ns};
use crate::metrics::proto::df_metrics_set_to_proto;
use crate::protobuf::datafusion_error_to_tonic_status;
use crate::worker::generated::worker::{FlightAppMetadata, TaskMetrics};
use crate::worker::worker_service::{TaskDataEntries, Worker};
use crate::{DistributedConfig, DistributedTaskContext};
use arrow_flight::encode::{DictionaryHandling, FlightDataEncoder, FlightDataEncoderBuilder};
use arrow_flight::error::FlightError;
use arrow_select::dictionary::garbage_collect_any_dictionary;
use datafusion::arrow::array::{Array, AsArray, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, exec_err, internal_err};

use crate::worker::generated::worker::ExecuteTaskRequest;
use crate::worker::generated::worker::worker_service_server::WorkerService;
use crate::worker::spawn_select_all::spawn_select_all;
use crate::worker::task_data::TaskDataMetrics;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::common::exec_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use futures::{Stream, TryStreamExt};
use prost::Message;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::oneshot::Sender;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

/// How many record batches to buffer from the plan execution.
const RECORD_BATCH_BUFFER_SIZE: usize = 2;
const WAIT_PLAN_TIMEOUT_SECS: u64 = 10;

/// Builds several per-partition streams by retrieving the appropriate entry from [TaskDataEntries]
/// based on the task key extracted from [ExecuteTaskRequest].
///
/// This method is async mainly for the key retrieval operation from [TaskDataEntries], but it does
/// not start polling any stream, it just instantiates them.
pub(crate) async fn execute_local_task(
    task_data_entries: &Arc<TaskDataEntries>,
    body: ExecuteTaskRequest,
) -> Result<(Vec<SendableRecordBatchStream>, Arc<TaskContext>)> {
    let Some(key) = body.task_key.as_ref().cloned() else {
        return internal_err!("Missing task_key in LocalWorkerConnection");
    };
    let Some(producer_head) = body.producer_head.as_ref().cloned() else {
        return internal_err!("Missing producer_head");
    };
    let entry = task_data_entries
        .get_with(key.clone(), async { Default::default() })
        .await;

    // Other request is responsible for writing the plan that belongs to this TaskKey, so
    // we'll resolve immediately if it was already there, or wait until it's ready.
    let task_data = entry
        .read(Duration::from_secs(WAIT_PLAN_TIMEOUT_SECS))
        .await
        .map_err(|e| exec_datafusion_err!("Worker::execute_task timed-out while waiting for the plan to be set by the coordinator. ({e})"))?
        .map_err(DataFusionError::Shared)?;
    task_data.task_data_metrics.mark_execution_started_once();

    let plan = task_data.plan(producer_head)?;
    let task_ctx = task_data.task_ctx;
    let d_cfg = DistributedConfig::from_config_options(task_ctx.session_config().options())?;
    let d_ctx = *DistributedTaskContext::from_ctx(&task_ctx).as_ref();

    let send_metrics = d_cfg.collect_metrics;
    let partition_count = plan.properties().partitioning.partition_count();
    let plan_name = plan.name();

    // Execute all the requested partitions at once, and collect all the streams so that they
    // can be merged into a single one at the end of this function.
    let n_streams = body.target_partition_end - body.target_partition_start;
    let mut streams = Vec::with_capacity(n_streams as usize);
    for partition in body.target_partition_start..body.target_partition_end {
        if partition >= partition_count as u64 {
            return exec_err!(
                "partition {partition} not available. The head plan {plan_name} of the stage just has {partition_count} partitions"
            );
        }

        let stream = plan.execute(partition as usize, Arc::clone(&task_ctx))?;

        let plan = Arc::clone(&plan);

        let task_data_entries = Arc::clone(task_data_entries);
        let num_partitions_remaining = Arc::clone(&task_data.num_partitions_remaining);
        let metrics_tx = Arc::clone(&task_data.metrics_tx);
        let task_data_metrics = Arc::clone(&task_data.task_data_metrics);
        let key = key.clone();
        let stream = WorkerTaskStream::new(stream, move || {
            // Send metrics via the coordinator channel once every partition
            // stream finished or was dropped, so they are not lost.
            if num_partitions_remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                task_data_metrics.mark_execution_finished();
                if send_metrics {
                    send_metrics_via_channel(&metrics_tx, &plan, d_ctx, &task_data_metrics);
                }
                // Fire-and-forget background tokio task to handle async
                // invalidate() within synchronous stream drop.
                #[allow(clippy::disallowed_methods)]
                tokio::spawn(async move {
                    task_data_entries.invalidate(&key).await;
                });
            }
        });
        streams.push(Box::pin(stream) as _);
    }
    Ok((streams, task_ctx))
}

struct WorkerTaskStream<F>
where
    F: FnOnce() + Unpin,
{
    schema: SchemaRef,
    inner: Option<SendableRecordBatchStream>,
    on_drop_after_inner: Option<F>,
}

impl<F> WorkerTaskStream<F>
where
    F: FnOnce() + Unpin,
{
    fn new(inner: SendableRecordBatchStream, on_drop_after_inner: F) -> Self {
        let schema = inner.schema();
        Self {
            schema,
            inner: Some(inner),
            on_drop_after_inner: Some(on_drop_after_inner),
        }
    }
}

impl<F> Stream for WorkerTaskStream<F>
where
    F: FnOnce() + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut() {
            Some(inner) => inner.as_mut().poll_next(cx),
            None => Poll::Ready(None),
        }
    }
}

impl<F> RecordBatchStream for WorkerTaskStream<F>
where
    F: FnOnce() + Unpin,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl<F> Drop for WorkerTaskStream<F>
where
    F: FnOnce() + Unpin,
{
    fn drop(&mut self) {
        // Some execution streams finalize metrics in Drop. Collect worker
        // metrics only after the inner stream had a chance to flush them.
        drop(self.inner.take());
        if let Some(on_drop_after_inner) = self.on_drop_after_inner.take() {
            on_drop_after_inner();
        }
    }
}

/// Builds several per-partition streams by retrieving the appropriate entry from [TaskDataEntries]
/// based on the task key extracted from the gRPC request.
///
/// This method eagerly starts streaming data from the task, and communicates via channels the
/// produced [RecordBatch]s already encoded as Arrow Flight data.
pub(crate) async fn execute_remote_task(
    task_data_entries: &Arc<TaskDataEntries>,
    request: Request<ExecuteTaskRequest>,
) -> Result<Response<<Worker as WorkerService>::ExecuteTaskStream>, Status> {
    let body = request.into_inner();
    let partition_range = body.target_partition_start..body.target_partition_end;

    let (arrow_streams, task_ctx) = execute_local_task(task_data_entries, body)
        .await
        .map_err(datafusion_error_to_tonic_status)?;

    let d_cfg = DistributedConfig::from_config_options(task_ctx.session_config().options())
        .map_err(datafusion_error_to_tonic_status)?;

    let compression = match d_cfg.compression.as_str() {
        "lz4" => Some(CompressionType::LZ4_FRAME),
        "zstd" => Some(CompressionType::ZSTD),
        "none" => None,
        v => Err(Status::invalid_argument(format!(
            "Unknown compression type {v}"
        )))?,
    };
    let mut flight_streams = Vec::with_capacity(arrow_streams.len());
    for (partition, arrow_stream) in partition_range.zip(arrow_streams) {
        let flight_stream = build_flight_data_stream(arrow_stream, compression)?.map(move |msg| {
            // For each FlightData produced by this stream, mark it with the appropriate
            // partition. This stream will be merged with several others from other partitions,
            // so marking it with the original partition allows it to be deconstructed into
            // the original per-partition streams in later steps.
            let flight_data = FlightAppMetadata {
                partition,
                created_timestamp_unix_nanos: now_ns(),
            };
            msg.map(|v| v.with_app_metadata(flight_data.encode_to_vec()))
        });

        flight_streams.push(flight_stream);
    }

    // Merge all the per-partition streams into one. Each message in the stream is marked with
    // the original partition, so they can be reconstructed at the other side of the boundary.
    let memory_pool = Arc::clone(&task_ctx.runtime_env().memory_pool);
    let stream = spawn_select_all(flight_streams, memory_pool, RECORD_BATCH_BUFFER_SIZE);

    Ok(Response::new(Box::pin(stream.map_err(|err| match err {
        FlightError::Tonic(status) => *status,
        _ => Status::internal(format!("Error during flight stream: {err}")),
    }))))
}

fn build_flight_data_stream(
    stream: SendableRecordBatchStream,
    compression_type: Option<CompressionType>,
) -> Result<FlightDataEncoder, Status> {
    let stream = FlightDataEncoderBuilder::new()
        .with_options(
            IpcWriteOptions::default()
                .try_with_compression(compression_type)
                .map_err(|err| Status::internal(err.to_string()))?,
        )
        .with_schema(stream.schema())
        // This tells the encoder to send dictionaries across the wire as-is.
        // The alternative (`DictionaryHandling::Hydrate`) would expand the dictionaries
        // into their value types, which can potentially blow up the size of the data transfer.
        // The main reason to use `DictionaryHandling::Hydrate` is for compatibility with clients
        // that do not support dictionaries, but since we are using the same server/client on both
        // sides, we can safely use `DictionaryHandling::Resend`.
        // Note that we do garbage collection of unused dictionary values above, so we are not sending
        // unused dictionary values over the wire.
        .with_dictionary_handling(DictionaryHandling::Resend)
        // Set max flight data size to unlimited.
        // This requires servers and clients to also be configured to handle unlimited sizes.
        // Using unlimited sizes avoids splitting RecordBatches into multiple FlightData messages,
        // which could add significant overhead for large RecordBatches.
        // The only reason to split them really is if the client/server are configured with a message size limit,
        // which mainly makes sense in a public network scenario where you want to avoid DoS attacks.
        // Since all of our Arrow Flight communication happens within trusted data plane networks,
        // we can safely use unlimited sizes here.
        .with_max_flight_data_size(usize::MAX)
        .build(
            stream
                // Apply garbage collection of dictionary and view arrays before sending over the network
                .and_then(|rb| std::future::ready(garbage_collect_arrays(rb)))
                .map_err(|err| FlightError::Tonic(Box::new(datafusion_error_to_tonic_status(err)))),
        );
    Ok(stream)
}

/// Collects metrics from the plan in pre-order traversal order and sends them via the
/// coordinator channel oneshot.
fn send_metrics_via_channel(
    metrics_tx: &Arc<Mutex<Option<Sender<TaskMetrics>>>>,
    plan: &Arc<dyn ExecutionPlan>,
    dt_ctx: DistributedTaskContext,
    task_data_metrics: &Arc<TaskDataMetrics>,
) {
    let mut pre_order_plan_metrics = vec![];
    let _ = plan.apply_with_dt_ctx(dt_ctx, |node, _| {
        pre_order_plan_metrics.push(
            node.metrics()
                .and_then(|m| df_metrics_set_to_proto(&m).ok())
                .unwrap_or_default(),
        );
        Ok(TreeNodeRecursion::Continue)
    });

    let tx = {
        let mut guard = match metrics_tx.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        guard.take()
    };
    let Some(tx) = tx else { return };
    // Ignore send errors — the coordinator channel may have been dropped (e.g. query cancelled).
    let _ = tx.send(TaskMetrics {
        pre_order_plan_metrics,
        task_metrics: Some(task_data_metrics.to_proto_metrics_set()),
    });
}

/// Garbage collects values sub-arrays.
///
/// We apply this before sending RecordBatches over the network to avoid sending
/// values that are not referenced by any dictionary keys or buffers that are not used.
///
/// Unused values can arise from operations such as filtering, where
/// some keys may no longer be referenced in the filtered result.
fn garbage_collect_arrays(batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
    let (schema, arrays, row_count) = batch.into_parts();

    let arrays = arrays
        .into_iter()
        .map(|array| {
            if let Some(array) = array.as_any_dictionary_opt() {
                garbage_collect_any_dictionary(array)
            } else if let Some(array) = array.as_string_view_opt() {
                Ok(Arc::new(array.gc()) as Arc<dyn Array>)
            } else if let Some(array) = array.as_binary_view_opt() {
                Ok(Arc::new(array.gc()) as Arc<dyn Array>)
            } else {
                Ok(array)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct DropFinalizedMetricStream {
        schema: SchemaRef,
        drop_count: Count,
    }

    impl Stream for DropFinalizedMetricStream {
        type Item = Result<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(None)
        }
    }

    impl RecordBatchStream for DropFinalizedMetricStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl Drop for DropFinalizedMetricStream {
        fn drop(&mut self) {
            self.drop_count.add(1);
        }
    }

    #[test]
    fn worker_task_stream_runs_callback_after_inner_stream_drop() {
        let metrics = ExecutionPlanMetricsSet::new();
        let drop_count = MetricBuilder::new(&metrics).counter("drop_count", 0);
        let inner = DropFinalizedMetricStream {
            schema: Arc::new(Schema::empty()),
            drop_count: drop_count.clone(),
        };
        let observed_drop_count = Arc::new(AtomicUsize::new(0));
        let observed_drop_count_clone = Arc::clone(&observed_drop_count);

        let stream = WorkerTaskStream::new(Box::pin(inner), move || {
            observed_drop_count_clone.store(drop_count.value(), Ordering::SeqCst);
        });
        drop(stream);

        assert_eq!(observed_drop_count.load(Ordering::SeqCst), 1);
    }
}
