use crate::DistributedConfig;
use crate::common::{map_last_stream, on_drop_stream};
use crate::metrics::TaskMetricsCollector;
use crate::metrics::proto::df_metrics_set_to_proto;
use crate::protobuf::datafusion_error_to_tonic_status;
use crate::worker::generated::worker::{
    FlightAppMetadata, MetricsCollection, TaskMetrics, flight_app_metadata,
};
use crate::worker::worker_service::Worker;
use arrow_flight::encode::{DictionaryHandling, FlightDataEncoder, FlightDataEncoderBuilder};
use arrow_flight::error::FlightError;
use arrow_select::dictionary::garbage_collect_any_dictionary;
use datafusion::arrow::array::{Array, AsArray, RecordBatch, RecordBatchOptions};

use crate::worker::generated::worker::worker_service_server::WorkerService;
use crate::worker::generated::worker::{ExecuteTaskRequest, TaskKey};
use crate::worker::spawn_select_all::spawn_select_all;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::common::exec_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use prost::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};

/// How many record batches to buffer from the plan execution.
const RECORD_BATCH_BUFFER_SIZE: usize = 2;
const WAIT_PLAN_TIMEOUT_SECS: u64 = 10;

impl Worker {
    pub(crate) async fn impl_execute_task(
        &self,
        request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<<Worker as WorkerService>::ExecuteTaskStream>, Status> {
        let body = request.into_inner();

        let key = body.task_key.ok_or_else(missing("task_key"))?;
        let entry = self
            .task_data_entries
            .get_with(key.clone(), async { Default::default() })
            .await;

        // Other request is responsible for writing the plan that belongs to this TaskKey, so
        // we'll resolve immediately if it was already there, or wait until it's ready.
        let task_data = entry
            .read(Duration::from_secs(WAIT_PLAN_TIMEOUT_SECS))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(datafusion_error_to_tonic_status)?;

        let plan = task_data.plan;
        let task_ctx = task_data.task_ctx;
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
        let send_metrics = d_cfg.collect_metrics;
        let partition_count = plan.properties().partitioning.partition_count();
        let plan_name = plan.name();

        // Execute all the requested partitions at once, and collect all the streams so that they
        // can be merged into a single one at the end of this function.
        let n_streams = body.target_partition_end - body.target_partition_start;
        let mut streams = Vec::with_capacity(n_streams as usize);
        for partition in body.target_partition_start..body.target_partition_end {
            if partition >= partition_count as u64 {
                return Err(datafusion_error_to_tonic_status(exec_datafusion_err!(
                    "partition {partition} not available. The head plan {plan_name} of the stage just has {partition_count} partitions"
                )));
            }

            let stream = plan
                .execute(partition as usize, Arc::clone(&task_ctx))
                .map_err(|err| Status::internal(format!("Error executing stage plan: {err:#?}")))?;

            let stream = build_flight_data_stream(stream, compression)?;

            let task_data_entries = Arc::clone(&self.task_data_entries);
            let num_partitions_remaining = Arc::clone(&task_data.num_partitions_remaining);

            let key = key.clone();
            let key_clone = key.clone();
            let plan = Arc::clone(&plan);
            let fully_finished = Arc::new(AtomicBool::new(false));
            let fully_finished_cloned = Arc::clone(&fully_finished);
            let stream = map_last_stream(stream, move |msg, last_msg_in_stream| {
                // For each FlightData produced by this stream, mark it with the appropriate
                // partition. This stream will be merged with several others from other partitions,
                // so marking it with the original partition allows it to be deconstructed into
                // the original per-partition streams in later steps.
                let mut flight_data = FlightAppMetadata {
                    partition,
                    created_timestamp_unix_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|duration| duration.as_nanos() as u64)
                        .unwrap_or(0),
                    content: None,
                };

                if last_msg_in_stream {
                    // If it's the last message from the last partition, clean up the entry from
                    // the cache and send the collected metrics.
                    if num_partitions_remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                        let entries = Arc::clone(&task_data_entries);
                        let k = key.clone();
                        tokio::spawn(async move {
                            entries.invalidate(&k).await;
                        });
                        if send_metrics {
                            // Last message of the last partition. This is the moment to send
                            // the metrics back.
                            flight_data.content = Some(collect_and_create_metrics_flight_data(
                                key.clone(),
                                plan.clone(),
                            )?);
                        }
                    }
                    fully_finished.store(true, Ordering::SeqCst);
                }

                msg.map(|v| v.with_app_metadata(flight_data.encode_to_vec()))
            });

            let num_partitions_remaining = Arc::clone(&task_data.num_partitions_remaining);
            let task_data_entries = Arc::clone(&self.task_data_entries);
            // When the stream is dropped before fully consumed (e.g. LIMIT on the client side),
            // metrics piggybacked on the last FlightData message are lost.
            // See https://github.com/datafusion-contrib/datafusion-distributed/issues/187
            let stream = on_drop_stream(stream, move || {
                if !fully_finished_cloned.load(Ordering::SeqCst) {
                    // If the stream was not fully consumed, but it was dropped (abandoned), we
                    // still need to remove the entry from `task_data_entries`, otherwise we
                    // might leak memory until the cache automatically evicts it after the TTL expires.
                    if num_partitions_remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                        let entries = Arc::clone(&task_data_entries);
                        let k = key_clone.clone();
                        // Fire-and-forget background tokio task to handle async
                        // invalidate() within synchronous on_drop_stream.
                        tokio::spawn(async move {
                            entries.invalidate(&k).await;
                        });
                    }
                }
            });
            streams.push(stream)
        }

        // Merge all the per-partition streams into one. Each message in the stream is marked with
        // the original partition, so they can be reconstructed at the other side of the boundary.
        let memory_pool = Arc::clone(&task_ctx.runtime_env().memory_pool);
        let stream = spawn_select_all(streams, memory_pool, RECORD_BATCH_BUFFER_SIZE);

        Ok(Response::new(Box::pin(stream.map_err(|err| match err {
            FlightError::Tonic(status) => *status,
            _ => Status::internal(format!("Error during flight stream: {err}")),
        }))))
    }
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

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}

/// Collects metrics from the provided stage and includes it in the flight data
fn collect_and_create_metrics_flight_data(
    task_key: TaskKey,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<flight_app_metadata::Content, FlightError> {
    // Get the metrics for the task executed on this worker + child tasks.
    let mut result = TaskMetricsCollector::new()
        .collect(plan)
        .map_err(|err| FlightError::ProtocolError(err.to_string()))?;

    // Add the metrics for this task into the collection of task metrics.
    // Skip any metrics that can't be converted to proto (unsupported types)
    let proto_task_metrics = result
        .task_metrics
        .iter()
        .map(|metrics| {
            df_metrics_set_to_proto(metrics)
                .map_err(|err| FlightError::ProtocolError(err.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    result
        .input_task_metrics
        .insert(task_key, proto_task_metrics);

    // Serialize the metrics for all tasks.
    let mut task_metrics_set = vec![];
    for (task_key, metrics) in result.input_task_metrics.into_iter() {
        task_metrics_set.push(TaskMetrics {
            task_key: Some(task_key),
            metrics,
        });
    }

    Ok(flight_app_metadata::Content::MetricsCollection(
        MetricsCollection {
            tasks: task_metrics_set,
        },
    ))
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
