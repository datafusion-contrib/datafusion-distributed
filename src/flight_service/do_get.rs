use crate::common::{map_last_stream, on_drop_stream, task_ctx_with_extension};
use crate::flight_service::worker::Worker;
use crate::metrics::TaskMetricsCollector;
use crate::metrics::proto::df_metrics_set_to_proto;
use crate::protobuf::{
    AppMetadata, FlightAppMetadata, MetricsCollection, StageKey, TaskMetrics,
    datafusion_error_to_tonic_status,
};
use crate::{DistributedConfig, DistributedTaskContext};
use arrow_flight::Ticket;
use arrow_flight::encode::{DictionaryHandling, FlightDataEncoder, FlightDataEncoderBuilder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_select::dictionary::garbage_collect_any_dictionary;
use datafusion::arrow::array::{Array, AsArray, RecordBatch};

use crate::flight_service::spawn_select_all::spawn_select_all;
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
use std::time::Duration;
use tonic::{Request, Response, Status};

/// How many record batches to buffer from the plan execution.
const RECORD_BATCH_BUFFER_SIZE: usize = 2;
const WAIT_PLAN_TIMEOUT_SECS: u64 = 10;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoGet {
    /// The index to the task within the stage that we want to execute
    #[prost(uint64, tag = "2")]
    pub target_task_index: u64,
    #[prost(uint64, tag = "3")]
    pub target_task_count: u64,
    /// lower bound for the list of partitions to execute (inclusive).
    #[prost(uint64, tag = "4")]
    pub target_partition_start: u64,
    /// upper bound for the list of partitions to execute (exclusive).
    #[prost(uint64, tag = "5")]
    pub target_partition_end: u64,
    /// The stage key that identifies the stage.  This is useful to keep
    /// outside of the stage proto as it is used to store the stage
    /// and we may not need to deserialize the entire stage proto
    /// if we already have stored it
    #[prost(message, optional, tag = "6")]
    pub stage_key: Option<StageKey>,
}

impl Worker {
    pub(super) async fn get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<<Worker as FlightService>::DoGetStream>, Status> {
        let body = request.into_inner();
        let doget = DoGet::decode(body.ticket).map_err(|err| {
            Status::invalid_argument(format!("Cannot decode DoGet message: {err}"))
        })?;

        let key = doget.stage_key.ok_or_else(missing("stage_key"))?;
        let entry = self
            .task_data_entries
            .get_with(key.clone(), async { Default::default() })
            .await;

        // Other request is responsible for writing the plan that belongs to this StageKey, so
        // we'll resolve immediately if it was already there, or wait until it's ready.
        let task_data = entry
            .read(Duration::from_secs(WAIT_PLAN_TIMEOUT_SECS))
            .await
            .map_err(|v| Status::invalid_argument(v.to_string()))??;

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
        let task_ctx = Arc::new(task_ctx_with_extension(
            &task_ctx,
            DistributedTaskContext {
                task_index: doget.target_task_index as usize,
                task_count: doget.target_task_count as usize,
            },
        ));

        let partition_count = plan.properties().partitioning.partition_count();
        let plan_name = plan.name();

        // Execute all the requested partitions at once, and collect all the streams so that they
        // can be merged into a single one at the end of this function.
        let n_streams = doget.target_partition_end - doget.target_partition_start;
        let mut streams = Vec::with_capacity(n_streams as usize);
        for partition in doget.target_partition_start..doget.target_partition_end {
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
                let mut flight_data = FlightAppMetadata::new(partition);

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
                            flight_data.set_content(collect_and_create_metrics_flight_data(
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
    stage_key: StageKey,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<AppMetadata, FlightError> {
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
        .insert(stage_key, proto_task_metrics);

    // Serialize the metrics for all tasks.
    let mut task_metrics_set = vec![];
    for (stage_key, metrics) in result.input_task_metrics.into_iter() {
        task_metrics_set.push(TaskMetrics {
            stage_key: Some(stage_key),
            metrics,
        });
    }

    Ok(AppMetadata::MetricsCollection(MetricsCollection {
        tasks: task_metrics_set,
    }))
}

/// Garbage collects values sub-arrays.
///
/// We apply this before sending RecordBatches over the network to avoid sending
/// values that are not referenced by any dictionary keys or buffers that are not used.
///
/// Unused values can arise from operations such as filtering, where
/// some keys may no longer be referenced in the filtered result.
fn garbage_collect_arrays(batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
    let (schema, arrays, _row_count) = batch.into_parts();

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

    Ok(RecordBatch::try_new(schema, arrays)?)
}
