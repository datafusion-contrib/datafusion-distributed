use super::errors::{
    datafusion_error_to_tonic_status, datafusion_error_to_tonic_status_with_retry,
    map_status_to_datafusion_error,
};
use super::generated::worker as pb;
use super::metrics_proto::df_metrics_set_to_proto;
use super::spawn_select_all::spawn_select_all;

use crate::common::{deserialize_uuid, now_ns};
use crate::protocol::grpc::{ObservabilityServiceImpl, ObservabilityServiceServer};
use crate::{
    CoordinatorToWorkerMsg, DistributedConfig, ExecuteTaskRequest, LoadInfo, MaybeEncoded,
    OpenTaskRequest, ProducerHead, SetPlanRequest, TaskCompletedDynamicFilters, TaskKey,
    TaskMetrics, WorkUnitBatch, WorkUnitFeedDeclaration, WorkUnitMsg, Worker,
    WorkerAdmissionRejection, WorkerResolver, WorkerToCoordinatorMsg,
};

use crate::worker::CoordinatorChannelResult;
use arrow_flight::FlightData;
use arrow_flight::encode::{DictionaryHandling, FlightDataEncoder, FlightDataEncoderBuilder};
use arrow_flight::error::FlightError;
use arrow_select::dictionary::garbage_collect_any_dictionary;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, AsArray, RecordBatch, RecordBatchOptions};
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use url::Url;

const RECORD_BATCH_BUFFER_SIZE: usize = 2;

impl Worker {
    /// Converts this [Worker] into a [`WorkerServiceServer`] with high default message size limits.
    ///
    /// This is a convenience method that wraps the endpoint in a [`WorkerServiceServer`] and
    /// configures it with `max_decoding_message_size(usize::MAX)` and
    /// `max_encoding_message_size(usize::MAX)` to avoid message size limitations for internal
    /// communication.
    ///
    /// You can further customize the returned server by chaining additional tonic methods.
    ///
    /// # Example
    ///
    /// ```
    /// # use datafusion_distributed::Worker;
    /// # use tonic::transport::Server;
    /// # use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    /// # async fn f() {
    ///
    /// let worker = Worker::default();
    /// let server = worker.into_worker_server();
    ///
    /// Server::builder()
    ///     .add_service(Worker::default().into_worker_server())
    ///     .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080))
    ///     .await;
    ///
    /// # }
    /// ```
    pub fn into_worker_server(self) -> pb::worker_service_server::WorkerServiceServer<Self> {
        pb::worker_service_server::WorkerServiceServer::new(self)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX)
    }

    /// Creates an [`ObservabilityServiceServer`] that exposes task progress and cluster
    /// worker discovery via the provided [`WorkerResolver`].
    ///
    /// The returned server is meant to be added to the same [`tonic::transport::Server`] as the
    /// Flight service — gRPC multiplexes both services on a single port.
    pub fn with_observability_service(
        &self,
        worker_resolver: Arc<dyn WorkerResolver + Send + Sync>,
    ) -> ObservabilityServiceServer<ObservabilityServiceImpl> {
        ObservabilityServiceServer::new(ObservabilityServiceImpl::new(
            self.task_data_entries.clone(),
            worker_resolver,
        ))
    }
}

/// Implementation of the `worker.proto` specification based on the generated Rust stubs.
///
/// The methods are delegated to plan `impl Worker` implementations so that they can be implemented
/// in different files.
#[async_trait]
impl pb::worker_service_server::WorkerService for Worker {
    type CoordinatorChannelStream = BoxStream<'static, Result<pb::WorkerToCoordinatorMsg, Status>>;

    async fn coordinator_channel(
        &self,
        request: Request<Streaming<pb::CoordinatorToWorkerMsg>>,
    ) -> Result<Response<Self::CoordinatorChannelStream>, Status> {
        let (metadata, _ext, mut body) = request.into_parts();

        let msg = body
            .message()
            .await?
            .ok_or_else(empty("Coordinator stream"))?
            .inner
            .ok_or_else(missing("CoordinatorToWorkerMsg.inner"))?;
        let pb::coordinator_to_worker_msg::Inner::OpenTaskRequest(open_task_request) = msg else {
            return Err(Status::invalid_argument(
                "First Coordinator to Worker message must be OpenTaskRequest",
            ));
        };

        let open_task_request = decode_open_task_request(open_task_request)?;
        let headers = metadata.into_headers();
        let reservation = self
            .admit_task(headers.clone(), open_task_request)
            .await
            .map_err(admission_rejection_to_tonic_status)?;

        let worker = self.clone();
        let reservation_timeout = self.task_reservation_timeout();
        let output_stream = futures::stream::once(async move {
            let msg = tokio::time::timeout(reservation_timeout, body.message())
                .await
                .map_err(|_| {
                    Status::deadline_exceeded("Task reservation expired before SetPlanRequest")
                })??
                .ok_or_else(empty("Coordinator stream after OpenTaskRequest"))?
                .inner
                .ok_or_else(missing("CoordinatorToWorkerMsg.inner"))?;
            let pb::coordinator_to_worker_msg::Inner::SetPlanRequest(set_plan_request) = msg else {
                return Err(Status::invalid_argument(
                    "Second Coordinator to Worker message must be SetPlanRequest",
                ));
            };

            let set_plan_request = decode_set_plan_request(set_plan_request)?;
            let permit = reservation
                .commit(&set_plan_request)
                .map_err(datafusion_error_to_tonic_status)?;

            let input_stream = body
                .map_err(map_status_to_datafusion_error)
                .map(move |msg| {
                    decode_coordinator_to_worker_msg(msg?).map_err(map_status_to_datafusion_error)
                })
                .boxed();

            let CoordinatorChannelResult { task_ctx, stream } = worker
                .coordinator_channel_with_permit(headers, set_plan_request, input_stream, permit)
                .await
                .map_err(datafusion_error_to_tonic_status)?;

            let stream = stream
                .map(move |msg| match msg {
                    Ok(msg) => encode_worker_to_coordinator_msg(msg, &task_ctx),
                    Err(err) => Err(datafusion_error_to_tonic_status(err)),
                })
                .boxed();

            Ok::<_, Status>(stream)
        })
        .try_flatten()
        .boxed();

        Ok(Response::new(output_stream))
    }

    type ExecuteTaskStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn execute_task(
        &self,
        request: Request<pb::ExecuteTaskRequest>,
    ) -> Result<Response<Self::ExecuteTaskStream>, Status> {
        let body = request.into_inner();
        let request = decode_execute_task_request(body).await?;
        let partition_range = request.target_partition_start..request.target_partition_end;

        let (arrow_streams, task_ctx) = self
            .execute_task(request)
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
            let flight_stream =
                build_flight_data_stream(arrow_stream, compression)?.map(move |msg| {
                    // For each FlightData produced by this stream, mark it with the appropriate
                    // partition. This stream will be merged with several others from other partitions,
                    // so marking it with the original partition allows it to be deconstructed into
                    // the original per-partition streams in later steps.
                    let flight_data = pb::FlightAppMetadata {
                        partition: partition as u64,
                        created_timestamp_unix_nanos: now_ns::<u64>(),
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

    async fn get_worker_info(
        &self,
        _request: Request<pb::GetWorkerInfoRequest>,
    ) -> Result<Response<pb::GetWorkerInfoResponse>, Status> {
        Ok(Response::new(pb::GetWorkerInfoResponse {
            version: self.version().to_string(),
        }))
    }
}

fn decode_coordinator_to_worker_msg(
    msg: pb::CoordinatorToWorkerMsg,
) -> Result<CoordinatorToWorkerMsg, Status> {
    Ok(
        match msg
            .inner
            .ok_or_else(missing("CoordinatorToWorkerMsg.inner"))?
        {
            pb::coordinator_to_worker_msg::Inner::SetPlanRequest(_) => {
                return Err(Status::invalid_argument(
                    "SetPlanRequest must immediately follow OpenTaskRequest",
                ));
            }
            pb::coordinator_to_worker_msg::Inner::WorkUnitBatch(batch) => {
                CoordinatorToWorkerMsg::WorkUnitBatch(decode_work_unit_batch(batch)?)
            }
            pb::coordinator_to_worker_msg::Inner::WorkUnitEos(_) => {
                CoordinatorToWorkerMsg::WorkUnitEos
            }
            pb::coordinator_to_worker_msg::Inner::OpenTaskRequest(_) => {
                return Err(Status::invalid_argument(
                    "OpenTaskRequest must be the first coordinator message",
                ));
            }
        },
    )
}

fn decode_open_task_request(request: pb::OpenTaskRequest) -> Result<OpenTaskRequest, Status> {
    Ok(OpenTaskRequest {
        task_key: decode_task_key(request.task_key.ok_or_else(missing("task_key"))?)?,
        attempt_id: deserialize_uuid(&request.attempt_id)
            .map_err(datafusion_error_to_tonic_status)?,
        task_count: request.task_count as usize,
    })
}

fn decode_set_plan_request(request: pb::SetPlanRequest) -> Result<SetPlanRequest, Status> {
    Ok(SetPlanRequest {
        task_key: decode_task_key(request.task_key.ok_or_else(missing("task_key"))?)?,
        attempt_id: deserialize_uuid(&request.attempt_id)
            .map_err(datafusion_error_to_tonic_status)?,
        task_count: request.task_count as usize,
        plan: MaybeEncoded::Encoded(request.plan_proto),
        work_unit_feed_declarations: request
            .work_unit_feed_declarations
            .into_iter()
            .map(decode_work_unit_feed_declaration)
            .collect::<Result<_, _>>()?,
        target_worker_url: parse_url(&request.target_worker_url, "target_worker_url")?,
        query_start_time_ns: request.query_start_time_ns as usize,
    })
}

fn admission_rejection_to_tonic_status(rejection: WorkerAdmissionRejection) -> Status {
    let (error, retry_target) = rejection.into_parts();
    match retry_target {
        Some(retry_target) => datafusion_error_to_tonic_status_with_retry(error, retry_target),
        None => datafusion_error_to_tonic_status(error),
    }
}

async fn decode_execute_task_request(
    request: pb::ExecuteTaskRequest,
) -> Result<ExecuteTaskRequest, Status> {
    Ok(ExecuteTaskRequest {
        task_key: decode_task_key(request.task_key.ok_or_else(missing("task_key"))?)?,
        target_partition_start: request.target_partition_start as usize,
        target_partition_end: request.target_partition_end as usize,
        producer_head: decode_producer_head(
            request.producer_head.ok_or_else(missing("producer_head"))?,
        ),
    })
}

pub(super) fn decode_producer_head(proto: pb::execute_task_request::ProducerHead) -> ProducerHead {
    match proto {
        pb::execute_task_request::ProducerHead::None(_) => ProducerHead::None,
        pb::execute_task_request::ProducerHead::Broadcast(v) => ProducerHead::BroadcastExec {
            output_partitions: v.output_partitions as usize,
        },
        pb::execute_task_request::ProducerHead::Repartition(v) => ProducerHead::RepartitionExec {
            partitioning: MaybeEncoded::Encoded(v.partitioning),
        },
    }
}

fn encode_worker_to_coordinator_msg(
    msg: WorkerToCoordinatorMsg,
    task_ctx: &Arc<TaskContext>,
) -> Result<pb::WorkerToCoordinatorMsg, Status> {
    Ok(pb::WorkerToCoordinatorMsg {
        inner: Some(match msg {
            WorkerToCoordinatorMsg::TaskMetrics(task_metrics) => {
                pb::worker_to_coordinator_msg::Inner::TaskMetrics(encode_task_metrics(
                    task_metrics,
                )?)
            }
            WorkerToCoordinatorMsg::LoadInfo(load_info) => {
                pb::worker_to_coordinator_msg::Inner::LoadInfo(encode_load_info(load_info))
            }
            WorkerToCoordinatorMsg::LoadInfoEos => {
                pb::worker_to_coordinator_msg::Inner::LoadInfoEos(true)
            }
            WorkerToCoordinatorMsg::TaskCompletedDynamicFilters(filters) => {
                pb::worker_to_coordinator_msg::Inner::TaskCompletedDynamicFilters(
                    encode_task_completed_dynamic_filters(filters, task_ctx)?,
                )
            }
        }),
    })
}

fn encode_task_completed_dynamic_filters(
    filters: TaskCompletedDynamicFilters,
    task_ctx: &Arc<TaskContext>,
) -> Result<pb::TaskCompletedDynamicFilters, Status> {
    Ok(pb::TaskCompletedDynamicFilters {
        filters: filters
            .filters
            .into_iter()
            .map(|filter| {
                Ok(pb::DynamicFilter {
                    expression_id: filter.expression_id,
                    expression_proto: filter
                        .expression
                        .encode(task_ctx)
                        .map_err(datafusion_error_to_tonic_status)?,
                })
            })
            .collect::<Result<_, Status>>()?,
    })
}

fn encode_task_metrics(task_metrics: TaskMetrics) -> Result<pb::TaskMetrics, Status> {
    Ok(pb::TaskMetrics {
        pre_order_plan_metrics: task_metrics
            .pre_order_plan_metrics
            .into_iter()
            .map(|metrics_set| {
                df_metrics_set_to_proto(&metrics_set).map_err(datafusion_error_to_tonic_status)
            })
            .collect::<Result<_, _>>()?,
        task_metrics: Some(
            df_metrics_set_to_proto(&task_metrics.task_metrics)
                .map_err(datafusion_error_to_tonic_status)?,
        ),
    })
}

fn encode_load_info(load_info: LoadInfo) -> pb::LoadInfo {
    pb::LoadInfo {
        partition: load_info.partition as u64,
        rows_ready: load_info.rows_ready as u64,
        per_column_bytes_ready: load_info
            .per_column_bytes_ready
            .into_iter()
            .map(|bytes| bytes as u64)
            .collect(),
        per_column_ndv_percentage: load_info.per_column_ndv_percentage,
        per_column_null_percentage: load_info.per_column_null_percentage,
        rows_pulled_from_leaf: load_info.rows_pulled_from_leaf as u64,
        reached_eos: load_info.reached_eos,
    }
}

fn decode_work_unit_batch(batch: pb::WorkUnitBatch) -> Result<WorkUnitBatch, Status> {
    Ok(WorkUnitBatch {
        batch: batch
            .batch
            .into_iter()
            .map(decode_work_unit)
            .collect::<Result<_, _>>()?,
    })
}

fn decode_work_unit(work_unit: pb::WorkUnit) -> Result<WorkUnitMsg, Status> {
    Ok(WorkUnitMsg {
        id: deserialize_uuid(&work_unit.id).map_err(datafusion_error_to_tonic_status)?,
        partition: work_unit.partition as usize,
        body: MaybeEncoded::Encoded(work_unit.body),
        created_timestamp_unix_nanos: work_unit.created_timestamp_unix_nanos as usize,
        sent_timestamp_unix_nanos: work_unit.sent_timestamp_unix_nanos as usize,
        received_timestamp_unix_nanos: work_unit.received_timestamp_unix_nanos as usize,
        processed_timestamp_unix_nanos: work_unit.processed_timestamp_unix_nanos as usize,
    })
}

fn decode_work_unit_feed_declaration(
    declaration: pb::set_plan_request::WorkUnitFeedDeclaration,
) -> Result<WorkUnitFeedDeclaration, Status> {
    Ok(WorkUnitFeedDeclaration {
        id: deserialize_uuid(&declaration.id).map_err(datafusion_error_to_tonic_status)?,
        partitions: declaration.partitions as usize,
    })
}

fn decode_task_key(task_key: pb::TaskKey) -> Result<TaskKey, Status> {
    Ok(TaskKey {
        query_id: deserialize_uuid(&task_key.query_id).map_err(datafusion_error_to_tonic_status)?,
        stage_id: task_key.stage_id as usize,
        task_number: task_key.task_number as usize,
    })
}

fn parse_url(value: &str, field: &'static str) -> Result<Url, Status> {
    Url::parse(value)
        .map_err(|err| Status::invalid_argument(format!("Invalid field '{field}': {err}")))
}

fn empty(stream_name: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Empty {stream_name}"))
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}

fn build_flight_data_stream(
    stream: SendableRecordBatchStream,
    compression_type: Option<CompressionType>,
) -> datafusion::common::Result<FlightDataEncoder, Status> {
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

/// Garbage collects values sub-arrays.
///
/// We apply this before sending RecordBatches over the network to avoid sending
/// values that are not referenced by any dictionary keys or buffers that are not used.
///
/// Unused values can arise from operations such as filtering, where
/// some keys may no longer be referenced in the filtered result.
fn garbage_collect_arrays(
    batch: RecordBatch,
) -> datafusion::common::Result<RecordBatch, DataFusionError> {
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
        .collect::<datafusion::common::Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{RetryOutcome, serialize_uuid};
    use crate::{
        DefaultSessionBuilder, RetryTarget, WorkerAdmissionController, WorkerAdmissionPermit,
        WorkerAdmissionRequest,
    };
    use async_trait::async_trait;
    use datafusion::common::exec_datafusion_err;
    use hyper_util::rt::TokioIo;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::transport::{Endpoint, Server};
    use tower::service_fn;
    use uuid::Uuid;

    #[tokio::test]
    async fn open_task_is_acknowledged_before_set_plan_side_effects() {
        let sessions_built = Arc::new(AtomicUsize::new(0));
        let sessions_built_clone = Arc::clone(&sessions_built);
        let worker = Worker::from_session_builder(move |ctx: crate::WorkerQueryContext| {
            let sessions_built = Arc::clone(&sessions_built_clone);
            async move {
                sessions_built.fetch_add(1, Ordering::SeqCst);
                Ok(ctx.builder.build())
            }
        });
        let worker_for_assert = worker.clone();
        let (mut client, server) = test_client_and_server(worker).await;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(open_task_message()).unwrap();

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.coordinator_channel(UnboundedReceiverStream::new(rx)),
        )
        .await
        .expect("OpenTaskRequest was not acknowledged before SetPlanRequest")
        .unwrap();

        assert_eq!(sessions_built.load(Ordering::SeqCst), 0);
        assert_eq!(worker_for_assert.tasks_running().await, 0);

        drop(response);
        drop(tx);
        server.abort();
    }

    #[tokio::test]
    async fn admission_rejection_carries_worker_retry_target() {
        let worker = Worker::from_session_builder(DefaultSessionBuilder)
            .with_admission_controller(RejectDifferentWorker);
        let (mut client, server) = test_client_and_server(worker).await;

        let error = client
            .coordinator_channel(futures::stream::iter([open_task_message()]))
            .await
            .unwrap_err();
        let error = super::super::errors::tonic_status_to_datafusion_error(error).unwrap();

        assert_eq!(
            RetryOutcome::try_from_err(&error),
            Some(RetryOutcome::OtherUrl)
        );
        assert!(error.to_string().contains("worker is full"));
        server.abort();
    }

    struct RejectDifferentWorker;

    #[async_trait]
    impl WorkerAdmissionController for RejectDifferentWorker {
        async fn admit(
            &self,
            _request: &WorkerAdmissionRequest,
        ) -> Result<WorkerAdmissionPermit, WorkerAdmissionRejection> {
            Err(WorkerAdmissionRejection::retry(
                exec_datafusion_err!("worker is full"),
                RetryTarget::DifferentWorker,
            ))
        }
    }

    fn open_task_message() -> pb::CoordinatorToWorkerMsg {
        pb::CoordinatorToWorkerMsg {
            inner: Some(pb::coordinator_to_worker_msg::Inner::OpenTaskRequest(
                pb::OpenTaskRequest {
                    task_key: Some(pb::TaskKey {
                        query_id: serialize_uuid(&Uuid::new_v4()),
                        stage_id: 1,
                        task_number: 2,
                    }),
                    attempt_id: serialize_uuid(&Uuid::new_v4()),
                    task_count: 3,
                },
            )),
        }
    }

    async fn test_client_and_server(
        worker: Worker,
    ) -> (
        pb::worker_service_client::WorkerServiceClient<tonic::transport::Channel>,
        tokio::task::JoinHandle<()>,
    ) {
        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);
        let mut client_io = Some(client_io);
        let channel = Endpoint::from_static("http://worker.test").connect_with_connector_lazy(
            service_fn(move |_| {
                let client_io = client_io.take().expect("connector called twice");
                async move { Ok::<_, std::io::Error>(TokioIo::new(client_io)) }
            }),
        );
        #[allow(clippy::disallowed_methods)]
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
                .await
                .unwrap();
        });
        (
            pb::worker_service_client::WorkerServiceClient::new(channel),
            server,
        )
    }
}
