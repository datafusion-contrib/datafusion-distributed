use crate::common::with_callback;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::execution_plans::{DistributedTaskContext, StageExec};
use crate::flight_service::service::ArrowFlightEndpoint;
use crate::flight_service::session_builder::DistributedSessionBuilderContext;
use crate::metrics::TaskMetricsCollector;
use crate::metrics::proto::df_metrics_set_to_proto;
use crate::protobuf::{
    AppMetadata, DistributedCodec, FlightAppMetadata, MetricsCollection, StageKey, TaskMetrics,
    datafusion_error_to_tonic_status, stage_from_proto,
};
use arrow_flight::FlightData;
use arrow_flight::Ticket;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use bytes::Bytes;

use datafusion::common::exec_datafusion_err;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use tonic::{Request, Response, Status};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoGet {
    /// The [StageExec] we are going to execute encoded as protobuf bytes.
    #[prost(bytes, tag = "1")]
    pub stage_proto: Bytes,
    /// The index to the task within the stage that we want to execute
    #[prost(uint64, tag = "2")]
    pub target_task_index: u64,
    /// the partition number we want to execute
    #[prost(uint64, tag = "3")]
    pub target_partition: u64,
    /// The stage key that identifies the stage.  This is useful to keep
    /// outside of the stage proto as it is used to store the stage
    /// and we may not need to deserialize the entire stage proto
    /// if we already have stored it
    #[prost(message, optional, tag = "4")]
    pub stage_key: Option<StageKey>,
}

#[derive(Clone, Debug)]
/// TaskData stores state for a single task being executed by this Endpoint. It may be shared
/// by concurrent requests for the same task which execute separate partitions.
pub struct TaskData {
    pub(super) stage: Arc<StageExec>,
    /// `num_partitions_remaining` is initialized to the total number of partitions in the task (not
    /// only tasks in the partition group). This is decremented for each request to the endpoint
    /// for this task. Once this count is zero, the task is likely complete. The task may not be
    /// complete because it's possible that the same partition was retried and this count was
    /// decremented more than once for the same partition.
    num_partitions_remaining: Arc<AtomicUsize>,
}

impl ArrowFlightEndpoint {
    pub(super) async fn get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<<ArrowFlightEndpoint as FlightService>::DoGetStream>, Status> {
        let (metadata, _ext, body) = request.into_parts();
        let doget = DoGet::decode(body.ticket).map_err(|err| {
            Status::invalid_argument(format!("Cannot decode DoGet message: {err}"))
        })?;

        let mut session_state = self
            .session_builder
            .build_session_state(DistributedSessionBuilderContext {
                runtime_env: Arc::clone(&self.runtime),
                headers: metadata.clone().into_headers(),
            })
            .await
            .map_err(|err| datafusion_error_to_tonic_status(&err))?;

        let codec = DistributedCodec::new_combined_with_user(session_state.config());
        let ctx = SessionContext::new_with_state(session_state.clone());

        // There's only 1 `StageExec` responsible for all requests that share the same `stage_key`,
        // so here we either retrieve the existing one or create a new one if it does not exist.
        let key = doget.stage_key.ok_or_else(missing("stage_key"))?;
        let once = self
            .task_data_entries
            .get_or_init(key.clone(), Default::default);

        let stage_data = once
            .get_or_try_init(|| async {
                let stage_proto = doget.stage_proto;
                let stage =
                    stage_from_proto(stage_proto, &ctx, &self.runtime, &codec).map_err(|err| {
                        Status::invalid_argument(format!("Cannot decode stage proto: {err}"))
                    })?;

                // Initialize partition count to the number of partitions in the stage
                let total_partitions = stage.plan.properties().partitioning.partition_count();
                Ok::<_, Status>(TaskData {
                    stage: Arc::new(stage),
                    num_partitions_remaining: Arc::new(AtomicUsize::new(total_partitions)),
                })
            })
            .await?;
        let stage = Arc::clone(&stage_data.stage);

        // Find out which partition group we are executing
        let cfg = session_state.config_mut();
        cfg.set_extension(Arc::clone(&stage));
        cfg.set_extension(Arc::new(ContextGrpcMetadata(metadata.into_headers())));
        cfg.set_extension(Arc::new(DistributedTaskContext::new(
            doget.target_task_index as usize,
        )));

        let partition_count = stage.plan.properties().partitioning.partition_count();
        let target_partition = doget.target_partition as usize;
        let plan_name = stage.plan.name();
        if target_partition >= partition_count {
            return Err(datafusion_error_to_tonic_status(&exec_datafusion_err!(
                "partition {target_partition} not available. The head plan {plan_name} of the stage just has {partition_count} partitions"
            )));
        }

        // Rather than executing the `StageExec` itself, we want to execute the inner plan instead,
        // as executing `StageExec` performs some worker assignation that should have already been
        // done in the head stage.
        let stream = stage
            .plan
            .execute(doget.target_partition as usize, session_state.task_ctx())
            .map_err(|err| Status::internal(format!("Error executing stage plan: {err:#?}")))?;

        let schema = stream.schema();

        // TODO: We don't need to do this since the stage / plan is captured again by the
        // TrailingFlightDataStream. However, we will eventuall only use the TrailingFlightDataStream
        // if we are running an `explain (analyze)` command. We should update this section
        // to only use one or the other - not both.
        let plan_capture = stage.plan.clone();
        let stream = with_callback(stream, move |_| {
            // We need to hold a reference to the plan for at least as long as the stream is
            // execution. Some plans might store state necessary for the stream to work, and
            // dropping the plan early could drop this state too soon.
            let _ = plan_capture;
        });

        let record_batch_stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));
        let task_data_capture = self.task_data_entries.clone();
        Ok(flight_stream_from_record_batch_stream(
            key.clone(),
            stage_data.clone(),
            move || {
                task_data_capture.remove(key.clone());
            },
            record_batch_stream,
        ))
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}

/// Creates a tonic response from a stream of record batches. Handles
/// - RecordBatch to flight conversion partition tracking, stage eviction, and trailing metrics.
fn flight_stream_from_record_batch_stream(
    stage_key: StageKey,
    stage_data: TaskData,
    evict_stage: impl FnOnce() + Send + 'static,
    stream: SendableRecordBatchStream,
) -> Response<<ArrowFlightEndpoint as FlightService>::DoGetStream> {
    let mut flight_data_stream =
        FlightDataEncoderBuilder::new()
            .with_schema(stream.schema().clone())
            .build(stream.map_err(|err| {
                FlightError::Tonic(Box::new(datafusion_error_to_tonic_status(&err)))
            }));

    // executed once when the stream ends
    // decorates the last flight data with our metrics
    let mut final_closure = Some(move |last_flight_data| {
        if stage_data
            .num_partitions_remaining
            .fetch_sub(1, Ordering::SeqCst)
            == 1
        {
            evict_stage();

            collect_and_create_metrics_flight_data(stage_key, stage_data.stage, last_flight_data)
        } else {
            Ok(last_flight_data)
        }
    });

    // this is used to peek the new value
    // so that we can add our metrics to the last flight data
    let mut current_value = None;

    let stream =
        stream::poll_fn(
            move |cx| match futures::ready!(flight_data_stream.poll_next_unpin(cx)) {
                Some(Ok(new_val)) => {
                    match current_value.take() {
                        // This is the first value, so we store it and repoll to get the next value
                        None => {
                            current_value = Some(new_val);
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }

                        Some(existing) => {
                            current_value = Some(new_val);

                            Poll::Ready(Some(Ok(existing)))
                        }
                    }
                }
                // this is our last value, so we add our metrics to this flight data
                None => match current_value.take() {
                    Some(existing) => {
                        // make sure we wake ourselves to finish the stream
                        cx.waker().wake_by_ref();

                        if let Some(closure) = final_closure.take() {
                            Poll::Ready(Some(closure(existing)))
                        } else {
                            unreachable!("the closure is only executed once")
                        }
                    }
                    None => Poll::Ready(None),
                },
                err => Poll::Ready(err),
            },
        );

    Response::new(Box::pin(stream.map_err(|err| match err {
        FlightError::Tonic(status) => *status,
        _ => Status::internal(format!("Error during flight stream: {err}")),
    })))
}

/// Collects metrics from the provided stage and includes it in the flight data
fn collect_and_create_metrics_flight_data(
    stage_key: StageKey,
    stage: Arc<StageExec>,
    incoming: FlightData,
) -> Result<FlightData, FlightError> {
    // Get the metrics for the task executed on this worker. Separately, collect metrics for child tasks.
    let mut result = TaskMetricsCollector::new()
        .collect(stage.plan.clone())
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

    let flight_app_metadata = FlightAppMetadata {
        content: Some(AppMetadata::MetricsCollection(MetricsCollection {
            tasks: task_metrics_set,
        })),
    };

    let mut buf = vec![];
    flight_app_metadata
        .encode(&mut buf)
        .map_err(|err| FlightError::ProtocolError(err.to_string()))?;

    Ok(incoming.with_app_metadata(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutionTask;
    use crate::flight_service::session_builder::DefaultSessionBuilder;
    use crate::protobuf::proto_from_stage;
    use arrow::datatypes::{Schema, SchemaRef};
    use arrow_flight::Ticket;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
    use prost::{Message, bytes::Bytes};
    use tonic::Request;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_task_data_partition_counting() {
        // Create ArrowFlightEndpoint with DefaultSessionBuilder
        let endpoint =
            ArrowFlightEndpoint::try_new(DefaultSessionBuilder).expect("Failed to create endpoint");

        // Create 3 tasks with 3 partitions each.
        let num_tasks = 3;
        let num_partitions_per_task = 3;
        let stage_id = 1;
        let query_id = Uuid::new_v4();

        // Set up protos.
        let mut tasks = Vec::new();
        for _ in 0..num_tasks {
            tasks.push(ExecutionTask { url: None });
        }

        let stage = StageExec {
            query_id,
            num: 1,
            name: format!("test_stage_{}", 1),
            plan: create_mock_physical_plan(num_partitions_per_task),
            inputs: vec![],
            tasks,
            depth: 0,
        };

        let task_keys = [
            StageKey {
                query_id: query_id.to_string(),
                stage_id,
                task_number: 0,
            },
            StageKey {
                query_id: query_id.to_string(),
                stage_id,
                task_number: 1,
            },
            StageKey {
                query_id: query_id.to_string(),
                stage_id,
                task_number: 2,
            },
        ];
        let stage_proto = proto_from_stage(&stage, &DefaultPhysicalExtensionCodec {}).unwrap();
        let stage_proto_for_closure = stage_proto.clone();
        let endpoint_ref = &endpoint;

        let do_get = async move |partition: u64, task_number: u64, stage_key: StageKey| {
            let stage_proto = stage_proto_for_closure.clone();
            let doget = DoGet {
                stage_proto: stage_proto.encode_to_vec().into(),
                target_task_index: task_number,
                target_partition: partition,
                stage_key: Some(stage_key),
            };

            let ticket = Ticket {
                ticket: Bytes::from(doget.encode_to_vec()),
            };

            let request = Request::new(ticket);
            let response = endpoint_ref.get(request).await?;
            let mut stream = response.into_inner();

            // Consume the stream.
            while let Some(_flight_data) = stream.try_next().await? {}
            Ok::<(), Status>(())
        };

        // For each task, call do_get() for each partition except the last.
        for (task_number, task_key) in task_keys.iter().enumerate() {
            for partition in 0..num_partitions_per_task - 1 {
                let result = do_get(partition as u64, task_number as u64, task_key.clone()).await;
                assert!(result.is_ok());
            }
        }

        // Check that the endpoint has not evicted any task states.
        assert_eq!(endpoint.task_data_entries.len(), num_tasks);

        // Run the last partition of task 0. Any partition number works. Verify that the task state
        // is evicted because all partitions have been processed.
        let result = do_get(2, 0, task_keys[0].clone()).await;
        assert!(result.is_ok());
        let stored_stage_keys = endpoint.task_data_entries.keys().collect::<Vec<StageKey>>();
        assert_eq!(stored_stage_keys.len(), 2);
        assert!(stored_stage_keys.contains(&task_keys[1]));
        assert!(stored_stage_keys.contains(&task_keys[2]));

        // Run the last partition of task 1.
        let result = do_get(2, 1, task_keys[1].clone()).await;
        assert!(result.is_ok());
        let stored_stage_keys = endpoint.task_data_entries.keys().collect::<Vec<StageKey>>();
        assert_eq!(stored_stage_keys.len(), 1);
        assert!(stored_stage_keys.contains(&task_keys[2]));

        // Run the last partition of the last task.
        let result = do_get(2, 2, task_keys[2].clone()).await;
        assert!(result.is_ok());
        let stored_stage_keys = endpoint.task_data_entries.keys().collect::<Vec<StageKey>>();
        assert_eq!(stored_stage_keys.len(), 0);
    }

    // Helper to create a mock physical plan
    fn create_mock_physical_plan(partitions: usize) -> Arc<dyn ExecutionPlan> {
        let node = Arc::new(EmptyExec::new(SchemaRef::new(Schema::empty())));
        Arc::new(RepartitionExec::try_new(node, Partitioning::RoundRobinBatch(partitions)).unwrap())
    }
}
