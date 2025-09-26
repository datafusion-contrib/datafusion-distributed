use crate::common::with_callback;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::execution_plans::{DistributedTaskContext, StageExec};
use crate::flight_service::service::ArrowFlightEndpoint;
use crate::flight_service::session_builder::DistributedSessionBuilderContext;
use crate::protobuf::{
    DistributedCodec, StageExecProto, StageKey, datafusion_error_to_tonic_status, stage_from_proto,
};
use arrow_flight::Ticket;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use datafusion::common::exec_datafusion_err;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;
use prost::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tonic::{Request, Response, Status};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoGet {
    /// The ExecutionStage that we are going to execute
    #[prost(message, optional, tag = "1")]
    pub stage_proto: Option<StageExecProto>,
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

        // There's only 1 `StageExec` responsible for all requests that share the same `stage_key`,
        // so here we either retrieve the existing one or create a new one if it does not exist.
        let key = doget.stage_key.ok_or_else(missing("stage_key"))?;
        let once = self
            .task_data_entries
            .get_or_init(key.clone(), Default::default);

        let stage_data = once
            .get_or_try_init(|| async {
                let stage_proto = doget.stage_proto.ok_or_else(missing("stage_proto"))?;
                let stage = stage_from_proto(stage_proto, &session_state, &self.runtime, &codec)
                    .map_err(|err| {
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
        let num_partitions_remaining = Arc::clone(&stage_data.num_partitions_remaining);

        // If all the partitions are done, remove the stage from the cache.
        if num_partitions_remaining.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.task_data_entries.remove(key);
        }

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
        let stream = with_callback(stream, move |_| {
            // We need to hold a reference to the plan for at least as long as the stream is
            // execution. Some plans might store state necessary for the stream to work, and
            // dropping the plan early could drop this state too soon.
            let _ = stage.plan;
        });

        Ok(record_batch_stream_to_response(Box::pin(
            RecordBatchStreamAdapter::new(schema, stream),
        )))
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}

fn record_batch_stream_to_response(
    stream: SendableRecordBatchStream,
) -> Response<<ArrowFlightEndpoint as FlightService>::DoGetStream> {
    let flight_data_stream =
        FlightDataEncoderBuilder::new()
            .with_schema(stream.schema().clone())
            .build(stream.map_err(|err| {
                FlightError::Tonic(Box::new(datafusion_error_to_tonic_status(&err)))
            }));

    Response::new(Box::pin(flight_data_stream.map_err(|err| match err {
        FlightError::Tonic(status) => *status,
        _ => Status::internal(format!("Error during flight stream: {err}")),
    })))
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
            // Create DoGet message
            let doget = DoGet {
                stage_proto: Some(stage_proto),
                target_task_index: task_number,
                target_partition: partition,
                stage_key: Some(stage_key),
            };

            // Create Flight ticket
            let ticket = Ticket {
                ticket: Bytes::from(doget.encode_to_vec()),
            };

            // Call the actual get() method
            let request = Request::new(ticket);
            endpoint_ref.get(request).await
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
        let result = do_get(1, 0, task_keys[0].clone()).await;
        assert!(result.is_ok());
        let stored_stage_keys = endpoint.task_data_entries.keys().collect::<Vec<StageKey>>();
        assert_eq!(stored_stage_keys.len(), 2);
        assert!(stored_stage_keys.contains(&task_keys[1]));
        assert!(stored_stage_keys.contains(&task_keys[2]));

        // Run the last partition of task 1.
        let result = do_get(1, 1, task_keys[1].clone()).await;
        assert!(result.is_ok());
        let stored_stage_keys = endpoint.task_data_entries.keys().collect::<Vec<StageKey>>();
        assert_eq!(stored_stage_keys.len(), 1);
        assert!(stored_stage_keys.contains(&task_keys[2]));

        // Run the last partition of the last task.
        let result = do_get(1, 2, task_keys[2].clone()).await;
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
