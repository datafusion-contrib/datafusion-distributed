use super::service::StageKey;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::errors::datafusion_error_to_tonic_status;
use crate::execution_plans::{PartitionGroup, StageExec};
use crate::flight_service::service::ArrowFlightEndpoint;
use crate::flight_service::session_builder::DistributedSessionBuilderContext;
use crate::protobuf::{stage_from_proto, DistributedCodec, StageExecProto};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use datafusion::execution::{SendableRecordBatchStream, SessionState};
use futures::TryStreamExt;
use http::HeaderMap;
use prost::Message;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::{Request, Response, Status};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoGet {
    /// The ExecutionStage that we are going to execute
    #[prost(message, optional, tag = "1")]
    pub stage_proto: Option<StageExecProto>,
    /// The index to the task within the stage that we want to execute
    #[prost(uint64, tag = "2")]
    pub task_number: u64,
    /// the partition number we want to execute
    #[prost(uint64, tag = "3")]
    pub partition: u64,
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
    pub(super) session_state: SessionState,
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

        // There's only 1 `StageExec` responsible for all requests that share the same `stage_key`,
        // so here we either retrieve the existing one or create a new one if it does not exist.
        let (mut session_state, stage) = self
            .get_state_and_stage(
                doget.stage_key.ok_or_else(missing("stage_key"))?,
                doget.stage_proto.ok_or_else(missing("stage_proto"))?,
                metadata.clone().into_headers(),
            )
            .await?;

        // Find out which partition group we are executing
        let partition = doget.partition as usize;
        let task_number = doget.task_number as usize;
        let task = stage.tasks.get(task_number).ok_or_else(invalid(format!(
            "Task number {task_number} not found in stage {}",
            stage.num
        )))?;

        let cfg = session_state.config_mut();
        cfg.set_extension(Arc::new(PartitionGroup(task.partition_group.clone())));
        cfg.set_extension(Arc::clone(&stage));
        cfg.set_extension(Arc::new(ContextGrpcMetadata(metadata.into_headers())));

        // Rather than executing the `StageExec` itself, we want to execute the inner plan instead,
        // as executing `StageExec` performs some worker assignation that should have already been
        // done in the head stage.
        let stream = stage
            .plan
            .execute(partition, session_state.task_ctx())
            .map_err(|err| Status::internal(format!("Error executing stage plan: {err:#?}")))?;

        Ok(record_batch_stream_to_response(stream))
    }

    async fn get_state_and_stage(
        &self,
        key: StageKey,
        stage_proto: StageExecProto,
        headers: HeaderMap,
    ) -> Result<(SessionState, Arc<StageExec>), Status> {
        let once = self
            .task_data_entries
            .get_or_init(key.clone(), || Arc::new(OnceCell::<TaskData>::new()));

        let stage_data = once
            .get_or_try_init(|| async {
                let session_state = self
                    .session_builder
                    .build_session_state(DistributedSessionBuilderContext {
                        runtime_env: Arc::clone(&self.runtime),
                        headers,
                    })
                    .await
                    .map_err(|err| datafusion_error_to_tonic_status(&err))?;

                let codec = DistributedCodec::new_combined_with_user(session_state.config());

                let stage = stage_from_proto(stage_proto, &session_state, &self.runtime, &codec)
                    .map_err(|err| {
                        Status::invalid_argument(format!("Cannot decode stage proto: {err}"))
                    })?;

                // Initialize partition count to the number of partitions in the stage
                let total_partitions = stage.plan.properties().partitioning.partition_count();
                Ok::<_, Status>(TaskData {
                    session_state,
                    stage: Arc::new(stage),
                    num_partitions_remaining: Arc::new(AtomicUsize::new(total_partitions)),
                })
            })
            .await?;

        // If all the partitions are done, remove the stage from the cache.
        let remaining_partitions = stage_data
            .num_partitions_remaining
            .fetch_sub(1, Ordering::SeqCst);
        if remaining_partitions <= 1 {
            self.task_data_entries.remove(key);
        }

        Ok((stage_data.session_state.clone(), stage_data.stage.clone()))
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}

fn invalid(msg: impl Display) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(msg.to_string())
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
    use crate::flight_service::session_builder::DefaultSessionBuilder;
    use crate::protobuf::proto_from_stage;
    use crate::ExecutionTask;
    use arrow::datatypes::{Schema, SchemaRef};
    use arrow_flight::Ticket;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
    use prost::{bytes::Bytes, Message};
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
        for i in 0..num_tasks {
            tasks.push(ExecutionTask {
                url: None,
                partition_group: vec![i], // Set a random partition in the partition group.
            });
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
                task_number,
                partition,
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
