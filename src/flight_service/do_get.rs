use crate::stage::StageKey;
use crate::common::ComposedPhysicalExtensionCodec;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::errors::datafusion_error_to_tonic_status;
use crate::flight_service::service::ArrowFlightEndpoint;
use crate::flight_service::session_builder::DistributedSessionBuilderContext;
use crate::plan::{DistributedCodec, PartitionGroup};
use crate::stage::{stage_from_proto, ExecutionStage, ExecutionStageProto};
use crate::user_codec_ext::get_distributed_user_codec;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use datafusion::execution::SessionState;
use futures::TryStreamExt;
use prost::Message;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoGet {
    /// The ExecutionStage that we are going to execute
    #[prost(message, optional, tag = "1")]
    pub stage_proto: Option<ExecutionStageProto>,
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
    pub(super) state: SessionState,
    pub(super) stage: Arc<ExecutionStage>,
    ///num_partitions_remaining is initialized to the total number of partitions in the task (not
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
        let (metadata, _ext, ticket) = request.into_parts();
        let Ticket { ticket } = ticket;
        let doget = DoGet::decode(ticket).map_err(|err| {
            Status::invalid_argument(format!("Cannot decode DoGet message: {err}"))
        })?;

        let partition = doget.partition as usize;
        let task_number = doget.task_number as usize;
        let task_data = self.get_state_and_stage(doget, metadata).await?;

        let stage = task_data.stage;
        let mut state = task_data.state;

        // find out which partition group we are executing
        let task = stage
            .tasks
            .get(task_number)
            .ok_or(Status::invalid_argument(format!(
                "Task number {} not found in stage {}",
                task_number,
                stage.name()
            )))?;

        let partition_group =
            PartitionGroup(task.partition_group.iter().map(|p| *p as usize).collect());
        state.config_mut().set_extension(Arc::new(partition_group));

        let inner_plan = stage.plan.clone();

        let stream = inner_plan
            .execute(partition, state.task_ctx())
            .map_err(|err| Status::internal(format!("Error executing stage plan: {err:#?}")))?;

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(inner_plan.schema().clone())
            .build(stream.map_err(|err| {
                FlightError::Tonic(Box::new(datafusion_error_to_tonic_status(&err)))
            }));

        Ok(Response::new(Box::pin(flight_data_stream.map_err(
            |err| match err {
                FlightError::Tonic(status) => *status,
                _ => Status::internal(format!("Error during flight stream: {err}")),
            },
        ))))
    }

    async fn get_state_and_stage(
        &self,
        doget: DoGet,
        metadata_map: MetadataMap,
    ) -> Result<TaskData, Status> {
        let key = doget
            .stage_key
            .ok_or(Status::invalid_argument("DoGet is missing the stage key"))?;
        let once_stage = self
            .stages
            .get_or_init(key.clone(), || Arc::new(OnceCell::<TaskData>::new()));

        let stage_data = once_stage
            .get_or_try_init(|| async {
                let stage_proto = doget
                    .stage_proto
                    .ok_or(Status::invalid_argument("DoGet is missing the stage proto"))?;

                let headers = metadata_map.into_headers();
                let mut state = self
                    .session_builder
                    .build_session_state(DistributedSessionBuilderContext {
                        runtime_env: Arc::clone(&self.runtime),
                        headers: headers.clone(),
                    })
                    .await
                    .map_err(|err| datafusion_error_to_tonic_status(&err))?;

                let mut combined_codec = ComposedPhysicalExtensionCodec::default();
                combined_codec.push(DistributedCodec);
                if let Some(ref user_codec) = get_distributed_user_codec(state.config()) {
                    combined_codec.push_arc(Arc::clone(user_codec));
                }

                let stage =
                    stage_from_proto(stage_proto, &state, self.runtime.as_ref(), &combined_codec)
                        .map(Arc::new)
                        .map_err(|err| {
                            Status::invalid_argument(format!("Cannot decode stage proto: {err}"))
                        })?;

                // Add the extensions that might be required for ExecutionPlan nodes in the plan
                let config = state.config_mut();
                config.set_extension(stage.clone());
                config.set_extension(Arc::new(ContextGrpcMetadata(headers)));

                // Initialize partition count to the number of partitions in the stage
                let total_partitions = stage.plan.properties().partitioning.partition_count();
                Ok::<_, Status>(TaskData {
                    state,
                    stage,
                    num_partitions_remaining: Arc::new(AtomicUsize::new(total_partitions)),
                })
            })
            .await?;

        // If all the partitions are done, remove the stage from the cache.
        let remaining_partitions = stage_data
            .num_partitions_remaining
            .fetch_sub(1, Ordering::SeqCst);
        if remaining_partitions <= 1 {
            self.stages.remove(key.clone());
        }

        Ok(stage_data.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_task_data_partition_counting() {
        use crate::flight_service::session_builder::DefaultSessionBuilder;
        use crate::task::ExecutionTask;
        use arrow_flight::Ticket;
        use prost::{bytes::Bytes, Message};
        use tonic::Request;

        // Create ArrowFlightEndpoint with DefaultSessionBuilder
        let endpoint =
            ArrowFlightEndpoint::new(DefaultSessionBuilder).expect("Failed to create endpoint");

        // Create 3 tasks with 3 partitions each.
        let num_tasks = 3;
        let num_partitions_per_task = 3;
        let stage_id = 1;
        let query_id_uuid = Uuid::new_v4();
        let query_id = query_id_uuid.as_bytes().to_vec();

        // Set up protos.
        let mut tasks = Vec::new();
        for i in 0..num_tasks {
            tasks.push(ExecutionTask {
                url_str: None,
                partition_group: vec![i], // Set a random partition in the partition group.
            });
        }

        let stage_proto = ExecutionStageProto {
            query_id: query_id.clone(),
            num: 1,
            name: format!("test_stage_{}", 1),
            plan: Some(Box::new(create_mock_physical_plan_proto(
                num_partitions_per_task,
            ))),
            inputs: vec![],
            tasks,
            task_metrics: Default::default(),
        };

        let task_keys = vec![
            StageKey {
                query_id: query_id_uuid.to_string(),
                stage_id,
                task_number: 0,
            },
            StageKey {
                query_id: query_id_uuid.to_string(),
                stage_id,
                task_number: 1,
            },
            StageKey {
                query_id: query_id_uuid.to_string(),
                stage_id,
                task_number: 2,
            },
        ];

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
        for task_number in 0..num_tasks {
            for partition in 0..num_partitions_per_task - 1 {
                let result = do_get(
                    partition as u64,
                    task_number,
                    task_keys[task_number as usize].clone(),
                )
                .await;
                assert!(result.is_ok());
            }
        }

        // Check that the endpoint has not evicted any task states.
        assert_eq!(endpoint.stages.len(), num_tasks as usize);

        // Run the last partition of task 0. Any partition number works. Verify that the task state
        // is evicted because all partitions have been processed.
        let result = do_get(1, 0, task_keys[0].clone()).await;
        assert!(result.is_ok());
        let stored_stage_keys = endpoint.stages.keys().collect::<Vec<StageKey>>();
        assert_eq!(stored_stage_keys.len(), 2);
        assert!(stored_stage_keys.contains(&task_keys[1]));
        assert!(stored_stage_keys.contains(&task_keys[2]));

        // Run the last partition of task 1.
        let result = do_get(1, 1, task_keys[1].clone()).await;
        assert!(result.is_ok());
        let stored_stage_keys = endpoint.stages.keys().collect::<Vec<StageKey>>();
        assert_eq!(stored_stage_keys.len(), 1);
        assert!(stored_stage_keys.contains(&task_keys[2]));

        // Run the last partition of the last task.
        let result = do_get(1, 2, task_keys[2].clone()).await;
        assert!(result.is_ok());
        let stored_stage_keys = endpoint.stages.keys().collect::<Vec<StageKey>>();
        assert_eq!(stored_stage_keys.len(), 0);
    }

    // Helper to create a mock physical plan proto
    fn create_mock_physical_plan_proto(
        partitions: usize,
    ) -> datafusion_proto::protobuf::PhysicalPlanNode {
        use datafusion_proto::protobuf::partitioning::PartitionMethod;
        use datafusion_proto::protobuf::{
            Partitioning, PhysicalPlanNode, RepartitionExecNode, Schema,
        };

        // Create a repartition node that will have the desired partition count
        PhysicalPlanNode {
            physical_plan_type: Some(
                datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType::Repartition(
                    Box::new(RepartitionExecNode {
                        input: Some(Box::new(PhysicalPlanNode {
                            physical_plan_type: Some(
                                datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType::Empty(
                                    datafusion_proto::protobuf::EmptyExecNode {
                                        schema: Some(Schema {
                                            columns: vec![],
                                            metadata: std::collections::HashMap::new(),
                                        })
                                    }
                                )
                            ),
                        })),
                        partitioning: Some(Partitioning {
                            partition_method: Some(PartitionMethod::RoundRobin(partitions as u64)),
                        }),
                    })
                )
            ),
        }
    }
}
