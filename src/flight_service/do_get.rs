use crate::common::map_last_stream;
use crate::config_extension_ext::{
    ContextGrpcMetadata, set_distributed_option_extension_from_headers,
};
use crate::flight_service::service::ArrowFlightEndpoint;
use crate::flight_service::session_builder::DistributedSessionBuilderContext;
use crate::metrics::TaskMetricsCollector;
use crate::metrics::proto::df_metrics_set_to_proto;
use crate::protobuf::{
    AppMetadata, DistributedCodec, FlightAppMetadata, MetricsCollection, StageKey, TaskMetrics,
    datafusion_error_to_tonic_status,
};
use crate::{DistributedConfig, DistributedTaskContext};
use arrow_flight::FlightData;
use arrow_flight::Ticket;
use arrow_flight::encode::{DictionaryHandling, FlightDataEncoderBuilder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_select::dictionary::garbage_collect_any_dictionary;
use bytes::Bytes;
use datafusion::arrow::array::{Array, AsArray, RecordBatch};

use datafusion::common::exec_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::TryStreamExt;
use prost::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tonic::{Request, Response, Status};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoGet {
    /// The [Arc<dyn ExecutionPlan>] we are going to execute encoded as protobuf bytes.
    #[prost(bytes, tag = "1")]
    pub plan_proto: Bytes,
    /// The index to the task within the stage that we want to execute
    #[prost(uint64, tag = "2")]
    pub target_task_index: u64,
    #[prost(uint64, tag = "3")]
    pub target_task_count: u64,
    /// the partition number we want to execute
    #[prost(uint64, tag = "4")]
    pub target_partition: u64,
    /// The stage key that identifies the stage.  This is useful to keep
    /// outside of the stage proto as it is used to store the stage
    /// and we may not need to deserialize the entire stage proto
    /// if we already have stored it
    #[prost(message, optional, tag = "5")]
    pub stage_key: Option<StageKey>,
}

#[derive(Clone, Debug)]
/// TaskData stores state for a single task being executed by this Endpoint. It may be shared
/// by concurrent requests for the same task which execute separate partitions.
pub struct TaskData {
    pub(super) plan: Arc<dyn ExecutionPlan>,
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

        let headers = metadata.into_headers();
        let mut session_state = self
            .session_builder
            .build_session_state(DistributedSessionBuilderContext {
                runtime_env: Arc::clone(&self.runtime),
                headers: headers.clone(),
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
                let proto_node = PhysicalPlanNode::try_decode(doget.plan_proto.as_ref())?;
                let mut plan = proto_node.try_into_physical_plan(&ctx.task_ctx(), &codec)?;
                for hook in self.hooks.on_plan.iter() {
                    plan = hook(plan)
                }

                // Initialize partition count to the number of partitions in the stage
                let total_partitions = plan.properties().partitioning.partition_count();
                Ok::<_, DataFusionError>(TaskData {
                    plan,
                    num_partitions_remaining: Arc::new(AtomicUsize::new(total_partitions)),
                })
            })
            .await
            .map_err(|err| Status::invalid_argument(format!("Cannot decode stage proto: {err}")))?;
        let plan = Arc::clone(&stage_data.plan);

        // Find out which partition group we are executing
        let cfg = session_state.config_mut();
        let d_cfg =
            set_distributed_option_extension_from_headers::<DistributedConfig>(cfg, &headers)
                .map_err(|err| datafusion_error_to_tonic_status(&err))?;
        let send_metrics = d_cfg.collect_metrics;
        cfg.set_extension(Arc::new(ContextGrpcMetadata(headers)));
        cfg.set_extension(Arc::new(DistributedTaskContext {
            task_index: doget.target_task_index as usize,
            task_count: doget.target_task_count as usize,
        }));

        let partition_count = plan.properties().partitioning.partition_count();
        let target_partition = doget.target_partition as usize;
        let plan_name = plan.name();
        if target_partition >= partition_count {
            return Err(datafusion_error_to_tonic_status(&exec_datafusion_err!(
                "partition {target_partition} not available. The head plan {plan_name} of the stage just has {partition_count} partitions"
            )));
        }

        // Rather than executing the `StageExec` itself, we want to execute the inner plan instead,
        // as executing `StageExec` performs some worker assignation that should have already been
        // done in the head stage.
        let stream = plan
            .execute(doget.target_partition as usize, session_state.task_ctx())
            .map_err(|err| Status::internal(format!("Error executing stage plan: {err:#?}")))?;

        let schema = stream.schema().clone();

        // Apply garbage collection of dictionary and view arrays before sending over the network
        let stream = stream.and_then(|rb| std::future::ready(garbage_collect_arrays(rb)));

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
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
            .build(stream.map_err(|err| {
                FlightError::Tonic(Box::new(datafusion_error_to_tonic_status(&err)))
            }));

        let task_data_entries = Arc::clone(&self.task_data_entries);
        let num_partitions_remaining = Arc::clone(&stage_data.num_partitions_remaining);

        let stream = map_last_stream(stream, move |last| {
            if num_partitions_remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                task_data_entries.remove(key.clone());
                return if send_metrics {
                    last.and_then(|el| collect_and_create_metrics_flight_data(key, plan, el))
                } else {
                    last
                };
            }
            last
        });

        Ok(Response::new(Box::pin(stream.map_err(|err| match err {
            FlightError::Tonic(status) => *status,
            _ => Status::internal(format!("Error during flight stream: {err}")),
        }))))
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}

/// Collects metrics from the provided stage and includes it in the flight data
fn collect_and_create_metrics_flight_data(
    stage_key: StageKey,
    plan: Arc<dyn ExecutionPlan>,
    incoming: FlightData,
) -> Result<FlightData, FlightError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flight_service::session_builder::DefaultSessionBuilder;
    use crate::stage::ExecutionTask;
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
        let mut endpoint =
            ArrowFlightEndpoint::try_new(DefaultSessionBuilder).expect("Failed to create endpoint");
        let plans_received = Arc::new(AtomicUsize::default());
        {
            let plans_received = Arc::clone(&plans_received);
            endpoint.add_on_plan_hook(move |plan| {
                plans_received.fetch_add(1, Ordering::SeqCst);
                plan
            });
        }

        // Create 3 tasks with 3 partitions each.
        let num_tasks = 3;
        let num_partitions_per_task = 3;
        let stage_id = 1;
        let query_id = Bytes::from(Uuid::new_v4().into_bytes().to_vec());

        // Set up protos.
        let mut tasks = Vec::new();
        for _ in 0..num_tasks {
            tasks.push(ExecutionTask { url: None });
        }
        let plan = create_mock_physical_plan(num_partitions_per_task);
        let plan_proto: Bytes =
            PhysicalPlanNode::try_from_physical_plan(plan, &DefaultPhysicalExtensionCodec {})
                .unwrap()
                .encode_to_vec()
                .into();

        let task_keys: Vec<_> = (0..3)
            .map(|i| StageKey::new(query_id.clone(), stage_id, i))
            .collect();

        let plan_proto_for_closure = plan_proto.clone();
        let endpoint_ref = &endpoint;

        let do_get = async move |partition: u64, task_number: u64, stage_key: StageKey| {
            let plan_proto = plan_proto_for_closure.clone();
            let doget = DoGet {
                plan_proto,
                target_task_index: task_number,
                target_task_count: num_tasks,
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
                if let Err(err) = result {
                    panic!("do_get call failed with error: {err}")
                }
            }
        }
        // As many plans as tasks should have been received.
        assert_eq!(plans_received.load(Ordering::SeqCst), task_keys.len());

        // Check that the endpoint has not evicted any task states.
        assert_eq!(endpoint.task_data_entries.len(), num_tasks as usize);

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
