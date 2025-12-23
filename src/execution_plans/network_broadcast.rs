use crate::channel_resolver_ext::get_distributed_channel_resolver;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::execution_plans::common::{
    manually_propagate_distributed_config, require_one_child, spawn_select_all,
};
use crate::flight_service::DoGet;
use crate::metrics::MetricsCollectingStream;
use crate::metrics::proto::MetricsSetProto;
use crate::protobuf::StageKey;
use crate::protobuf::{map_flight_to_datafusion_error, map_status_to_datafusion_error};
use crate::stage::{MaybeEncodedPlan, Stage};
use crate::{ChannelResolver, DistributedConfig, InputStageInfo, NetworkBoundary};
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use bytes::Bytes;
use dashmap::DashMap;
use datafusion::common::{exec_err, internal_datafusion_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use http::Extensions;
use prost::Message;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tonic::Request;
use tonic::metadata::MetadataMap;

/// [ExecutionPlan] implementation that broadcasts data across the network in to all tasks in a
/// stage.
///
/// This operators is easiest to visualize in the context of a hash join where one table is much
/// smaller than the other (typically a CollectLeft joinn in DataFusion plans) in a distributed system.
/// Without this operator the join would be forced to execute on a single node. It would be much more
/// efficient to broadcast the entire small table to each task (node) in the stage, having each task
/// complete a the join with a partition of the large table and full small table.
///
/// This node allows broadcasting data from N tasks to M tasks, being N and M arbitrary non-zero
/// positive numbers. This comes with the caveat that broadcasting with > 1 input tasks requires
/// coalescing their partitions to a single producer task which avoids partial or duplicate data being broadcast.
///
/// Here are some examples of how data can be broadcast in different scenarios:
///
/// # 1 stage with 1 partition to M tasks
///
/// ```text
/// ┌────────────────────────┐     ┌────────────────────────┐     ┌────────────────────────┐       ■
/// │  NetworkBroadcastExec  │     │  NetworkBroadcastExec  │ ... │  NetworkBroadcastExec  │       │
/// │        (task 1)        │     │        (task 2)        │     │        (task M)        │   Stage N+1
/// └──────────┬─┬───────────┘     └───────────┬─┬──────────┘     └───────────┬─┬──────────┘       │
///            │1│                             │1│                            │1│                  │
///            └▲┘                             └▲┘                            └▲┘                  ■
///             │                               │                              │
///          Populate                         Cache                          Cache
///           Cache                            Hit                            Hit
///             │                               │                              │
///             └───────────────────────────────┼──────────────────────────────┘
///                                            ┌┴┐
///                                            │1│                                                 ■
///                                       ┌────┴─┴────┐                                            │
///                                       │Batch Cache│                                            │
///                                 ┌─────┴───────────┴──────┐                                  Stage N
///                                 │ Arc<dyn ExecutionPlan> │                                     │
///                                 │        (task 1)        │                                     │
///                                 └────────────────────────┘                                     ■
/// ```
/// All consumer stages are fetching the same partition. The first [NetworkBroadcastExec] causes
/// execution and populates the cache with the resulting batches. Subsequent
/// [NetworkBroadcastExec] operators read the results from the cache preventing; this is more
/// efficient and prevents duplicate execution of a partition.
///
/// # N stages to M stages
///
/// ```text
/// ┌────────────────────────┐     ┌────────────────────────┐     ┌────────────────────────┐       ■
/// │  NetworkBroadcastExec  │     │  NetworkBroadcastExec  │ ... │  NetworkBroadcastExec  │       │
/// │        (task 1)        │     │        (task 2)        │     │        (task M)        │   Stage N+1
/// └──────────┬─┬───────────┘     └───────────┬─┬──────────┘     └───────────┬─┬──────────┘       │
///            │1│                             │1│                            │1│                  │
///            └▲┘                             └▲┘                            └▲┘                  ■
///             │                               │                              │
///          Populate                         Cache                          Cache
///           Cache                            Hit                            Hit
///             │                               │                              │
///             └───────────────────────────────┼──────────────────────────────┘
///                                            ┌┴┐
///                                            │1│
///                                       ┌────┴─┴────┐                                            ■
///                                       │Batch Cache│                                            │
///                                ┌──────┴───────────┴─────┐                                      │
///                                │ CoalescePartitionsExec │                                   Stage N
///                                │                        │                                      │
///                                └┬─┬─────┬──┬┬─┬─────┬──┬┘                                      │
///                                 │1│     │P1││1│     │PN│                                       │
///                                 └▲┘ ... └─▲┘└▲┘ ... └─▲┘                                       ■
///                                  │        │  │        │
///                   ┌──────────────┘    ┌───┘  └───┐    └───────────────┐
///                   │                   │          │                    │
///                   │                   │          │                    │
///                  ┌┴┐       ...       ┌┴─┐       ┌┴┐       ...       ┌─┴┐                       ■
///                  │1│                 │P1│       │1│                 │PN│                       │
///                 ┌┴─┴─────────────────┴──┴┐     ┌┴─┴─────────────────┴──┴┐                  Stage N-1
///                 │ Arc<dyn ExecutionPlan> │ ... │ Arc<dyn ExecutionPlan> │                      │
///                 │        (task 1)        │     │        (task N)        │                      │
///                 └────────────────────────┘     └────────────────────────┘                      ■
/// ```
/// Here there are multiple input tasks, each with multiple partitionns. In a case like this a
/// [CoalescePartitionsExec] is inserted. You can imagine if this coalesce was not here partial or
/// duplicate data could be broadcast. Similarly, the first [NetworkBroadcastExec] triggers
/// execution and populates the cache while subsequent operators read results from the cache.
///
/// # Multiple Broadcasts in One Stage
/// Unlike other network operators there can be multiple [NetworkBroadcastExec] operators in a
/// single stage as they do not force a new stage to be created. If multiple small tables can fit
/// in a worker's memory then they can all be broadcast.
///
/// This node has two variants.
/// 1. Pending: it acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///    using Arrow Flight.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkBroadcastExec {
    Pending(NetworkBroadcastPendingExec),
    Ready(NetworkBroadcastReadyExec),
}

/// Placeholder version of the [NetworkBroadcastExec] node. It acts as a marker for the
/// distributed optimization step, which will replace it with the appropriate
/// [NetworkBroadcastReadyExec] node.
#[derive(Debug, Clone)]
pub struct NetworkBroadcastPendingExec {
    input: Arc<dyn ExecutionPlan>,
    input_tasks: usize,
}

/// Ready version of the [NetworkBroadcastExec] node. This node can be created in
/// just two ways:
/// - by the distributed optimization step based on an original [NetworkBroadcastPendingExec]
/// - deserialized from a protobuf plan sent over the network.
#[derive(Debug, Clone)]
pub struct NetworkBroadcastReadyExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: PlanProperties,
    pub(crate) input_stage: Stage,
    pub(crate) metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
}

impl NetworkBroadcastExec {
    /// Builds a new [NetworkBroadcastExec] in "Pending" state.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        input_tasks: usize,
    ) -> Result<Self, DataFusionError> {
        Ok(Self::Pending(NetworkBroadcastPendingExec {
            input,
            input_tasks,
        }))
    }
}

impl NetworkBoundary for NetworkBroadcastExec {
    fn get_input_stage_info(&self, _n_tasks: usize) -> Result<InputStageInfo, DataFusionError> {
        let Self::Pending(pending) = self else {
            return plan_err!("cannot only return wrapped child if on Pending state");
        };

        Ok(InputStageInfo {
            plan: Arc::clone(&pending.input),
            task_count: pending.input_tasks,
        })
    }

    fn with_input_task_count(
        &self,
        input_tasks: usize,
    ) -> Result<Arc<dyn NetworkBoundary>, DataFusionError> {
        Ok(Arc::new(match self {
            Self::Pending(prev) => Self::Pending(NetworkBroadcastPendingExec {
                input: Arc::clone(&prev.input),
                input_tasks,
            }),
            Self::Ready(_) => plan_err!(
                "NetworkBroadcastExec can only re-assign input tasks if in 'Pending' state"
            )?,
        }))
    }

    fn input_task_count(&self) -> usize {
        match self {
            Self::Pending(v) => v.input_tasks,
            Self::Ready(v) => v.input_stage.tasks.len(),
        }
    }

    fn with_input_stage(
        &self,
        input_stage: Stage,
        consumer_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match self {
            Self::Pending(_pending) => {
                let input_stage_plan = input_stage.plan.decoded()?;
                // Only wrap with CoalescePartitionsExec if the input has multiple partitions
                // and isn't already a CoalescePartitionsExec. This avoids double coalescing
                // when the build side already has a CoalescePartitionsExec (common for CollectLeft joins).
                let coalesced_plan = if input_stage_plan.output_partitioning().partition_count() > 1
                    && input_stage_plan
                        .as_any()
                        .downcast_ref::<CoalescePartitionsExec>()
                        .is_none()
                {
                    Arc::new(CoalescePartitionsExec::new(input_stage_plan.clone()))
                        as Arc<dyn ExecutionPlan>
                } else {
                    input_stage_plan.clone()
                };
                let coalesced_stage = Stage::new(
                    input_stage.query_id,
                    input_stage.num,
                    coalesced_plan.clone(),
                    input_stage.tasks.len(),
                    Some(consumer_task_count),
                );
                let ready = NetworkBroadcastReadyExec {
                    properties: coalesced_plan.properties().clone(),
                    input_stage: coalesced_stage,
                    metrics_collection: Default::default(),
                };
                Ok(Arc::new(Self::Ready(ready)))
            }
            Self::Ready(ready) => {
                let mut ready = ready.clone();
                ready.input_stage = input_stage;
                Ok(Arc::new(Self::Ready(ready)))
            }
        }
    }

    fn input_stage(&self) -> Option<&Stage> {
        match self {
            Self::Pending(_) => None,
            Self::Ready(v) => Some(&v.input_stage),
        }
    }
}

impl DisplayAs for NetworkBroadcastExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let Self::Ready(self_ready) = self else {
            return write!(f, "NetworkBroadcastExec: Pending");
        };

        let input_tasks = self_ready.input_stage.tasks.len();
        let partitions = self_ready.properties.partitioning.partition_count();
        let stage = self_ready.input_stage.num;
        write!(
            f,
            "[Stage {stage}] => NetworkBroadcastExec: output_partitions={partitions}, input_tasks={input_tasks}",
        )
    }
}

impl ExecutionPlan for NetworkBroadcastExec {
    fn name(&self) -> &str {
        "NetworkBroadcastExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        match self {
            NetworkBroadcastExec::Pending(v) => v.input.properties(),
            NetworkBroadcastExec::Ready(v) => &v.properties,
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match self {
            NetworkBroadcastExec::Pending(v) => vec![&v.input],
            NetworkBroadcastExec::Ready(v) => match &v.input_stage.plan {
                MaybeEncodedPlan::Decoded(v) => vec![v],
                MaybeEncodedPlan::Encoded(_) => vec![],
            },
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match self.as_ref() {
            Self::Pending(v) => {
                let mut v = v.clone();
                v.input = require_one_child(children)?;
                Ok(Arc::new(Self::Pending(v)))
            }
            Self::Ready(v) => {
                let mut v = v.clone();
                v.input_stage.plan = MaybeEncodedPlan::Decoded(require_one_child(children)?);
                Ok(Arc::new(Self::Ready(v)))
            }
        }
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let NetworkBroadcastExec::Ready(self_ready) = self else {
            return exec_err!(
                "NetworkBroadcastExec is not ready, was the distributed optimization step performed?"
            );
        };

        // get the channel manager and current stage from our context
        let channel_resolver = get_distributed_channel_resolver(context.session_config())?;

        let d_cfg = DistributedConfig::from_config_options(context.session_config().options())?;
        let retrieve_metrics = d_cfg.collect_metrics;

        let input_stage = &self_ready.input_stage;
        let encoded_input_plan = input_stage.plan.encoded()?;
        let input_stage_tasks = input_stage.tasks.to_vec();
        let input_task_count = input_stage_tasks.len();
        let input_stage_num = input_stage.num as u64;
        let query_id = Bytes::from(input_stage.query_id.as_bytes().to_vec());

        let context_headers = ContextGrpcMetadata::headers_from_ctx(&context);
        // TODO: this propagation should be automatic https://github.com/datafusion-contrib/datafusion-distributed/issues/247
        let context_headers = manually_propagate_distributed_config(context_headers, d_cfg);

        let stream = input_stage_tasks.into_iter().enumerate().map(|(i, task)| {
            let channel_resolver = Arc::clone(&channel_resolver);

            let ticket = Request::from_parts(
                MetadataMap::from_headers(context_headers.clone()),
                Extensions::default(),
                Ticket {
                    ticket: DoGet {
                        plan_proto: encoded_input_plan.clone(),
                        target_partition: 0, // Always 0 since coalesced to single partition
                        stage_key: Some(StageKey::new(query_id.clone(), input_stage_num, i as u64)),
                        target_task_index: i as u64,
                        target_task_count: input_task_count as u64,
                        consumer_count: self_ready
                            .input_stage
                            .consumer_task_count
                            .map(|c| c as u64),
                    }
                    .encode_to_vec()
                    .into(),
                },
            );

            let metrics_collection_capture = self_ready.metrics_collection.clone();
            async move {
                let url = task.url.ok_or(internal_datafusion_err!(
                    "NetworkBroadcastExec: task is unassigned, cannot proceed"
                ))?;

                let mut client = channel_resolver.get_flight_client_for_url(&url).await?;
                let stream = client
                    .do_get(ticket)
                    .await
                    .map_err(map_status_to_datafusion_error)?
                    .into_inner()
                    .map_err(|err| FlightError::Tonic(Box::new(err)));

                let stream = if retrieve_metrics {
                    MetricsCollectingStream::new(stream, metrics_collection_capture).left_stream()
                } else {
                    stream.right_stream()
                };

                Ok(FlightRecordBatchStream::new_from_flight_data(stream)
                    .map_err(map_flight_to_datafusion_error))
            }
            .try_flatten_stream()
            .boxed()
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            spawn_select_all(stream.collect(), Arc::clone(context.memory_pool())),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;
    use std::sync::Arc;
    use uuid::Uuid;

    fn create_test_plan(partition_count: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        Arc::new(EmptyExec::new(schema).with_partitions(partition_count))
    }

    #[test]
    fn test_network_broadcast_pending_creation() {
        let input = create_test_plan(3);
        let broadcast = NetworkBroadcastExec::try_new(input, 1).unwrap();
        assert!(matches!(broadcast, NetworkBroadcastExec::Pending(_)));
        assert_eq!(broadcast.input_task_count(), 1);
    }

    #[test]
    fn test_get_input_stage_info_pending() {
        let input = create_test_plan(3);
        let broadcast = NetworkBroadcastExec::try_new(Arc::clone(&input), 1).unwrap();

        let info = broadcast.get_input_stage_info(4).unwrap();
        assert_eq!(info.task_count, 1);
        assert!(Arc::ptr_eq(&info.plan, &input));
    }

    #[test]
    fn test_with_input_task_count_pending() {
        let input = create_test_plan(3);
        let broadcast = NetworkBroadcastExec::try_new(input, 1).unwrap();

        // Should be able to reassign task count in Pending state
        let new_broadcast = broadcast.with_input_task_count(4).unwrap();
        assert_eq!(new_broadcast.input_task_count(), 4);
    }

    #[test]
    fn test_coalesce_wrapping_multiple_partitions() {
        let input = create_test_plan(3); // 3 partitions
        let broadcast = NetworkBroadcastExec::try_new(input, 1).unwrap();
        let stage = Stage::new(Uuid::new_v4(), 1, create_test_plan(3), 1, None);
        let result = broadcast.with_input_stage(stage, 4).unwrap();

        // The result should be Ready state
        let ready = result
            .as_any()
            .downcast_ref::<NetworkBroadcastExec>()
            .unwrap();
        assert!(matches!(ready, NetworkBroadcastExec::Ready(_)));

        // The stage plan should be wrapped with CoalescePartitionsExec
        if let NetworkBroadcastExec::Ready(r) = ready {
            let plan = r.input_stage.plan.decoded().unwrap();
            assert!(
                plan.as_any()
                    .downcast_ref::<CoalescePartitionsExec>()
                    .is_some()
            );
        }
    }

    #[test]
    fn test_coalesce_wrapping_single_partition() {
        let input = create_test_plan(1); // 1 partition - no coalesce needed
        let broadcast = NetworkBroadcastExec::try_new(input, 1).unwrap();
        let stage = Stage::new(Uuid::new_v4(), 1, create_test_plan(1), 1, None);
        let result = broadcast.with_input_stage(stage, 4).unwrap();

        // The result should be Ready state
        let ready = result
            .as_any()
            .downcast_ref::<NetworkBroadcastExec>()
            .unwrap();

        // The stage plan should not be wrapped with CoalescePartitionsExec
        if let NetworkBroadcastExec::Ready(r) = ready {
            let plan = r.input_stage.plan.decoded().unwrap();
            assert!(
                plan.as_any()
                    .downcast_ref::<CoalescePartitionsExec>()
                    .is_none()
            );
        }
    }

    #[test]
    fn test_coalesce_not_double_wrapped() {
        // Create a plan that's already CoalescePartitionsExec
        let inner = create_test_plan(3);
        let coalesced = Arc::new(CoalescePartitionsExec::new(inner));
        let broadcast = NetworkBroadcastExec::try_new(coalesced.clone(), 1).unwrap();
        let stage = Stage::new(Uuid::new_v4(), 1, coalesced, 1, None);
        let result = broadcast.with_input_stage(stage, 4).unwrap();

        // The result should be Ready state
        let ready = result
            .as_any()
            .downcast_ref::<NetworkBroadcastExec>()
            .unwrap();

        // Should not be double wrapped
        if let NetworkBroadcastExec::Ready(r) = ready {
            let plan = r.input_stage.plan.decoded().unwrap();
            if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
                assert!(
                    coalesce.children()[0]
                        .as_any()
                        .downcast_ref::<CoalescePartitionsExec>()
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn test_consumer_task_count_propagation() {
        let input = create_test_plan(1);
        let broadcast = NetworkBroadcastExec::try_new(input, 1).unwrap();
        let stage = Stage::new(Uuid::new_v4(), 1, create_test_plan(1), 1, None);
        let result = broadcast.with_input_stage(stage, 8).unwrap();

        let ready = result
            .as_any()
            .downcast_ref::<NetworkBroadcastExec>()
            .unwrap();

        if let NetworkBroadcastExec::Ready(r) = ready {
            assert_eq!(r.input_stage.consumer_task_count, Some(8));
        } else {
            panic!("Expected Ready state");
        }
    }
}
