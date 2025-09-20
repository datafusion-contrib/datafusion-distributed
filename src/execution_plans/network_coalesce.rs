use crate::channel_resolver_ext::get_distributed_channel_resolver;
use crate::common::scale_partitioning_props;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::distributed_physical_optimizer_rule::{limit_tasks_err, DistributedExecutionPlan};
use crate::errors::{map_flight_to_datafusion_error, map_status_to_datafusion_error};
use crate::execution_plans::{DistributedTaskContext, StageExec};
use crate::flight_service::{DoGet, StageKey};
use crate::protobuf::{proto_from_stage, DistributedCodec};
use crate::ChannelResolver;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use datafusion::common::{exec_err, internal_datafusion_err, internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{TryFutureExt, TryStreamExt};
use http::Extensions;
use prost::Message;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::Request;

/// This node has two variants.
/// 1. Pending: it acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///     using Arrow Flight.
#[derive(Debug, Clone)]
pub enum NetworkCoalesceExec {
    Pending(NetworkCoalescePending),
    Ready(NetworkCoalesceReady),
}

/// Placeholder version of the [NetworkCoalesceExec] node. It acts as a marker for the
/// distributed optimization step, which will replace it with the appropriate
/// [NetworkCoalesceReady] node.
#[derive(Debug, Clone)]
pub struct NetworkCoalescePending {
    properties: PlanProperties,
    input_tasks: usize,
    child: Arc<dyn ExecutionPlan>,
}

/// Ready version of the [NetworkCoalesceExec] node. This node can be created in
/// just two ways:
/// - by the distributed optimization step based on an original [NetworkCoalescePending]
/// - deserialized from a protobuf plan sent over the network.
#[derive(Debug, Clone)]
pub struct NetworkCoalesceReady {
    /// the properties we advertise for this execution plan
    pub(crate) properties: PlanProperties,
    pub(crate) stage_num: usize,
    pub(crate) input_tasks: usize,
}

impl NetworkCoalesceExec {
    pub fn from_coalesce_partitions_exec(
        input: &CoalescePartitionsExec,
        input_tasks: usize,
    ) -> Result<Self, DataFusionError> {
        Self::from_input(input, input_tasks)
    }

    pub fn from_sort_preserving_merge_exec(
        input: &SortPreservingMergeExec,
        input_tasks: usize,
    ) -> Result<Self, DataFusionError> {
        Self::from_input(input, input_tasks)
    }

    pub fn from_input(
        input: &dyn ExecutionPlan,
        input_tasks: usize,
    ) -> Result<Self, DataFusionError> {
        let children = input.children();
        let Some(child) = children.first() else {
            return internal_err!("Expected a single child");
        };

        Ok(Self::Pending(NetworkCoalescePending {
            properties: child.properties().clone(),
            input_tasks,
            child: Arc::clone(child),
        }))
    }
}

impl DistributedExecutionPlan for NetworkCoalesceExec {
    fn to_stage_info(
        &self,
        n_tasks: usize,
    ) -> Result<(Arc<dyn ExecutionPlan>, usize), DataFusionError> {
        let Self::Pending(ref pending) = self else {
            return plan_err!("can only return wrapped child if on Pending state");
        };

        if n_tasks > 1 {
            return Err(limit_tasks_err(1));
        }

        Ok((Arc::clone(&pending.child), pending.input_tasks))
    }

    fn to_distributed(
        &self,
        stage_num: usize,
        stage_head: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let NetworkCoalesceExec::Pending(pending) = self else {
            return internal_err!("NetworkCoalesceExec is already distributed");
        };

        let ready = NetworkCoalesceReady {
            properties: scale_partitioning_props(stage_head.properties(), |p| {
                p * pending.input_tasks
            }),
            stage_num,
            input_tasks: pending.input_tasks,
        };

        Ok(Arc::new(Self::Ready(ready)))
    }

    fn with_input_tasks(&self, input_tasks: usize) -> Arc<dyn DistributedExecutionPlan> {
        Arc::new(match self {
            NetworkCoalesceExec::Pending(pending) => {
                NetworkCoalesceExec::Pending(NetworkCoalescePending {
                    properties: pending.properties.clone(),
                    input_tasks,
                    child: pending.child.clone(),
                })
            }
            NetworkCoalesceExec::Ready(ready) => NetworkCoalesceExec::Ready(NetworkCoalesceReady {
                properties: scale_partitioning_props(&ready.properties, |p| {
                    p * input_tasks / ready.input_tasks
                }),
                stage_num: ready.stage_num,
                input_tasks,
            }),
        })
    }
}

impl DisplayAs for NetworkCoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NetworkCoalesceExec")
    }
}

impl ExecutionPlan for NetworkCoalesceExec {
    fn name(&self) -> &str {
        "NetworkCoalesceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        match self {
            NetworkCoalesceExec::Pending(v) => &v.properties,
            NetworkCoalesceExec::Ready(v) => &v.properties,
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match self {
            NetworkCoalesceExec::Pending(v) => vec![&v.child],
            NetworkCoalesceExec::Ready(_) => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !children.is_empty() {
            return plan_err!(
                "NetworkCoalesceExec: wrong number of children, expected 0, got {}",
                children.len()
            );
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let NetworkCoalesceExec::Ready(self_ready) = self else {
            return exec_err!(
                "NetworkCoalesceExec is not ready, was the distributed optimization step performed?"
            );
        };

        // get the channel manager and current stage from our context
        let channel_resolver = get_distributed_channel_resolver(context.session_config())?;

        // the `NetworkCoalesceExec` node can only be executed in the context of a `StageExec`
        let stage = StageExec::from_ctx(&context)?;

        // of our child stages find the one that matches the one we are supposed to be
        // reading from
        let child_stage = stage.child_stage(self_ready.stage_num)?;

        let codec = DistributedCodec::new_combined_with_user(context.session_config());
        let child_stage_proto = proto_from_stage(child_stage, &codec).map_err(|e| {
            internal_datafusion_err!("NetworkCoalesceExec: failed to convert stage to proto: {e}")
        })?;

        let context_headers = ContextGrpcMetadata::headers_from_ctx(&context);
        let task_context = DistributedTaskContext::from_ctx(&context);
        if task_context.task_index > 0 {
            return exec_err!("NetworkCoalesceExec cannot be executed in more than one task");
        }

        let partitions_per_task =
            self.properties().partitioning.partition_count() / child_stage.tasks.len();

        let target_task = partition / partitions_per_task;
        let target_partition = partition % partitions_per_task;

        let ticket = Request::from_parts(
            MetadataMap::from_headers(context_headers.clone()),
            Extensions::default(),
            Ticket {
                ticket: DoGet {
                    stage_proto: Some(child_stage_proto.clone()),
                    target_partition: target_partition as u64,
                    stage_key: Some(StageKey {
                        query_id: stage.query_id.to_string(),
                        stage_id: child_stage.num as u64,
                        task_number: target_task as u64,
                    }),
                    target_task_index: target_task as u64,
                }
                .encode_to_vec()
                .into(),
            },
        );

        let Some(task) = child_stage.tasks.get(target_task) else {
            return internal_err!("ProgrammingError: Task {target_task} not found");
        };

        let Some(url) = task.url.clone() else {
            return internal_err!("NetworkCoalesceExec: task is unassigned, cannot proceed");
        };

        let stream = async move {
            let channel = channel_resolver.get_channel_for_url(&url).await?;
            let stream = FlightServiceClient::new(channel)
                .do_get(ticket)
                .await
                .map_err(map_status_to_datafusion_error)?
                .into_inner()
                .map_err(|err| FlightError::Tonic(Box::new(err)));

            Ok(FlightRecordBatchStream::new_from_flight_data(stream)
                .map_err(map_flight_to_datafusion_error))
        }
        .try_flatten_stream();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
