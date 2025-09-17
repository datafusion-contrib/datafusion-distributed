use crate::channel_resolver_ext::get_distributed_channel_resolver;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::distributed_physical_optimizer_rule::DistributedExecutionPlan;
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
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
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
pub enum NetworkHashShuffleExec {
    Pending(NetworkHashShufflePendingExec),
    Ready(NetworkHashShuffleReadyExec),
}

/// Placeholder version of the [NetworkHashShuffleExec] node. It acts as a marker for the
/// distributed optimization step, which will replace it with the appropriate
/// [NetworkHashShuffleReadyExec] node.
#[derive(Debug, Clone)]
pub struct NetworkHashShufflePendingExec {
    properties: PlanProperties,
    hash: Vec<Arc<dyn PhysicalExpr>>,
    input_tasks: usize,
    child: Arc<dyn ExecutionPlan>,
}

/// Ready version of the [NetworkHashShuffleExec] node. This node can be created in
/// just two ways:
/// - by the distributed optimization step based on an original [NetworkHashShufflePendingExec]
/// - deserialized from a protobuf plan sent over the network.
#[derive(Debug, Clone)]
pub struct NetworkHashShuffleReadyExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: PlanProperties,
    pub(crate) stage_num: usize,
}

impl NetworkHashShuffleExec {
    pub fn from_repartition_exec(
        input: &RepartitionExec,
        input_tasks: usize,
    ) -> Result<Self, DataFusionError> {
        let children = input.children();
        let Some(child) = children.first() else {
            return internal_err!("Expected RepartitionExec to have a child");
        };
        let Partitioning::Hash(hash, _) = input.partitioning() else {
            return plan_err!("NetworkHashShuffleExec can only take a RepartitionExec with hash partitioning as input");
        };

        Ok(Self::Pending(NetworkHashShufflePendingExec {
            properties: input.properties().clone(),
            input_tasks,
            hash: hash.clone(),
            child: Arc::clone(child),
        }))
    }
}

impl DistributedExecutionPlan for NetworkHashShuffleExec {
    fn to_stage_info(
        &self,
        n_tasks: usize,
    ) -> Result<(Arc<dyn ExecutionPlan>, usize), DataFusionError> {
        let Self::Pending(ref pending) = self else {
            return plan_err!("cannot only return wrapped child if on Pending state");
        };

        let next_stage_plan = Arc::new(RepartitionExec::try_new(
            pending.child.clone(),
            Partitioning::Hash(
                pending.hash.clone(),
                pending.properties.partitioning.partition_count() * n_tasks,
            ),
        )?);

        Ok((next_stage_plan, pending.input_tasks))
    }

    fn with_input_tasks(&self, input_tasks: usize) -> Arc<dyn DistributedExecutionPlan> {
        Arc::new(match self {
            NetworkHashShuffleExec::Pending(prev) => {
                NetworkHashShuffleExec::Pending(NetworkHashShufflePendingExec {
                    properties: prev.properties.clone(),
                    hash: prev.hash.clone(),
                    input_tasks,
                    child: Arc::clone(&prev.child),
                })
            }
            NetworkHashShuffleExec::Ready(prev) => {
                NetworkHashShuffleExec::Ready(NetworkHashShuffleReadyExec {
                    properties: prev.properties.clone(),
                    stage_num: prev.stage_num,
                })
            }
        })
    }

    fn to_distributed(
        &self,
        stage_num: usize,
        _stage_head: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let NetworkHashShuffleExec::Pending(pending) = self else {
            return internal_err!("NetworkHashShuffleExec is already distributed");
        };

        let ready = NetworkHashShuffleReadyExec {
            properties: pending.properties.clone(),
            stage_num,
        };

        Ok(Arc::new(Self::Ready(ready)))
    }
}

impl DisplayAs for NetworkHashShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NetworkHashShuffleExec")
    }
}

impl ExecutionPlan for NetworkHashShuffleExec {
    fn name(&self) -> &str {
        "NetworkHashShuffleExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        match self {
            NetworkHashShuffleExec::Pending(v) => &v.properties,
            NetworkHashShuffleExec::Ready(v) => &v.properties,
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match self {
            NetworkHashShuffleExec::Pending(v) => vec![&v.child],
            NetworkHashShuffleExec::Ready(_) => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !children.is_empty() {
            return plan_err!(
                "NetworkHashShuffleExec: wrong number of children, expected 0, got {}",
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
        let NetworkHashShuffleExec::Ready(self_ready) = self else {
            return exec_err!("NetworkHashShuffleExec is not ready, was the distributed optimization step performed?");
        };

        // get the channel manager and current stage from our context
        let channel_resolver = get_distributed_channel_resolver(context.session_config())?;

        // the `NetworkHashShuffleExec` node can only be executed in the context of a `StageExec`
        let stage = StageExec::from_ctx(&context)?;

        // of our child stages find the one that matches the one we are supposed to be
        // reading from
        let child_stage = stage.child_stage(self_ready.stage_num)?;

        let codec = DistributedCodec::new_combined_with_user(context.session_config());
        let child_stage_proto = proto_from_stage(child_stage, &codec).map_err(|e| {
            internal_datafusion_err!(
                "NetworkHashShuffleExec: failed to convert stage to proto: {e}"
            )
        })?;

        let child_stage_tasks = child_stage.tasks.clone();
        let child_stage_num = child_stage.num as u64;
        let query_id = stage.query_id.to_string();

        let context_headers = ContextGrpcMetadata::headers_from_ctx(&context);
        let task_context = DistributedTaskContext::from_ctx(&context);
        let off = self_ready.properties.partitioning.partition_count() * task_context.task_index;

        let stream = child_stage_tasks.into_iter().enumerate().map(|(i, task)| {
            let channel_resolver = Arc::clone(&channel_resolver);

            let ticket = Request::from_parts(
                MetadataMap::from_headers(context_headers.clone()),
                Extensions::default(),
                Ticket {
                    ticket: DoGet {
                        stage_proto: Some(child_stage_proto.clone()),
                        target_partition: (off + partition) as u64,
                        stage_key: Some(StageKey {
                            query_id: query_id.clone(),
                            stage_id: child_stage_num,
                            task_number: i as u64,
                        }),
                        target_task_index: i as u64,
                    }
                    .encode_to_vec()
                    .into(),
                },
            );

            async move {
                let url = task.url.ok_or(internal_datafusion_err!(
                    "NetworkHashShuffleExec: task is unassigned, cannot proceed"
                ))?;

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
            .try_flatten_stream()
            .boxed()
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::select_all(stream),
        )))
    }
}
