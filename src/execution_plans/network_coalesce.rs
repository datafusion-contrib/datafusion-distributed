use crate::channel_resolver_ext::get_distributed_channel_resolver;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::distributed_planner::{InputStageInfo, NetworkBoundary, limit_tasks_err};
use crate::execution_plans::common::{
    manually_propagate_distributed_config, require_one_child, scale_partitioning_props,
};
use crate::flight_service::DoGet;
use crate::metrics::MetricsCollectingStream;
use crate::metrics::proto::MetricsSetProto;
use crate::protobuf::{StageKey, map_flight_to_datafusion_error, map_status_to_datafusion_error};
use crate::stage::{MaybeEncodedPlan, Stage};
use crate::{ChannelResolver, DistributedConfig, DistributedTaskContext};
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use bytes::Bytes;
use dashmap::DashMap;
use datafusion::common::{exec_err, internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use http::Extensions;
use prost::Message;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tonic::Request;
use tonic::metadata::MetadataMap;

/// [ExecutionPlan] that coalesces partitions from multiple tasks into a single task without
/// performing any repartition, and maintaining the same partitioning scheme.
///
/// This is the equivalent of a [CoalescePartitionsExec] but coalescing tasks across the network
/// into one.
///
/// ```text
///                                ┌───────────────────────────┐                                   ■
///                                │    NetworkCoalesceExec    │                                   │
///                                │         (task 1)          │                                   │
///                                └┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┘                                Stage N+1
///                                 │1││2││3││4││5││6││7││8││9│                                    │
///                                 └─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘                                    │
///                                 ▲  ▲  ▲   ▲  ▲  ▲   ▲  ▲  ▲                                    ■
///   ┌──┬──┬───────────────────────┴──┴──┘   │  │  │   └──┴──┴──────────────────────┬──┬──┐
///   │  │  │                                 │  │  │                                │  │  │       ■
///  ┌─┐┌─┐┌─┐                               ┌─┐┌─┐┌─┐                              ┌─┐┌─┐┌─┐      │
///  │1││2││3│                               │4││5││6│                              │7││8││9│      │
/// ┌┴─┴┴─┴┴─┴──────────────────┐  ┌─────────┴─┴┴─┴┴─┴─────────┐ ┌──────────────────┴─┴┴─┴┴─┴┐  Stage N
/// │  Arc<dyn ExecutionPlan>   │  │  Arc<dyn ExecutionPlan>   │ │  Arc<dyn ExecutionPlan>   │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// The communication between two stages across a [NetworkCoalesceExec] has two implications:
///
/// - Stage N+1 must have exactly 1 task. The distributed planner ensures this is true.
/// - The amount of partitions in the single task of Stage N+1 is equal to the sum of all
///   partitions in all tasks in Stage N+1 (e.g. (1,2,3,4,5,6,7,8,9) = (1,2,3)+(4,5,6)+(7,8,9) )
///
/// This node has two variants.
/// 1. Pending: it acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///    using Arrow Flight.
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
    input: Arc<dyn ExecutionPlan>,
}

/// Ready version of the [NetworkCoalesceExec] node. This node can be created in
/// just two ways:
/// - by the distributed optimization step based on an original [NetworkCoalescePending]
/// - deserialized from a protobuf plan sent over the network.
#[derive(Debug, Clone)]
pub struct NetworkCoalesceReady {
    /// the properties we advertise for this execution plan
    pub(crate) properties: PlanProperties,
    pub(crate) input_stage: Stage,
    /// metrics_collection is used to collect metrics from child tasks. It is empty when an
    /// is instantiated (deserialized, created via [NetworkCoalesceExec::new_ready] etc...).
    /// Metrics are populated in this map via [NetworkCoalesceExec::execute].
    ///
    /// An instance may receive metrics for 0 to N child tasks, where N is the number of tasks in
    /// the stage it is reading from. This is because, by convention, the ArrowFlightEndpoint
    /// sends metrics for a task to the last NetworkCoalesceExec to read from it, which may or may
    /// not be this instance.
    pub(crate) metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
}

impl NetworkCoalesceExec {
    /// Builds a new [NetworkCoalesceExec] in "Pending" state.
    ///
    /// Typically, this node should be place right after nodes that coalesce all the input
    /// partitions into one, for example:
    /// - [CoalescePartitionsExec]
    /// - [SortPreservingMergeExec]
    pub fn new(input: Arc<dyn ExecutionPlan>, input_tasks: usize) -> Self {
        Self::Pending(NetworkCoalescePending {
            properties: input.properties().clone(),
            input_tasks,
            input,
        })
    }
}

impl NetworkBoundary for NetworkCoalesceExec {
    fn get_input_stage_info(&self, n_tasks: usize) -> Result<InputStageInfo, DataFusionError> {
        let Self::Pending(pending) = self else {
            return plan_err!("can only return wrapped child if on Pending state");
        };

        // As this node coalesces multiple tasks into 1, it must run in a stage with 1 task.
        if n_tasks > 1 {
            return Err(limit_tasks_err(1));
        }

        Ok(InputStageInfo {
            plan: Arc::clone(&pending.input),
            task_count: pending.input_tasks,
        })
    }

    fn with_input_stage(
        &self,
        input_stage: Stage,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match self {
            Self::Pending(pending) => {
                let properties = input_stage.plan.decoded()?.properties();
                let ready = NetworkCoalesceReady {
                    properties: scale_partitioning_props(properties, |p| p * pending.input_tasks),
                    input_stage,
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

    fn with_input_task_count(
        &self,
        input_tasks: usize,
    ) -> Result<Arc<dyn NetworkBoundary>, DataFusionError> {
        Ok(Arc::new(match self {
            Self::Pending(pending) => Self::Pending(NetworkCoalescePending {
                properties: pending.properties.clone(),
                input_tasks,
                input: pending.input.clone(),
            }),
            Self::Ready(_) => {
                plan_err!("Self can only re-assign input tasks if in 'Pending' state")?
            }
        }))
    }

    fn input_task_count(&self) -> usize {
        match self {
            Self::Pending(v) => v.input_tasks,
            Self::Ready(v) => v.input_stage.tasks.len(),
        }
    }
}

impl DisplayAs for NetworkCoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let Self::Ready(self_ready) = self else {
            return write!(f, "NetworkCoalesceExec");
        };

        let input_tasks = self_ready.input_stage.tasks.len();
        let partitions = self_ready.properties.partitioning.partition_count();
        let stage = self_ready.input_stage.num;
        write!(
            f,
            "[Stage {stage}] => NetworkCoalesceExec: output_partitions={partitions}, input_tasks={input_tasks}",
        )
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
            NetworkCoalesceExec::Pending(v) => vec![&v.input],
            NetworkCoalesceExec::Ready(v) => match &v.input_stage.plan {
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

        let d_cfg = DistributedConfig::from_config_options(context.session_config().options())?;
        let retrieve_metrics = d_cfg.collect_metrics;

        let input_stage = &self_ready.input_stage;
        let encoded_input_plan = input_stage.plan.encoded()?;

        let context_headers = ContextGrpcMetadata::headers_from_ctx(&context);
        let task_context = DistributedTaskContext::from_ctx(&context);
        if task_context.task_index > 0 {
            return exec_err!("NetworkCoalesceExec cannot be executed in more than one task");
        }

        let partitions_per_task =
            self.properties().partitioning.partition_count() / input_stage.tasks.len();

        let target_task = partition / partitions_per_task;
        let target_partition = partition % partitions_per_task;

        // TODO: this propagation should be automatic <link to issue>
        let context_headers = manually_propagate_distributed_config(context_headers, d_cfg);
        let ticket = Request::from_parts(
            MetadataMap::from_headers(context_headers),
            Extensions::default(),
            Ticket {
                ticket: DoGet {
                    plan_proto: encoded_input_plan.clone(),
                    target_partition: target_partition as u64,
                    stage_key: Some(StageKey::new(
                        Bytes::from(input_stage.query_id.as_bytes().to_vec()),
                        input_stage.num as u64,
                        target_task as u64,
                    )),
                    target_task_index: target_task as u64,
                    target_task_count: input_stage.tasks.len() as u64,
                }
                .encode_to_vec()
                .into(),
            },
        );

        let Some(task) = input_stage.tasks.get(target_task) else {
            return internal_err!("ProgrammingError: Task {target_task} not found");
        };

        let Some(url) = task.url.clone() else {
            return internal_err!("NetworkCoalesceExec: task is unassigned, cannot proceed");
        };

        let metrics_collection_capture = self_ready.metrics_collection.clone();
        let stream = async move {
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
        .try_flatten_stream();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
