use crate::ChannelResolver;
use crate::channel_resolver_ext::get_distributed_channel_resolver;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::distributed_planner::{InputStageInfo, NetworkBoundary};
use crate::execution_plans::common::require_one_child;
use crate::flight_service::DoGet;
use crate::metrics::MetricsCollectingStream;
use crate::metrics::proto::MetricsSetProto;
use crate::protobuf::{StageKey, map_flight_to_datafusion_error, map_status_to_datafusion_error};
use crate::stage::{MaybeEncodedPlan, Stage};
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use bytes::Bytes;
use dashmap::DashMap;
use datafusion::common::{exec_err, internal_datafusion_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use http::Extensions;
use prost::Message;
use std::any::Any;
use std::sync::Arc;
use tonic::Request;
use tonic::metadata::MetadataMap;

/// [ExecutionPlan] that broadcasts data from a single task to multiple tasks across the network.
///
/// This operator is used when a small dataset needs to be replicated to all workers in the next
/// stage. The most common use case is hash joins with `CollectLeft` partition mode, where the
/// small build side (left table) is collected into a single partition and then broadcast to all
/// workers processing the large probe side (right table).
///
/// Unlike [NetworkShuffleExec] which redistributes data across tasks, [NetworkBroadcastExec]
/// replicates the entire input to each task in the next stage. This allows parallel execution
/// of operations that would otherwise be forced to run single-threaded.
///
/// 1 to many (broadcast)
///
/// ┌───────────────────────────┐  ┌───────────────────────────┐ ┌───────────────────────────┐     ■
/// │   NetworkBroadcastExec    │  │   NetworkBroadcastExec    │ │   NetworkBroadcastExec    │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// │      (full copy)          │  │      (full copy)          │ │      (full copy)          │  Stage N+1
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     │
///             ▲                              ▲                              ▲                    │
///             │                              │                              │                    ■
///             └──────────────────────────────┴──────────────────────────────┘
///                                            │                                                   ■
///                                ┌───────────────────────────┐                                   │
///                                │      CoalesceExec or      │                                   │
///                                │    HashJoinExec build     │                                Stage N
///                                │         (task 1)          │                                   │
///                                └───────────────────────────┘                                   │
///                                                                                                ■
///
/// Broadcast join example (CollectLeft hash join)
///
/// Stage N+1: Hash Join (3 tasks running in parallel)
/// ┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
/// │   HashJoinExec t1    │   │   HashJoinExec t2    │   │   HashJoinExec t3    │
/// │  left: small (bcast) │   │  left: small (bcast) │   │  left: small (bcast) │
/// │  right: large (p1)   │   │  right: large (p2)   │   │  right: large (p3)   │
/// └──┬─────────────────┬─┘   └──┬─────────────────┬─┘   └──┬─────────────────┬─┘
///    │                 │        │                 │        │                 │
///    ▼                 │        ▼                 │        ▼                 │
/// NetworkBroadcast     │     NetworkBroadcast     │     NetworkBroadcast     │
/// (full copy)          │     (full copy)          │     (full copy)          │
///    │                 │        │                 │        │                 │
///    │                 │        │                 │        │                 │
///    │                 ▼        │                 ▼        │                 ▼
///    │            Large table   │            Large table   │            Large table
///    │            partition 1   │            partition 2   │            partition 3
///    │                 │        │                 │        │                 │
///    └─────────────────┼────────┴─────────────────┼────────┴─────────────────┘
///                      │                          │
/// Stage N: Small table collected + Large table partitioned
///                      │                          │
///              ┌───────▼──────┐          ┌────────▼────────┐
///              │ Small table  │          │  Large table    │
///              │  (1 task,    │          │  (3 partitions) │
///              │  collected)  │          └─────────────────┘
///              └──────────────┘
///
/// The communication between two stages across a [NetworkBroadcastExec] has these characteristics:
///
/// - The input stage typically has 1 task containing the collected/small dataset
/// - Each task in Stage N+1 receives a complete copy of the data from Stage N
/// - This enables parallel execution while ensuring all tasks have access to the full dataset
/// - Commonly used for broadcast joins where the build side is small enough to replicate
///
/// This node has two variants:
/// 1. Pending: acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the input stage over the network
///    using Arrow Flight, broadcasting the data to all tasks.
///
/// [NetworkShuffleExec]: crate::execution_plans::NetworkShuffleExec
#[derive(Debug, Clone)]
pub enum NetworkBroadcastExec {
    Pending(NetworkBroadcastPending),
    Ready(NetworkBroadcastReady),
}

/// Placeholder version of the [NetworkBroadcastExec] node. It acts as a marker for the
/// distributed optimization step, which will replace it with the appropriate
/// [NetworkBroadcastReady] node.
#[derive(Debug, Clone)]
pub struct NetworkBroadcastPending {
    properties: PlanProperties,
    input_tasks: usize,
    input: Arc<dyn ExecutionPlan>,
}

/// Ready version of the [NetworkBroadcastExec] node. This node is created by:
/// - the distributed optimization step based on an original [NetworkBroadcastPending]
/// - deserialized from a protobuf plan sent over the network.
///
/// This variant contains the input [Stage] information and executes by broadcasting
/// data from the input stage to all tasks in the current stage over Arrow Flight.
#[derive(Debug, Clone)]
pub struct NetworkBroadcastReady {
    pub(crate) properties: PlanProperties,
    pub(crate) input_stage: Stage,
    pub(crate) metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
}

impl NetworkBroadcastExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, input_tasks: usize) -> Self {
        Self::Pending(NetworkBroadcastPending {
            properties: input.properties().clone(),
            input_tasks,
            input,
        })
    }
}

impl NetworkBoundary for NetworkBroadcastExec {
    fn get_input_stage_info(
        &self,
        _n_tasks: usize,
    ) -> datafusion::common::Result<InputStageInfo, DataFusionError> {
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
    ) -> datafusion::common::Result<Arc<dyn NetworkBoundary>> {
        match self {
            Self::Pending(pending) => Ok(Arc::new(Self::Pending(NetworkBroadcastPending {
                properties: pending.properties.clone(),
                input_tasks,
                input: pending.input.clone(),
            }))),
            Self::Ready(_) => {
                plan_err!("Self can only re-assign input tasks if in 'Pending' state")
            }
        }
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
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match self {
            Self::Pending(pending) => {
                let ready = NetworkBroadcastReady {
                    properties: pending.properties.clone(),
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
}

impl DisplayAs for NetworkBroadcastExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NetworkBroadcastExec::Pending(_) => {
                write!(f, "NetworkBroadcastExec: [Pending]")
            }
            NetworkBroadcastExec::Ready(ready) => {
                write!(
                    f,
                    "NetworkBroadcastExec: [Stage {}] ({} tasks)",
                    ready.input_stage.num,
                    ready.input_stage.tasks.len()
                )
            }
        }
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
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let NetworkBroadcastExec::Ready(self_ready) = self else {
            return exec_err!(
                "NetworkBroadcastExec is not ready, was the distributed optimization step performed?"
            );
        };

        let channel_resolver = get_distributed_channel_resolver(context.session_config())?;
        let input_stage = &self_ready.input_stage;
        let encoded_input_plan = input_stage.plan.encoded()?;
        let input_stage_tasks = input_stage.tasks.to_vec();
        let input_task_count = input_stage_tasks.len();
        let input_stage_num = input_stage.num as u64;
        let query_id = Bytes::from(input_stage.query_id.as_bytes().to_vec());
        let context_headers = ContextGrpcMetadata::headers_from_ctx(&context);

        let stream = input_stage_tasks.into_iter().enumerate().map(|(i, task)| {
            let channel_resolver = Arc::clone(&channel_resolver);
            let ticket = Request::from_parts(
                MetadataMap::from_headers(context_headers.clone()),
                Extensions::default(),
                Ticket {
                    ticket: DoGet {
                        plan_proto: encoded_input_plan.clone(),
                        target_partition: partition as u64,
                        stage_key: Some(StageKey::new(query_id.clone(), input_stage_num, i as u64)),
                        target_task_index: i as u64,
                        target_task_count: input_task_count as u64,
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
                let metrics_collecting_stream =
                    MetricsCollectingStream::new(stream, metrics_collection_capture);
                Ok(
                    FlightRecordBatchStream::new_from_flight_data(metrics_collecting_stream)
                        .map_err(map_flight_to_datafusion_error),
                )
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
