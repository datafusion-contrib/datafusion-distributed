use crate::ChannelResolver;
use crate::channel_resolver_ext::get_distributed_channel_resolver;
use crate::common::scale_partitioning;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::distributed_physical_optimizer_rule::NetworkBoundary;
use crate::execution_plans::{DistributedTaskContext, StageExec};
use crate::flight_service::DoGet;
use crate::metrics::MetricsCollectingStream;
use crate::metrics::proto::MetricsSetProto;
use crate::protobuf::{DistributedCodec, StageKey, proto_from_input_stage};
use crate::protobuf::{map_flight_to_datafusion_error, map_status_to_datafusion_error};
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use dashmap::DashMap;
use datafusion::common::{exec_err, internal_datafusion_err, internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::repartition::RepartitionExec;
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

/// [ExecutionPlan] implementation that shuffles data across the network in a distributed context.
///
/// The easiest way of thinking about this node is as a plan [RepartitionExec] node that is
/// capable of fanning out the different produced partitions to different tasks.
/// This allows redistributing data across different tasks in different stages, that way different
/// physical machines can make progress on different non-overlapping sets of data.
///
/// This node allows fanning out data from N tasks to M tasks, being N and M arbitrary non-zero
/// positive numbers. Here are some examples of how data can be shuffled in different scenarios:
///
/// # 1 to many
///
/// ```text
/// ┌───────────────────────────┐  ┌───────────────────────────┐ ┌───────────────────────────┐     ■
/// │    NetworkShuffleExec     │  │    NetworkShuffleExec     │ │    NetworkShuffleExec     │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └┬─┬┬─┬┬─┬──────────────────┘  └─────────┬─┬┬─┬┬─┬─────────┘ └──────────────────┬─┬┬─┬┬─┬┘  Stage N+1
///  │1││2││3│                               │4││5││6│                              │7││8││9│      │
///  └─┘└─┘└─┘                               └─┘└─┘└─┘                              └─┘└─┘└─┘      │
///   ▲  ▲  ▲                                 ▲  ▲  ▲                                ▲  ▲  ▲       ■
///   └──┴──┴────────────────────────┬──┬──┐  │  │  │  ┌──┬──┬───────────────────────┴──┴──┘
///                                  │  │  │  │  │  │  │  │  │                                     ■
///                                 ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐                                    │
///                                 │1││2││3││4││5││6││7││8││9│                                    │
///                                ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐                                Stage N
///                                │      RepartitionExec      │                                   │
///                                │         (task 1)          │                                   │
///                                └───────────────────────────┘                                   ■
/// ```
///
/// # many to 1
///
/// ```text
///                                ┌───────────────────────────┐                                   ■
///                                │    NetworkShuffleExec     │                                   │
///                                │         (task 1)          │                                   │
///                                └┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┘                                Stage N+1
///                                 │1││2││3││4││5││6││7││8││9│                                    │
///                                 └─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘                                    │
///                                 ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲                                    ■
///   ┌──┬──┬──┬──┬──┬──┬──┬──┬─────┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴┴┼┴────┬──┬──┬──┬──┬──┬──┬──┬──┐
///   │  │  │  │  │  │  │  │  │      │  │  │  │  │  │  │  │  │     │  │  │  │  │  │  │  │  │       ■
///  ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐    ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐   ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐      │
///  │1││2││3││4││5││6││7││8││9│    │1││2││3││4││5││6││7││8││9│   │1││2││3││4││5││6││7││8││9│      │
/// ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐  ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐ ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐  Stage N
/// │      RepartitionExec      │  │      RepartitionExec      │ │      RepartitionExec      │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// # many to many
///
/// ```text
///                    ┌───────────────────────────┐  ┌───────────────────────────┐                ■
///                    │    NetworkShuffleExec     │  │    NetworkShuffleExec     │                │
///                    │         (task 1)          │  │         (task 2)          │                │
///                    └┬─┬┬─┬┬─┬┬─┬───────────────┘  └───────────────┬─┬┬─┬┬─┬┬─┬┘             Stage N+1
///                     │1││2││3││4│                                  │5││6││7││8│                 │
///                     └─┘└─┘└─┘└─┘                                  └─┘└─┘└─┘└─┘                 │
///                     ▲▲▲▲▲▲▲▲▲▲▲▲                                  ▲▲▲▲▲▲▲▲▲▲▲▲                 ■
///     ┌──┬──┬──┬──┬──┬┴┴┼┴┴┼┴┴┴┴┴┴───┬──┬──┬──┬──┬──┬──┬──┬────────┬┴┴┼┴┴┼┴┴┼┴┴┼──┬──┬──┐
///     │  │  │  │  │  │  │  │         │  │  │  │  │  │  │  │        │  │  │  │  │  │  │  │        ■
///    ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐       ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐      ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐       │
///    │1││2││3││4││5││6││7││8│       │1││2││3││4││5││6││7││8│      │1││2││3││4││5││6││7││8│       │
/// ┌──┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴─┐  ┌──┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴─┐ ┌──┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴─┐  Stage N
/// │      RepartitionExec      │  │      RepartitionExec      │ │      RepartitionExec      │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// The communication between two stages across a [NetworkShuffleExec] has two implications:
///
/// - Each task in Stage N+1 gather data from all tasks in Stage N
/// - The sum of the number of partitions in all tasks in Stage N+1 is equal to the
///   number of partitions in a single task in Stage N. (e.g. (1,2,3,4)+(5,6,7,8) = (1,2,3,4,5,6,7,8) )
///
/// This node has two variants.
/// 1. Pending: it acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///    using Arrow Flight.
#[derive(Debug, Clone)]
pub enum NetworkShuffleExec {
    Pending(NetworkShufflePendingExec),
    Ready(NetworkShuffleReadyExec),
}

/// Placeholder version of the [NetworkShuffleExec] node. It acts as a marker for the
/// distributed optimization step, which will replace it with the appropriate
/// [NetworkShuffleReadyExec] node.
#[derive(Debug, Clone)]
pub struct NetworkShufflePendingExec {
    repartition_exec: Arc<dyn ExecutionPlan>,
    input_tasks: usize,
}

/// Ready version of the [NetworkShuffleExec] node. This node can be created in
/// just two ways:
/// - by the distributed optimization step based on an original [NetworkShufflePendingExec]
/// - deserialized from a protobuf plan sent over the network.
#[derive(Debug, Clone)]
pub struct NetworkShuffleReadyExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: PlanProperties,
    pub(crate) stage_num: usize,
    /// metrics_collection is used to collect metrics from child tasks. It is empty when an
    /// is instantiated (deserialized, created via [NetworkShuffleExec::new_ready] etc...).
    /// Metrics are populated in this map via [NetworkShuffleExec::execute].
    ///
    /// An instance may receive metrics for 0 to N child tasks, where N is the number of tasks in
    /// the stage it is reading from. This is because, by convention, the ArrowFlightEndpoint
    /// sends metrics for a task to the last NetworkShuffleExec to read from it, which may or may
    /// not be this instance.
    pub(crate) metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
}

impl NetworkShuffleExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        input_tasks: usize,
    ) -> Result<Self, DataFusionError> {
        Ok(Self::Pending(NetworkShufflePendingExec {
            repartition_exec: Arc::new(RepartitionExec::try_new(input, partitioning)?),
            input_tasks,
        }))
    }

    pub fn from_repartition_exec(
        r_exe: &Arc<dyn ExecutionPlan>,
        input_tasks: usize,
    ) -> Result<Self, DataFusionError> {
        if !r_exe.as_any().is::<RepartitionExec>() {
            return plan_err!("Expected RepartitionExec");
        };

        Ok(Self::Pending(NetworkShufflePendingExec {
            repartition_exec: Arc::clone(r_exe),
            input_tasks,
        }))
    }
}

impl NetworkBoundary for NetworkShuffleExec {
    fn to_stage_info(
        &self,
        n_tasks: usize,
    ) -> Result<(Arc<dyn ExecutionPlan>, usize), DataFusionError> {
        let Self::Pending(pending) = self else {
            return plan_err!("cannot only return wrapped child if on Pending state");
        };

        let children = pending.repartition_exec.children();
        let Some(child) = children.first() else {
            return plan_err!("RepartitionExec must have a child");
        };

        let next_stage_plan = Arc::new(RepartitionExec::try_new(
            Arc::clone(child),
            scale_partitioning(pending.repartition_exec.output_partitioning(), |p| {
                p * n_tasks
            }),
        )?);

        Ok((next_stage_plan, pending.input_tasks))
    }

    fn with_input_tasks(&self, input_tasks: usize) -> Arc<dyn NetworkBoundary> {
        Arc::new(match self {
            NetworkShuffleExec::Pending(prev) => {
                NetworkShuffleExec::Pending(NetworkShufflePendingExec {
                    repartition_exec: Arc::clone(&prev.repartition_exec),
                    input_tasks,
                })
            }
            NetworkShuffleExec::Ready(prev) => NetworkShuffleExec::Ready(NetworkShuffleReadyExec {
                properties: prev.properties.clone(),
                stage_num: prev.stage_num,
                metrics_collection: Arc::clone(&prev.metrics_collection),
            }),
        })
    }

    fn to_distributed(
        &self,
        stage_num: usize,
        _stage_head: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let NetworkShuffleExec::Pending(pending) = self else {
            return internal_err!("NetworkShuffleExec is already distributed");
        };

        let ready = NetworkShuffleReadyExec {
            properties: pending.repartition_exec.properties().clone(),
            stage_num,
            metrics_collection: Default::default(),
        };

        Ok(Arc::new(Self::Ready(ready)))
    }
}

impl DisplayAs for NetworkShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NetworkShuffleExec")
    }
}

impl ExecutionPlan for NetworkShuffleExec {
    fn name(&self) -> &str {
        "NetworkShuffleExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        match self {
            NetworkShuffleExec::Pending(v) => v.repartition_exec.properties(),
            NetworkShuffleExec::Ready(v) => &v.properties,
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match self {
            NetworkShuffleExec::Pending(v) => vec![&v.repartition_exec],
            NetworkShuffleExec::Ready(_) => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !children.is_empty() {
            return plan_err!(
                "NetworkShuffleExec: wrong number of children, expected 0, got {}",
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
        let NetworkShuffleExec::Ready(self_ready) = self else {
            return exec_err!(
                "NetworkShuffleExec is not ready, was the distributed optimization step performed?"
            );
        };

        // get the channel manager and current stage from our context
        let channel_resolver = get_distributed_channel_resolver(context.session_config())?;

        // the `NetworkShuffleExec` node can only be executed in the context of a `StageExec`
        let stage = StageExec::from_ctx(&context)?;

        // of our child stages find the one that matches the one we are supposed to be
        // reading from
        let input_stage = stage.input_stage(self_ready.stage_num)?;

        let codec = DistributedCodec::new_combined_with_user(context.session_config());
        let input_stage_proto = proto_from_input_stage(input_stage, &codec).map_err(|e| {
            internal_datafusion_err!("NetworkShuffleExec: failed to convert stage to proto: {e}")
        })?;

        let input_stage_tasks = input_stage.tasks().to_vec();
        let input_stage_num = input_stage.num() as u64;
        let query_id = stage.query_id.to_string();

        let context_headers = ContextGrpcMetadata::headers_from_ctx(&context);
        let task_context = DistributedTaskContext::from_ctx(&context);
        let off = self_ready.properties.partitioning.partition_count() * task_context.task_index;

        let stream = input_stage_tasks.into_iter().enumerate().map(|(i, task)| {
            let channel_resolver = Arc::clone(&channel_resolver);

            let ticket = Request::from_parts(
                MetadataMap::from_headers(context_headers.clone()),
                Extensions::default(),
                Ticket {
                    ticket: DoGet {
                        stage_proto: input_stage_proto.clone(),
                        target_partition: (off + partition) as u64,
                        stage_key: Some(StageKey {
                            query_id: query_id.clone(),
                            stage_id: input_stage_num,
                            task_number: i as u64,
                        }),
                        target_task_index: i as u64,
                    }
                    .encode_to_vec()
                    .into(),
                },
            );

            let metrics_collection_capture = self_ready.metrics_collection.clone();
            async move {
                let url = task.url.ok_or(internal_datafusion_err!(
                    "NetworkShuffleExec: task is unassigned, cannot proceed"
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
