use crate::common::require_one_child;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::execution_plans::common::{
    manually_propagate_distributed_config, scale_partitioning, spawn_select_all,
};
use crate::flight_service::DoGet;
use crate::metrics::MetricsCollectingStream;
use crate::metrics::proto::MetricsSetProto;
use crate::networking::get_distributed_channel_resolver;
use crate::protobuf::StageKey;
use crate::protobuf::{map_flight_to_datafusion_error, map_status_to_datafusion_error};
use crate::stage::{MaybeEncodedPlan, Stage};
use crate::{DistributedConfig, DistributedTaskContext, ExecutionTask, NetworkBoundary};
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use bytes::Bytes;
use dashmap::DashMap;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_datafusion_err, plan_err};
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
use uuid::Uuid;

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
pub struct NetworkShuffleExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: PlanProperties,
    pub(crate) input_stage: Stage,
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
    /// Builds a new [NetworkShuffleExec] in "Pending" state.
    ///
    /// Typically, the `input` to this
    /// node is a [RepartitionExec] with a [Partitioning::Hash] partition scheme.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        num: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Self, DataFusionError> {
        if !matches!(input.output_partitioning(), Partitioning::Hash(_, _)) {
            return plan_err!("NetworkShuffleExec input must be hash partitioned");
        }

        let transformed = Arc::clone(&input).transform_down(|plan| {
            if let Some(r_exe) = plan.as_any().downcast_ref::<RepartitionExec>() {
                // Scale the input RepartitionExec to account for all the tasks to which it will
                // need to fan data out.
                let scaled = Arc::new(RepartitionExec::try_new(
                    require_one_child(r_exe.children())?,
                    scale_partitioning(r_exe.partitioning(), |p| p * task_count),
                )?);
                Ok(Transformed::new(scaled, true, TreeNodeRecursion::Stop))
            } else if matches!(plan.output_partitioning(), Partitioning::Hash(_, _)) {
                // This might be a passthrough node, like a CoalesceBatchesExec or something like that.
                // This is fine, we can let the node be here.
                Ok(Transformed::no(plan))
            } else {
                return plan_err!(
                    "NetworkShuffleExec input must be hash partitioned, but {} is not",
                    plan.name()
                );
            }
        })?;

        Ok(Self {
            input_stage: Stage {
                query_id,
                num,
                plan: MaybeEncodedPlan::Decoded(transformed.data),
                tasks: vec![ExecutionTask { url: None }; input_task_count],
            },
            properties: input.properties().clone(),
            metrics_collection: Default::default(),
        })
    }
}

impl NetworkBoundary for NetworkShuffleExec {
    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }

    fn with_input_stage(
        &self,
        input_stage: Stage,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.clone();
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }
}

impl DisplayAs for NetworkShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_tasks = self.input_stage.tasks.len();
        let partitions = self.properties.partitioning.partition_count();
        let stage = self.input_stage.num;
        write!(
            f,
            "[Stage {stage}] => NetworkShuffleExec: output_partitions={partitions}, input_tasks={input_tasks}",
        )
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
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.input_stage.plan {
            MaybeEncodedPlan::Decoded(v) => vec![v],
            MaybeEncodedPlan::Encoded(_) => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut self_clone = self.as_ref().clone();
        self_clone.input_stage.plan = MaybeEncodedPlan::Decoded(require_one_child(children)?);
        Ok(Arc::new(self_clone))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        // get the channel manager and current stage from our context
        let channel_resolver = get_distributed_channel_resolver(context.as_ref());

        let d_cfg = DistributedConfig::from_config_options(context.session_config().options())?;
        let retrieve_metrics = d_cfg.collect_metrics;

        let input_stage = &self.input_stage;
        let encoded_input_plan = input_stage.plan.encoded()?;

        let input_stage_tasks = input_stage.tasks.to_vec();
        let input_task_count = input_stage_tasks.len();
        let input_stage_num = input_stage.num as u64;
        let query_id = Bytes::from(input_stage.query_id.as_bytes().to_vec());

        let context_headers = ContextGrpcMetadata::headers_from_ctx(&context);
        let task_context = DistributedTaskContext::from_ctx(&context);
        let off = self.properties.partitioning.partition_count() * task_context.task_index;

        // TODO: this propagation should be automatic https://github.com/datafusion-contrib/datafusion-distributed/issues/247
        let context_headers = manually_propagate_distributed_config(context_headers, d_cfg);
        let stream = input_stage_tasks.into_iter().enumerate().map(|(i, task)| {
            let channel_resolver = channel_resolver.clone();

            let ticket = Request::from_parts(
                MetadataMap::from_headers(context_headers.clone()),
                Extensions::default(),
                Ticket {
                    ticket: DoGet {
                        plan_proto: encoded_input_plan.clone(),
                        target_partition: (off + partition) as u64,
                        stage_key: Some(StageKey::new(query_id.clone(), input_stage_num, i as u64)),
                        target_task_index: i as u64,
                        target_task_count: input_task_count as u64,
                    }
                    .encode_to_vec()
                    .into(),
                },
            );

            let metrics_collection_capture = self.metrics_collection.clone();
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
