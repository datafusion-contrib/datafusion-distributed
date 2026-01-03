use crate::DistributedConfig;
use crate::DistributedTaskContext;
use crate::common::require_one_child;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::distributed_planner::NetworkBoundary;
use crate::execution_plans::common::{manually_propagate_distributed_config, spawn_select_all};
use crate::flight_service::DoGet;
use crate::metrics::MetricsCollectingStream;
use crate::metrics::proto::MetricsSetProto;
use crate::networking::get_distributed_channel_resolver;
use crate::protobuf::StageKey;
use crate::protobuf::{map_flight_to_datafusion_error, map_status_to_datafusion_error};
use crate::stage::{MaybeEncodedPlan, Stage};
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use bytes::Bytes;
use dashmap::DashMap;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
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

/// Network boundary for broadcasting data to all consumer tasks.
///
/// This operator works with [BroadcastExec] which scales up partitions so each
/// consumer task fetches a unique set of partition numbers. Each partition request
/// is sent to all stage tasks because PartitionIsolatorExec maps the same logical
/// partition to different actual data on each task.
///
/// Here are some examples of how [NetworkBroadcastExec] distributes data:
///
/// # 1 to many
///
/// ```text
/// ┌────────────────────────┐                        ┌────────────────────────┐           ■
/// │  NetworkBroadcastExec  │                        │  NetworkBroadcastExec  │           │
/// │        (task 1)        │           ...          │        (task M)        │           │
/// │                        │                        │                        │        Stage N
/// │    Populates Caches    │                        │       Cache Hits       │           │
/// └────────┬─┬┬─┬┬─┬───────┘                        └────────┬─┬┬─┬┬─┬───────┘           │
///          │1││2││3│                                         │1││2││3│                   │
///          └▲┘└▲┘└▲┘                                         └▲┘└▲┘└▲┘                   ■
///           │  │  │                                           │  │  │
///           │  │  │                                           │  │  │
///           │  │  │                                           │  │  │
///           │  │  └─────────────┐          ┌──────────────────┘  │  │
///           │  └─────────────┐  │          │     ┌───────────────┘  │
///           └─────────────┐  │  │          │     │    ┌─────────────┘
///                         │  │  │          │     │    │
///                        ┌┴┐┌┴┐┌┴┐  ... ┌──┴─┐┌──┴─┐┌─┴┐
///                        │1││2││3│      │NM-2││NM-1││NM│                                 ■
///                       ┌┴─┴┴─┴┴─┴──────┴────┴┴────┴┴──┴┐                                │
///                       │         BroadcastExec         │                                │
///                       │       ┌───────────────┐       │                            Stage N-1
///                       │       │  Batch Cache  │       │                                │
///                       │       │  ┌─┐ ┌─┐ ┌─┐  │       │                                │
///                       │       │  │1│ │2│ │3│  │       │                                │
///                       │       │  └─┘ └─┘ └─┘  │       │                                │
///                       │       └───────────────┘       │                                │
///                       └──────────┬─┬─┬─┬─┬─┬──────────┘                                │
///                                  │1│ │2│ │3│                                           │
///                                  └▲┘ └▲┘ └▲┘                                           ■
///                                   │   │   │
///                                   │   │   │
///                                   │   │   │
///                                  ┌┴┐ ┌┴┐ ┌┴┐                                           ■
///                                  │1│ │2│ │3│                                           │
///                           ┌──────┴─┴─┴─┴─┴─┴──────┐                                Stage N-2
///                           │Arc<dyn ExecutionPlan> │                                    │
///                           │       (task 1)        │                                    │
///                           └───────────────────────┘                                    ■
/// ```
///
/// # Many to many
///
/// ```text
///    ┌────────────────────────┐                        ┌────────────────────────┐         ■
///    │  NetworkBroadcastExec  │                        │  NetworkBroadcastExec  │         │
///    │        (task 1)        │                        │        (task M)        │         │
///    │                        │           ...          │                        │      Stage N
///    │    Populates Caches    │                        │       Cache Hits       │         │
///    └────────┬─┬┬─┬┬─┬───────┘                        └────────┬─┬┬─┬┬─┬───────┘         │
///             │1││2││3│                                         │1││2││3│                 │
///             └▲┘└▲┘└▲┘                                         └▲┘└▲┘└▲┘                 ■
///              │  │  │                                           │  │  │
///   ┌──────────┴──┼──┼────────────────────────────────┐          │  │  │
///   │  ┌──────────┴──┼────────────────────────────────┼──┐       │  │  │
///   │  │  ┌──────────┴────────────────────────────────┼──┼──┐    │  │  │
///   │  │  │                                           │  │  │    │  │  │
///   │  │  │          ┌────────────────────────────────┼──┼──┼────┴──┼─┐│
///   │  │  │          │     ┌──────────────────────────┼──┼──┼───────┴─┼┼─────┐
///   │  │  │          │     │    ┌─────────────────────┼──┼──┼─────────┼┴─────┼────┐
///   │  │  │          │     │    │                     │  │  │         │      │    │
///  ┌┴┐┌┴┐┌┴┐  ... ┌──┴─┐┌──┴─┐┌─┴┐                   ┌┴┐┌┴┐┌┴┐  ... ┌─┴──┐┌──┴─┐┌─┴┐      ■
///  │1││2││3│      │3M-2││3M-1││3M│                   │1││2││3│      │3M-2││3M-1││3M│      │
/// ┌┴─┴┴─┴┴─┴──────┴────┴┴────┴┴──┴┐                 ┌┴─┴┴─┴┴─┴──────┴────┴┴────┴┴──┴┐     │
/// │         BroadcastExec         │                 │         BroadcastExec         │     │
/// │       ┌───────────────┐       │                 │       ┌───────────────┐       │     │
/// │       │  Batch Cache  │       │                 │       │  Batch Cache  │       │     │
/// │       │  ┌─┐ ┌─┐ ┌─┐  │       │       ...       │       │  ┌─┐ ┌─┐ ┌─┐  │       │ Stage N-1
/// │       │  │1│ │2│ │3│  │       │                 │       │  │1│ │2│ │3│  │       │     │
/// │       │  └─┘ └─┘ └─┘  │       │                 │       │  └─┘ └─┘ └─┘  │       │     │
/// │       └───────────────┘       │                 │       └───────────────┘       │     │
/// └──────────┬─┬─┬─┬─┬─┬──────────┘                 └──────────┬─┬─┬─┬─┬─┬──────────┘     │
///            │1│ │2│ │3│                                       │1│ │2│ │3│                │
///            └▲┘ └▲┘ └▲┘                                       └▲┘ └▲┘ └▲┘                ■
///             │   │   │                                         │   │   │
///             │   │   │                                         │   │   │
///             │   │   │                                         │   │   │
///            ┌┴┐ ┌┴┐ ┌┴┐                                       ┌┴┐ ┌┴┐ ┌┴┐                ■
///            │1│ │2│ │3│                                       │1│ │2│ │3│                │
///     ┌──────┴─┴─┴─┴─┴─┴──────┐                         ┌──────┴─┴─┴─┴─┴─┴──────┐     Stage N-2
///     │Arc<dyn ExecutionPlan> │           ...           │Arc<dyn ExecutionPlan> │         │
///     │       (task 1)        │                         │       (task N)        │         │
///     └───────────────────────┘                         └───────────────────────┘         ■
/// ```
///
/// Notice in this diagram that each [NetworkBroadcastExec] sends a request to fetch data from each
/// [BroadcastExec] in the stage below per partition. This is because each [BroadcastExec] has its
/// own cache which contains partial results for the partition. It is the [NetworkBroadcastExec]'s
/// job to merge these partial partitions to then broadcast complete data to the consumers.
#[derive(Debug, Clone)]
pub struct NetworkBroadcastExec {
    pub(crate) properties: PlanProperties,
    pub(crate) input_stage: Stage,
    pub(crate) metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
}

impl NetworkBroadcastExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        num: usize,
        input_task_count: usize,
    ) -> Result<Self, DataFusionError> {
        let total_partitions = input.properties().partitioning.partition_count();
        let input_partition_count =
            if let Some(broadcast) = input.as_any().downcast_ref::<super::BroadcastExec>() {
                broadcast.input_partition_count()
            } else {
                total_partitions
            };
        let input_stage = Stage::new(query_id, num, input.clone(), input_task_count);
        let properties = input
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(input_partition_count));

        Ok(Self {
            properties,
            input_stage,
            metrics_collection: Default::default(),
        })
    }
}

impl NetworkBoundary for NetworkBroadcastExec {
    fn with_input_stage(
        &self,
        input_stage: Stage,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut self_clone = self.clone();
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }

    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }
}

impl DisplayAs for NetworkBroadcastExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_tasks = self.input_stage.tasks.len();
        let stage = self.input_stage.num;
        let consumer_partitions = self.properties.partitioning.partition_count();
        let stage_partitions = self
            .input_stage
            .plan
            .decoded()
            .map(|p| p.properties().partitioning.partition_count())
            .unwrap_or(0);
        write!(
            f,
            "[Stage {stage}] => NetworkBroadcastExec: partitions_per_consumer={consumer_partitions}, stage_partitions={stage_partitions}, input_tasks={input_tasks}",
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
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.input_stage.plan {
            MaybeEncodedPlan::Decoded(plan) => vec![plan],
            MaybeEncodedPlan::Encoded(_) => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let new_child = require_one_child(children)?;
        let mut new_stage = self.input_stage.clone();
        new_stage.plan = MaybeEncodedPlan::Decoded(new_child);

        Ok(Arc::new(Self {
            properties: self.properties.clone(),
            input_stage: new_stage,
            metrics_collection: self.metrics_collection.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let task_context = DistributedTaskContext::from_ctx(&context);
        let task_index = task_context.task_index;

        let channel_resolver = get_distributed_channel_resolver(context.as_ref());
        let d_cfg = DistributedConfig::from_config_options(context.session_config().options())?;
        let retrieve_metrics = d_cfg.collect_metrics;

        let input_stage = &self.input_stage;
        let encoded_input_plan = input_stage.plan.encoded()?;
        let input_stage_tasks = input_stage.tasks.to_vec();
        let input_task_count = input_stage_tasks.len();
        let input_stage_num = input_stage.num as u64;
        let query_id = Bytes::from(input_stage.query_id.as_bytes().to_vec());
        let input_partition_count = self.properties.partitioning.partition_count();
        let stage_partition = task_index * input_partition_count + partition;

        let context_headers = ContextGrpcMetadata::headers_from_ctx(&context);
        let context_headers = manually_propagate_distributed_config(context_headers, d_cfg);
        let metrics_collection = self.metrics_collection.clone();

        let stream = input_stage_tasks.into_iter().enumerate().map(|(i, task)| {
            let channel_resolver = Arc::clone(&channel_resolver);
            let metrics_collection_capture = metrics_collection.clone();

            let ticket = Request::from_parts(
                MetadataMap::from_headers(context_headers.clone()),
                Extensions::default(),
                Ticket {
                    ticket: DoGet {
                        plan_proto: encoded_input_plan.clone(),
                        target_partition: stage_partition as u64,
                        stage_key: Some(StageKey::new(query_id.clone(), input_stage_num, i as u64)),
                        target_task_index: i as u64,
                        target_task_count: input_task_count as u64,
                    }
                    .encode_to_vec()
                    .into(),
                },
            );

            async move {
                let url = task.url.ok_or(internal_datafusion_err!(
                    "NetworkBroadcastExec: task is unassigned"
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
