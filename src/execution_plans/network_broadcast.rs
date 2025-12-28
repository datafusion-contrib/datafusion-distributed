use crate::DistributedConfig;
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
use uuid::Uuid;

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
/// Broadcasts the build side of a CollectLeft hash join to all consumer tasks.
///
/// The input is coalesced to a single partition, executed once on a single task,
/// and the results are cached and sent to all consumer tasks.
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
        consumer_task_count: usize,
        input_task_count: usize,
    ) -> Result<Self, DataFusionError> {
        let coalesced = if input.output_partitioning().partition_count() > 1
            && input
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
                .is_none()
        {
            Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>
        } else {
            input
        };

        let input_stage = Stage::new(
            query_id,
            num,
            coalesced.clone(),
            input_task_count,
            Some(consumer_task_count),
        );

        Ok(Self {
            properties: coalesced.properties().clone(),
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
        let partitions = self.properties.partitioning.partition_count();
        let stage = self.input_stage.num;
        let consumer_count = self
            .input_stage
            .consumer_task_count
            .map(|c| format!(", consumer_tasks={c}"))
            .unwrap_or_default();
        write!(
            f,
            "[Stage {stage}] => NetworkBroadcastExec: output_partitions={partitions}, input_tasks={input_tasks}{consumer_count}",
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
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let channel_resolver = get_distributed_channel_resolver(context.as_ref());
        let d_cfg = DistributedConfig::from_config_options(context.session_config().options())?;
        let retrieve_metrics = d_cfg.collect_metrics;

        let input_stage = &self.input_stage;
        let encoded_input_plan = input_stage.plan.encoded()?;
        let input_stage_tasks = input_stage.tasks.to_vec();
        let input_task_count = input_stage_tasks.len();
        let input_stage_num = input_stage.num as u64;
        let query_id = Bytes::from(input_stage.query_id.as_bytes().to_vec());
        let consumer_task_count = input_stage.consumer_task_count.map(|c| c as u64);

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
                        target_partition: 0,
                        stage_key: Some(StageKey::new(query_id.clone(), input_stage_num, i as u64)),
                        target_task_index: i as u64,
                        target_task_count: input_task_count as u64,
                        consumer_task_count,
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
