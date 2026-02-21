use crate::common::require_one_child;
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::distributed_planner::NetworkBoundaryExt;
use crate::flight_service::{INIT_ACTION_TYPE, InitAction};
use crate::networking::get_distributed_worker_resolver;
use crate::passthrough_headers::get_passthrough_headers;
use crate::protobuf::{DistributedCodec, tonic_status_to_datafusion_error};
use crate::stage::{ExecutionTask, Stage};
use crate::{ChannelResolver, StageKey, WorkerResolver, get_distributed_channel_resolver};
use arrow_flight::Action;
use bytes::Bytes;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err, internal_err};
use datafusion::common::{exec_datafusion_err, internal_datafusion_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use http::Extensions;
use prost::Message;
use rand::Rng;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use tonic::Request;
use tonic::metadata::MetadataMap;
use url::Url;

/// [ExecutionPlan] that executes the inner plan in distributed mode.
/// Before executing it, two modifications are lazily performed on the plan:
/// 1. Assigns worker URLs to all the stages. A random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    pub plan: Arc<dyn ExecutionPlan>,
    pub prepared_plan: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
}

struct PreparedPlan {
    plan: Arc<dyn ExecutionPlan>,
    tasks: Vec<BoxFuture<'static, Result<()>>>,
}

impl DistributedExec {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan,
            prepared_plan: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the plan which is lazily prepared on execute() and actually gets executed.
    /// It is updated on every call to execute(). Returns an error if .execute() has not been called.
    pub(crate) fn prepared_plan(&self) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.prepared_plan
            .lock()
            .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {}", e))?
            .clone()
            .ok_or_else(|| {
                internal_datafusion_err!("No prepared plan found. Was execute() called?")
            })
    }

    fn prepare_plan(&self, ctx: &Arc<TaskContext>) -> Result<PreparedPlan> {
        let worker_resolver = get_distributed_worker_resolver(ctx.session_config())?;
        let codec = DistributedCodec::new_combined_with_user(ctx.session_config());

        let mut headers = get_config_extension_propagation_headers(ctx.session_config())?;
        headers.extend(get_passthrough_headers(ctx.session_config()));
        let urls = worker_resolver.get_urls()?;

        let mut tasks = vec![];

        let prepared = Arc::clone(&self.plan).transform_up(|plan| {
            let Some(plan) = plan.as_network_boundary() else {
                return Ok(Transformed::no(plan));
            };
            let stage = plan.input_stage();
            let Some(input_plan) = &stage.plan else {
                return internal_err!("Plan is not set for stage {}", stage.num);
            };

            let start_idx = rand::rng().random_range(0..urls.len());

            let bytes: Bytes =
                PhysicalPlanNode::try_from_physical_plan(Arc::clone(input_plan), &codec)?
                    .encode_to_vec()
                    .into();

            let tasks = stage
                .tasks
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    let url = urls[(start_idx + i) % urls.len()].clone();
                    let action = InitAction {
                        plan_proto: bytes.clone(),
                        stage_key: Some(StageKey {
                            query_id: stage.query_id.as_bytes().to_vec().into(),
                            stage_id: stage.num as _,
                            task_number: i as _,
                        }),
                    };
                    tasks.push(send_plan_task(Arc::clone(ctx), url.clone(), action).boxed());

                    ExecutionTask { url: Some(url) }
                })
                .collect::<Vec<_>>();

            Ok(Transformed::yes(plan.with_input_stage(Stage {
                query_id: stage.query_id,
                num: stage.num,
                plan: None,
                tasks,
            })?))
        })?;
        Ok(PreparedPlan {
            plan: prepared.data,
            tasks,
        })
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedExec")
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        "DistributedExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedExec {
            plan: require_one_child(&children)?,
            prepared_plan: self.prepared_plan.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition > 0 {
            // The DistributedExec node calls try_assign_urls() lazily upon calling .execute(). This means
            // that .execute() must only be called once, as we cannot afford to perform several
            // random URL assignation while calling multiple partitions, as they will differ,
            // producing an invalid plan
            return exec_err!(
                "DistributedExec must only have 1 partition, but it was called with partition index {partition}"
            );
        }

        let PreparedPlan { plan, tasks } = self.prepare_plan(&context)?;
        {
            let mut guard = self
                .prepared_plan
                .lock()
                .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {e}"))?;
            *guard = Some(plan.clone());
        }
        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), 1);
        let tx = builder.tx();
        builder.spawn(async move {
            let mut stream = plan.execute(partition, context)?;
            while let Some(msg) = stream.next().await {
                if tx.send(msg).await.is_err() {
                    break; // channel closed
                }
            }
            Ok(())
        });
        builder.spawn(async move {
            let mut join_set = JoinSet::new();
            for task in tasks {
                join_set.spawn(task);
            }
            for res in join_set.join_all().await {
                res?;
            }
            Ok(())
        });
        Ok(builder.build())
    }
}

async fn send_plan_task(ctx: Arc<TaskContext>, url: Url, init_action: InitAction) -> Result<()> {
    let channel_resolver = get_distributed_channel_resolver(ctx.as_ref());
    let mut client = channel_resolver.get_flight_client_for_url(&url).await?;

    let mut headers = get_config_extension_propagation_headers(ctx.session_config())?;
    headers.extend(get_passthrough_headers(ctx.session_config()));
    let request = Request::from_parts(
        MetadataMap::from_headers(headers),
        Extensions::default(),
        Action {
            r#type: INIT_ACTION_TYPE.to_string(),
            body: init_action.encode_to_vec().into(),
        },
    );

    client.do_action(request).await.map_err(|e| {
        tonic_status_to_datafusion_error(&e)
            .unwrap_or_else(|| exec_datafusion_err!("Error sending plan to worker {url}: {e}"))
    })?;
    Ok(())
}
