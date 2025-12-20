use crate::channel_resolver_ext::get_distributed_channel_resolver;
use crate::distributed_planner::NetworkBoundaryExt;
use crate::execution_plans::common::require_one_child;
use crate::protobuf::DistributedCodec;
use crate::stage::{ExecutionTask, Stage};
use datafusion::common::exec_err;
use datafusion::common::internal_datafusion_err;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use rand::Rng;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
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

    fn prepare_plan(
        &self,
        urls: &[Url],
        codec: &dyn PhysicalExtensionCodec,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let prepared = Arc::clone(&self.plan).transform_up(|plan| {
            let Some(plan) = plan.as_network_boundary() else {
                return Ok(Transformed::no(plan));
            };

            let mut rng = rand::thread_rng();
            let start_idx = rng.gen_range(0..urls.len());

            let Some(stage) = plan.input_stage() else {
                return exec_err!(
                    "NetworkBoundary '{}' has not been assigned a stage",
                    plan.name()
                );
            };

            let ready_stage = Stage {
                query_id: stage.query_id,
                num: stage.num,
                plan: stage.plan.to_encoded(codec)?,
                tasks: stage
                    .tasks
                    .iter()
                    .enumerate()
                    .map(|(i, _)| ExecutionTask {
                        url: Some(urls[(start_idx + i) % urls.len()].clone()),
                    })
                    .collect::<Vec<_>>(),
                consumer_task_count: stage.consumer_task_count,
            };

            // consumer_task_count was already set during distribute_plan, so use it
            // If not set, default to 1 for head stage case
            let consumer_count = stage.consumer_task_count.unwrap_or(1);
            Ok(Transformed::yes(
                plan.with_input_stage(ready_stage, consumer_count)?,
            ))
        })?;
        Ok(prepared.data)
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
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedExec {
            plan: require_one_child(&children)?,
            prepared_plan: self.prepared_plan.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if partition > 0 {
            // The DistributedExec node calls try_assign_urls() lazily upon calling .execute(). This means
            // that .execute() must only be called once, as we cannot afford to perform several
            // random URL assignation while calling multiple partitions, as they will differ,
            // producing an invalid plan
            return exec_err!(
                "DistributedExec must only have 1 partition, but it was called with partition index {partition}"
            );
        }

        let channel_resolver = get_distributed_channel_resolver(context.session_config())?;
        let codec = DistributedCodec::new_combined_with_user(context.session_config());

        let prepared = self.prepare_plan(&channel_resolver.get_urls()?, &codec)?;
        {
            let mut guard = self
                .prepared_plan
                .lock()
                .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {}", e))?;
            *guard = Some(prepared.clone());
        }

        prepared.execute(partition, context)
    }
}
