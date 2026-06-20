use crate::common::{TreeNodeExt, serialize_uuid};
use crate::execution_plans::{ChildrenIsolatorUnionExec, DistributedLeafExec};
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::{DistributedCodec, DistributedConfig, DistributedTaskContext};
use datafusion::common::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::sync::Arc;

/// A stage plan specialized for one task and encoded for delivery, together with the work-unit
/// feeds it declares.
pub struct EncodedTaskPlan {
    pub plan_proto: Vec<u8>,
    pub feed_declarations: Vec<WorkUnitFeedDeclaration>,
    /// Output partitions of the specialized plan. Task-isolated nodes (a union running one child
    /// per task) make this differ from the unspecialized stage plan, so a push-based transport
    /// sizes its per-task sinks from this. The pull transports ignore it; a push transport (such
    /// as the shared-memory one) reads it.
    pub partitions: usize,
}

/// Specializes `plan` for `task_index` and encodes it with the session's combined codec.
///
/// Shared by every transport's dispatch path so task specialization and codec selection cannot
/// drift between transports: each task must see its own slice of task-isolated nodes (a union
/// executing one child per task, an unexecuted [DistributedLeafExec] variant), and the worker
/// decodes with the same codec stack the coordinator encoded with.
pub fn encode_task_plan(
    plan: &Arc<dyn ExecutionPlan>,
    task_index: usize,
    task_count: usize,
    cfg: &SessionConfig,
) -> Result<EncodedTaskPlan> {
    let d_cfg = DistributedConfig::from_config_options(cfg.options())?;
    let wuf_registry = &d_cfg.__private_work_unit_feed_registry;

    let mut feed_declarations = vec![];
    let d_ctx = DistributedTaskContext {
        task_index,
        task_count,
    };

    let specialized = Arc::clone(plan).transform_down_with_dt_ctx(d_ctx, |plan, d_ctx| {
        if let Some(wuf) = wuf_registry.get_work_unit_feed(&plan) {
            feed_declarations.push(WorkUnitFeedDeclaration {
                id: serialize_uuid(&wuf.id()),
                partitions: plan.properties().partitioning.partition_count() as u64,
            });
        };

        if let Some(ciu) = plan.downcast_ref::<ChildrenIsolatorUnionExec>() {
            let ciu = ciu.to_task_specialized(d_ctx.task_index);
            return Ok(Transformed::yes(Arc::new(ciu)));
        };

        if let Some(dle) = plan.downcast_ref::<DistributedLeafExec>() {
            let specialized = dle.to_task_specialized(d_ctx.task_index);
            return Ok(Transformed::yes(specialized));
        }

        Ok(Transformed::no(plan))
    })?;

    let codec = DistributedCodec::new_combined_with_user(cfg);
    let partitions = specialized.data.properties().partitioning.partition_count();
    let plan_proto =
        PhysicalPlanNode::try_from_physical_plan(specialized.data, &codec)?.encode_to_vec();

    Ok(EncodedTaskPlan {
        plan_proto,
        feed_declarations,
        partitions,
    })
}
