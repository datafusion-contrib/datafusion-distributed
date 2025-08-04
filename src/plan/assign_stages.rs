use crate::context::StageContext;
use crate::{ArrowFlightReadExec, ChannelManager};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::cell::RefCell;
use std::sync::Arc;
use uuid::Uuid;

pub fn assign_stages(
    plan: Arc<dyn ExecutionPlan>,
    channel_manager: impl TryInto<ChannelManager, Error = DataFusionError>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let stack = RefCell::new(vec![]);
    let mut i = 0;

    let urls = channel_manager.try_into()?.get_urls()?;

    Ok(plan
        .transform_down_up(
            |plan| {
                let Some(node) = plan.as_any().downcast_ref::<ArrowFlightReadExec>() else {
                    return Ok(Transformed::no(plan));
                };

                // If the current ArrowFlightReadExec already has a task assigned, do nothing.
                if let Some(ref stage_context) = node.stage_context {
                    stack.borrow_mut().push(stage_context.clone());
                    return Ok(Transformed::no(plan.clone()));
                }

                let mut input_urls = vec![];
                for _ in 0..node.properties().output_partitioning().partition_count() {
                    // Just round-robin the workers for assigning tasks.
                    input_urls.push(urls[i % urls.len()].clone());
                    i += 1;
                }

                let stage_context = if let Some(prev_stage) = stack.borrow().last() {
                    StageContext {
                        id: prev_stage.input_id,
                        n_tasks: prev_stage.input_urls.len(),
                        input_id: Uuid::new_v4(),
                        input_urls,
                    }
                } else {
                    // This is the first ArrowFlightReadExec encountered in the plan, that's
                    // why there is no stage yet.
                    //
                    // As this task will not need to fan out data to upper stages, it does not
                    // care about output tasks.
                    StageContext {
                        id: Uuid::new_v4(),
                        n_tasks: 1,
                        input_id: Uuid::new_v4(),
                        input_urls,
                    }
                };

                stack.borrow_mut().push(stage_context.clone());
                let mut node = node.clone();
                node.stage_context = Some(stage_context);
                Ok(Transformed::yes(Arc::new(node)))
            },
            |plan| {
                if plan.name() == "ArrowFlightReadExec" {
                    stack.borrow_mut().pop();
                }
                Ok(Transformed::no(plan))
            },
        )?
        .data)
}
