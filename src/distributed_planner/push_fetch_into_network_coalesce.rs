use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::limit::GlobalLimitExec;

use crate::NetworkCoalesceExec;

/// Pushes fetch limits through [NetworkCoalesceExec] stage boundaries.
///
/// DataFusion's normal `LimitPushdown` rule runs before the distributed planner inserts network
/// boundaries, so a fetch-bearing parent can end up directly above a [NetworkCoalesceExec] whose
/// producer stage is still unbounded.
///
/// This pass rewrites:
///
/// ```text
///         ┌─────────────────────────────┐
///         │ CoalescePartitions(fetch=10)│
///         └──────────────▲──────────────┘
///                        │
///         ┌──────────────┴──────────────┐
///         │     NetworkCoalesceExec     │
///         └──────────────▲──────────────┘
///                        │ input_stage.plan
///         ┌──────────────┴──────────────┐
///         │        AggregateExec        │
///         └──────────────▲──────────────┘
///                        │
///         ┌──────────────┴──────────────┐
///         │      NetworkShuffleExec     │
///         └─────────────────────────────┘
/// ```
///
/// into:
///
/// ```text
///         ┌─────────────────────────────┐
///         │ CoalescePartitions(fetch=10)│
///         └──────────────▲──────────────┘
///                        │
///         ┌──────────────┴──────────────┐
///         │     NetworkCoalesceExec     │
///         └──────────────▲──────────────┘
///                        │ input_stage.plan
///         ┌──────────────┴──────────────┐
///         │    LocalLimitExec(fetch=10) │
///         └──────────────▲──────────────┘
///                        │
///         ┌──────────────┴──────────────┐
///         │        AggregateExec        │
///         └──────────────▲──────────────┘
///                        │
///         ┌──────────────┴──────────────┐
///         │      NetworkShuffleExec     │
///         └─────────────────────────────┘
/// ```
///
/// The parent fetch remains in place because it enforces the global result limit. The producer-stage
/// fetch is only a per-task bound that lets remote work stop earlier.
pub(crate) fn push_fetch_into_network_coalesce(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_down(|node| {
        let Some(fetch) = fetch_required_from_child(&node) else {
            return Ok(Transformed::no(node));
        };

        let mut changed = false;
        let new_children = node
            .children()
            .into_iter()
            .map(|child| {
                if let Some(network_coalesce) = child.as_any().downcast_ref::<NetworkCoalesceExec>()
                {
                    changed = true;
                    network_coalesce.with_fetch_on_input_stage(fetch)
                } else {
                    Ok(Arc::clone(child))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        if changed {
            node.with_new_children(new_children).map(Transformed::yes)
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|t| t.data)
}

fn fetch_required_from_child(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        return global_limit
            .fetch()
            .and_then(|fetch| fetch.checked_add(global_limit.skip()));
    }

    plan.fetch()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};

    use crate::{NetworkBoundary, Stage};

    use super::*;

    #[test]
    fn fetch_pushed_into_network_coalesce_input_stage() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let network_coalesce: Arc<dyn ExecutionPlan> =
            Arc::new(NetworkCoalesceExec::try_new(input, 1, 1)?);
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(network_coalesce).with_fetch(Some(7)));

        let rewritten = push_fetch_into_network_coalesce(plan)?;
        assert_eq!(rewritten.fetch(), Some(7));

        let parent = rewritten
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .expect("root should remain CoalescePartitionsExec");
        let network_coalesce = parent.children()[0]
            .as_any()
            .downcast_ref::<NetworkCoalesceExec>()
            .expect("child should remain NetworkCoalesceExec");
        let Stage::Local(local_stage) = network_coalesce.input_stage() else {
            panic!("test network boundary should still hold a local stage");
        };
        let local_limit = local_stage
            .plan
            .as_any()
            .downcast_ref::<LocalLimitExec>()
            .expect("input stage should be bounded by LocalLimitExec");

        assert_eq!(local_limit.fetch(), 7);
        Ok(())
    }

    #[test]
    fn global_limit_with_offset_pushes_skip_plus_fetch_into_network_coalesce_input_stage()
    -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let network_coalesce: Arc<dyn ExecutionPlan> =
            Arc::new(NetworkCoalesceExec::try_new(input, 2, 1)?);
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(network_coalesce, 100, Some(10)));

        let rewritten = push_fetch_into_network_coalesce(plan)?;
        let global_limit = rewritten
            .as_any()
            .downcast_ref::<GlobalLimitExec>()
            .expect("root should remain GlobalLimitExec");
        assert_eq!(global_limit.skip(), 100);
        assert_eq!(global_limit.fetch(), Some(10));

        let network_coalesce = global_limit
            .input()
            .as_any()
            .downcast_ref::<NetworkCoalesceExec>()
            .expect("child should remain NetworkCoalesceExec");
        let Stage::Local(local_stage) = network_coalesce.input_stage() else {
            panic!("test network boundary should still hold a local stage");
        };
        let local_limit = local_stage
            .plan
            .as_any()
            .downcast_ref::<LocalLimitExec>()
            .expect("input stage should be bounded by LocalLimitExec");

        assert_eq!(local_limit.fetch(), 110);
        Ok(())
    }
}
