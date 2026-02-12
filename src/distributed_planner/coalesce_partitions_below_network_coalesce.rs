use crate::NetworkCoalesceExec;
use crate::common::require_one_child;
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

pub(crate) fn coalesce_partitions_below_network_coalesce(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let result = plan.transform_down(|parent| {
        let Some(child) = parent.children().pop() else {
            return Ok(Transformed::no(parent));
        };

        let Some(network_coalesce) = child.as_any().downcast_ref::<NetworkCoalesceExec>() else {
            return Ok(Transformed::no(parent));
        };

        let network_coalesce_input = require_one_child(network_coalesce.children())?;

        if network_coalesce_input
            .output_partitioning()
            .partition_count()
            == 1
        {
            return Ok(Transformed::no(parent));
        }

        if let Some(sort_merge_exec) = parent.as_any().downcast_ref::<SortPreservingMergeExec>() {
            let child = Arc::clone(child).with_new_children(vec![Arc::new(
                SortPreservingMergeExec::new(
                    sort_merge_exec.expr().clone(),
                    require_one_child(network_coalesce.children())?,
                ),
            )])?;

            let parent = parent.with_new_children(vec![child])?;

            return Ok(Transformed::yes(parent));
        }

        if let Some(_coalesce_exec) = parent.as_any().downcast_ref::<CoalescePartitionsExec>() {
            let child = Arc::clone(child).with_new_children(vec![Arc::new(
                CoalescePartitionsExec::new(require_one_child(network_coalesce.children())?),
            )])?;

            let parent = parent.with_new_children(vec![child])?;

            return Ok(Transformed::yes(parent));
        }

        Ok(Transformed::no(parent))
    })?;

    Ok(result.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::test_utils::parquet::register_parquet_tables;
    use crate::{DistributedExt, DistributedPhysicalOptimizerRule};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use itertools::Itertools;

    #[tokio::test]
    async fn coalesce_partitions() {
        let query = r#"
        SELECT DISTINCT "RainToday", "WindGustDir" FROM weather
        "#;
        let plan = sql_to_plan(query).await;
        let mut at_least_one_coalesce = false;
        // No CoalesceBatchExec is placed before sending data over the network.
        plan.transform_down(|plan| {
            let Some(network_coalesce) = plan.as_any().downcast_ref::<NetworkCoalesceExec>() else {
                return Ok(Transformed::no(plan));
            };

            at_least_one_coalesce = true;
            let child = require_one_child(network_coalesce.children())?;
            assert!(child.as_any().is::<CoalescePartitionsExec>());

            Ok(Transformed::no(plan))
        })
        .unwrap();

        assert!(at_least_one_coalesce);
    }

    #[tokio::test]
    async fn sort_merge_preserving_exec() {
        let query = r#"
        SELECT DISTINCT "RainToday", "WindGustDir" FROM weather ORDER BY "WindGustDir" DESC
        "#;
        let plan = sql_to_plan(query).await;
        let mut at_least_one_coalesce = false;
        // No CoalesceBatchExec is placed before sending data over the network.
        plan.transform_down(|plan| {
            let Some(network_coalesce) = plan.as_any().downcast_ref::<NetworkCoalesceExec>() else {
                return Ok(Transformed::no(plan));
            };

            at_least_one_coalesce = true;
            let child = require_one_child(network_coalesce.children())?;
            assert!(child.as_any().is::<SortPreservingMergeExec>());

            Ok(Transformed::no(plan))
        })
        .unwrap();

        assert!(at_least_one_coalesce);
    }

    #[tokio::test]
    async fn sort_merge_preserving_exec_no_double_inject() {
        let query = r#"
        SELECT DISTINCT "RainToday", "WindGustDir" FROM weather ORDER BY "WindGustDir" DESC
        "#;
        let plan = sql_to_plan(query).await;
        let plan = coalesce_partitions_below_network_coalesce(plan).unwrap();
        let mut at_least_one_coalesce = false;
        // No CoalesceBatchExec is placed before sending data over the network.
        plan.transform_down(|plan| {
            let Some(network_coalesce) = plan.as_any().downcast_ref::<NetworkCoalesceExec>() else {
                return Ok(Transformed::no(plan));
            };

            at_least_one_coalesce = true;
            let child = require_one_child(network_coalesce.children())?;
            assert!(child.as_any().is::<SortPreservingMergeExec>());

            let grand_child = require_one_child(child.children())?;
            assert!(!grand_child.as_any().is::<SortPreservingMergeExec>());

            Ok(Transformed::no(plan))
        })
        .unwrap();

        assert!(at_least_one_coalesce);
    }

    async fn sql_to_plan(query: &str) -> Arc<dyn ExecutionPlan> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
            .build();

        let ctx = SessionContext::new_with_state(state);
        let mut queries = query.split(";").collect_vec();
        let last_query = queries.pop().unwrap();
        for query in queries {
            ctx.sql(query).await.unwrap();
        }
        register_parquet_tables(&ctx).await.unwrap();
        let df = ctx.sql(last_query).await.unwrap();

        df.create_physical_plan().await.unwrap()
    }
}
