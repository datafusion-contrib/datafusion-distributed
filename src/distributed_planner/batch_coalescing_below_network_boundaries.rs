use crate::common::require_one_child;
use crate::{DistributedConfig, NetworkBoundaryExt};
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use std::sync::Arc;

/// Rearranges the [CoalesceBatchesExec] nodes in the plan so that they are placed right below
/// the network boundaries, so that fewer but bigger record batches are sent over the wire across
/// stages.
pub(crate) fn batch_coalescing_below_network_boundaries(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;

    // Only apply this rule if the normal execution batch size is not already bigger than the
    // shuffle batch size. Exchanging data between workers is better done with batches as big as
    // possible, and if the normal execution batch size is already big, we don't want to proactively
    // reduce it.
    if d_cfg.shuffle_batch_size <= cfg.execution.batch_size {
        return Ok(plan);
    }

    let transformed = plan.transform_up(|plan| {
        if !plan.is_network_boundary() {
            return Ok(Transformed::no(plan));
        }

        let input = require_one_child(plan.children())?;
        if let Some(existing_coalesce) = input.as_any().downcast_ref::<CoalesceBatchesExec>() {
            // There was already a CoalesceBatchesExec below...
            if existing_coalesce.target_batch_size() == d_cfg.shuffle_batch_size {
                // ...so either leave it alone if the batch size is correctly set...
                Ok(Transformed::no(plan))
            } else {
                // ... or replace it with one with the correct batch size.
                let coalesce_input = existing_coalesce.input();
                let new_coalesce =
                    CoalesceBatchesExec::new(Arc::clone(coalesce_input), d_cfg.shuffle_batch_size)
                        .with_fetch(existing_coalesce.fetch());
                let new_plan = plan.with_new_children(vec![Arc::new(new_coalesce)])?;
                Ok(Transformed::yes(new_plan))
            }
        } else {
            // No CoalesceBatchesExec below, need to put one.
            let coalesce_input = input;
            let new_coalesce = CoalesceBatchesExec::new(coalesce_input, d_cfg.shuffle_batch_size);
            let new_plan = plan.with_new_children(vec![Arc::new(new_coalesce)])?;
            Ok(Transformed::yes(new_plan))
        }
    })?;

    Ok(transformed.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::test_utils::parquet::register_parquet_tables;
    use crate::{
        DistributedExt, DistributedPhysicalOptimizerRule, assert_snapshot, display_plan_ascii,
    };
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use itertools::Itertools;

    #[tokio::test]
    async fn same_batch_size_and_shuffle_batch_size() {
        let query = r#"
        SET datafusion.execution.batch_size=100;
        SET distributed.shuffle_batch_size=100;
        SELECT DISTINCT "RainToday", "WindGustDir" FROM weather
        "#;
        let explain = sql_to_explain(query).await;
        // No CoalesceBatchExec is placed before sending data over the network.
        assert_snapshot!(explain, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=2, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0] t1:[p1] 
          │ CoalescePartitionsExec
          │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7] 
            │ RepartitionExec: partitioning=Hash([RainToday@0, WindGustDir@1], 8), input_partitions=4
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │       PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday, WindGustDir], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn batch_size_greater_than_shuffle_batch_size() {
        let query = r#"
        SET datafusion.execution.batch_size=101;
        SET distributed.shuffle_batch_size=100;
        SELECT DISTINCT "RainToday", "WindGustDir" FROM weather
        "#;
        let explain = sql_to_explain(query).await;
        // No CoalesceBatchExec is placed before sending data over the network.
        assert_snapshot!(explain, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=2, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0] t1:[p1] 
          │ CoalescePartitionsExec
          │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7] 
            │ RepartitionExec: partitioning=Hash([RainToday@0, WindGustDir@1], 8), input_partitions=4
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │       PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday, WindGustDir], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn shuffle_batch_size_greater_than_batch_size() {
        let query = r#"
        SET datafusion.execution.batch_size=100;
        SET distributed.shuffle_batch_size=101;
        SELECT DISTINCT "RainToday", "WindGustDir" FROM weather
        "#;
        let explain = sql_to_explain(query).await;
        // CoalesceBatchExec is placed before sending data over the network.
        assert_snapshot!(explain, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=2, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0] t1:[p1] 
          │ CoalesceBatchesExec: target_batch_size=101
          │   CoalescePartitionsExec
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7] 
            │ CoalesceBatchesExec: target_batch_size=101
            │   RepartitionExec: partitioning=Hash([RainToday@0, WindGustDir@1], 8), input_partitions=4
            │     AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
            │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday, WindGustDir], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    async fn sql_to_explain(query: &str) -> String {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(4))
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

        let physical_plan = df.create_physical_plan().await.unwrap();
        display_plan_ascii(physical_plan.as_ref(), false)
    }
}
