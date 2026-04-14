use crate::NetworkBoundaryExt;
use crate::common::require_one_child;
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::repartition::RepartitionExec;
use std::sync::Arc;

/// Inserts [`AggregateMode::PartialReduce`] above the hash [`RepartitionExec`] in each
/// producer stage, merging partial-aggregate rows after hash repartitioning has grouped
/// same keys into the same partition.
pub(crate) fn partial_reduce_below_network_shuffles(
    plan: Arc<dyn ExecutionPlan>,
    _cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let transformed = plan.transform_up(|plan| {
        if !plan.is_network_boundary() {
            return Ok(Transformed::no(plan));
        }

        let child = require_one_child(plan.children())?;

        let Some(repartition) = child.as_any().downcast_ref::<RepartitionExec>() else {
            return Ok(Transformed::no(plan));
        };

        if !matches!(repartition.partitioning(), Partitioning::Hash(_, _)) {
            return Ok(Transformed::no(plan));
        }

        let grandchild = require_one_child(repartition.children())?;

        let Some(agg) = grandchild.as_any().downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(plan));
        };

        if *agg.mode() != AggregateMode::Partial {
            return Ok(Transformed::no(plan));
        }

        // Group-by columns in the Partial output schema appear at indices 0..N-1.
        let partial_reduce_group_by = {
            let orig = agg.group_expr();
            let exprs = orig
                .expr()
                .iter()
                .enumerate()
                .map(|(i, (_, name))| {
                    (
                        std::sync::Arc::new(Column::new(name, i))
                            as std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                        name.clone(),
                    )
                })
                .collect();
            let null_exprs = orig
                .null_expr()
                .iter()
                .enumerate()
                .map(|(i, (_, name))| {
                    (
                        std::sync::Arc::new(Column::new(name, i))
                            as std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                        name.clone(),
                    )
                })
                .collect();
            PhysicalGroupBy::new(
                exprs,
                null_exprs,
                orig.groups().to_vec(),
                orig.has_grouping_set(),
            )
        };

        // PartialReduce wraps the existing RepartitionExec, running on
        // hash-partitioned data where same keys are already in the same partition.
        let partial_reduce: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::PartialReduce,
            partial_reduce_group_by,
            agg.aggr_expr().to_vec(),
            agg.filter_expr().to_vec(),
            Arc::clone(&child),
            agg.input_schema(),
        )?);

        let new_plan = plan.with_new_children(vec![partial_reduce])?;
        Ok(Transformed::yes(new_plan))
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

    async fn sql_to_explain(query: &str) -> String {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
            .build();

        let ctx = SessionContext::new_with_state(state);
        register_parquet_tables(&ctx).await.unwrap();
        let df = ctx.sql(query).await.unwrap();
        let physical_plan = df.create_physical_plan().await.unwrap();
        display_plan_ascii(physical_plan.as_ref(), false)
    }

    #[tokio::test]
    async fn grouped_aggregation() {
        let explain =
            sql_to_explain(r#"SELECT "RainToday", COUNT(*) FROM weather GROUP BY "RainToday""#)
                .await;
        assert_snapshot!(explain, @"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3] 
          │ ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as count(*)]
          │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7] 
            │ AggregateExec: mode=PartialReduce, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │   RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=1
            │     AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │       PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn non_aggregation() {
        let explain = sql_to_explain(r#"SELECT * FROM weather LIMIT 10"#).await;
        assert!(
            !explain.contains("PartialReduce"),
            "PartialReduce should not be present in:\n{explain}"
        );
    }

    #[tokio::test]
    async fn global_aggregation() {
        let explain = sql_to_explain(r#"SELECT COUNT(*) FROM weather"#).await;
        assert!(
            !explain.contains("PartialReduce"),
            "PartialReduce should not be present in:\n{explain}"
        );
    }
}
