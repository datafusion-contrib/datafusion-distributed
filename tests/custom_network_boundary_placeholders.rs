#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::config::ConfigOptions;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::{ExecutionPlan, displayable, execute_stream};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        ApplyNetworkBoundaries, DefaultSessionBuilder, DistributedExt, NetworkBoundaryKind,
        NetworkBoundaryPlaceholder, SessionStateBuilderExt, assert_snapshot,
    };
    use futures::TryStreamExt;
    use itertools::Itertools;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn custom_network_boundary_placeholder() -> Result<(), Box<dyn Error>> {
        // stacks an extra Coalesce network boundary on top of existing ones, with a reduced number
        // of tasks. This rule needs to run between the InjectNetworkBoundaryPlaceholders and the
        // ApplyNetworkBoundaries rule, as it uses the NetworkBoundaryPlaceholder API for injecting
        // network boundaries in custom places.
        #[derive(Debug)]
        struct HierarchicalCoalesce;

        impl PhysicalOptimizerRule for HierarchicalCoalesce {
            fn optimize(
                &self,
                plan: Arc<dyn ExecutionPlan>,
                _: &ConfigOptions,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                plan.transform_up(|plan| {
                    // upon finding a NetworkBoundaryPlaceholder of Coalesce kind, stack another
                    //  one on top with half the input tasks.
                    if let Some(nb) = plan.as_any().downcast_ref::<NetworkBoundaryPlaceholder>()
                        && matches!(nb.kind, NetworkBoundaryKind::Coalesce)
                    {
                        return Ok(Transformed::yes(Arc::new(NetworkBoundaryPlaceholder {
                            kind: NetworkBoundaryKind::Coalesce,
                            input_task_count: nb.input_task_count.div_ceil(2),
                            input: plan,
                        })));
                    };
                    Ok(Transformed::no(plan))
                })
                .map(|v| v.data)
            }

            fn name(&self) -> &str {
                "HierarchicalCoalesce"
            }

            fn schema_check(&self) -> bool {
                true
            }
        }

        let (ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        // NOTE: for some reason SessionStateBuilder::new_from_existing does not propagate the
        //  optimizer rules, so we need to build one new session from scratch.
        let mut builder = SessionStateBuilder::new()
            .with_default_features()
            .with_config(ctx.copied_config())
            .with_distributed_cardinality_effect_task_scale_factor(1.0)?
            .with_distributed_physical_optimizer_rules();

        // Insert the custom rule right before ApplyNetworkBoundaries.
        let rules = builder.physical_optimizer_rules().get_or_insert_default();
        insert_before(rules, HierarchicalCoalesce, ApplyNetworkBoundaries);

        let ctx = SessionContext::from(builder.build());
        register_parquet_tables(&ctx).await?;

        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;

        let df = ctx.sql(query).await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = displayable(physical.as_ref()).indent(true).to_string();

        assert_snapshot!(physical_str,
            @r"
        DistributedExec
          ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
            SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
              [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=2
                [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
                  SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
                    ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
                      AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                        [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
                          RepartitionExec: partitioning=Hash([RainToday@0], 9), input_partitions=1
                            AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                              PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
                                DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ",
        );

        let batches = pretty_format_batches(
            &execute_stream(physical, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        assert_snapshot!(batches, @r"
        +----------+-----------+
        | count(*) | RainToday |
        +----------+-----------+
        | 66       | Yes       |
        | 300      | No        |
        +----------+-----------+
        ");

        Ok(())
    }

    /// Inserts a [PhysicalOptimizerRule] before another one.
    fn insert_before<T: PhysicalOptimizerRule + Send + Sync + 'static>(
        rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
        value: T,
        reference: impl PhysicalOptimizerRule,
    ) {
        let Some((pos, _)) = rules.iter().find_position(|v| v.name() == reference.name()) else {
            return;
        };
        rules.insert(pos, Arc::new(value));
    }
}
