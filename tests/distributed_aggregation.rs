#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion::prelude::SessionConfig;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::test_utils::plan::distribute_aggregate;
    use datafusion_distributed::{
        assert_snapshot, display_stage_graphviz, DefaultSessionBuilder,
        DistributedPhysicalOptimizerRule, DistributedSessionBuilderContext, ExecutionStage,
    };
    use futures::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

    async fn build_state(
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError> {
        let config = SessionConfig::new().with_target_partitions(3);

        let rule = DistributedPhysicalOptimizerRule::new().with_maximum_partitions_per_task(2);
        Ok(SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(ctx.runtime_env)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(rule))
            .build())
    }

    #[tokio::test]
    async fn distributed_aggregation() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) = start_localhost_context(3, build_state).await;
        register_parquet_tables(&ctx).await?;

        let df = ctx
            .sql(r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#)
            .await?;
        let physical = df.create_physical_plan().await?;

        let physical_str = displayable(physical.as_ref()).indent(true).to_string();

        assert_snapshot!(physical_str,
            @r"
        ┌───── Stage 3   Task: partitions: 0,unassigned]
        │partitions [out:1  <-- in:1  ] ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │partitions [out:1  <-- in:3  ]   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │partitions [out:3  <-- in:3  ]     SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
        │partitions [out:3  <-- in:3  ]       ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
        │partitions [out:3  <-- in:3  ]         AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        │partitions [out:3  <-- in:3  ]           CoalesceBatchesExec: target_batch_size=8192
        │partitions [out:3            ]             ArrowFlightReadExec: Stage 2  
        └──────────────────────────────────────────────────
          ┌───── Stage 2   Task: partitions: 0,1,unassigned],Task: partitions: 2,unassigned]
          │partitions [out:3  <-- in:2  ] RepartitionExec: partitioning=Hash([RainToday@0], 3), input_partitions=2
          │partitions [out:2  <-- in:3  ]   PartitionIsolatorExec [providing upto 2 partitions]
          │partitions [out:3            ]     ArrowFlightReadExec: Stage 1  
          └──────────────────────────────────────────────────
            ┌───── Stage 1   Task: partitions: 0,1,unassigned],Task: partitions: 2,unassigned]
            │partitions [out:3  <-- in:2  ] RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
            │partitions [out:2  <-- in:1  ]   PartitionIsolatorExec [providing upto 2 partitions]
            │partitions [out:1  <-- in:1  ]     AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │partitions [out:1            ]       DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ",
        );

        println!("{}", physical_str);
        println!("{}", display_stage_graphviz(physical.clone())?);

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
}
