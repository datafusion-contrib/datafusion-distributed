#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_distributed::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::test_utils::plans::{
        StagePartitioningEstimator, StagePartitioningExprEstimator,
    };
    use datafusion_distributed::{
        DistributedConfig, DistributedExt, DistributedPhysicalOptimizerRule, TaskEstimator,
        assert_snapshot, display_plan_ascii,
    };
    use std::error::Error;
    use std::sync::Arc;

    async fn distributed_plan_with_estimator(
        query: &str,
        estimator: impl TaskEstimator + Send + Sync + 'static,
    ) -> Result<String, Box<dyn Error>> {
        let mut config = SessionConfig::new()
            .with_target_partitions(3)
            .with_information_schema(true);
        config.set_distributed_option_extension(DistributedConfig::default());

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
            .build();

        let mut ctx = SessionContext::new_with_state(state);
        ctx.set_distributed_task_estimator(estimator);
        register_parquet_tables(&ctx).await?;

        let df = ctx.sql(query).await?;
        let plan = df.create_physical_plan().await?;
        Ok(display_plan_ascii(plan.as_ref(), false))
    }

    #[tokio::test]
    async fn stage_partitioning_elides_shuffle_for_subset() -> Result<(), Box<dyn Error>> {
        let query = r#"
            SELECT count(*) AS cnt, "RainToday", "RainTomorrow"
            FROM weather
            GROUP BY "RainToday", "RainTomorrow"
        "#;

        let plan =
            distributed_plan_with_estimator(query, StagePartitioningEstimator::new(["RainToday"]))
                .await?;

        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2]
          │ ProjectionExec: expr=[count(Int64(1))@2 as cnt, RainToday@0 as RainToday, RainTomorrow@1 as RainTomorrow]
          │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, RainTomorrow@1 as RainTomorrow], aggr=[count(Int64(1))]
          │     RepartitionExec: partitioning=Hash([RainToday@0, RainTomorrow@1], 3), input_partitions=1
          │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, RainTomorrow@1 as RainTomorrow], aggr=[count(Int64(1))]
          │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday, RainTomorrow], file_type=parquet
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn stage_partitioning_non_subset_keeps_shuffle() -> Result<(), Box<dyn Error>> {
        let query = r#"
            SELECT max("MaxTemp") AS max_temp, "RainToday", "RainTomorrow"
            FROM weather
            GROUP BY "RainToday", "RainTomorrow"
        "#;

        let plan =
            distributed_plan_with_estimator(query, StagePartitioningEstimator::new(["MaxTemp"]))
                .await?;

        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2]
          │ ProjectionExec: expr=[max(weather.MaxTemp)@2 as max_temp, RainToday@0 as RainToday, RainTomorrow@1 as RainTomorrow]
          │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, RainTomorrow@1 as RainTomorrow], aggr=[max(weather.MaxTemp)]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5]
            │ RepartitionExec: partitioning=Hash([RainToday@0, RainTomorrow@1], 6), input_partitions=1
            │   AggregateExec: mode=Partial, gby=[RainToday@1 as RainToday, RainTomorrow@2 as RainTomorrow], aggr=[max(weather.MaxTemp)]
            │     PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn stage_partitioning_through_projection_alias_elides_shuffle()
    -> Result<(), Box<dyn Error>> {
        let query = r#"
            SELECT count(*) AS cnt, rt, rtm
            FROM (
                SELECT "RainToday" AS rt, ("MaxTemp" + 1.0) AS rtm
                FROM weather
            ) t
            GROUP BY rt, rtm
        "#;

        // projection gets pushed into DataSourceExec
        let plan =
            distributed_plan_with_estimator(query, StagePartitioningEstimator::new(["rt"])).await?;

        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2]
          │ ProjectionExec: expr=[count(Int64(1))@2 as cnt, rt@0 as rt, rtm@1 as rtm]
          │   AggregateExec: mode=FinalPartitioned, gby=[rt@0 as rt, rtm@1 as rtm], aggr=[count(Int64(1))]
          │     RepartitionExec: partitioning=Hash([rt@0, rtm@1], 3), input_partitions=1
          │       AggregateExec: mode=Partial, gby=[rt@0 as rt, rtm@1 as rtm], aggr=[count(Int64(1))]
          │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday@19 as rt, MaxTemp@1 + 1 as rtm], file_type=parquet
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn stage_partitioning_expression_key_elides_shuffle() -> Result<(), Box<dyn Error>> {
        let query = r#"
            SELECT count(*) AS cnt, ("MaxTemp" + "MinTemp") AS temp_sum
            FROM weather
            GROUP BY temp_sum
        "#;

        let estimator = StagePartitioningExprEstimator::with_expr_builder(|schema, _cfg| {
            let max_idx = schema.index_of("MaxTemp").ok()?;
            let min_idx = schema.index_of("MinTemp").ok()?;

            let max_field = schema.field(max_idx);
            let min_field = schema.field(min_idx);
            let max_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(max_field.name(), max_idx));
            let min_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(min_field.name(), min_idx));

            let expr: Arc<dyn PhysicalExpr> =
                Arc::new(BinaryExpr::new(max_col, Operator::Plus, min_col));

            Some(vec![expr])
        });
        let plan = distributed_plan_with_estimator(query, estimator).await?;

        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2]
          │ ProjectionExec: expr=[count(Int64(1))@1 as cnt, weather.MaxTemp + weather.MinTemp@0 as temp_sum]
          │   AggregateExec: mode=FinalPartitioned, gby=[weather.MaxTemp + weather.MinTemp@0 as weather.MaxTemp + weather.MinTemp], aggr=[count(Int64(1))]
          │     RepartitionExec: partitioning=Hash([weather.MaxTemp + weather.MinTemp@0], 3), input_partitions=1
          │       AggregateExec: mode=Partial, gby=[MaxTemp@1 + MinTemp@0 as weather.MaxTemp + weather.MinTemp], aggr=[count(Int64(1))]
          │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp], file_type=parquet
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn stage_partitioning_superset_keeps_shuffle() -> Result<(), Box<dyn Error>> {
        let query = r#"
            SELECT count(*) AS cnt, "RainToday"
            FROM weather
            GROUP BY "RainToday"
        "#;

        let plan = distributed_plan_with_estimator(
            query,
            StagePartitioningEstimator::new(["RainToday", "RainTomorrow"]),
        )
        .await?;

        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2]
          │ ProjectionExec: expr=[count(Int64(1))@1 as cnt, RainToday@0 as RainToday]
          │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5]
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=1
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }
}
