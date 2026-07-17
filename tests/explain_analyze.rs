#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::{Result, assert_contains};
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DefaultSessionBuilder, DistributedExt};
    use futures::TryStreamExt;
    use test_case::test_case;

    #[test_case(false ; "static_task_count")]
    #[test_case(true ; "dynamic_task_count")]
    #[tokio::test]
    async fn explain_analyze_displays_distributed_metrics(
        dynamic_task_count: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        ctx.set_distributed_dynamic_task_count(dynamic_task_count)?;
        register_parquet_tables(&ctx).await?;

        let plan = ctx
            .sql(
                r#"EXPLAIN ANALYZE
                   SELECT count(*), "RainToday"
                   FROM weather
                   GROUP BY "RainToday"
                   ORDER BY count(*)"#,
            )
            .await?
            .create_physical_plan()
            .await?;
        assert_eq!(plan.name(), "DistributedAnalyzeExec");
        assert_eq!(plan.children()[0].name(), "DistributedExec");

        let batches = execute_stream(plan, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let formatted = pretty_format_batches(&batches)?.to_string();
        println!("{formatted}");

        assert_contains!(&formatted, "Plan with Metrics");
        assert_contains!(&formatted, "DistributedExec");
        assert_contains!(&formatted, "NetworkShuffleExec");
        assert_contains!(&formatted, "metrics=[output_rows=");

        Ok(())
    }

    #[tokio::test]
    async fn explain_analyze_verbose_displays_additional_metrics()
    -> Result<(), Box<dyn std::error::Error>> {
        let (ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        register_parquet_tables(&ctx).await?;

        let plan = ctx
            .sql(
                r#"EXPLAIN ANALYZE VERBOSE
                   SELECT count(*), "RainToday"
                   FROM weather
                   GROUP BY "RainToday"
                   ORDER BY count(*)"#,
            )
            .await?
            .create_physical_plan()
            .await?;
        assert_eq!(plan.name(), "DistributedAnalyzeExec");
        assert_eq!(plan.children()[0].name(), "DistributedExec");

        let batches = execute_stream(plan, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let formatted = pretty_format_batches(&batches)?.to_string();
        println!("{formatted}");

        assert_contains!(&formatted, "Plan with Full Metrics");
        assert_contains!(&formatted, "Output Rows");
        assert_contains!(&formatted, "Duration");
        assert_contains!(&formatted, "metrics=[output_rows=");

        let plan_types = batches[0].column(0).as_any().downcast_ref::<StringArray>();
        let plans = batches[0].column(1).as_any().downcast_ref::<StringArray>();
        let (plan_types, plans) = match (plan_types, plans) {
            (Some(plan_types), Some(plans)) => (plan_types, plans),
            _ => panic!("EXPLAIN ANALYZE columns should be strings"),
        };
        let output_rows = (0..plan_types.len())
            .find(|index| plan_types.value(*index) == "Output Rows")
            .expect("verbose output should contain an Output Rows entry");
        assert_eq!(plans.value(output_rows), "2");

        Ok(())
    }
}
