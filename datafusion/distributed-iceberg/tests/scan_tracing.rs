mod tests {
    use std::fs::read_to_string;

    use datafusion::common::Result;
    use datafusion::physical_plan::collect;
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg_test_tools::{IcebergTestHarness, create_taxi_table};
    use tempfile::NamedTempFile;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::filter::filter_fn;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::prelude::*;

    #[tokio::test]
    async fn eager_discovery_is_a_child_of_scan_planning() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .build()
            .await?;
        let (trace, _guard) = capture_planning_spans()?;
        harness.scan().await?;
        insta::assert_snapshot!(read_to_string(trace.path())?, @"
        iceberg.plan_scan{eager=true}:iceberg.plan_files{eager=true file_tasks=7}: close
        iceberg.plan_scan{eager=true}: close
        ");
        Ok(())
    }

    #[tokio::test]
    async fn lazy_discovery_is_deferred_until_execution() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("datafusion.execution.target_partitions", 1)
            .build()
            .await?;
        let (trace, _guard) = capture_planning_spans()?;
        let plan = harness.scan().await?;
        insta::assert_snapshot!(read_to_string(trace.path())?, @"iceberg.plan_scan{eager=false}: close");
        collect(plan, harness.state().task_ctx()).await?;
        insta::assert_snapshot!(read_to_string(trace.path())?, @"
        iceberg.plan_scan{eager=false}: close
        iceberg.plan_files{eager=false file_tasks=7}: close
        ");
        Ok(())
    }

    fn capture_planning_spans() -> Result<(NamedTempFile, DefaultGuard)> {
        let trace = NamedTempFile::new()?;
        let layer = tracing_subscriber::fmt::layer()
            .without_time()
            .with_target(false)
            .with_level(false)
            .with_ansi(false)
            .with_span_events(FmtSpan::CLOSE)
            .with_writer(trace.reopen()?)
            .with_filter(filter_fn(|m| m.name().starts_with("iceberg.plan_")));
        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        Ok((trace, guard))
    }
}
