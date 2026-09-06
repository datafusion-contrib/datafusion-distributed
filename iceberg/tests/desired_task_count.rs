#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionConfig;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::{
        DesiredTaskCountEvent, DistributedConfig, DistributedExt, WorkerQueryContext,
        display_plan_ascii,
    };
    use datafusion_distributed_iceberg::test_utils::{
        IcebergTestHarness, taxi_metadata, taxi_metadata_builder,
    };
    use datafusion_distributed_iceberg::{IcebergExt, iceberg_desired_task_count};
    use iceberg::spec::{Snapshot, TableMetadata};
    use test_case::test_case;

    #[tokio::test]
    async fn executes_with_estimated_scan_tasks() -> Result<()> {
        let (plan, results) = run_distributed_query().await?;
        insta::assert_snapshot!(plan + &results, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [pickup_date@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=2
          │ SortExec: expr=[pickup_date@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[pickup_date@0 as pickup_date, count(Int64(1))@1 as trips]
          │     AggregateExec: mode=FinalPartitioned, gby=[pickup_date@0 as pickup_date], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=3, partitions=4
            │ RepartitionExec: partitioning=Hash([pickup_date@0], 4), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[pickup_date@0 as pickup_date], aggr=[count(Int64(1))]
            │     DataSourceExec: format=iceberg, projection=[pickup_date]
            └──────────────────────────────────────────────────
        +-------------+-------+
        | pickup_date | trips |
        +-------------+-------+
        | 2024-01-08  | 25000 |
        | 2024-01-09  | 25000 |
        | 2024-01-10  | 25000 |
        | 2024-01-11  | 25000 |
        | 2024-01-12  | 25000 |
        | 2024-01-13  | 25000 |
        | 2024-01-14  | 25000 |
        +-------------+-------+
        ");
        Ok(())
    }

    #[test_case(metadata_with_file_size(Some("24000000")), None, Some(12); "current snapshot")]
    #[test_case(metadata_with_file_size(Some("24000000")), taxi_metadata().current_snapshot_id(), Some(3); "selected snapshot")]
    #[test_case(empty_metadata(), None, Some(0); "empty table")]
    #[test_case(metadata_with_file_size(None), None, None; "missing size")]
    #[test_case(metadata_with_file_size(Some("invalid")), None, None; "malformed size")]
    #[test_case(metadata_with_file_size(Some("-1")), None, None; "negative size")]
    #[test_case(metadata_with_file_size(Some("18446744073709551616")), None, None; "overflowing size")]
    #[tokio::test]
    async fn estimates_file_size(
        metadata: TableMetadata,
        snapshot_id: Option<i64>,
        expected: Option<usize>,
    ) -> Result<()> {
        let mut builder = IcebergTestHarness::builder().with_table_metadata(metadata);
        if let Some(id) = snapshot_id {
            builder = builder.with_table_option("iceberg.snapshot_id", id.to_string());
        }
        let harness = builder.build().await?;
        assert_eq!(estimate(&scan(&harness).await?)?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn remote_feed_does_not_estimate_again() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness.roundtrip_plan(scan(&harness).await?)?;
        assert_eq!(estimate(&plan)?, None);
        Ok(())
    }

    async fn run_distributed_query() -> Result<(String, String)> {
        let fixture = IcebergTestHarness::builder();
        let options = fixture.integration_options()?;
        let harness = fixture.build().await?;
        let worker_options = options.clone();
        let mut ctx = start_in_memory_context(4, move |ctx: WorkerQueryContext| {
            let state = ctx
                .builder
                .with_iceberg_integration(worker_options.clone())
                .build();
            async move { Ok(state) }
        })
        .await;
        ctx.set_iceberg_integration(options);
        ctx.register_table("taxi", harness.ctx.table_provider("taxi").await?)?;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 2;
        ctx.set_distributed_dynamic_task_count(false)?;
        // 4,480,382 bytes / 1 MB / 2 partitions rounds up to 3 tasks, not all 4 workers.
        ctx.set_distributed_file_scan_config_bytes_per_partition(1_000_000)?;

        // Grouping prevents COUNT(*) from being answered from snapshot metadata alone.
        let plan = ctx
            .sql(
                "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
                 GROUP BY pickup_date ORDER BY pickup_date",
            )
            .await?
            .create_physical_plan()
            .await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        let batches = collect(plan, ctx.task_ctx()).await?;
        Ok((display, pretty_format_batches(&batches)?.to_string()))
    }

    async fn scan(harness: &IcebergTestHarness) -> Result<Arc<dyn ExecutionPlan>> {
        // Call the public table provider so empty-table optimization cannot remove the scan.
        harness
            .ctx
            .table_provider("taxi")
            .await?
            .scan(&harness.ctx.state(), None, &[], None)
            .await
    }

    fn estimate(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<usize>> {
        let mut config = SessionConfig::new().with_target_partitions(2);
        config.set_distributed_option_extension(DistributedConfig::default());
        config.set_distributed_file_scan_config_bytes_per_partition(1_000_000)?;
        iceberg_desired_task_count(DesiredTaskCountEvent {
            plan,
            session_config: &config,
        })
        .transpose()
        .map(|response| response.map(|response| response.task_count.as_usize()))
    }

    fn empty_metadata() -> TableMetadata {
        taxi_metadata_builder()
            .build()
            .expect("empty taxi metadata is valid")
            .metadata
    }

    fn metadata_with_file_size(size: Option<&str>) -> TableMetadata {
        let metadata = taxi_metadata();
        let current = metadata.current_snapshot().expect("taxi has a snapshot");
        let mut summary = current.summary().clone();
        match size {
            Some(size) => {
                summary
                    .additional_properties
                    .insert("total-files-size".into(), size.into());
            }
            None => {
                summary.additional_properties.remove("total-files-size");
            }
        }
        // Keep the original snapshot for time travel; only the new snapshot's summary differs.
        let snapshot = Snapshot::builder()
            .with_snapshot_id(current.snapshot_id() + 1)
            .with_parent_snapshot_id(Some(current.snapshot_id()))
            .with_sequence_number(current.sequence_number() + 1)
            .with_timestamp_ms(current.timestamp_ms() + 1)
            .with_manifest_list(current.manifest_list())
            .schema_id_opt(current.schema_id())
            .with_summary(summary)
            .build();
        metadata
            .into_builder(None)
            .set_branch_snapshot(snapshot, "main")
            .expect("new snapshot is valid")
            .build()
            .expect("taxi metadata is valid")
            .metadata
    }
}
