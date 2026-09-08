#[cfg(test)]
mod tests {
    use datafusion::common::Result;
    use datafusion_distributed_iceberg::test_utils::{
        IcebergTestHarness, empty_taxi_metadata_builder, taxi_metadata,
    };
    use iceberg::spec::{Snapshot, TableMetadata};

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn executes_with_estimated_scan_tasks() -> Result<()> {
        // 4,480,382 bytes / 1 MB / 2 partitions rounds up to 3 tasks, not all 4 workers.
        let harness = IcebergTestHarness::builder()
            .with_workers(4)
            .with_target_partitions(2)
            .with_scan_bytes_per_partition(1_000_000)
            .build()
            .await?;
        // Grouping prevents COUNT(*) from being answered from snapshot metadata alone.
        let (plan, results) = harness
            .query(
                "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
             GROUP BY pickup_date ORDER BY pickup_date",
            )
            .await?;
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

    #[tokio::test]
    async fn estimates_current_snapshot() -> Result<()> {
        assert_task_count(metadata_with_file_size(Some("24000000")), None, Some(12)).await
    }

    #[tokio::test]
    async fn estimates_selected_snapshot() -> Result<()> {
        assert_task_count(
            metadata_with_file_size(Some("24000000")),
            taxi_metadata().current_snapshot_id(),
            Some(3),
        )
        .await
    }

    #[tokio::test]
    async fn empty_table_needs_no_tasks() -> Result<()> {
        assert_task_count(empty_metadata(), None, Some(0)).await
    }

    #[tokio::test]
    async fn missing_size_does_not_estimate() -> Result<()> {
        assert_task_count(metadata_with_file_size(None), None, None).await
    }

    #[tokio::test]
    async fn malformed_size_does_not_estimate() -> Result<()> {
        assert_task_count(metadata_with_file_size(Some("invalid")), None, None).await
    }

    #[tokio::test]
    async fn negative_size_does_not_estimate() -> Result<()> {
        assert_task_count(metadata_with_file_size(Some("-1")), None, None).await
    }

    #[tokio::test]
    async fn overflowing_size_does_not_estimate() -> Result<()> {
        assert_task_count(
            metadata_with_file_size(Some("18446744073709551616")),
            None,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn remote_feed_does_not_estimate_again() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness.roundtrip_plan(harness.scan().await?)?;
        assert_eq!(harness.estimate_task_count(&plan)?, None);
        Ok(())
    }

    async fn assert_task_count(
        metadata: TableMetadata,
        snapshot_id: Option<i64>,
        expected: Option<usize>,
    ) -> Result<()> {
        let mut builder = IcebergTestHarness::builder()
            .with_table_metadata(metadata)
            .with_target_partitions(2)
            .with_scan_bytes_per_partition(1_000_000);
        if let Some(id) = snapshot_id {
            builder = builder.with_table_option("iceberg.snapshot_id", id.to_string());
        }
        let harness = builder.build().await?;
        assert_eq!(
            harness.estimate_task_count(&harness.scan().await?)?,
            expected
        );
        Ok(())
    }

    fn empty_metadata() -> TableMetadata {
        empty_taxi_metadata_builder()
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
