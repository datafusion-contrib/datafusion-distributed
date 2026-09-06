#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::Result;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionConfig;
    use datafusion_distributed::{DesiredTaskCountEvent, DistributedConfig, DistributedExt};
    use datafusion_distributed_iceberg::iceberg_desired_task_count;
    use datafusion_distributed_iceberg::test_utils::{
        IcebergTestHarness, taxi_metadata, taxi_metadata_builder,
    };
    use iceberg::spec::{Snapshot, TableMetadata};

    #[tokio::test]
    async fn estimates_current_snapshot() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        assert_eq!(estimate(&scan(&harness).await?)?, Some(3));
        Ok(())
    }

    #[tokio::test]
    async fn estimates_selected_snapshot_instead_of_current() -> Result<()> {
        let metadata = metadata_with_file_size(Some("24000000"));
        let selected_id = taxi_metadata()
            .current_snapshot_id()
            .expect("taxi has a snapshot");
        let current = IcebergTestHarness::builder()
            .with_table_metadata(metadata.clone())
            .build()
            .await?;
        let selected = IcebergTestHarness::builder()
            .with_table_metadata(metadata)
            .with_table_option("iceberg.snapshot_id", selected_id.to_string())
            .build()
            .await?;

        assert_eq!(estimate(&scan(&current).await?)?, Some(12));
        assert_eq!(estimate(&scan(&selected).await?)?, Some(3));
        Ok(())
    }

    #[tokio::test]
    async fn declines_missing_or_invalid_file_size() -> Result<()> {
        for size in [
            None,
            Some("invalid"),
            Some("-1"),
            Some("18446744073709551616"),
        ] {
            let harness = IcebergTestHarness::builder()
                .with_table_metadata(metadata_with_file_size(size))
                .build()
                .await?;
            assert_eq!(estimate(&scan(&harness).await?)?, None, "size: {size:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn empty_table_has_zero_scan_work() -> Result<()> {
        let metadata = taxi_metadata_builder()
            .build()
            .expect("empty taxi metadata is valid")
            .metadata;
        let harness = IcebergTestHarness::builder()
            .with_table_metadata(metadata)
            .build()
            .await?;
        assert_eq!(estimate(&scan(&harness).await?)?, Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn remote_feed_does_not_estimate_again() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness.roundtrip_plan(scan(&harness).await?)?;
        assert_eq!(estimate(&plan)?, None);
        Ok(())
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
