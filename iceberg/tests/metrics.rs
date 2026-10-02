#[cfg(all(test, feature = "integration"))]
mod tests {
    use datafusion::common::Result;
    use datafusion_distributed::DistributedExt;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    #[tokio::test]
    async fn reports_scan_and_work_unit_metrics() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_workers(3)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let (_, results) = harness
            .query("EXPLAIN ANALYZE VERBOSE SELECT vendor_id FROM taxi")
            .await?;
        let scan = results
            .lines()
            .find(|line| line.contains("DataSourceExec: format=iceberg"))
            .expect("distributed explain contains the Iceberg scan");
        assert!(scan.contains("work_unit_count=7"), "{scan}");
        for name in [
            "output_rows=175.0 K",
            "batches_split=0",
            "work_unit_bytes=",
            "work_unit_send_latency_max=",
            "work_unit_received_latency_max=",
            "work_unit_processed_latency_max=",
        ] {
            assert!(scan.contains(name), "missing {name}: {scan}");
        }
        Ok(())
    }
}
