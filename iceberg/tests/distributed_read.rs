#[cfg(all(test, feature = "integration"))]
mod tests {
    use datafusion::error::Result;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    #[tokio::test]
    async fn reads_iceberg_work_units_on_remote_workers() -> Result<()> {
        let harness = IcebergTestHarness::new_distributed().await?;
        let (plan, results) = harness
            .query("SELECT pickup_date, COUNT(*) AS trips FROM taxi GROUP BY pickup_date")
            .await?;

        assert!(plan.contains("Stage"), "{plan}");
        assert_eq!(results.matches("25000").count(), 7);
        Ok(())
    }
}
