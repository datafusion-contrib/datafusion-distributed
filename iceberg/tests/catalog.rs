#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::catalog::CatalogProvider;
    use datafusion::error::{DataFusionError, Result};
    use datafusion_distributed_iceberg::IcebergCatalog;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;
    use iceberg::{NamespaceIdent, Runtime};

    #[tokio::test]
    async fn exposes_namespaces_as_schemas() -> Result<()> {
        let harness = catalog_harness().await?;
        let (_, batches) = harness
            .query(
                "SELECT table_catalog, table_schema, table_name, table_type \
                 FROM information_schema.tables \
                 WHERE table_catalog = 'iceberg' AND table_schema <> 'information_schema'",
            )
            .await?;

        insta::assert_snapshot!(batches, @"
        +---------------+--------------+------------+------------+
        | table_catalog | table_schema | table_name | table_type |
        +---------------+--------------+------------+------------+
        | iceberg       | nyc          | taxi       | BASE TABLE |
        +---------------+--------------+------------+------------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn resolves_a_fully_qualified_table() -> Result<()> {
        let harness = catalog_harness().await?;
        let (_, batches) = harness
            .query("SELECT COUNT(*) AS trips FROM iceberg.nyc.taxi")
            .await?;

        insta::assert_snapshot!(batches, @"
        +--------+
        | trips  |
        +--------+
        | 175000 |
        +--------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn resolves_a_table_through_the_default_catalog_and_schema() -> Result<()> {
        let harness = catalog_harness().await?;
        let (_, batches) = harness.query("DESCRIBE taxi").await?;

        insta::assert_snapshot!(batches, @"
        +---------------------+---------------+-------------+
        | column_name         | data_type     | is_nullable |
        +---------------------+---------------+-------------+
        | vendor_id           | Int32         | YES         |
        | pickup_at           | Timestamp(µs) | YES         |
        | dropoff_at          | Timestamp(µs) | YES         |
        | passenger_count     | Int64         | YES         |
        | trip_distance       | Float64       | YES         |
        | pickup_location_id  | Int32         | YES         |
        | dropoff_location_id | Int32         | YES         |
        | payment_type        | Int64         | YES         |
        | fare_amount         | Float64       | YES         |
        | tip_amount          | Float64       | YES         |
        | tolls_amount        | Float64       | YES         |
        | total_amount        | Float64       | YES         |
        | pickup_date         | Date32        | YES         |
        +---------------------+---------------+-------------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn scans_through_the_catalog_table_provider() -> Result<()> {
        let harness = catalog_harness().await?;
        let (plan, batches) = harness
            .query(
                "SELECT vendor_id, pickup_date FROM nyc.taxi \
                 WHERE pickup_date = DATE '2024-01-10' LIMIT 3",
            )
            .await?;

        insta::assert_snapshot!(plan + &batches, @"
        CoalescePartitionsExec: fetch=3
          FilterExec: pickup_date@1 = 2024-01-10, fetch=3
            DataSourceExec: format=iceberg, projection=[vendor_id, pickup_date], predicate=pickup_date = 2024-01-10
        +-----------+-------------+
        | vendor_id | pickup_date |
        +-----------+-------------+
        | 2         | 2024-01-10  |
        | 1         | 2024-01-10  |
        | 2         | 2024-01-10  |
        +-----------+-------------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn rejects_an_unknown_table() -> Result<()> {
        let harness = catalog_harness().await?;
        let error = harness
            .query("SELECT * FROM nyc.missing")
            .await
            .unwrap_err();

        insta::assert_snapshot!(error.to_string(), @"Error during planning: table 'iceberg.nyc.missing' not found");
        Ok(())
    }

    #[tokio::test]
    async fn rejects_an_unknown_namespace() -> Result<()> {
        let harness = catalog_harness().await?;
        let error = harness
            .query("SELECT * FROM iceberg.missing.taxi")
            .await
            .unwrap_err();

        insta::assert_snapshot!(error.to_string(), @"Error during planning: table 'iceberg.missing.taxi' not found");
        Ok(())
    }

    #[tokio::test]
    async fn exposes_a_namespace_without_tables_as_an_empty_schema() -> Result<()> {
        let harness = catalog_harness().await?;
        let client = harness.iceberg_catalog()?;
        client
            .create_namespace(&NamespaceIdent::new("empty".to_string()), HashMap::new())
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let catalog = IcebergCatalog::try_new(client, Runtime::current()).await?;

        let mut schema_names = catalog.schema_names();
        schema_names.sort();
        assert_eq!(schema_names, vec!["empty", "nyc"]);
        let empty = catalog
            .schema("empty")
            .expect("empty namespace must resolve");
        assert!(empty.table_names().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_registering_tables_through_sql() -> Result<()> {
        let harness = catalog_harness().await?;
        let error = harness
            .query("CREATE TABLE nyc.trips (id INT)")
            .await
            .unwrap_err();

        insta::assert_snapshot!(error.to_string(), @"Execution error: schema provider does not support registering tables");
        Ok(())
    }

    #[tokio::test]
    async fn rejects_dropping_tables_through_sql() -> Result<()> {
        let harness = catalog_harness().await?;
        let error = harness.query("DROP TABLE nyc.taxi").await.unwrap_err();

        // DataFusion reports any deregistration failure as a missing table.
        insta::assert_snapshot!(error.to_string(), @"Execution error: Table 'nyc.taxi' doesn't exist.");
        assert!(
            harness
                .query("SELECT 1 FROM nyc.taxi LIMIT 1")
                .await
                .is_ok()
        );
        Ok(())
    }

    #[tokio::test]
    async fn rejects_table_options_with_a_catalog() -> Result<()> {
        let error = IcebergTestHarness::builder()
            .with_catalog()
            .with_table_option("iceberg.snapshot_id", "42")
            .build()
            .await
            .err()
            .expect("table options must be rejected with a catalog-backed fixture");

        insta::assert_snapshot!(error.to_string(), @"Error during planning: table options are not supported with a catalog-backed fixture");
        Ok(())
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn executes_a_catalog_table_across_workers() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_catalog()
            .with_workers(2)
            .build()
            .await?;
        // Grouping prevents COUNT(*) from being answered from snapshot metadata alone.
        let (plan, batches) = harness
            .query(
                "SELECT vendor_id, COUNT(*) AS trips FROM iceberg.nyc.taxi \
                 GROUP BY vendor_id ORDER BY vendor_id",
            )
            .await?;

        insta::assert_snapshot!(plan + &batches, @"
        SortPreservingMergeExec: [vendor_id@0 ASC NULLS LAST]
          ProjectionExec: expr=[vendor_id@0 as vendor_id, count(Int64(1))@1 as trips]
            SortExec: expr=[vendor_id@0 ASC NULLS LAST], preserve_partitioning=[true]
              AggregateExec: mode=FinalPartitioned, gby=[vendor_id@0 as vendor_id], aggr=[count(Int64(1))]
                RepartitionExec: partitioning=Hash([vendor_id@0], 4), input_partitions=4
                  AggregateExec: mode=Partial, gby=[vendor_id@0 as vendor_id], aggr=[count(Int64(1))]
                    DataSourceExec: format=iceberg, projection=[vendor_id]
        +-----------+--------+
        | vendor_id | trips  |
        +-----------+--------+
        | 1         | 45743  |
        | 2         | 129212 |
        | 6         | 45     |
        +-----------+--------+
        ");
        Ok(())
    }

    /// Builds a catalog-backed fixture with `information_schema` enabled.
    async fn catalog_harness() -> Result<IcebergTestHarness> {
        let harness = IcebergTestHarness::builder().with_catalog().build().await?;
        harness
            .query("SET datafusion.catalog.information_schema = true")
            .await?;
        Ok(harness)
    }
}
