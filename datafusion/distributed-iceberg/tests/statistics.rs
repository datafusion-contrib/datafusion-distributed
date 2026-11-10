mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use datafusion_distributed_iceberg_test_tools::{IcebergTestHarness, without_snapshots};

    use datafusion_distributed_iceberg::IcebergExt;

    use datafusion_distributed_iceberg_test_tools::{
        FixtureCatalog, FixtureTable, create_taxi_table,
    };

    use datafusion::arrow::array::record_batch;
    // `record_batch!` refers to `arrow_schema` by name.
    use datafusion::arrow::datatypes as arrow_schema;
    use datafusion::common::stats::Precision;
    use datafusion::common::{ColumnStatistics, Statistics};
    use datafusion::error::Result;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use datafusion::parquet::schema::types::ColumnPath;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion::scalar::ScalarValue;
    use iceberg::TableIdent;
    use iceberg::spec::{
        DataFile, MAIN_BRANCH, NestedField, Operation, PrimitiveType, Schema, Snapshot, Summary,
        TableMetadata, Type,
    };

    // Values from the generated taxi snapshot summary.
    const TAXI_ROWS: usize = 175_000;
    const TAXI_BYTES: usize = 5_656_412;
    // The rows across both files of the metrics table.
    const METRICS_ROWS: usize = 9;

    #[tokio::test]
    async fn missing_snapshot_summary_statistics_are_absent() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_metadata(metadata_without_summary_statistics)
            .build()
            .await?;
        let stats = query_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(stats.num_rows, Precision::Absent);
        assert_eq!(stats.total_byte_size, Precision::Absent);
        Ok(())
    }

    #[tokio::test]
    async fn planned_file_statistics_use_manifest_totals_when_snapshot_summary_is_missing()
    -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_metadata(metadata_without_summary_statistics)
            .build()
            .await?;
        let stats = query_statistics(&harness, "SELECT * FROM taxi").await?;
        assert_eq!(stats.num_rows, Precision::Exact(TAXI_ROWS));
        assert_eq!(stats.total_byte_size, Precision::Exact(TAXI_BYTES));

        let filtered = query_statistics(
            &harness,
            "SELECT pickup_date FROM taxi WHERE pickup_date = DATE '2024-01-10'",
        )
        .await?;
        assert!(matches!(filtered.num_rows, Precision::Inexact(_)));
        assert!(matches!(filtered.total_byte_size, Precision::Inexact(_)));
        Ok(())
    }

    #[tokio::test]
    async fn eager_filtered_scan_skips_unused_column_statistics() -> Result<(), Box<dyn Error>> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_table_option("iceberg.snapshot_id", "1")
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("iceberg.column_stats_enabled", true)
            .build()
            .await?;
        // The manifest's partition summary excludes this query. Delete the manifest
        // so a redundant whole-snapshot column-statistics read fails the test.
        let table = harness.table().table();
        let snapshot = table
            .metadata()
            .current_snapshot()
            .expect("taxi has a snapshot");
        for manifest in table.manifest_list_reader(snapshot).load().await?.entries() {
            table.file_io().delete(&manifest.manifest_path).await?;
        }
        let stats = query_statistics(
            &harness,
            "SELECT * FROM taxi WHERE pickup_date = DATE '1900-01-01'",
        )
        .await?;
        assert_eq!(stats.num_rows, Precision::Exact(0));
        Ok(())
    }

    #[tokio::test]
    async fn full_scan_without_column_stats() -> Result<(), Box<dyn Error>> {
        let (table, files) = create_metrics_table().await?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table)
            .with_table_option("iceberg.snapshot_id", "2")
            .build()
            .await?;
        let stats = query_statistics(&harness, "SELECT * FROM metrics").await?;

        assert_eq!(stats.num_rows, Precision::Exact(METRICS_ROWS));
        assert_eq!(
            stats.total_byte_size,
            Precision::Exact(
                files
                    .iter()
                    .map(|file| file.file_size_in_bytes() as usize)
                    .sum()
            )
        );
        assert_eq!(
            stats.column_statistics,
            vec![ColumnStatistics::new_unknown(); 3]
        );
        Ok(())
    }

    #[tokio::test]
    async fn full_scan_with_column_stats() -> Result<(), Box<dyn Error>> {
        let (table, files) = create_metrics_table().await?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table)
            .with_table_option("iceberg.snapshot_id", "2")
            .with_config_option("iceberg.column_stats_enabled", true)
            .build()
            .await?;
        let stats = query_statistics(&harness, "SELECT * FROM metrics").await?;

        assert_eq!(stats.num_rows, Precision::Exact(METRICS_ROWS));
        assert_eq!(
            stats.total_byte_size,
            Precision::Exact(
                files
                    .iter()
                    .map(|file| file.file_size_in_bytes() as usize)
                    .sum()
            )
        );
        assert_eq!(
            stats.column_statistics,
            expected_column_statistics(&files).to_vec()
        );
        Ok(())
    }

    #[tokio::test]
    async fn projection_without_column_stats() -> Result<(), Box<dyn Error>> {
        let (table, _) = create_metrics_table().await?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table)
            .with_table_option("iceberg.snapshot_id", "2")
            .build()
            .await?;
        let stats = query_statistics(
            &harness,
            "SELECT passenger_count, vendor_id, trip_distance FROM metrics",
        )
        .await?;

        assert_eq!(stats.num_rows, Precision::Exact(METRICS_ROWS));
        assert_eq!(
            stats.column_statistics,
            vec![ColumnStatistics::new_unknown(); 3]
        );
        Ok(())
    }

    #[tokio::test]
    async fn projection_with_column_stats() -> Result<(), Box<dyn Error>> {
        let (table, files) = create_metrics_table().await?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table)
            .with_table_option("iceberg.snapshot_id", "2")
            .with_config_option("iceberg.column_stats_enabled", true)
            .build()
            .await?;
        let stats = query_statistics(
            &harness,
            "SELECT passenger_count, vendor_id, trip_distance FROM metrics",
        )
        .await?;

        assert_eq!(stats.num_rows, Precision::Exact(METRICS_ROWS));
        let [vendor_id, passenger_count, trip_distance] = expected_column_statistics(&files);
        assert_eq!(
            stats.column_statistics,
            vec![passenger_count, vendor_id, trip_distance]
        );
        Ok(())
    }

    #[tokio::test]
    async fn reports_statistics_for_the_selected_snapshot() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_table_option("iceberg.snapshot_id", "42")
            .with_metadata(historical_taxi_metadata)
            .build()
            .await?;
        let stats = query_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(stats.num_rows, Precision::Exact(42));
        assert_eq!(stats.total_byte_size, Precision::Exact(4_242));
        Ok(())
    }

    #[tokio::test]
    async fn statistics_propagate_through_filter() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .build()
            .await?;
        let stats = query_statistics(
            &harness,
            "SELECT vendor_id FROM taxi WHERE pickup_date = DATE '2024-01-10'",
        )
        .await?;

        assert!(matches!(stats.num_rows, Precision::Inexact(_)));
        assert_eq!(stats.column_statistics.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn statistics_propagate_through_projection_and_sort() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .build()
            .await?;
        let stats = query_statistics(
            &harness,
            "SELECT vendor_id, trip_distance FROM taxi ORDER BY pickup_at",
        )
        .await?;

        assert_eq!(stats.num_rows, Precision::Exact(TAXI_ROWS));
        assert_eq!(stats.column_statistics.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn explain_shows_statistics_on_the_iceberg_source() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .build()
            .await?;
        let plan = harness.physical_plan("SELECT vendor_id FROM taxi").await?;
        let display = displayable(plan.as_ref())
            .set_show_statistics(true)
            .indent(true)
            .to_string();
        insta::assert_snapshot!(display, @r"
        CooperativeExec, statistics=[Rows=Exact(175000), Bytes=Exact(5656412), [(Col[0]:)]]
          DataSourceExec: format=iceberg, table=default.taxi, projection=[vendor_id], statistics=[Rows=Exact(175000), Bytes=Exact(5656412), [(Col[0]:)]]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn count_star_skips_scan_without_column_stats() -> Result<()> {
        let (plan, batches) = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .query("SELECT count(*) FROM taxi")
            .await?;

        insta::assert_snapshot!(plan, @"
        ProjectionExec: expr=[175000 as count(*)]
          PlaceholderRowExec
        ");
        insta::assert_snapshot!(batches, @"
        +----------+
        | count(*) |
        +----------+
        | 175000   |
        +----------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn count_star_skips_scan_with_column_stats() -> Result<()> {
        let (plan, batches) = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.column_stats_enabled", true)
            .query("SELECT count(*) FROM taxi")
            .await?;

        insta::assert_snapshot!(plan, @"
        ProjectionExec: expr=[175000 as count(*)]
          PlaceholderRowExec
        ");
        insta::assert_snapshot!(batches, @"
        +----------+
        | count(*) |
        +----------+
        | 175000   |
        +----------+
        ");
        Ok(())
    }

    // Observe the query's output statistics, including projection and propagation.
    async fn query_statistics(harness: &IcebergTestHarness, sql: &str) -> Result<Arc<Statistics>> {
        let plan = harness.physical_plan(sql).await?;
        StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new())
    }

    fn metadata_without_summary_statistics(metadata: &TableMetadata) -> TableMetadata {
        let current = metadata.current_snapshot().expect("taxi has a snapshot");
        let snapshot = Snapshot::builder()
            .with_snapshot_id(current.snapshot_id())
            .with_parent_snapshot_id(current.parent_snapshot_id())
            .with_sequence_number(current.sequence_number())
            .with_timestamp_ms(current.timestamp_ms())
            .with_manifest_list(current.manifest_list())
            .schema_id_opt(current.schema_id())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: Default::default(),
            })
            .build();
        without_snapshots(metadata)
            .set_branch_snapshot(snapshot, MAIN_BRANCH)
            .expect("taxi snapshot can be added")
            .build()
            .expect("taxi metadata is valid")
            .metadata
    }

    fn historical_taxi_metadata(metadata: &TableMetadata) -> TableMetadata {
        let current = metadata.current_snapshot().expect("taxi has a snapshot");
        // The summary totals are synthetic; the manifest still describes the full taxi data.
        // This fixture is for statistics planning, not executing a 42-row scan.
        let historical = Snapshot::builder()
            .with_snapshot_id(42)
            .with_sequence_number(current.sequence_number() - 1)
            .with_timestamp_ms(current.timestamp_ms() - 1)
            .with_manifest_list(current.manifest_list())
            .schema_id_opt(current.schema_id())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: [
                    ("total-records".to_string(), "42".to_string()),
                    ("total-files-size".to_string(), "4242".to_string()),
                ]
                .into_iter()
                .collect(),
            })
            .build();
        without_snapshots(metadata)
            .add_snapshot(historical)
            .expect("historical snapshot can be added")
            .set_branch_snapshot(current.as_ref().clone(), MAIN_BRANCH)
            .expect("taxi snapshot can be added")
            .build()
            .expect("taxi metadata is valid")
            .metadata
    }

    // A table of two files on the current snapshot, committed separately. The second is
    // written without Parquet statistics for passenger_count, so its manifest entry has a
    // size for that column but no null count or bounds.
    async fn create_metrics_table() -> Result<(FixtureTable, Vec<DataFile>), Box<dyn Error>> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "vendor_id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "passenger_count", Type::Primitive(PrimitiveType::Long))
                    .into(),
                NestedField::optional(3, "trip_distance", Type::Primitive(PrimitiveType::Double))
                    .into(),
            ])
            .build()?;
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "metrics"])?, schema)
            .create()
            .await?;
        let mut files = table
            .append()
            .file(record_batch!(
                ("vendor_id", Int32, [Some(2), Some(9), None, None]),
                ("passenger_count", Int64, [Some(10), Some(20), None, None]),
                ("trip_distance", Float64, [1.5, 2.5, 0.5, 3.0])
            )?)
            .commit()
            .await?;
        let without_passenger_count_statistics = WriterProperties::builder()
            .set_column_statistics_enabled(
                ColumnPath::from("passenger_count"),
                EnabledStatistics::None,
            )
            .build();
        files.extend(
            table
                .append()
                .writer_properties(without_passenger_count_statistics)
                .file(record_batch!(
                    ("vendor_id", Int32, [Some(1), Some(5), None, None, None]),
                    ("passenger_count", Int64, [30, 40, 35, 33, 31]),
                    ("trip_distance", Float64, [4.0, 0.25, 1.0, 2.0, 3.0])
                )?)
                .commit()
                .await?,
        );
        Ok((table, files))
    }

    // The column statistics of the metrics table, in schema order, with column stats enabled.
    fn expected_column_statistics(files: &[DataFile]) -> [ColumnStatistics; 3] {
        let size = |field_id| {
            let size = files
                .iter()
                .map(|file| file.column_sizes()[&field_id])
                .sum::<u64>();
            Precision::Inexact(size as usize)
        };
        [
            ColumnStatistics {
                null_count: Precision::Exact(5),
                min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(9))),
                byte_size: size(1),
                ..ColumnStatistics::new_unknown()
            },
            // The second file has no null count or bounds for passenger_count, so the totals
            // must stay unknown.
            ColumnStatistics {
                byte_size: size(2),
                ..ColumnStatistics::new_unknown()
            },
            ColumnStatistics {
                null_count: Precision::Exact(0),
                min_value: Precision::Inexact(ScalarValue::Float64(Some(0.25))),
                max_value: Precision::Inexact(ScalarValue::Float64(Some(4.0))),
                byte_size: size(3),
                ..ColumnStatistics::new_unknown()
            },
        ]
    }
}
