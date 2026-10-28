mod common;

mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use crate::common::{IcebergTestHarness, without_snapshots};

    use datafusion_distributed_iceberg_test_tools::{
        FixtureCatalog, create_taxi_table, taxi_schema,
    };

    use datafusion::common::stats::Precision;
    use datafusion::common::{ColumnStatistics, Statistics};
    use datafusion::error::Result;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion::scalar::ScalarValue;
    use datafusion_distributed_iceberg_test_tools::{ColumnMetrics, DescribedFile};
    use iceberg::TableIdent;
    use iceberg::spec::Transform;
    use iceberg::spec::{Datum, Literal, MAIN_BRANCH, Operation, Snapshot, Summary, TableMetadata};

    // Values from the generated taxi snapshot summary.
    const TAXI_ROWS: usize = 175_000;
    const TAXI_BYTES: usize = 5_656_412;
    const TAXI_COLUMNS: usize = 13;
    // The size of each of the two described files in the manifest-metrics tests.
    const METRICS_FILE_ROWS: u64 = 87_500;
    const METRICS_FILE_BYTES: u64 = 2_240_191;

    #[tokio::test]
    async fn missing_snapshot_summary_statistics_are_absent() -> Result<()> {
        let harness = IcebergTestHarness::builder()
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
            .with_table(create_taxi_table().await?)
            .configure_iceberg(|config| config.eager_file_planning = true)
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
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "taxi"])?, taxi_schema())
            .partition_by("pickup_date", Transform::Identity)
            .create()
            .await?;
        table
            .append()
            .described_file(
                DescribedFile::new(
                    "metrics-first.parquet",
                    METRICS_FILE_ROWS,
                    METRICS_FILE_BYTES,
                )
                .partition([Literal::date_from_str("2024-01-10")?])
                .column(
                    "vendor_id",
                    ColumnMetrics::default()
                        .null_count(2)
                        .size(100)
                        .bounds(Datum::int(2), Datum::int(9)),
                )
                .column(
                    "passenger_count",
                    ColumnMetrics::default()
                        .null_count(4)
                        .size(200)
                        .bounds(Datum::long(10), Datum::long(20)),
                ),
            )
            .described_file(
                DescribedFile::new(
                    "metrics-second.parquet",
                    METRICS_FILE_ROWS,
                    METRICS_FILE_BYTES,
                )
                .partition([Literal::date_from_str("2024-01-10")?])
                .column(
                    "vendor_id",
                    ColumnMetrics::default()
                        .null_count(3)
                        .size(300)
                        .bounds(Datum::int(1), Datum::int(5)),
                )
                // No null count, so the scan's total must stay unknown.
                .column(
                    "passenger_count",
                    ColumnMetrics::default()
                        .size(400)
                        .bounds(Datum::long(30), Datum::long(40)),
                ),
            )
            .commit()
            .await?;
        let harness = IcebergTestHarness::builder()
            .with_table(table)
            .with_table_option("iceberg.snapshot_id", "1")
            .with_eager_file_planning()
            .with_column_stats_enabled()
            .build()
            .await?;
        // The manifest's partition summary excludes this query. Delete the manifest
        // so a redundant whole-snapshot column-statistics read fails the test.
        let table = harness.table().expect("the harness has a table").table();
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
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "taxi"])?, taxi_schema())
            .partition_by("pickup_date", Transform::Identity)
            .create()
            .await?;
        table
            .append()
            .described_file(
                DescribedFile::new(
                    "metrics-first.parquet",
                    METRICS_FILE_ROWS,
                    METRICS_FILE_BYTES,
                )
                .partition([Literal::date_from_str("2024-01-10")?])
                .column(
                    "vendor_id",
                    ColumnMetrics::default()
                        .null_count(2)
                        .size(100)
                        .bounds(Datum::int(2), Datum::int(9)),
                )
                .column(
                    "passenger_count",
                    ColumnMetrics::default()
                        .null_count(4)
                        .size(200)
                        .bounds(Datum::long(10), Datum::long(20)),
                ),
            )
            .described_file(
                DescribedFile::new(
                    "metrics-second.parquet",
                    METRICS_FILE_ROWS,
                    METRICS_FILE_BYTES,
                )
                .partition([Literal::date_from_str("2024-01-10")?])
                .column(
                    "vendor_id",
                    ColumnMetrics::default()
                        .null_count(3)
                        .size(300)
                        .bounds(Datum::int(1), Datum::int(5)),
                )
                // No null count, so the scan's total must stay unknown.
                .column(
                    "passenger_count",
                    ColumnMetrics::default()
                        .size(400)
                        .bounds(Datum::long(30), Datum::long(40)),
                ),
            )
            .commit()
            .await?;
        let harness = IcebergTestHarness::builder()
            .with_table(table)
            .with_table_option("iceberg.snapshot_id", "1")
            .build()
            .await?;
        let stats = query_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(
            stats.num_rows,
            Precision::Exact(2 * METRICS_FILE_ROWS as usize)
        );
        assert_eq!(
            stats.total_byte_size,
            Precision::Exact(2 * METRICS_FILE_BYTES as usize)
        );
        assert_eq!(
            stats.column_statistics,
            vec![ColumnStatistics::new_unknown(); TAXI_COLUMNS]
        );
        Ok(())
    }

    #[tokio::test]
    async fn full_scan_with_column_stats() -> Result<(), Box<dyn Error>> {
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "taxi"])?, taxi_schema())
            .partition_by("pickup_date", Transform::Identity)
            .create()
            .await?;
        table
            .append()
            .described_file(
                DescribedFile::new(
                    "metrics-first.parquet",
                    METRICS_FILE_ROWS,
                    METRICS_FILE_BYTES,
                )
                .partition([Literal::date_from_str("2024-01-10")?])
                .column(
                    "vendor_id",
                    ColumnMetrics::default()
                        .null_count(2)
                        .size(100)
                        .bounds(Datum::int(2), Datum::int(9)),
                )
                .column(
                    "passenger_count",
                    ColumnMetrics::default()
                        .null_count(4)
                        .size(200)
                        .bounds(Datum::long(10), Datum::long(20)),
                ),
            )
            .described_file(
                DescribedFile::new(
                    "metrics-second.parquet",
                    METRICS_FILE_ROWS,
                    METRICS_FILE_BYTES,
                )
                .partition([Literal::date_from_str("2024-01-10")?])
                .column(
                    "vendor_id",
                    ColumnMetrics::default()
                        .null_count(3)
                        .size(300)
                        .bounds(Datum::int(1), Datum::int(5)),
                )
                // No null count, so the scan's total must stay unknown.
                .column(
                    "passenger_count",
                    ColumnMetrics::default()
                        .size(400)
                        .bounds(Datum::long(30), Datum::long(40)),
                ),
            )
            .commit()
            .await?;
        let harness = IcebergTestHarness::builder()
            .with_table(table)
            .with_table_option("iceberg.snapshot_id", "1")
            .with_column_stats_enabled()
            .build()
            .await?;
        let stats = query_statistics(&harness, "SELECT * FROM taxi").await?;

        let vendor_id = ColumnStatistics {
            null_count: Precision::Exact(5),
            min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
            max_value: Precision::Inexact(ScalarValue::Int32(Some(9))),
            byte_size: Precision::Inexact(400),
            ..ColumnStatistics::new_unknown()
        };
        let passenger_count = ColumnStatistics {
            min_value: Precision::Inexact(ScalarValue::Int64(Some(10))),
            max_value: Precision::Inexact(ScalarValue::Int64(Some(40))),
            byte_size: Precision::Inexact(600),
            ..ColumnStatistics::new_unknown()
        };
        assert_eq!(
            stats.num_rows,
            Precision::Exact(2 * METRICS_FILE_ROWS as usize)
        );
        assert_eq!(
            stats.total_byte_size,
            Precision::Exact(2 * METRICS_FILE_BYTES as usize)
        );
        assert_eq!(stats.column_statistics, {
            let mut columns = vec![ColumnStatistics::new_unknown(); TAXI_COLUMNS];
            columns[0] = vendor_id;
            columns[3] = passenger_count;
            columns
        });
        Ok(())
    }

    #[tokio::test]
    async fn projection_without_column_stats() -> Result<(), Box<dyn Error>> {
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "taxi"])?, taxi_schema())
            .partition_by("pickup_date", Transform::Identity)
            .create()
            .await?;
        table
            .append()
            .described_file(
                DescribedFile::new(
                    "metrics-first.parquet",
                    METRICS_FILE_ROWS,
                    METRICS_FILE_BYTES,
                )
                .partition([Literal::date_from_str("2024-01-10")?])
                .column(
                    "vendor_id",
                    ColumnMetrics::default()
                        .null_count(2)
                        .size(100)
                        .bounds(Datum::int(2), Datum::int(9)),
                )
                .column(
                    "passenger_count",
                    ColumnMetrics::default()
                        .null_count(4)
                        .size(200)
                        .bounds(Datum::long(10), Datum::long(20)),
                ),
            )
            .described_file(
                DescribedFile::new(
                    "metrics-second.parquet",
                    METRICS_FILE_ROWS,
                    METRICS_FILE_BYTES,
                )
                .partition([Literal::date_from_str("2024-01-10")?])
                .column(
                    "vendor_id",
                    ColumnMetrics::default()
                        .null_count(3)
                        .size(300)
                        .bounds(Datum::int(1), Datum::int(5)),
                )
                // No null count, so the scan's total must stay unknown.
                .column(
                    "passenger_count",
                    ColumnMetrics::default()
                        .size(400)
                        .bounds(Datum::long(30), Datum::long(40)),
                ),
            )
            .commit()
            .await?;
        let harness = IcebergTestHarness::builder()
            .with_table(table)
            .with_table_option("iceberg.snapshot_id", "1")
            .build()
            .await?;
        let stats = query_statistics(
            &harness,
            "SELECT passenger_count, vendor_id, trip_distance FROM taxi",
        )
        .await?;

        assert_eq!(
            stats.num_rows,
            Precision::Exact(2 * METRICS_FILE_ROWS as usize)
        );
        assert_eq!(
            stats.column_statistics,
            vec![ColumnStatistics::new_unknown(); 3]
        );
        Ok(())
    }

    #[tokio::test]
    async fn projection_with_column_stats() -> Result<(), Box<dyn Error>> {
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "taxi"])?, taxi_schema())
            .partition_by("pickup_date", Transform::Identity)
            .create()
            .await?;
        table
            .append()
            .described_file(
                DescribedFile::new(
                    "metrics-first.parquet",
                    METRICS_FILE_ROWS,
                    METRICS_FILE_BYTES,
                )
                .partition([Literal::date_from_str("2024-01-10")?])
                .column(
                    "vendor_id",
                    ColumnMetrics::default()
                        .null_count(2)
                        .size(100)
                        .bounds(Datum::int(2), Datum::int(9)),
                )
                .column(
                    "passenger_count",
                    ColumnMetrics::default()
                        .null_count(4)
                        .size(200)
                        .bounds(Datum::long(10), Datum::long(20)),
                ),
            )
            .described_file(
                DescribedFile::new(
                    "metrics-second.parquet",
                    METRICS_FILE_ROWS,
                    METRICS_FILE_BYTES,
                )
                .partition([Literal::date_from_str("2024-01-10")?])
                .column(
                    "vendor_id",
                    ColumnMetrics::default()
                        .null_count(3)
                        .size(300)
                        .bounds(Datum::int(1), Datum::int(5)),
                )
                // No null count, so the scan's total must stay unknown.
                .column(
                    "passenger_count",
                    ColumnMetrics::default()
                        .size(400)
                        .bounds(Datum::long(30), Datum::long(40)),
                ),
            )
            .commit()
            .await?;
        let harness = IcebergTestHarness::builder()
            .with_table(table)
            .with_table_option("iceberg.snapshot_id", "1")
            .with_column_stats_enabled()
            .build()
            .await?;
        let stats = query_statistics(
            &harness,
            "SELECT passenger_count, vendor_id, trip_distance FROM taxi",
        )
        .await?;

        let vendor_id = ColumnStatistics {
            null_count: Precision::Exact(5),
            min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
            max_value: Precision::Inexact(ScalarValue::Int32(Some(9))),
            byte_size: Precision::Inexact(400),
            ..ColumnStatistics::new_unknown()
        };
        let passenger_count = ColumnStatistics {
            min_value: Precision::Inexact(ScalarValue::Int64(Some(10))),
            max_value: Precision::Inexact(ScalarValue::Int64(Some(40))),
            byte_size: Precision::Inexact(600),
            ..ColumnStatistics::new_unknown()
        };
        assert_eq!(
            stats.num_rows,
            Precision::Exact(2 * METRICS_FILE_ROWS as usize)
        );
        assert_eq!(
            stats.column_statistics,
            vec![passenger_count, vendor_id, ColumnStatistics::new_unknown()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn reports_statistics_for_the_selected_snapshot() -> Result<()> {
        let harness = IcebergTestHarness::builder()
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
        let harness = IcebergTestHarness::builder()
            .with_table(create_taxi_table().await?)
            .build()
            .await?;
        let (plan, batches) = harness.query("SELECT count(*) FROM taxi").await?;

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
        let harness = IcebergTestHarness::builder()
            .with_table(create_taxi_table().await?)
            .with_column_stats_enabled()
            .build()
            .await?;
        let (plan, batches) = harness.query("SELECT count(*) FROM taxi").await?;

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
}
