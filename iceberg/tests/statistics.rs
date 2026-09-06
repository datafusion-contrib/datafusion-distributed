mod common;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use datafusion::common::stats::Precision;
    use datafusion::common::{ColumnStatistics, Statistics};
    use datafusion::error::Result;
    use datafusion::physical_plan::displayable;
    use datafusion::scalar::ScalarValue;
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg::test_utils::{
        FIXTURE_URI, IcebergTestHarness, taxi_metadata, taxi_metadata_builder,
    };
    use iceberg::io::{MemoryStorage, Storage};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestListWriter,
        ManifestWriterBuilder, Operation, Snapshot, Struct, Summary, TableMetadata,
    };
    use test_case::test_case;

    use crate::common::assert_scalar_result;

    // Values from the checked-in taxi snapshot summary.
    const TAXI_ROWS: usize = 175_000;
    const TAXI_BYTES: usize = 4_480_382;
    const TAXI_COLUMNS: usize = 13;

    #[tokio::test]
    async fn missing_snapshot_summary_statistics_are_absent() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_table_metadata(metadata_without_summary_statistics())
            .build()
            .await?;
        let stats = query_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(stats.num_rows, Precision::Absent);
        assert_eq!(stats.total_byte_size, Precision::Absent);
        Ok(())
    }

    #[test_case(false, false ; "full_scan_without_column_stats")]
    #[test_case(true, false ; "full_scan_with_column_stats")]
    #[test_case(false, true ; "projection_without_column_stats")]
    #[test_case(true, true ; "projection_with_column_stats")]
    #[tokio::test]
    async fn reports_manifest_statistics(
        enabled: bool,
        projected: bool,
    ) -> Result<(), Box<dyn Error>> {
        let mut harness = harness_with_manifest_metrics().await?;
        harness.ctx.set_iceberg_column_stats_enabled(enabled);
        let sql = if projected {
            "SELECT passenger_count, vendor_id, trip_distance FROM taxi"
        } else {
            "SELECT * FROM taxi"
        };
        let mut columns = vec![ColumnStatistics::new_unknown(); TAXI_COLUMNS];
        if enabled {
            columns[0] = ColumnStatistics {
                null_count: Precision::Exact(5),
                min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(9))),
                byte_size: Precision::Inexact(400),
                ..ColumnStatistics::new_unknown()
            };
            columns[3] = ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Int64(Some(10))),
                max_value: Precision::Inexact(ScalarValue::Int64(Some(40))),
                byte_size: Precision::Inexact(600),
                // One file omits this column's null count: the total must stay unknown.
                ..ColumnStatistics::new_unknown()
            };
        }
        if projected {
            columns = vec![columns[3].clone(), columns[0].clone(), columns[4].clone()];
        }
        let stats = query_statistics(&harness, sql).await?;
        assert_eq!(stats.num_rows, Precision::Exact(TAXI_ROWS));
        if !projected {
            assert_eq!(stats.total_byte_size, Precision::Exact(TAXI_BYTES));
        }
        assert_eq!(stats.column_statistics, columns);
        Ok(())
    }

    #[tokio::test]
    async fn statistics_propagate_through_filter() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
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
        let harness = IcebergTestHarness::new().await?;
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
        let harness = IcebergTestHarness::new().await?;
        let plan = harness.physical_plan("SELECT vendor_id FROM taxi").await?;
        let display = displayable(plan.as_ref())
            .set_show_statistics(true)
            .indent(true)
            .to_string();
        insta::assert_snapshot!(display, @"
        CooperativeExec, statistics=[Rows=Exact(175000), Bytes=Exact(4480382), [(Col[0]:)]]
          DataSourceExec: format=iceberg, projection=[vendor_id], statistics=[Rows=Exact(175000), Bytes=Exact(4480382), [(Col[0]:)]]
        ");
        Ok(())
    }

    #[test_case(false ; "without_column_stats")]
    #[test_case(true ; "with_column_stats")]
    #[tokio::test]
    async fn exact_row_count_lets_count_star_skip_the_scan(enabled: bool) -> Result<()> {
        let mut harness = IcebergTestHarness::new().await?;
        harness.ctx.set_iceberg_column_stats_enabled(enabled);
        let (plan, batches) = harness.query_raw("SELECT count(*) FROM taxi").await?;

        assert_eq!(plan.name(), "ProjectionExec");
        let children = plan.children();
        assert_eq!(children.len(), 1, "count projection must have one child");
        assert_eq!(children[0].name(), "PlaceholderRowExec");
        assert_scalar_result(&batches, "count(*)", (TAXI_ROWS as i64).into())
    }

    // Observe the query's output statistics, including projection and propagation.
    async fn query_statistics(harness: &IcebergTestHarness, sql: &str) -> Result<Arc<Statistics>> {
        harness.physical_plan(sql).await?.partition_statistics(None)
    }

    fn metadata_without_summary_statistics() -> TableMetadata {
        let metadata = taxi_metadata();
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
        taxi_metadata_builder()
            .set_branch_snapshot(snapshot, "main")
            .expect("taxi snapshot can be added")
            .build()
            .expect("taxi metadata is valid")
            .metadata
    }

    // Planning-only fixture, explicitly selecting the original snapshot. Synthetic data
    // paths are never opened. Metric IDs 1 and 4 are vendor_id and passenger_count, not
    // schema indexes; trip_distance (ID 5) has no metrics.
    async fn harness_with_manifest_metrics() -> Result<IcebergTestHarness, Box<dyn Error>> {
        let metadata = taxi_metadata();
        let snapshot = metadata.current_snapshot().expect("taxi has a snapshot");
        let storage = MemoryStorage::new();
        let uri = format!("{FIXTURE_URI}/metadata/column-metrics.avro");
        let mut writer = ManifestWriterBuilder::new(
            storage.new_output(&uri)?,
            Some(snapshot.snapshot_id()),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        let mut file = DataFileBuilder::default();
        file.content(DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .record_count(87_500)
            .file_size_in_bytes(2_240_191)
            .partition(Struct::from_iter([Some(Literal::date_from_str(
                "2024-01-10",
            )?)]));
        writer.add_file(
            file.file_path(format!("{FIXTURE_URI}/data/metrics-first.parquet"))
                .null_value_counts([(1, 2), (4, 4)].into())
                .column_sizes([(1, 100), (4, 200)].into())
                .lower_bounds([(1, Datum::int(2)), (4, Datum::long(10))].into())
                .upper_bounds([(1, Datum::int(9)), (4, Datum::long(20))].into())
                .build()?,
            snapshot.sequence_number(),
        )?;
        writer.add_file(
            file.file_path(format!("{FIXTURE_URI}/data/metrics-second.parquet"))
                .null_value_counts([(1, 3)].into())
                .column_sizes([(1, 300), (4, 400)].into())
                .lower_bounds([(1, Datum::int(1)), (4, Datum::long(30))].into())
                .upper_bounds([(1, Datum::int(5)), (4, Datum::long(40))].into())
                .build()?,
            snapshot.sequence_number(),
        )?;
        let manifest = writer.write_manifest_file().await?;
        let mut list = ManifestListWriter::v2(
            storage
                .new_output(snapshot.manifest_list())?
                .writer()
                .await?,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        );
        list.add_manifests([manifest].into_iter())?;
        list.close().await?;
        Ok(IcebergTestHarness::builder()
            .with_file(&uri, storage.read(&uri).await?.to_vec())
            .with_file(
                snapshot.manifest_list(),
                storage.read(snapshot.manifest_list()).await?.to_vec(),
            )
            .with_table_option("iceberg.snapshot_id", snapshot.snapshot_id().to_string())
            .with_table_metadata(metadata)
            .build()
            .await?)
    }
}
