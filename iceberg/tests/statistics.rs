#[cfg(all(feature = "integration", test))]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use datafusion::common::stats::Precision;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::common::{ColumnStatistics, Statistics, internal_datafusion_err};
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::datasource::source::{DataSource, DataSourceExec};
    use datafusion::error::Result;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion::scalar::ScalarValue;
    use datafusion_distributed::DistributedExt;
    use datafusion_distributed_iceberg::test_utils::{
        FIXTURE_URI, IcebergTestHarness, IcebergTestHarnessBuilder, empty_taxi_metadata_builder,
        taxi_metadata,
    };
    use datafusion_distributed_iceberg::{
        IcebergConfig, IcebergExt, IcebergFileSource, IcebergFileTask,
    };
    use iceberg::puffin::APACHE_DATASKETCHES_THETA_V1;
    use iceberg::spec::{
        BlobMetadata, DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, Operation,
        Snapshot, StatisticsFile, Struct, Summary, TableMetadata,
    };

    // Values from the checked-in taxi snapshot summary.
    const TAXI_ROWS: usize = 175_000;
    const TAXI_BYTES: usize = 4_480_382;
    const TAXI_COLUMNS: usize = 13;

    #[tokio::test]
    async fn eager_file_metrics_and_statistics_survive_transport() -> Result<(), Box<dyn Error>> {
        let harness = harness_with_manifest_metrics(true).await?;
        let original = harness.scan().await?;
        let decoded = harness.roundtrip_plan(Arc::clone(&original))?;
        for plan in [original, decoded] {
            let exec = plan.downcast_ref::<DataSourceExec>().unwrap();
            let scan = exec.data_source().downcast_ref::<FileScanConfig>().unwrap();
            assert!(scan.file_source.is::<IcebergFileSource>());
            let mut rows = 0;
            for (partition, group) in scan.file_groups.iter().enumerate() {
                let mut partition_rows = 0;
                for file in group.iter() {
                    let task = file.extension::<IcebergFileTask>().unwrap().task();
                    assert!(task.column_stats().is_some());
                    let stats = file.statistics.as_ref().unwrap();
                    assert_eq!(
                        stats.num_rows,
                        Precision::Exact(task.record_count().unwrap() as usize)
                    );
                    assert!(
                        stats
                            .column_statistics
                            .iter()
                            .any(|column| column.byte_size.get_value().is_some())
                    );
                    partition_rows += task.record_count().unwrap() as usize;
                }
                assert_eq!(
                    scan.partition_statistics(Some(partition))?.num_rows,
                    Precision::Exact(partition_rows)
                );
                rows += partition_rows;
            }
            assert_eq!(
                scan.partition_statistics(None)?.num_rows,
                Precision::Exact(rows)
            );
        }
        Ok(())
    }

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

    #[tokio::test]
    async fn planned_file_statistics_use_manifest_totals_when_snapshot_summary_is_missing()
    -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_table_metadata(metadata_without_summary_statistics())
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.eager_file_planning = true;
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .extensions
                    .insert(config);
                Ok(state)
            })?
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
        // The manifest's partition summary excludes this query. Poison its contents
        // so a redundant whole-snapshot column-statistics read fails the test.
        let harness = manifest_metrics_builder()
            .await?
            .with_file(
                format!("{FIXTURE_URI}/metadata/column-metrics.avro"),
                vec![],
            )
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.eager_file_planning = true;
                config.column_stats_enabled = true;
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .extensions
                    .insert(config);
                Ok(state)
            })?
            .build()
            .await?;
        let stats = query_statistics(
            &harness,
            "SELECT * FROM taxi WHERE pickup_date = DATE '1900-01-01'",
        )
        .await?;
        assert_eq!(stats.num_rows, Precision::Exact(0));
        for column in &stats.column_statistics {
            assert_eq!(column.null_count, Precision::Exact(0));
            assert_eq!(column.distinct_count, Precision::Exact(0));
        }
        Ok(())
    }

    #[tokio::test]
    async fn full_scan_without_column_stats() -> Result<(), Box<dyn Error>> {
        assert_manifest_statistics(false, false).await
    }

    #[tokio::test]
    async fn full_scan_with_column_stats() -> Result<(), Box<dyn Error>> {
        assert_manifest_statistics(true, false).await
    }

    #[tokio::test]
    async fn projection_without_column_stats() -> Result<(), Box<dyn Error>> {
        assert_manifest_statistics(false, true).await
    }

    #[tokio::test]
    async fn projection_with_column_stats() -> Result<(), Box<dyn Error>> {
        assert_manifest_statistics(true, true).await
    }

    #[tokio::test]
    async fn reports_statistics_for_the_selected_snapshot() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_table_metadata(historical_taxi_metadata())
            .with_table_option("iceberg.snapshot_id", "42")
            .build()
            .await?;
        let stats = query_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(stats.num_rows, Precision::Exact(42));
        assert_eq!(stats.total_byte_size, Precision::Exact(4_242));
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

    #[tokio::test]
    async fn count_star_skips_scan_without_column_stats() -> Result<()> {
        assert_count_star_skips_scan(false).await
    }

    #[tokio::test]
    async fn count_star_skips_scan_with_column_stats() -> Result<()> {
        assert_count_star_skips_scan(true).await
    }

    async fn assert_manifest_statistics(
        enabled: bool,
        projected: bool,
    ) -> Result<(), Box<dyn Error>> {
        let harness = harness_with_manifest_metrics(enabled).await?;
        let sql = if projected {
            "SELECT passenger_count, vendor_id, trip_distance FROM taxi"
        } else {
            "SELECT * FROM taxi"
        };
        let mut columns = vec![ColumnStatistics::new_unknown(); TAXI_COLUMNS];
        if enabled {
            columns[0] = ColumnStatistics {
                null_count: Precision::Exact(5),
                distinct_count: Precision::Inexact(100_000),
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

    async fn assert_count_star_skips_scan(enabled: bool) -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.eager_file_planning = true;
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 1;
                session.options_mut().extensions.insert(config);
                state
                    .with_iceberg_column_stats_enabled(enabled)
                    .with_distributed_file_scan_config_bytes_per_partition(1_000_000)
            })?
            .build()
            .await?;
        let (plan, batches) = harness.query("SELECT count(*) FROM taxi").await?;

        // Both named tests intentionally share these inline snapshots.
        insta::allow_duplicates! {
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn filtered_statistics_use_selected_files_and_cap_snapshot_ndv()
    -> Result<(), Box<dyn Error>> {
        let harness = harness_with_manifest_metrics(true).await?;
        let source = scan_node(
            &harness
                .physical_plan("SELECT vendor_id FROM taxi WHERE passenger_count < 25")
                .await?,
        )?;
        let stats = StatisticsContext::new().compute(source.as_ref(), &StatisticsArgs::new())?;
        let column = source.schema().index_of("vendor_id")?;
        assert_eq!(stats.num_rows, Precision::Inexact(87_500));
        // First file: 100 bytes. A retained-row fraction would incorrectly yield 200.
        assert_eq!(
            stats.column_statistics[column].byte_size,
            Precision::Inexact(100)
        );
        assert_eq!(
            stats.column_statistics[column].min_value,
            Precision::Inexact(ScalarValue::Int32(Some(2)))
        );
        assert_eq!(
            stats.column_statistics[column].max_value,
            Precision::Inexact(ScalarValue::Int32(Some(9)))
        );
        assert_eq!(
            stats.column_statistics[column].distinct_count,
            Precision::Inexact(87_500)
        );
        assert_eq!(
            stats.column_statistics[column].null_count,
            Precision::Inexact(2)
        );
        assert_eq!(stats.total_byte_size, Precision::Inexact(300));
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_filtered_metrics_keep_unknown_columns_and_file_byte_fallback()
    -> Result<(), Box<dyn Error>> {
        let harness = harness_with_manifest_metrics(true).await?;
        let source = scan_node(
            &harness
                .physical_plan("SELECT trip_distance FROM taxi WHERE passenger_count < 25")
                .await?,
        )?;
        let stats = StatisticsContext::new().compute(source.as_ref(), &StatisticsArgs::new())?;
        let column = source.schema().index_of("trip_distance")?;
        assert_eq!(stats.column_statistics[column].byte_size, Precision::Absent);
        assert_eq!(stats.total_byte_size, Precision::Inexact(2_240_191));
        Ok(())
    }

    #[tokio::test]
    async fn filtered_scan_merges_bounds_and_preserves_missing_null_counts()
    -> Result<(), Box<dyn Error>> {
        let harness = harness_with_manifest_metrics(true).await?;
        let source = scan_node(
            &harness
                .physical_plan(
                    "SELECT passenger_count, vendor_id FROM taxi WHERE passenger_count < 100",
                )
                .await?,
        )?;
        let stats = StatisticsContext::new().compute(source.as_ref(), &StatisticsArgs::new())?;
        let passenger = &stats.column_statistics[source.schema().index_of("passenger_count")?];
        assert_eq!(
            passenger.min_value,
            Precision::Inexact(ScalarValue::Int64(Some(10)))
        );
        assert_eq!(
            passenger.max_value,
            Precision::Inexact(ScalarValue::Int64(Some(40)))
        );
        assert_eq!(passenger.null_count, Precision::Absent);
        assert_eq!(passenger.distinct_count, Precision::Absent);
        let vendor = &stats.column_statistics[source.schema().index_of("vendor_id")?];
        assert_eq!(vendor.null_count, Precision::Inexact(5));
        assert_eq!(vendor.distinct_count, Precision::Inexact(100_000));
        Ok(())
    }

    #[tokio::test]
    async fn projected_bytes_do_not_reduce_scan_task_count() -> Result<(), Box<dyn Error>> {
        let harness = harness_with_manifest_metrics(true).await?;
        let source = scan_node(
            &harness
                .physical_plan("SELECT vendor_id FROM taxi WHERE passenger_count < 100")
                .await?,
        )?;
        let stats = StatisticsContext::new().compute(source.as_ref(), &StatisticsArgs::new())?;
        assert_eq!(stats.total_byte_size, Precision::Inexact(1000));
        // 4.48 MB of file work / 1 MB / 1 partition, capped at the two files.
        assert_eq!(harness.estimate_task_count(&source)?, Some(2));
        Ok(())
    }

    fn scan_node(plan: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut source = None;
        plan.apply(|node| {
            if node.is::<DataSourceExec>() {
                source = Some(Arc::clone(node));
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        source.ok_or_else(|| internal_datafusion_err!("expected an Iceberg scan"))
    }

    // Observe the query's output statistics, including projection and propagation.
    async fn query_statistics(harness: &IcebergTestHarness, sql: &str) -> Result<Arc<Statistics>> {
        let plan = harness.physical_plan(sql).await?;
        StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new())
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
        empty_taxi_metadata_builder()
            .set_branch_snapshot(snapshot, "main")
            .expect("taxi snapshot can be added")
            .build()
            .expect("taxi metadata is valid")
            .metadata
    }

    fn historical_taxi_metadata() -> TableMetadata {
        let metadata = taxi_metadata();
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
        empty_taxi_metadata_builder()
            .add_snapshot(historical)
            .expect("historical snapshot can be added")
            .set_branch_snapshot(current.as_ref().clone(), "main")
            .expect("taxi snapshot can be added")
            .build()
            .expect("taxi metadata is valid")
            .metadata
    }

    // Planning-only fixture, explicitly selecting the original snapshot. Synthetic data
    // paths are never opened. Metric IDs 1 and 4 are vendor_id and passenger_count, not
    // schema indexes; trip_distance (ID 5) has no metrics.
    async fn harness_with_manifest_metrics(
        enabled: bool,
    ) -> Result<IcebergTestHarness, Box<dyn Error>> {
        Ok(manifest_metrics_builder()
            .await?
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.eager_file_planning = true;
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 1;
                session.options_mut().extensions.insert(config);
                state
                    .with_iceberg_column_stats_enabled(enabled)
                    .with_distributed_file_scan_config_bytes_per_partition(1_000_000)
            })?
            .build()
            .await?)
    }

    async fn manifest_metrics_builder() -> Result<IcebergTestHarnessBuilder, Box<dyn Error>> {
        let mut file = DataFileBuilder::default();
        file.content(DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .record_count(87_500)
            .file_size_in_bytes(2_240_191)
            .partition(Struct::from_iter([Some(Literal::date_from_str(
                "2024-01-10",
            )?)]));
        let first = file
            .file_path(format!("{FIXTURE_URI}/data/metrics-first.parquet"))
            .null_value_counts([(1, 2), (4, 4)].into())
            .column_sizes([(1, 100), (4, 200)].into())
            .lower_bounds([(1, Datum::int(2)), (4, Datum::long(10))].into())
            .upper_bounds([(1, Datum::int(9)), (4, Datum::long(20))].into())
            .build()?;
        let second = file
            .file_path(format!("{FIXTURE_URI}/data/metrics-second.parquet"))
            .null_value_counts([(1, 3)].into())
            .column_sizes([(1, 300), (4, 400)].into())
            .lower_bounds([(1, Datum::int(1)), (4, Datum::long(30))].into())
            .upper_bounds([(1, Datum::int(5)), (4, Datum::long(40))].into())
            .build()?;
        let metadata = taxi_metadata();
        let snapshot = metadata.current_snapshot().unwrap();
        let statistics = StatisticsFile {
            snapshot_id: snapshot.snapshot_id(),
            statistics_path: format!("{FIXTURE_URI}/metadata/unopened-statistics.puffin"),
            file_size_in_bytes: 0,
            file_footer_size_in_bytes: 0,
            key_metadata: None,
            blob_metadata: vec![BlobMetadata {
                r#type: APACHE_DATASKETCHES_THETA_V1.to_owned(),
                snapshot_id: snapshot.snapshot_id(),
                sequence_number: snapshot.sequence_number(),
                fields: vec![1],
                properties: [("ndv".to_owned(), "100000".to_owned())].into(),
            }],
        };
        let metadata = metadata
            .into_builder(None)
            .set_statistics(statistics)
            .build()?
            .metadata;
        Ok(IcebergTestHarness::builder()
            .with_table_metadata(metadata)
            .with_data_files(
                &format!("{FIXTURE_URI}/metadata/column-metrics.avro"),
                [first, second],
            )
            .await?)
    }
}
