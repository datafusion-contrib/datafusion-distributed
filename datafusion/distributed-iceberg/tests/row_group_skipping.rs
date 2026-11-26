mod tests {
    use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion_distributed::test_utils::metrics::node_metrics;
    use datafusion_distributed::{
        DistributedExt, DistributedLeafExec, DistributedMetricsFormat,
        rewrite_distributed_plan_with_metrics,
    };
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg_test_tools::{
        FixtureCatalog, FixtureTable, IcebergTestHarness,
    };
    use iceberg::TableIdent;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use parquet::file::properties::WriterProperties;
    use std::error::Error;
    use std::sync::Arc;

    // The join keeps one id from each of the two `events` files.
    const TWO_IDS: &str = "SELECT events.id FROM events \
        JOIN (VALUES (5), (150005)) AS ids(id) ON events.id = ids.id \
        ORDER BY events.id";

    #[tokio::test]
    async fn the_reader_skips_row_groups_the_join_filter_rules_out() -> Result<(), Box<dyn Error>> {
        let query = "SELECT events.id, events.payload FROM events \
            JOIN (VALUES (150005), (150010)) AS ids(id) ON events.id = ids.id \
            ORDER BY events.id";
        let with_filter = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(events_by_id().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .build()
            .await?;
        let without_filter = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(events_by_id().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", false)
            .build()
            .await?;

        let (with_filter_results, with_filter_plan) = run(&with_filter, query).await?;
        let (without_filter_results, without_filter_plan) = run(&without_filter, query).await?;

        insta::assert_snapshot!(with_filter_results, @"
        +--------+----------------------------------------+
        | id     | payload                                |
        +--------+----------------------------------------+
        | 150005 | event                           150005 |
        | 150010 | event                           150010 |
        +--------+----------------------------------------+
        ");
        assert_eq!(with_filter_results, without_filter_results);
        // `DataSourceExec` 0 is the join's `VALUES` table, and 1 is the `events` scan.
        // Both ids are in one row group of the second file, so with the filter iceberg-rust's
        // `ArrowReader` reads 1 of the 40 row groups. Both runs also read the last 512 KiB of each
        // file to get its footer, so we only check that the filter cuts the bytes read by more
        // than 5x.
        let with_filter_bytes =
            node_metrics::<DataSourceExec>(&with_filter_plan, "bytes_scanned", 1);
        let without_filter_bytes =
            node_metrics::<DataSourceExec>(&without_filter_plan, "bytes_scanned", 1);
        assert!(
            with_filter_bytes * 5 < without_filter_bytes,
            "read {with_filter_bytes} bytes with the join filter and {without_filter_bytes} without"
        );
        Ok(())
    }

    #[tokio::test]
    async fn eager_planning_workers_give_the_reader_the_join_filter() -> Result<(), Box<dyn Error>>
    {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(events_by_id().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .with_workers(2)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;

        let (results, plan) = run(&harness, TWO_IDS).await?;

        insta::assert_snapshot!(results, @"
        +--------+
        | id     |
        +--------+
        | 5      |
        | 150005 |
        +--------+
        ");
        // The coordinator plans the files and sends them to the workers. Each worker still gives
        // `ArrowReader` the join filter, so `ArrowReader` drops the rows it rules out and the scan
        // returns only the two ids.
        assert_eq!(
            node_metrics::<DistributedLeafExec>(&plan, "output_rows", 0),
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_top_n_filter_reaches_files_that_start_later() -> Result<(), Box<dyn Error>> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(events_by_id().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            // Read one file at a time, so the top-N filter can tighten before the next file starts.
            .with_config_option("iceberg.data_file_concurrency_limit", 1)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 1;
                Ok(state)
            })?
            .build()
            .await?;

        let (results, plan) = run(&harness, "SELECT id FROM events ORDER BY id LIMIT 3").await?;

        insta::assert_snapshot!(results, @"
        +----+
        | id |
        +----+
        | 0  |
        | 1  |
        | 2  |
        +----+
        ");
        // When the first file starts, the top 3 aren't known yet, so the scan returns all 100,000
        // of its rows. By the time the second file starts, they are, and `ArrowReader` drops
        // every row in it.
        assert_eq!(
            node_metrics::<DataSourceExec>(&plan, "output_rows", 0),
            100_000
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_reader_keeps_the_where_filter_too() -> Result<(), Box<dyn Error>> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(events_by_id().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .build()
            .await?;

        let (results, plan) = run(
            &harness,
            "SELECT events.id, events.kind FROM events \
             JOIN (VALUES (5), (60000), (150004)) AS ids(id) ON events.id = ids.id \
             WHERE events.kind = 'click' ORDER BY events.id",
        )
        .await?;

        insta::assert_snapshot!(results, @"
        +--------+-------+
        | id     | kind  |
        +--------+-------+
        | 60000  | click |
        | 150004 | click |
        +--------+-------+
        ");
        // `ArrowReader` applies both filters. The join filter keeps ids 5, 60000 and 150004. The
        // `WHERE` filter keeps only clicks, which drops the odd id 5. If adding the join filter
        // lost the `WHERE` filter, the scan would return 3 rows.
        assert_eq!(node_metrics::<DataSourceExec>(&plan, "output_rows", 1), 2);
        Ok(())
    }

    /// Runs `sql`, and returns its results and its plan with the metrics from the run.
    async fn run(
        harness: &IcebergTestHarness,
        sql: &str,
    ) -> Result<(String, Arc<dyn ExecutionPlan>), Box<dyn Error>> {
        let plan = harness.physical_plan(sql).await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .await?;
        Ok((pretty_format_batches(&batches)?.to_string(), plan))
    }

    // The `events` table has two files of 100,000 rows each, sorted by `id`:
    // - The first file has ids 0 to 99,999, and the second has 100,000 to 199,999.
    // - `kind` is "click" for even ids and "view" for odd ones.
    // - Each file has 20 row groups of 5,000 rows.
    // - Each file is larger than the 512 KiB `ArrowReader` reads first to find the footer, so
    //   skipping row groups reads fewer bytes.
    async fn events_by_id() -> Result<FixtureTable, Box<dyn Error>> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "kind", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "payload", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "events"])?, schema)
            .create()
            .await?;
        let writer_properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(5_000))
            .build();
        let mut append = table.append().writer_properties(writer_properties);
        for ids in [0..100_000, 100_000..200_000] {
            let kinds: Vec<&str> = ids
                .clone()
                .map(|id| if id % 2 == 0 { "click" } else { "view" })
                .collect();
            let payloads: Vec<String> = ids.clone().map(|id| format!("event {id:>32}")).collect();
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(ids));
            let kinds: ArrayRef = Arc::new(StringArray::from(kinds));
            let payloads: ArrayRef = Arc::new(StringArray::from(payloads));
            append = append.file(RecordBatch::try_from_iter([
                ("id", ids),
                ("kind", kinds),
                ("payload", payloads),
            ])?);
        }
        append.commit().await?;
        Ok(table)
    }
}
