mod tests {
    use datafusion::arrow::array::{ArrayRef, Date32Array, Int32Array, RecordBatch};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::collect;
    use datafusion_distributed::test_utils::metrics::node_metrics;
    use datafusion_distributed::{
        DistributedExt, DistributedLeafExec, DistributedMetricsFormat,
        rewrite_distributed_plan_with_metrics,
    };
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg_test_tools::create_taxi_table;
    use datafusion_distributed_iceberg_test_tools::{
        FixtureCatalog, FixtureTable, IcebergTestHarness,
    };
    use iceberg::TableIdent;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type};
    use std::error::Error;
    use std::sync::Arc;

    // The taxi table has one file per day, for the 7 days from 2024-01-08. The join keeps two of
    // them, so its filter lets the scan skip the other 5 files.
    const TWO_DAYS_OF_TRIPS: &str = "SELECT COUNT(*) AS trips FROM taxi \
        JOIN (VALUES (DATE '2024-01-10'), (DATE '2024-01-12')) AS days(day) \
        ON taxi.pickup_date = days.day";

    #[tokio::test]
    async fn lazy_planning_workers_skip_files_the_join_filter_rules_out() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .with_workers(2)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;

        let plan = harness.physical_plan(TWO_DAYS_OF_TRIPS).await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .await?;

        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +-------+
        | trips |
        +-------+
        | 50000 |
        +-------+
        ");
        // The first `DataSourceExec` is the join's `VALUES` table. The second is the taxi scan.
        assert_eq!(node_metrics::<DataSourceExec>(&plan, "files_opened", 1), 2);
        Ok(())
    }

    #[tokio::test]
    async fn eager_planning_workers_skip_files_the_join_filter_rules_out() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .with_workers(2)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;

        let plan = harness.physical_plan(TWO_DAYS_OF_TRIPS).await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .await?;

        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +-------+
        | trips |
        +-------+
        | 50000 |
        +-------+
        ");
        assert_eq!(
            node_metrics::<DistributedLeafExec>(&plan, "files_opened", 0),
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn top_n_skips_files_once_it_fills_up() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            // Read one file at a time, so top-N's filter tightens before the next file starts.
            .with_config_option("iceberg.data_file_concurrency_limit", 1)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 1;
                Ok(state)
            })?
            .build()
            .await?;

        let plan = harness
            .physical_plan("SELECT pickup_date FROM taxi ORDER BY pickup_date LIMIT 3")
            .await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .await?;

        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +-------------+
        | pickup_date |
        +-------------+
        | 2024-01-08  |
        | 2024-01-08  |
        | 2024-01-08  |
        +-------------+
        ");
        // The first file, 2024-01-08, fills the top 3. The filter then becomes
        // `pickup_date < 2024-01-08`, which rules out the other 6 days.
        assert_eq!(node_metrics::<DataSourceExec>(&plan, "files_opened", 0), 1);
        Ok(())
    }

    #[tokio::test]
    async fn month_partitions_skip_files_from_other_months() -> Result<(), Box<dyn Error>> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(events_by_month().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .build()
            .await?;

        let plan = harness
            .physical_plan(
                "SELECT events.day FROM events \
                 JOIN (VALUES (DATE '2024-02-14')) AS days(day) ON events.day = days.day",
            )
            .await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .await?;

        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +------------+
        | day        |
        +------------+
        | 2024-02-14 |
        +------------+
        ");
        // The first `DataSourceExec` is the join's `VALUES` table. The second is the events scan,
        // which opens only February's file.
        assert_eq!(node_metrics::<DataSourceExec>(&plan, "files_opened", 1), 1);
        Ok(())
    }

    #[tokio::test]
    async fn null_partitions_are_read_when_the_filter_can_match_null() -> Result<(), Box<dyn Error>>
    {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(groups_by_key().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            // Read one file at a time, so top-N's filter tightens before the next file starts.
            .with_config_option("iceberg.data_file_concurrency_limit", 1)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 1;
                Ok(state)
            })?
            .build()
            .await?;

        let plan = harness
            .physical_plan("SELECT k FROM groups ORDER BY k NULLS FIRST LIMIT 2")
            .await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .await?;

        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +---+
        | k |
        +---+
        |   |
        |   |
        +---+
        ");
        // The scan reads `k = 1` first, and top-N's filter becomes `k IS NULL OR k < 1`. That
        // skips the `k = 2` file and keeps the null file.
        assert_eq!(node_metrics::<DataSourceExec>(&plan, "files_opened", 0), 2);
        Ok(())
    }

    // Table `events`, partitioned by the month of `day`, with one file for each of January,
    // February and March 2024.
    async fn events_by_month() -> Result<FixtureTable, Box<dyn Error>> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "day", Type::Primitive(PrimitiveType::Date)).into(),
            ])
            .build()?;
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "events"])?, schema)
            .partition_by("day", Transform::Month)
            .create()
            .await?;
        // Days since 1970-01-01.
        let january = [19727, 19742]; // 2024-01-05, 2024-01-20
        let february = [19756, 19767]; // 2024-02-03, 2024-02-14
        let march = [19783]; // 2024-03-01
        let mut append = table.append();
        for days in [&january[..], &february[..], &march[..]] {
            let days: ArrayRef = Arc::new(Date32Array::from(days.to_vec()));
            append = append.file(RecordBatch::try_from_iter([("day", days)])?);
        }
        append.commit().await?;
        Ok(table)
    }

    // Table `groups`, partitioned by `k`, with one file each for `k = 1`, `k = 2` and a null `k`,
    // in that order. Each file has two rows.
    async fn groups_by_key() -> Result<FixtureTable, Box<dyn Error>> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "k", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "groups"])?, schema)
            .partition_by("k", Transform::Identity)
            .create()
            .await?;
        let mut append = table.append();
        for k in [Some(1), Some(2), None] {
            let keys: ArrayRef = Arc::new(Int32Array::from(vec![k, k]));
            append = append.file(RecordBatch::try_from_iter([("k", keys)])?);
        }
        append.commit().await?;
        Ok(table)
    }
}
