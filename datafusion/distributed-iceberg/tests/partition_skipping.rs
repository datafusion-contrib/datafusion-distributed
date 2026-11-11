mod tests {
    use datafusion::arrow::array::{
        ArrayRef, Date32Array, Int32Array, Int64Array, RecordBatch, StringArray,
        TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::metrics::MetricValue;
    use datafusion::physical_plan::{ExecutionPlan, collect};
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

    #[tokio::test]
    async fn validate_predicate_translation() -> Result<(), Box<dyn Error>> {
        let skipping = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(readings().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            // Read one file at a time, so top-N's filter tightens before the next file starts.
            .with_config_option("iceberg.data_file_concurrency_limit", 1)
            // A join with more than 2 keys filters only on their bounds, so the cases cover
            // both kinds of join filter.
            .with_config_option(
                "datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values",
                2,
            )
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 1;
                Ok(state)
            })?
            .build()
            .await?;
        let reading_every_file = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(readings().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", false)
            .with_config_option("iceberg.data_file_concurrency_limit", 1)
            .with_config_option(
                "datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values",
                2,
            )
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 1;
                Ok(state)
            })?
            .build()
            .await?;

        // Keys on partition edges, keys in no partition, and nulls, for each transform.
        let schema = skipping.table().schema()?;
        let cases = [
            join(&schema, "id", &["3"]),
            join(&schema, "id", &["0"]),
            join(&schema, "id", &["2", "6"]),
            join(&schema, "id", &["2", "3", "4"]),
            join(&schema, "id", &["NULL"]),
            join(&schema, "id", &["1", "NULL"]),
            join(&schema, "name", &["'avocado'"]),
            join(&schema, "name", &["'b'"]),
            join(&schema, "name", &["'éclair'"]),
            join(&schema, "name", &["'é'"]),
            join(&schema, "name", &["'zebra'"]),
            join(&schema, "day", &["'2024-02-01'"]),
            join(&schema, "day", &["'2024-01-15'"]),
            join(&schema, "day", &["'2024-04-01'"]),
            join(&schema, "day", &["'2023-12-31'", "'2024-03-01'"]),
            join(&schema, "n", &["12"]),
            join(&schema, "n", &["99"]),
            join(&schema, "n", &["-1", "13"]),
            join(&schema, "at", &["'2024-01-02T00:00:00'"]),
            join(&schema, "at", &["'2024-01-01T23:59:59.999999'"]),
            join(&schema, "at", &["'2024-01-04T00:00:00'"]),
            top_n("id", 1),
            top_n("id DESC", 1),
            top_n("id NULLS FIRST", 2),
            top_n("id DESC NULLS LAST", 2),
            top_n("name", 1),
            top_n("name DESC", 1),
            top_n("day", 2),
            top_n("day DESC", 1),
            top_n("n", 1),
            top_n("at", 1),
            top_n("at DESC", 2),
        ];

        // Each case must return the same rows whether the scan skips files or reads them all.
        // The snapshot shows how many files each case kept.
        let mut skipped = Vec::new();
        for (case, sql) in cases {
            let plan = skipping.physical_plan(&sql).await?;
            let rows = collect(Arc::clone(&plan), skipping.state().task_ctx()).await?;
            let plan =
                rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                    .await?;
            let expected = reading_every_file.query(&sql).await?.1;
            assert_eq!(
                pretty_format_batches(&rows)?.to_string(),
                expected,
                "{case}"
            );
            let files = scan_metric(&plan, "files_pruned_by_partition")
                .unwrap_or_else(|| panic!("{case}: the scan has no dynamic filter"));
            let rows: usize = rows.iter().map(RecordBatch::num_rows).sum();
            skipped.push(format!("{case}: rows: {rows}, files: {files}"));
        }
        insta::assert_snapshot!(skipped.join("\n"), @"
        join on id with 3: rows: 1, files: 6 total → 1 matched
        join on id with 0: rows: 0, files: 6 total → 0 matched
        join on id with 2, 6: rows: 2, files: 6 total → 2 matched
        join on id with 2, 3, 4: rows: 3, files: 6 total → 3 matched
        join on id with NULL: rows: 0, files: 0 total → 0 matched
        join on id with 1, NULL: rows: 1, files: 6 total → 1 matched
        join on name with 'avocado': rows: 1, files: 6 total → 2 matched
        join on name with 'b': rows: 0, files: 6 total → 1 matched
        join on name with 'éclair': rows: 1, files: 6 total → 1 matched
        join on name with 'é': rows: 0, files: 6 total → 1 matched
        join on name with 'zebra': rows: 0, files: 6 total → 0 matched
        join on day with '2024-02-01': rows: 1, files: 6 total → 2 matched
        join on day with '2024-01-15': rows: 0, files: 6 total → 1 matched
        join on day with '2024-04-01': rows: 0, files: 6 total → 0 matched
        join on day with '2023-12-31', '2024-03-01': rows: 2, files: 6 total → 2 matched
        join on n with 12: rows: 1, files: 6 total → 3 matched
        join on n with 99: rows: 0, files: 6 total → 1 matched
        join on n with -1, 13: rows: 2, files: 6 total → 4 matched
        join on at with '2024-01-02T00:00:00': rows: 1, files: 6 total → 2 matched
        join on at with '2024-01-01T23:59:59.999999': rows: 1, files: 6 total → 1 matched
        join on at with '2024-01-04T00:00:00': rows: 0, files: 6 total → 0 matched
        ORDER BY id LIMIT 1: rows: 1, files: 6 total → 1 matched
        ORDER BY id DESC LIMIT 1: rows: 1, files: 6 total → 6 matched
        ORDER BY id NULLS FIRST LIMIT 2: rows: 2, files: 6 total → 3 matched
        ORDER BY id DESC NULLS LAST LIMIT 2: rows: 2, files: 6 total → 5 matched
        ORDER BY name LIMIT 1: rows: 1, files: 6 total → 2 matched
        ORDER BY name DESC LIMIT 1: rows: 1, files: 6 total → 5 matched
        ORDER BY day LIMIT 2: rows: 2, files: 6 total → 3 matched
        ORDER BY day DESC LIMIT 1: rows: 1, files: 6 total → 5 matched
        ORDER BY n LIMIT 1: rows: 1, files: 6 total → 6 matched
        ORDER BY at LIMIT 1: rows: 1, files: 6 total → 2 matched
        ORDER BY at DESC LIMIT 2: rows: 2, files: 6 total → 5 matched
        ");
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

    // Table `readings`, partitioned by `id`, the first letter of `name`, the month of `day`, 4
    // buckets of `n` and the day of `at`. Each row is its own file, and the last row is all
    // nulls.
    async fn readings() -> Result<FixtureTable, Box<dyn Error>> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "day", Type::Primitive(PrimitiveType::Date)).into(),
                NestedField::optional(4, "n", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(5, "at", Type::Primitive(PrimitiveType::Timestamp)).into(),
            ])
            .build()?;
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "readings"])?, schema)
            .partition_by("id", Transform::Identity)
            .partition_by("name", Transform::Truncate(1))
            .partition_by("day", Transform::Month)
            .partition_by("n", Transform::Bucket(4))
            .partition_by("at", Transform::Day)
            .create()
            .await?;
        let id: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(6),
            None,
        ]));
        let name: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            Some("avocado"),
            Some("banana"),
            Some("éclair"),
            Some("cherry"),
            None,
        ]));
        // Days since 1970-01-01: 2024-01-31, 2024-02-01, 2024-02-29, 2024-03-01, 2023-12-31.
        let day: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(19753),
            Some(19754),
            Some(19782),
            Some(19783),
            Some(19722),
            None,
        ]));
        let n: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(10),
            Some(11),
            Some(12),
            Some(13),
            Some(-1),
            None,
        ]));
        // Microseconds since 1970-01-01: 2024-01-01 23:59:59.999999, 2024-01-02 00:00:00,
        // 2024-01-02 12:00:00, 2024-01-03 00:00:00, 2023-12-31 23:59:59.
        let at: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1704153599999999),
            Some(1704153600000000),
            Some(1704196800000000),
            Some(1704240000000000),
            Some(1704067199000000),
            None,
        ]));
        let rows = RecordBatch::try_from_iter([
            ("id", id),
            ("name", name),
            ("day", day),
            ("n", n),
            ("at", at),
        ])?;
        let mut append = table.append();
        for row in 0..rows.num_rows() {
            append = append.file(rows.slice(row, 1));
        }
        append.commit().await?;
        Ok(table)
    }

    /// A case that joins `readings` with `keys` on `column`. Each key is cast to the column's
    /// type, so the join filters the column itself, without a cast around it.
    fn join(schema: &SchemaRef, column: &str, keys: &[&str]) -> (String, String) {
        let data_type = schema.field_with_name(column).unwrap().data_type();
        let values = keys
            .iter()
            .map(|key| format!("(arrow_cast({key}, '{data_type}'))"))
            .collect::<Vec<_>>()
            .join(", ");
        (
            format!("join on {column} with {}", keys.join(", ")),
            format!(
                "SELECT t.* FROM readings t JOIN (VALUES {values}) AS k(x) \
                 ON t.{column} = k.x ORDER BY t.id"
            ),
        )
    }

    /// A case that reads the first `limit` rows of `readings` in `order`.
    fn top_n(order: &str, limit: usize) -> (String, String) {
        (
            format!("ORDER BY {order} LIMIT {limit}"),
            format!("SELECT * FROM readings ORDER BY {order} LIMIT {limit}"),
        )
    }

    /// The metric `name` of the Iceberg scan in `plan`, summed over its partitions.
    fn scan_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<MetricValue> {
        plan.metrics()
            .and_then(|metrics| metrics.sum_by_name(name))
            .or_else(|| {
                plan.children()
                    .into_iter()
                    .find_map(|child| scan_metric(child, name))
            })
    }
}
