use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::{Result, ScalarValue, SplitPoint};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::logical_expr::{Partitioning, RangePartitioning};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionContext, col};
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::test_utils::parquet::register_parquet_tables;
use datafusion_distributed::test_utils::routing::url_emitter_route_tasks;
use datafusion_distributed::{
    DefaultSessionBuilder, DistributedExt, display_plan_ascii,
    rewrite_distributed_plan_with_dynamic_filters,
};
use std::sync::Arc;

pub(crate) const LOCAL_AND_REMOTE_UNION_QUERY: &str = r#"
    WITH remote_probe AS (
        SELECT probe."WindGustDir" AS key
        FROM (
            SELECT DISTINCT "RainToday" AS key
            FROM weather
        ) nested_build
        JOIN weather probe
            ON nested_build.key = probe."RainToday"
    )
    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT "WindGustDir" AS key
        FROM weather
    ) build
    JOIN (
        SELECT "WindGustDir" AS key FROM weather
        UNION ALL
        SELECT key FROM remote_probe
    ) probe ON build.key = probe.key
"#;

pub(crate) struct TestQuery<'a> {
    sql: &'a str,
    expected_rows: usize,
    broadcast_joins: bool,
    one_task_per_leaf: bool,
    dynamic_task_count: bool,
    collect_dynamic_filters: bool,
}

impl<'a> TestQuery<'a> {
    pub(crate) fn new(sql: &'a str) -> Self {
        Self {
            sql,
            expected_rows: 1,
            broadcast_joins: false,
            one_task_per_leaf: false,
            dynamic_task_count: false,
            collect_dynamic_filters: true,
        }
    }

    /// Assert the number of rows after the query runs.
    pub(crate) fn with_expected_rows(mut self, expected_rows: usize) -> Self {
        self.expected_rows = expected_rows;
        self
    }

    /// Forces collect left joins and enables distributed broadcast joins.
    pub(crate) fn with_broadcast_joins(mut self) -> Self {
        self.broadcast_joins = true;
        self
    }

    /// Sets the desired task count to 1.
    pub(crate) fn with_one_task_per_leaf(mut self) -> Self {
        self.one_task_per_leaf = true;
        self
    }

    /// Enables the dynamic task-count planner.
    pub(crate) fn with_dynamic_task_count(mut self) -> Self {
        self.dynamic_task_count = true;
        self
    }

    /// Disables dynamic filter collection.
    pub(crate) fn without_dynamic_filter_collection(mut self) -> Self {
        self.collect_dynamic_filters = false;
        self
    }

    pub(crate) async fn execute(self) -> Result<String> {
        let (ctx, _guard, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let mut ctx = ctx
            .with_distributed_broadcast_joins(self.broadcast_joins)?
            .with_distributed_dynamic_filter_collection(self.collect_dynamic_filters)?;
        ctx.set_distributed_dynamic_task_count(self.dynamic_task_count)?;
        if self.one_task_per_leaf {
            ctx = ctx.with_distributed_desired_task_count_handler(1usize);
        }
        if !self.broadcast_joins {
            // Force partitioned hash joins.
            let state = ctx.state_ref();
            let mut state = state.write();
            let optimizer = &mut state.config_mut().options_mut().optimizer;
            optimizer.hash_join_single_partition_threshold = 0;
            optimizer.hash_join_single_partition_threshold_rows = 0;
        }
        register_parquet_tables(&ctx).await?;
        execute_query_and_display(
            &ctx,
            self.sql,
            self.expected_rows,
            self.collect_dynamic_filters,
            true,
        )
        .await
    }
}

pub(crate) async fn execute_range_partitioned_query(
    sql: &str,
    expected_rows: usize,
) -> Result<String> {
    let (ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
    let ctx = ctx
        .with_distributed_broadcast_joins(false)?
        .with_distributed_desired_task_count_handler(2usize)
        .with_distributed_route_tasks_handler(url_emitter_route_tasks);
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        let options = state.config_mut().options_mut();
        options.execution.target_partitions = 2;
        options.optimizer.hash_join_single_partition_threshold = 0;
        options.optimizer.hash_join_single_partition_threshold_rows = 0;
    }

    register_range_partitioned_table(&ctx, "dim", "testdata/join/parquet/dim", "d_dkey").await?;
    register_range_partitioned_table(&ctx, "fact", "testdata/join/parquet/fact", "f_dkey").await?;

    execute_query_and_display(&ctx, sql, expected_rows, true, false).await
}

async fn register_range_partitioned_table(
    ctx: &SessionContext,
    name: &str,
    path: &str,
    partition_column: &str,
) -> Result<()> {
    let table_url = ListingTableUrl::parse(path)?;
    let output_partitioning = Partitioning::Range(RangePartitioning::try_new(
        vec![col(partition_column).sort(true, false)],
        vec![SplitPoint::new(vec![ScalarValue::Utf8(Some(
            "C".to_string(),
        ))])],
    )?);
    let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_table_partition_cols(vec![(partition_column.to_string(), DataType::Utf8)])
        .with_output_partitioning(Some(output_partitioning));
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await?;
    ctx.register_table(name, Arc::new(ListingTable::try_new(config)?))?;
    Ok(())
}

async fn execute_query_and_display(
    ctx: &SessionContext,
    sql: &str,
    expected_rows: usize,
    collect_dynamic_filters: bool,
    compare_without_dynamic_filters: bool,
) -> Result<String> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let task_ctx = ctx.task_ctx();

    let results_with_dynamic_filters =
        collect(Arc::clone(&plan), Arc::clone(&task_ctx)).await?;
    assert_eq!(
        results_with_dynamic_filters
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        expected_rows
    );

    if compare_without_dynamic_filters {
        set_dynamic_filter_pushdown(ctx, false)?;
        let plan_without_dynamic_filters = ctx.sql(sql).await?.create_physical_plan().await?;
        let results_without_dynamic_filters =
            collect(plan_without_dynamic_filters, ctx.task_ctx()).await?;
        assert_eq!(
            pretty_format_batches(&results_with_dynamic_filters)?.to_string(),
            pretty_format_batches(&results_without_dynamic_filters)?.to_string(),
        );
    }

    let original_display = display_plan_ascii(plan.as_ref(), false);
    let plan_with_dynamic_filters =
        rewrite_distributed_plan_with_dynamic_filters(Arc::clone(&plan), &task_ctx).await?;
    assert_eq!(
        Arc::ptr_eq(&plan, &plan_with_dynamic_filters),
        !collect_dynamic_filters
    );
    assert_eq!(display_plan_ascii(plan.as_ref(), false), original_display);

    Ok(display_plan_ascii(
        plan_with_dynamic_filters.as_ref(),
        false,
    ))
}

/// Replaces task-scheduling-dependent dynamic-filter values while retaining the complete plan.
pub(crate) fn normalize_runtime_dynamic_filters(display: String) -> String {
    const START: &str = "predicate=DynamicFilter [";
    const END: &str = "dynamic_rg_pruning=eligible";

    display
        .lines()
        .map(|line| {
            let (Some(start), Some(end)) = (line.find(START), line.find(END)) else {
                return line.to_owned();
            };
            format!(
                "{}predicate=DynamicFilter [ <runtime> ], {}",
                &line[..start],
                &line[end..end + END.len()]
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn set_dynamic_filter_pushdown(ctx: &SessionContext, enabled: bool) -> Result<()> {
    let state = ctx.state_ref();
    state.write().config_mut().options_mut().set(
        "datafusion.optimizer.enable_dynamic_filter_pushdown",
        &enabled.to_string(),
    )
}
