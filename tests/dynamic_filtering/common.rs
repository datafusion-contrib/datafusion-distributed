use datafusion::arrow::datatypes::DataType;
use datafusion::common::test_util::batches_to_sort_string;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, Result, ScalarValue, SplitPoint, internal_err};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::logical_expr::{Partitioning, RangePartitioning};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{DynamicFilterPhysicalExpr, UnKnownColumn};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionContext, col};
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::test_utils::parquet::register_parquet_tables;
use datafusion_distributed::test_utils::routing::url_emitter_route_tasks;
use datafusion_distributed::{
    DefaultSessionBuilder, DistributedExt, DistributedLeafExec, display_plan_ascii,
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
    label_dynamic_filters: bool,
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
            label_dynamic_filters: false,
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

    /// Replaces task-scheduling-dependent dynamic-filter predicates with stable labels while
    /// retaining distinct expression IDs in the displayed plan.
    pub(crate) fn with_dynamic_filter_labels(mut self) -> Self {
        self.label_dynamic_filters = true;
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
        let mut labels = self
            .label_dynamic_filters
            .then(DynamicFilterLabels::default);
        execute_query_and_display(
            &ctx,
            self.sql,
            self.expected_rows,
            self.collect_dynamic_filters,
            labels.as_mut(),
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

    execute_query_and_display(&ctx, sql, expected_rows, true, None).await
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
    labels: Option<&mut DynamicFilterLabels>,
) -> Result<String> {
    set_dynamic_filter_pushdown(ctx, true)?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let task_ctx = ctx.task_ctx();

    let results_with_dynamic_filters = collect(Arc::clone(&plan), Arc::clone(&task_ctx)).await?;
    assert_eq!(
        results_with_dynamic_filters
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        expected_rows
    );

    set_dynamic_filter_pushdown(ctx, false)?;
    let plan_without_dynamic_filters = ctx.sql(sql).await?.create_physical_plan().await?;
    let results_without_dynamic_filters =
        collect(plan_without_dynamic_filters, ctx.task_ctx()).await?;
    assert_eq!(
        batches_to_sort_string(&results_with_dynamic_filters),
        batches_to_sort_string(&results_without_dynamic_filters),
        "query results changed when dynamic filtering was enabled",
    );

    let original_display = display_plan_ascii(plan.as_ref(), false);
    let plan_with_dynamic_filters =
        rewrite_distributed_plan_with_dynamic_filters(Arc::clone(&plan), &task_ctx).await?;
    assert_eq!(
        Arc::ptr_eq(&plan, &plan_with_dynamic_filters),
        !collect_dynamic_filters
    );
    assert_eq!(display_plan_ascii(plan.as_ref(), false), original_display);

    match labels {
        Some(labels) => labels.normalize(plan_with_dynamic_filters),
        None => Ok(display_plan_ascii(
            plan_with_dynamic_filters.as_ref(),
            false,
        )),
    }
}

/// Assigns stable, per-test labels to populated dynamic filters using their real expression IDs.
/// Empty filters are left untouched so snapshots still verify that remote updates arrived.
#[derive(Default)]
struct DynamicFilterLabels {
    labels: HashMap<u64, usize>,
}

impl DynamicFilterLabels {
    fn normalize(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        self.label_plan(&plan)?;
        let display = display_plan_ascii(plan.as_ref(), false);
        Ok(remove_runtime_pruning_details(display))
    }

    fn label_plan(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        plan.apply(|node| {
            if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() {
                for variant in leaf.variants() {
                    self.label_variant(variant)?;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    fn label_variant(&mut self, variant: &Arc<dyn ExecutionPlan>) -> Result<()> {
        variant.apply(|node| {
            node.apply_expressions(&mut |root| {
                root.apply(|expression| {
                    let Some(dynamic_filter) =
                        expression.downcast_ref::<DynamicFilterPhysicalExpr>()
                    else {
                        return Ok(TreeNodeRecursion::Continue);
                    };
                    if expression.snapshot_generation() == 1 {
                        return Ok(TreeNodeRecursion::Continue);
                    }
                    let Some(expression_id) = expression.expression_id() else {
                        return internal_err!("dynamic filter did not have an expression ID");
                    };
                    let next_label = self.labels.len();
                    let label = self.labels.entry(expression_id).or_insert(next_label);
                    dynamic_filter.update(Arc::new(UnKnownColumn::new(&format!(
                        "expression_id_{label}"
                    ))))?;
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
}

fn remove_runtime_pruning_details(display: String) -> String {
    const START: &str = "predicate=DynamicFilter [ expression_id_";
    const END: &str = "dynamic_rg_pruning=eligible";

    display
        .lines()
        .map(|line| {
            let (Some(start), Some(end)) = (line.find(START), line.find(END)) else {
                return line.to_owned();
            };
            format!("{}{}", &line[..start], &line[start..end + END.len()])
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
