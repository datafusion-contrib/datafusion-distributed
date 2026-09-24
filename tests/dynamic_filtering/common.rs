use datafusion::arrow::datatypes::DataType;
use datafusion::common::test_util::batches_to_sort_string;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, Result, ScalarValue, SplitPoint, internal_err};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::logical_expr::{Partitioning, RangePartitioning};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, UnKnownColumn};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionContext, col};
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::test_utils::parquet::register_parquet_tables;
use datafusion_distributed::test_utils::routing::{
    ColocateAllTasksHandler, UrlEmitterRouteTaskHandler,
};
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
    expect_dynamic_filter_updates: bool,
    normalized_filter_hashes: bool,
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
            expect_dynamic_filter_updates: false,
            normalized_filter_hashes: false,
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

    pub(crate) fn expect_dynamic_filter_updates(mut self) -> Self {
        self.expect_dynamic_filter_updates = true;
        self
    }

    /// Adds a normalized hash to the dynamic filter display formatter. See
    /// [`DynamicFilterLabels`] below.
    pub(crate) fn with_normalized_filter_hashes(mut self) -> Self {
        self.normalized_filter_hashes = true;
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
        {
            let state = ctx.state_ref();
            let mut state = state.write();
            let optimizer = &mut state.config_mut().options_mut().optimizer;
            if !self.broadcast_joins {
                // Force partitioned hash joins.
                optimizer.hash_join_single_partition_threshold = 0;
                optimizer.hash_join_single_partition_threshold_rows = 0;
            }
        }
        register_parquet_tables(&ctx).await?;
        execute_query_and_display(
            &ctx,
            self.sql,
            self.expected_rows,
            self.collect_dynamic_filters,
            self.expect_dynamic_filter_updates,
            self.normalized_filter_hashes,
        )
        .await
    }
}

pub(crate) async fn execute_range_partitioned_query(
    sql: &str,
    expected_rows: usize,
    colocate_tasks: bool,
) -> Result<String> {
    let (ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
    let mut ctx = ctx
        .with_distributed_broadcast_joins(false)?
        .with_distributed_desired_task_count_handler(2usize);
    ctx = if colocate_tasks {
        ctx.with_distributed_route_task_handler(ColocateAllTasksHandler::default())
    } else {
        ctx.with_distributed_route_task_handler(UrlEmitterRouteTaskHandler)
    };
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

    execute_query_and_display(&ctx, sql, expected_rows, true, false, false).await
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
    expect_dynamic_filter_updates: bool,
    normalized_filter_hashes: bool,
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

    if expect_dynamic_filter_updates {
        let updates = plan
            .metrics()
            .expect("DistributedExec has metrics")
            .sum(|metric| metric.value().name() == "dynamic_filter_updates_received")
            .map_or(0, |metric| metric.as_usize());
        assert!(
            updates > 0,
            "expected dynamic_filter_updates_received > 0, got {updates}"
        );
    }
    DynamicFilterLabels {
        normalized_predicates: normalized_filter_hashes.then(Vec::new),
        ..Default::default()
    }
    .normalize(plan_with_dynamic_filters)
}

/// Encodes dynamic filter expressions in the plan deterministically for consistent snapshots.
///
/// They are encoded as `expression_id_{}_hash_{}` where expression_id is the expression id
/// of the filter (which generally denotes the producer it came from) and hash of the expression.
///
/// Both of these are normalized to be monotonic integers starting from `0` for readability,
/// assigned during a pre-order traversal of the plan.
///
/// If `normalized_predicates` is non empty, then we append the suffix `_normalized_hash_{}`
/// containing the hash of the expression without column names.
#[derive(Default)]
struct DynamicFilterLabels {
    expression_ids: HashMap<u64, usize>,
    // InListExpr ignores element order for equality, but not hashing.
    predicates: Vec<Arc<dyn PhysicalExpr>>,
    normalized_predicates: Option<Vec<Arc<dyn PhysicalExpr>>>,
}

impl DynamicFilterLabels {
    fn normalize(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        self.label_plan(&plan)?;
        Ok(remove_runtime_pruning_details(display_plan_ascii(
            plan.as_ref(),
            false,
        )))
    }

    fn label_plan(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        let mut updates = vec![];
        plan.apply(|node| {
            if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() {
                for variant in leaf.variants() {
                    self.label_variant(variant, &mut updates)?;
                }
            } else if node.is::<FilterExec>() {
                self.label_node(node, &mut updates)?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        // Read every predicate before changing any potentially shared filter state.
        for (dynamic_filter, label) in updates {
            dynamic_filter.update(Arc::new(UnKnownColumn::new(&label)))?;
        }
        Ok(())
    }

    fn label_variant(
        &mut self,
        variant: &Arc<dyn ExecutionPlan>,
        updates: &mut Vec<(Arc<DynamicFilterPhysicalExpr>, String)>,
    ) -> Result<()> {
        variant.apply(|node| {
            self.label_node(node, updates)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    fn label_node(
        &mut self,
        node: &Arc<dyn ExecutionPlan>,
        updates: &mut Vec<(Arc<DynamicFilterPhysicalExpr>, String)>,
    ) -> Result<()> {
        node.apply_expressions(&mut |root| {
            root.apply(|expression| {
                let Ok(dynamic_filter) =
                    Arc::downcast::<DynamicFilterPhysicalExpr>(expression.clone())
                else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                if expression.snapshot_generation() == 1 {
                    return Ok(TreeNodeRecursion::Continue);
                }
                let Some(expression_id) = expression.expression_id() else {
                    return internal_err!("dynamic filter did not have an expression ID");
                };
                let next_expression_id = self.expression_ids.len();
                let expression_id = *self
                    .expression_ids
                    .entry(expression_id)
                    .or_insert(next_expression_id);
                let predicate = dynamic_filter.current()?;
                let predicate_id = intern_predicate(&mut self.predicates, Arc::clone(&predicate));
                let mut label = format!("expression_id_{expression_id}_hash_{predicate_id}");
                if let Some(predicates) = &mut self.normalized_predicates {
                    let normalized_id = intern_predicate(predicates, normalize_columns(predicate)?);
                    label.push_str(&format!("_normalized_hash_{normalized_id}"));
                }
                updates.push((dynamic_filter, label));
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(())
    }
}

fn intern_predicate(
    predicates: &mut Vec<Arc<dyn PhysicalExpr>>,
    predicate: Arc<dyn PhysicalExpr>,
) -> usize {
    predicates
        .iter()
        .position(|existing| existing.eq(&predicate))
        .unwrap_or_else(|| {
            let id = predicates.len();
            predicates.push(predicate);
            id
        })
}

fn normalize_columns(predicate: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    let mut columns = HashMap::new();
    Ok(predicate
        .transform_down(|expression| {
            let Some(column) = expression.downcast_ref::<Column>() else {
                return Ok(Transformed::no(expression));
            };
            let next_id = columns.len();
            let id = *columns.entry(column.clone()).or_insert(next_id);
            Ok(Transformed::yes(
                Arc::new(Column::new(&format!("column_{id}"), id)) as Arc<dyn PhysicalExpr>,
            ))
        })?
        .data)
}

fn remove_runtime_pruning_details(display: String) -> String {
    const START: &str = "DynamicFilter [ expression_id_";
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
