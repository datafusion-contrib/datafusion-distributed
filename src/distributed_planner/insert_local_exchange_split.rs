use crate::distributed_planner::DistributedConfig;
use crate::{LocalExchangeSplitExec, NetworkShuffleExec};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use std::sync::Arc;

/// Inserts [LocalExchangeSplitExec] above narrow post-shuffle consumers.
///
/// This pass runs after network exchange insertion, when every [NetworkShuffleExec] already has an
/// assigned [crate::distributed_planner::ExchangeLayout]. The pass does not change stage
/// boundaries or global shuffle ownership. It only adds local fanout inside the consumer task when
/// the next operator would otherwise execute with too few local partitions.
///
/// The default policy targets final partitioned aggregates and partitioned hash joins. The
/// `all_narrow_shuffles` mode applies the same narrow-shuffle rule to any direct shuffle consumer.
///
/// ```text
/// TODO diagram: NetworkShuffleExec output owned by one task -> LocalExchangeSplitExec -> multiple
/// local partitions consumed by a final aggregate or partitioned join.
/// ```
pub(crate) fn insert_local_exchange_split_execs(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    let transformed = plan.transform_up(|plan| {
        if d_cfg.local_exchange_split_mode_is_all_narrow_shuffles() {
            let wrapped = maybe_split_narrow_shuffle_child(Arc::clone(&plan), d_cfg)?;
            if !Arc::ptr_eq(&wrapped, &plan) {
                return Ok(Transformed::yes(wrapped));
            }
            return Ok(Transformed::no(plan));
        }

        if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() {
            if d_cfg.local_exchange_split_mode_allows_final_agg()
                && aggregate.mode() == &AggregateMode::FinalPartitioned
            {
                return wrap_final_partitioned_aggregate_input(plan, d_cfg);
            }
            return Ok(Transformed::no(plan));
        }

        if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            if d_cfg.local_exchange_split_mode_allows_partitioned_join()
                && join.partition_mode() == &PartitionMode::Partitioned
            {
                return wrap_partitioned_join_children(plan, d_cfg);
            }
            return Ok(Transformed::no(plan));
        }

        Ok(Transformed::no(plan))
    })?;

    Ok(transformed.data)
}

fn derive_split_factor(
    owned: usize,
    target_partitions_per_task: usize,
    max_factor: usize,
) -> Option<usize> {
    if owned == 0
        || target_partitions_per_task == 0
        || max_factor == 0
        || owned >= target_partitions_per_task
    {
        return None;
    }

    let split_factor = target_partitions_per_task.div_ceil(owned).min(max_factor);
    (split_factor > 1).then_some(split_factor)
}

fn maybe_split_narrow_shuffle_child(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(candidate) = NarrowShuffleSplitCandidate::try_new(Arc::clone(&plan), d_cfg)? else {
        return Ok(plan);
    };
    let Some(split_factor) = derive_split_factor(
        candidate.owned,
        d_cfg.local_exchange_split_target_partitions_per_task,
        d_cfg.local_exchange_split_max_factor,
    ) else {
        return Ok(plan);
    };
    candidate.wrap(split_factor)
}

fn wrap_final_partitioned_aggregate_input(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let children = plan.children();
    if children.is_empty() {
        return Ok(Transformed::no(plan));
    }

    let input = Arc::clone(children[0]);
    let wrapped = maybe_split_narrow_shuffle_child(Arc::clone(&input), d_cfg)?;
    if Arc::ptr_eq(&wrapped, &input) {
        return Ok(Transformed::no(plan));
    }

    let mut new_children = Vec::with_capacity(children.len());
    new_children.push(wrapped);
    for child in children.into_iter().skip(1) {
        new_children.push(Arc::clone(child));
    }

    Ok(Transformed::yes(plan.with_new_children(new_children)?))
}

fn derive_join_split_factors(
    left_owned: usize,
    right_owned: usize,
    target_per_task: usize,
    max_factor: usize,
) -> Option<(usize, usize)> {
    if left_owned == 0 || right_owned == 0 || target_per_task == 0 || max_factor == 0 {
        return None;
    }

    let common_multiple = lcm(left_owned, right_owned);
    let minimum_target = target_per_task.max(left_owned).max(right_owned);
    let common_target = minimum_target.div_ceil(common_multiple) * common_multiple;

    let left_factor = common_target / left_owned;
    let right_factor = common_target / right_owned;
    if left_factor > max_factor || right_factor > max_factor {
        return None;
    }

    Some((left_factor, right_factor))
}

fn gcd(mut lhs: usize, mut rhs: usize) -> usize {
    while rhs != 0 {
        let rem = lhs % rhs;
        lhs = rhs;
        rhs = rem;
    }
    lhs
}

fn lcm(lhs: usize, rhs: usize) -> usize {
    lhs / gcd(lhs, rhs) * rhs
}

fn wrap_partitioned_join_children(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let children = plan.children();
    if children.len() < 2 {
        return Ok(Transformed::no(plan));
    }

    let left = Arc::clone(children[0]);
    let right = Arc::clone(children[1]);
    let left_candidate = NarrowShuffleSplitCandidate::try_new(Arc::clone(&left), d_cfg)?;
    let right_candidate = NarrowShuffleSplitCandidate::try_new(Arc::clone(&right), d_cfg)?;

    let (Some(left_candidate), Some(right_candidate)) = (&left_candidate, &right_candidate) else {
        return Ok(Transformed::no(plan));
    };

    if left_candidate.base_partition_count != right_candidate.base_partition_count {
        return Ok(Transformed::no(plan));
    }

    let Some((left_factor, right_factor)) = derive_join_split_factors(
        left_candidate.owned,
        right_candidate.owned,
        d_cfg.local_exchange_split_target_partitions_per_task,
        d_cfg.local_exchange_split_max_factor,
    ) else {
        return Ok(Transformed::no(plan));
    };

    let mut new_children = Vec::with_capacity(children.len());
    let mut changed = false;
    for (idx, child) in children.into_iter().enumerate() {
        let child = Arc::clone(child);
        match idx {
            0 => {
                let wrapped = left_candidate.wrap(left_factor)?;
                changed |= !Arc::ptr_eq(&wrapped, &child);
                new_children.push(wrapped);
            }
            1 => {
                let wrapped = right_candidate.wrap(right_factor)?;
                changed |= !Arc::ptr_eq(&wrapped, &child);
                new_children.push(wrapped);
            }
            _ => new_children.push(child),
        }
    }

    if !changed {
        return Ok(Transformed::no(plan));
    }

    Ok(Transformed::yes(plan.with_new_children(new_children)?))
}

#[derive(Clone)]
struct NarrowShuffleSplitCandidate {
    plan: Arc<dyn ExecutionPlan>,
    hash_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    base_partition_count: usize,
    owned: usize,
}

impl NarrowShuffleSplitCandidate {
    fn try_new(plan: Arc<dyn ExecutionPlan>, d_cfg: &DistributedConfig) -> Result<Option<Self>> {
        let Some(shuffle) = plan.as_any().downcast_ref::<NetworkShuffleExec>() else {
            return Ok(None);
        };

        let layout = &shuffle.layout;
        let owned = layout.max_partition_count_per_consumer();
        if owned == 0
            || owned > d_cfg.local_exchange_split_max_owned_partitions
            || layout.consumer_task_count() <= 1
        {
            return Ok(None);
        }

        let Some(Partitioning::Hash(hash_exprs, base_partition_count)) =
            layout.producer_partitioning().cloned()
        else {
            return Ok(None);
        };

        Ok(Some(Self {
            plan,
            hash_exprs,
            base_partition_count,
            owned,
        }))
    }

    fn wrap(&self, split_factor: usize) -> Result<Arc<dyn ExecutionPlan>> {
        if split_factor <= 1 {
            return Ok(Arc::clone(&self.plan));
        }

        Ok(Arc::new(LocalExchangeSplitExec::try_new(
            Arc::clone(&self.plan),
            self.hash_exprs.clone(),
            self.base_partition_count,
            split_factor,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        derive_join_split_factors, derive_split_factor, insert_local_exchange_split_execs,
    };
    use crate::assert_snapshot;
    use crate::distributed_planner::distribute_plan::test_helpers::insert_network_boundaries_for_test;
    use crate::test_utils::plans::{base_session_builder, context_with_query};
    use crate::{
        DistributedConfig, DistributedExec, LOCAL_EXCHANGE_SPLIT_MODE_ALL_NARROW_SHUFFLES,
        LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN, NetworkShuffleExec, display_plan_ascii,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::common::Result;
    use datafusion::config::ConfigOptions;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::prelude::ParquetReadOptions;
    use std::sync::Arc;
    use uuid::Uuid;

    #[derive(Clone, Copy)]
    struct SplitTestOptions {
        mode: &'static str,
        max_owned_partitions: usize,
        target_partitions_per_task: usize,
        max_factor: usize,
        force_partitioned_join: bool,
    }

    const FINAL_AGG_SPLIT_OPTIONS: SplitTestOptions = SplitTestOptions {
        mode: LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN,
        max_owned_partitions: 2,
        target_partitions_per_task: 4,
        max_factor: 4,
        force_partitioned_join: false,
    };

    const FINAL_AGG_SKIP_OPTIONS: SplitTestOptions = SplitTestOptions {
        max_owned_partitions: 0,
        ..FINAL_AGG_SPLIT_OPTIONS
    };

    const PARTITIONED_JOIN_SPLIT_OPTIONS: SplitTestOptions = SplitTestOptions {
        mode: LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN,
        max_owned_partitions: 2,
        target_partitions_per_task: 8,
        max_factor: 8,
        force_partitioned_join: true,
    };

    const ALL_NARROW_WINDOW_OPTIONS: SplitTestOptions = SplitTestOptions {
        mode: LOCAL_EXCHANGE_SPLIT_MODE_ALL_NARROW_SHUFFLES,
        max_owned_partitions: 2,
        target_partitions_per_task: 4,
        max_factor: 4,
        force_partitioned_join: false,
    };

    async fn weather_query_to_split_plan(query: &str, options: SplitTestOptions) -> String {
        let builder = base_session_builder(4, 4, false);
        let (ctx, query) = context_with_query(builder, query).await;
        split_plan_from_ctx(&ctx, &query, options).await
    }

    async fn join_query_to_split_plan(query: &str, options: SplitTestOptions) -> String {
        let state = base_session_builder(4, 2, false).build();
        let ctx = SessionContext::new_with_state(state);
        register_join_tables(&ctx).await.unwrap();
        split_plan_from_ctx(&ctx, query, options).await
    }

    fn apply_test_options(cfg: &mut ConfigOptions, options: SplitTestOptions) {
        if options.force_partitioned_join {
            cfg.optimizer.hash_join_single_partition_threshold = 0;
            cfg.optimizer.hash_join_single_partition_threshold_rows = 0;
        }

        let d_cfg = cfg.extensions.get_mut::<DistributedConfig>().unwrap();
        d_cfg.local_exchange_split_mode = options.mode.to_string();
        d_cfg.local_exchange_split_max_owned_partitions = options.max_owned_partitions;
        d_cfg.local_exchange_split_target_partitions_per_task = options.target_partitions_per_task;
        d_cfg.local_exchange_split_max_factor = options.max_factor;
    }

    async fn split_plan_from_ctx(
        ctx: &SessionContext,
        query: &str,
        options: SplitTestOptions,
    ) -> String {
        {
            let state_ref = ctx.state_ref();
            let mut state = state_ref.write();
            apply_test_options(state.config_mut().options_mut(), options);
        }

        let df = ctx.sql(query).await.unwrap();
        let (state, logical_plan) = df.into_parts();
        let physical_plan = state.create_physical_plan(&logical_plan).await.unwrap();
        let distributed =
            insert_network_boundaries_for_test(Arc::clone(&physical_plan), state.config_options())
                .await
                .unwrap()
                .unwrap_or(physical_plan);
        let d_cfg = DistributedConfig::from_config_options(state.config_options()).unwrap();
        let split = insert_local_exchange_split_execs(distributed, &d_cfg).unwrap();
        let distributed_exec = DistributedExec::new(split);
        display_plan_ascii(&distributed_exec, false)
    }

    async fn register_join_tables(ctx: &SessionContext) -> Result<()> {
        let dim_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("d_dkey".to_string(), DataType::Utf8)]);
        ctx.register_parquet("dim", "testdata/join/parquet/dim", dim_options)
            .await?;

        let fact_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("f_dkey".to_string(), DataType::Utf8)]);
        ctx.register_parquet("fact", "testdata/join/parquet/fact", fact_options)
            .await?;
        Ok(())
    }

    fn key_expr() -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new("key", 0))
    }

    fn empty_key_exec() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Int32,
            false,
        )]))))
    }

    #[test]
    fn join_split_factors_use_target_when_divisible() {
        assert_eq!(derive_join_split_factors(1, 2, 8, 8), Some((8, 4)));
    }

    #[test]
    fn join_split_factors_use_common_multiple() {
        assert_eq!(derive_join_split_factors(2, 3, 8, 8), Some((6, 4)));
    }

    #[test]
    fn join_split_factors_respect_max_factor() {
        assert_eq!(derive_join_split_factors(2, 3, 8, 4), None);
    }

    #[test]
    fn split_factor_uses_target_and_cap() {
        assert_eq!(derive_split_factor(2, 8, 8), Some(4));
        assert_eq!(derive_split_factor(2, 8, 2), Some(2));
        assert_eq!(derive_split_factor(8, 8, 8), None);
    }

    #[tokio::test]
    async fn final_agg_inserts_split() {
        let query = r#"
        SELECT count(*), "RainToday"
        FROM weather
        GROUP BY "RainToday"
        ORDER BY count(*)
        "#;
        let plan = weather_query_to_split_plan(query, FINAL_AGG_SPLIT_OPTIONS).await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3] 
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       LocalExchangeSplitExec: input_partitions=1, base_partitions=2, local_partitions=4, exprs=[RainToday@0]
          │         [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p0..p1] t2:[p0..p1] 
            │ RepartitionExec: partitioning=Hash([RainToday@0], 2), input_partitions=1
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: tasks=3 partitions=3
            │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn final_agg_respects_owned_partition_cap() {
        let query = r#"
        SELECT count(*), "RainToday"
        FROM weather
        GROUP BY "RainToday"
        ORDER BY count(*)
        "#;
        let plan = weather_query_to_split_plan(query, FINAL_AGG_SKIP_OPTIONS).await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=2, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0] t1:[p0] 
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p0..p1] t2:[p0..p1] 
            │ RepartitionExec: partitioning=Hash([RainToday@0], 2), input_partitions=1
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: tasks=3 partitions=3
            │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[test]
    fn partitioned_join_rejects_one_sided_split() -> Result<()> {
        let left: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            empty_key_exec(),
            Partitioning::Hash(vec![key_expr()], 2),
        )?);
        let left: Arc<dyn ExecutionPlan> =
            Arc::new(NetworkShuffleExec::try_new(left, Uuid::new_v4(), 1, 2, 2)?);
        let right: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            empty_key_exec(),
            Partitioning::Hash(vec![key_expr()], 8),
        )?);
        let join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            left,
            right,
            vec![(key_expr(), key_expr())],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(join));

        let d_cfg = DistributedConfig {
            local_exchange_split_mode: LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN.to_string(),
            local_exchange_split_max_owned_partitions: 2,
            local_exchange_split_target_partitions_per_task: 8,
            local_exchange_split_max_factor: 8,
            ..DistributedConfig::default()
        };
        let plan = insert_local_exchange_split_execs(plan, &d_cfg)?;
        let distributed = DistributedExec::new(plan);

        assert_snapshot!(display_plan_ascii(&distributed, false), @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(key@0, key@0)]
        │     [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=2
        │     RepartitionExec: partitioning=Hash([key@0], 8), input_partitions=1
        │       EmptyExec
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p0..p1] 
          │ RepartitionExec: partitioning=Hash([key@0], 2), input_partitions=1
          │   EmptyExec
          └──────────────────────────────────────────────────
        ");

        Ok(())
    }

    #[tokio::test]
    async fn partitioned_join_splits_both_sides() {
        let query = r#"
        SELECT d.env, f.timestamp, f.value
        FROM dim d
        INNER JOIN fact f ON d.d_dkey = f.f_dkey
        WHERE d.service = 'log'
        "#;
        let plan = join_query_to_split_plan(query, PARTITIONED_JOIN_SPLIT_OPTIONS).await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=16, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p7] t1:[p0..p7] 
          │ HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@1, f_dkey@2)], projection=[env@0, timestamp@2, value@3]
          │   LocalExchangeSplitExec: input_partitions=1, base_partitions=2, local_partitions=8, exprs=[d_dkey@1]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=2
          │   LocalExchangeSplitExec: input_partitions=1, base_partitions=2, local_partitions=8, exprs=[f_dkey@2]
          │     [Stage 2] => NetworkShuffleExec: output_partitions=1, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p0..p1] 
            │ RepartitionExec: partitioning=Hash([d_dkey@1], 2), input_partitions=2
            │   FilterExec: service@1 = log, projection=[env@0, d_dkey@2]
            │     PartitionIsolatorExec: tasks=2 partitions=4
            │       DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, d_dkey], file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p1] t1:[p0..p1] 
            │ RepartitionExec: partitioning=Hash([f_dkey@2], 2), input_partitions=2
            │   PartitionIsolatorExec: tasks=2 partitions=4
            │     DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=B/data0.parquet], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], file_type=parquet, predicate=DynamicFilter [ empty ]
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn all_narrow_mode_splits_window() {
        let query = r#"
        SELECT
            "RainToday",
            row_number() OVER (PARTITION BY "RainToday" ORDER BY "MinTemp") AS rn
        FROM weather
        "#;
        let plan = weather_query_to_split_plan(query, ALL_NARROW_WINDOW_OPTIONS).await;
        assert_snapshot!(plan, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3] t2:[p0..p3] 
          │ ProjectionExec: expr=[RainToday@1 as RainToday, row_number() PARTITION BY [weather.RainToday] ORDER BY [weather.MinTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@2 as rn]
          │   BoundedWindowAggExec: wdw=[row_number() PARTITION BY [weather.RainToday] ORDER BY [weather.MinTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Field { "row_number() PARTITION BY [weather.RainToday] ORDER BY [weather.MinTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW": UInt64 }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
          │     SortExec: expr=[RainToday@1 ASC NULLS LAST, MinTemp@0 ASC NULLS LAST], preserve_partitioning=[true]
          │       LocalExchangeSplitExec: input_partitions=1, base_partitions=3, local_partitions=4, exprs=[RainToday@1]
          │         [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] 
            │ RepartitionExec: partitioning=Hash([RainToday@1], 3), input_partitions=1
            │   PartitionIsolatorExec: tasks=3 partitions=3
            │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        "#);
    }

    #[tokio::test]
    async fn default_mode_skips_window() {
        let query = r#"
        SELECT
            "RainToday",
            row_number() OVER (PARTITION BY "RainToday" ORDER BY "MinTemp") AS rn
        FROM weather
        "#;
        let plan = weather_query_to_split_plan(query, FINAL_AGG_SPLIT_OPTIONS).await;
        assert_snapshot!(plan, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=3, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0] t1:[p0] t2:[p0] 
          │ ProjectionExec: expr=[RainToday@1 as RainToday, row_number() PARTITION BY [weather.RainToday] ORDER BY [weather.MinTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@2 as rn]
          │   BoundedWindowAggExec: wdw=[row_number() PARTITION BY [weather.RainToday] ORDER BY [weather.MinTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Field { "row_number() PARTITION BY [weather.RainToday] ORDER BY [weather.MinTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW": UInt64 }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
          │     SortExec: expr=[RainToday@1 ASC NULLS LAST, MinTemp@0 ASC NULLS LAST], preserve_partitioning=[true]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] 
            │ RepartitionExec: partitioning=Hash([RainToday@1], 3), input_partitions=1
            │   PartitionIsolatorExec: tasks=3 partitions=3
            │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        "#);
    }
}
