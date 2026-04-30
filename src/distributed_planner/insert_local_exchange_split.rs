use crate::distributed_planner::DistributedConfig;
use crate::{LocalExchangeSplitExec, NetworkShuffleExec};
use datafusion::common::Result;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning};
use std::sync::Arc;

/// Inserts [LocalExchangeSplitExec] above narrow post-shuffle consumers.
///
/// # Ordering
///
/// This pass runs after network-boundary insertion in [crate::distributed_planner::distribute_plan].
/// Thus, by the time it executes, every [NetworkShuffleExec] has its
/// [crate::distributed_planner::ExchangeLayout] assigned. This pass does not change network
/// ownership; it only adds local parallelism inside each consumer task.
///
/// # What it inserts
///
/// A [LocalExchangeSplitExec] is placed directly above a narrow shuffle consumer:
/// - [AggregateExec] in [AggregateMode::FinalPartitioned] mode
/// - [HashJoinExec] in [PartitionMode::Partitioned] mode
///
/// It splits the owned logical partition range into `local_partition_count` local output
/// partitions per owned partition, re-hashing on the same keys the upstream
/// [datafusion::physical_plan::repartition::RepartitionExec] used.
///
/// # Modes
///
/// Behavior is determined by [DistributedConfig::local_exchange_split_mode]:
///
/// - [crate::distributed_planner::LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG]
///   — only insert above final partitioned aggregates.
/// - [crate::distributed_planner::LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN]
///   — insert above final partitioned aggregates and partitioned hash joins (default).
/// - [crate::distributed_planner::LOCAL_EXCHANGE_SPLIT_MODE_ALL_NARROW_SHUFFLES]
///   — insert above any narrow post-shuffle consumer that requires more partitions.
///
/// # Before
///
/// ```text
///               ┌─┐                                 ┌─┐
///               │1│                                 │1│
/// ┌─────────────┴─┴─────────────┐     ┌─────────────┴─┴─────────────┐
/// │  AggregationExec(Final) /   │     │  AggregationExec(Final) /   │
/// │  HashJoinExec(Partitioned)  │ ... │  HashJoinExec(Partitioned)  │
/// │          (Task 1)           │     │          (Task M)           │
/// │                             │     │                             │
/// └─────────────┬─┬─────────────┘     └─────────────┬─┬─────────────┘
///               │1│                                 │1│
///               └▲┘                                 └▲┘
///                │                                   │
///                │                                   │
///                │                                   │
///               ┌─┐                                 ┌─┐
///               │1│                                 │1│
/// ┌─────────────┴─┴─────────────┐     ┌─────────────┴─┴─────────────┐
/// │                             │     │                             │
/// │     NetworkShuffleExec      │ ... │     NetworkShuffleExec      │
/// │          (Task 1)           │     │          (Task M)           │
/// │                             │     │                             │
/// └─────────────┬─┬─────────────┘     └─────────────┬─┬─────────────┘
///               │1│                                 │1│
///               └▲┘                                 └▲┘
///                │                                   │
///                │                                   │
///                └─────────────┐       ┌─────────────┘
///                              │       │
///                              │       │
///                              │       │
///                             ┌─┐ ... ┌─┐
///                             │1│     │N│
///                   ┌─────────┴─┴─────┴─┴─────────┐
///                   │                             │
///                   │       RepartitionExec       │
///                   │          (Task 1)           │
///                   │                             │
///                   └─────────┬─┬─────┬─┬─────────┘
///                             │1│     │N│
///                             └─┘ ... └─┘
/// ```
///
/// # After
///
/// ```text
///           ┌─┐ ... ┌─┐                         ┌─┐ ... ┌─┐
///           │1│     │L│                         │1│     │L│
/// ┌─────────┴─┴─────┴─┴─────────┐     ┌─────────┴─┴─────┴─┴─────────┐
/// │  AggregationExec(Final) /   │     │  AggregationExec(Final) /   │
/// │  HashJoinExec(Partitioned)  │ ... │  HashJoinExec(Partitioned)  │
/// │          (Task 1)           │     │          (Task M)           │
/// │                             │     │                             │
/// └─────────┬─┬─────┬─┬─────────┘     └─────────┬─┬─────┬─┬─────────┘
///           │1│     │L│                         │1│     │L│
///           └▲┘ ... └▲┘                         └▲┘ ... └▲┘
///            │       │                           │       │
///            │       │                           │       │
///           ┌─┐ ... ┌─┐                         ┌─┐ ... ┌─┐
///           │1│     │L│                         │1│     │L│
/// ┌─────────┴─┴─────┴─┴─────────┐     ┌─────────┴─┴─────┴─┴─────────┐
/// │                             │     │                             │
/// │   LocalExchangeSplitExec    │ ... │   LocalExchangeSplitExec    │
/// │          (Task 1)           │     │          (Task M)           │
/// │                             │     │                             │
/// └─────────────┬─┬─────────────┘     └─────────────┬─┬─────────────┘
///               │1│                                 │1│
///               └▲┘                                 └▲┘
///                │                                   │
///                │                                   │
///               ┌─┐                                 ┌─┐
///               │1│                                 │1│
/// ┌─────────────┴─┴─────────────┐     ┌─────────────┴─┴─────────────┐
/// │                             │     │                             │
/// │     NetworkShuffleExec      │ ... │     NetworkShuffleExec      │
/// │          (Task 1)           │     │          (Task M)           │
/// │                             │     │                             │
/// └─────────────┬─┬─────────────┘     └─────────────┬─┬─────────────┘
///               │1│                                 │1│
///               └▲┘                                 └▲┘
///                │                                   │
///                │                                   │
///                └─────────────┐       ┌─────────────┘
///                              │       │
///                              │       │
///                              │       │
///                             ┌─┐ ... ┌─┐
///                             │1│     │N│
///                   ┌─────────┴─┴─────┴─┴─────────┐
///                   │                             │
///                   │       RepartitionExec       │
///                   │          (Task 1)           │
///                   │                             │
///                   └─────────┬─┬─────┬─┬─────────┘
///                             │1│     │N│
///                             └─┘ ... └─┘
/// ```
pub(crate) fn insert_local_exchange_split_execs(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    rewrite_with_requirement(plan, d_cfg, FanoutRequirement::PolicyDriven)
}

#[derive(Clone, Copy)]
enum FanoutRequirement {
    PolicyDriven,
    RequiredOutputPartitions(usize),
}

fn rewrite_with_requirement(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    requirement: FanoutRequirement,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>()
        && d_cfg.local_exchange_split_mode_allows_partitioned_join()
        && join.partition_mode() == &PartitionMode::Partitioned
    {
        return rewrite_partitioned_join(plan, d_cfg, requirement);
    }

    if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>()
        && aggregate.mode() == &AggregateMode::FinalPartitioned
        && (d_cfg.local_exchange_split_mode_allows_final_agg()
            || matches!(requirement, FanoutRequirement::RequiredOutputPartitions(_)))
    {
        return rewrite_final_partitioned_aggregate(plan, d_cfg, requirement);
    }

    // A shuffle's producer stage is independent of the consumer-local fanout requirement. Rewrite
    // that producer stage first using normal policy, then decide whether to split this shuffle.
    let child_requirement =
        if LocalExchangeSplitCandidate::try_from_plan(Arc::clone(&plan), d_cfg)?.is_some() {
            FanoutRequirement::PolicyDriven
        } else {
            requirement
        };
    let plan = rewrite_children(plan, d_cfg, child_requirement)?;

    if let Some(candidate) = LocalExchangeSplitCandidate::try_from_plan(Arc::clone(&plan), d_cfg)?
        && let Some(target_partition_count) =
            target_partition_count_for_candidate(&candidate, d_cfg, requirement)
    {
        return candidate.into_plan(target_partition_count);
    }

    Ok(plan)
}

fn default_target_partition_count_for_owned(
    owned: usize,
    target_partitions_per_task: usize,
) -> Option<usize> {
    if owned == 0 || target_partitions_per_task == 0 || owned >= target_partitions_per_task {
        return None;
    }

    Some(target_partitions_per_task.div_ceil(owned) * owned)
}

fn target_partition_count_for_candidate(
    candidate: &LocalExchangeSplitCandidate,
    d_cfg: &DistributedConfig,
    requirement: FanoutRequirement,
) -> Option<usize> {
    match requirement {
        FanoutRequirement::RequiredOutputPartitions(target) => {
            candidate.can_produce(target).then_some(target)
        }
        FanoutRequirement::PolicyDriven
            if d_cfg.local_exchange_split_mode_is_all_narrow_shuffles() =>
        {
            default_target_partition_count_for_owned(
                candidate.owned_partition_count,
                d_cfg.local_exchange_split_target_partitions_per_task,
            )
        }
        FanoutRequirement::PolicyDriven => None,
    }
}

fn rewrite_final_partitioned_aggregate(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    requirement: FanoutRequirement,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }

    let input = Arc::clone(children[0]);
    let target = match requirement {
        FanoutRequirement::RequiredOutputPartitions(target) => Some(target),
        FanoutRequirement::PolicyDriven => {
            let candidate =
                LocalExchangeSplitCandidate::try_find_source(Arc::clone(&input), d_cfg)?;
            candidate.as_ref().and_then(|candidate| {
                default_target_partition_count_for_owned(
                    candidate.owned_partition_count,
                    d_cfg.local_exchange_split_target_partitions_per_task,
                )
            })
        }
    };

    let mut new_children = Vec::with_capacity(children.len());
    let input_requirement = target
        .map(FanoutRequirement::RequiredOutputPartitions)
        .unwrap_or(FanoutRequirement::PolicyDriven);
    new_children.push(rewrite_with_requirement(input, d_cfg, input_requirement)?);
    for child in children.into_iter().skip(1) {
        new_children.push(rewrite_with_requirement(
            Arc::clone(child),
            d_cfg,
            FanoutRequirement::PolicyDriven,
        )?);
    }

    plan.with_new_children(new_children)
}

fn derive_join_target_partition_count(
    left: &LocalExchangeSplitCandidate,
    right: &LocalExchangeSplitCandidate,
    target_per_task: usize,
) -> Option<usize> {
    if left.base_partition_count != right.base_partition_count
        || left.owned_partition_count == 0
        || right.owned_partition_count == 0
        || target_per_task == 0
    {
        return None;
    }

    let common_multiple = lcm(left.owned_partition_count, right.owned_partition_count);
    let minimum_target = target_per_task
        .max(left.output_partition_count)
        .max(right.output_partition_count);
    let common_target = minimum_target.div_ceil(common_multiple) * common_multiple;

    if !left.can_produce(common_target) || !right.can_produce(common_target) {
        return None;
    }

    Some(common_target)
}

fn can_join_produce_target_partition_count(
    left: &LocalExchangeSplitCandidate,
    right: &LocalExchangeSplitCandidate,
    target_partition_count: usize,
) -> bool {
    if !left.can_produce(target_partition_count) || !right.can_produce(target_partition_count) {
        return false;
    }

    if target_partition_count == left.output_partition_count
        && target_partition_count == right.output_partition_count
    {
        return true;
    }

    // Widening both join sides is only safe when they refine the same upstream hash.
    left.base_partition_count == right.base_partition_count
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

fn rewrite_partitioned_join(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    requirement: FanoutRequirement,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    if children.len() < 2 {
        return Ok(plan);
    }

    let left = Arc::clone(children[0]);
    let right = Arc::clone(children[1]);
    let left_candidate = LocalExchangeSplitCandidate::try_find_source(Arc::clone(&left), d_cfg)?;
    let right_candidate = LocalExchangeSplitCandidate::try_find_source(Arc::clone(&right), d_cfg)?;

    let (Some(left_candidate), Some(right_candidate)) = (&left_candidate, &right_candidate) else {
        // Partitioned joins require equal input partition counts. If only one side can be split,
        // recurse with each child's current count rather than widening one side independently.
        return rewrite_partitioned_join_children_preserving_output_counts(plan, d_cfg);
    };

    let target_partition_count = match requirement {
        FanoutRequirement::RequiredOutputPartitions(target)
            if can_join_produce_target_partition_count(left_candidate, right_candidate, target) =>
        {
            target
        }
        FanoutRequirement::RequiredOutputPartitions(_) => {
            return rewrite_partitioned_join_children_preserving_output_counts(plan, d_cfg);
        }
        FanoutRequirement::PolicyDriven => {
            let Some(target) = derive_join_target_partition_count(
                left_candidate,
                right_candidate,
                d_cfg.local_exchange_split_target_partitions_per_task,
            ) else {
                return rewrite_partitioned_join_children_preserving_output_counts(plan, d_cfg);
            };
            target
        }
    };

    if target_partition_count == 0 {
        return rewrite_partitioned_join_children_preserving_output_counts(plan, d_cfg);
    }

    let mut new_children = Vec::with_capacity(children.len());
    for (idx, child) in children.into_iter().enumerate() {
        let child = Arc::clone(child);
        let requirement = match idx {
            0 | 1 => FanoutRequirement::RequiredOutputPartitions(target_partition_count),
            _ => FanoutRequirement::PolicyDriven,
        };
        new_children.push(rewrite_with_requirement(child, d_cfg, requirement)?);
    }

    plan.with_new_children(new_children)
}

fn rewrite_partitioned_join_children_preserving_output_counts(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    let mut new_children = Vec::with_capacity(children.len());

    for child in children {
        let required_partition_count = child.output_partitioning().partition_count();
        new_children.push(rewrite_with_requirement(
            Arc::clone(child),
            d_cfg,
            FanoutRequirement::RequiredOutputPartitions(required_partition_count),
        )?);
    }

    plan.with_new_children(new_children)
}

fn rewrite_children(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    requirement: FanoutRequirement,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }

    let required_output_partition_count = match requirement {
        FanoutRequirement::RequiredOutputPartitions(count) => Some(count),
        FanoutRequirement::PolicyDriven => None,
    };
    let plan_output_partition_count = plan.output_partitioning().partition_count();

    let new_children = children
        .into_iter()
        .map(|child| {
            // A required output partition count must flow into children that determine this node's
            // output partitioning, including the streamed side of collect-left joins.
            let child_requirement = if required_output_partition_count.is_some()
                && child.output_partitioning().partition_count() == plan_output_partition_count
            {
                requirement
            } else {
                FanoutRequirement::PolicyDriven
            };
            rewrite_with_requirement(Arc::clone(child), d_cfg, child_requirement)
        })
        .collect::<Result<Vec<_>>>()?;

    plan.with_new_children(new_children)
}

#[derive(Clone)]
struct LocalExchangeSplitCandidate {
    plan: Arc<dyn ExecutionPlan>,
    hash_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    base_partition_count: usize,
    owned_partition_count: usize,
    output_partition_count: usize,
    can_insert_split: bool,
}

impl LocalExchangeSplitCandidate {
    fn try_from_plan(
        plan: Arc<dyn ExecutionPlan>,
        d_cfg: &DistributedConfig,
    ) -> Result<Option<Self>> {
        if let Some(split) = plan.as_any().downcast_ref::<LocalExchangeSplitExec>() {
            let output_partition_count = plan.output_partitioning().partition_count();
            let base_partition_count = split.base_partition_count();
            let local_partition_count = split.local_partition_count();
            return Ok(Some(Self {
                plan,
                hash_exprs: Vec::new(),
                base_partition_count,
                owned_partition_count: output_partition_count / local_partition_count,
                output_partition_count,
                can_insert_split: false,
            }));
        }

        let Some(shuffle) = plan.as_any().downcast_ref::<NetworkShuffleExec>() else {
            return Ok(None);
        };

        let layout = &shuffle.layout;
        let owned_partition_count = layout.max_partition_count_per_consumer();
        let consumer_task_count = layout.consumer_task_count();
        if owned_partition_count == 0
            || owned_partition_count > d_cfg.local_exchange_split_max_owned_partitions
            || consumer_task_count <= 1
        {
            return Ok(None);
        }

        let Some(Partitioning::Hash(hash_exprs, base_partition_count)) =
            layout.producer_partitioning().cloned()
        else {
            return Ok(None);
        };
        let output_partition_count = layout.max_partition_count_per_consumer();

        Ok(Some(Self {
            plan,
            hash_exprs,
            base_partition_count,
            owned_partition_count,
            output_partition_count,
            can_insert_split: true,
        }))
    }

    /// Finds a shuffle or existing split through unary nodes that preserve partition count.
    fn try_find_source(
        plan: Arc<dyn ExecutionPlan>,
        d_cfg: &DistributedConfig,
    ) -> Result<Option<Self>> {
        if let Some(candidate) = Self::try_from_plan(Arc::clone(&plan), d_cfg)? {
            return Ok(Some(candidate));
        }

        let children = plan.children();
        let [child] = children.as_slice() else {
            return Ok(None);
        };
        let Some(candidate) = Self::try_find_source(Arc::clone(child), d_cfg)? else {
            return Ok(None);
        };

        if plan.output_partitioning().partition_count() != candidate.output_partition_count {
            return Ok(None);
        }

        Ok(Some(candidate))
    }

    fn can_produce(&self, target_partition_count: usize) -> bool {
        if self.output_partition_count == target_partition_count {
            return true;
        }
        if !self.can_insert_split
            || target_partition_count == 0
            || target_partition_count % self.owned_partition_count != 0
        {
            return false;
        }

        target_partition_count > self.output_partition_count
    }

    fn into_plan(&self, target_partition_count: usize) -> Result<Arc<dyn ExecutionPlan>> {
        if self.output_partition_count == target_partition_count {
            return Ok(Arc::clone(&self.plan));
        }

        if !self.can_produce(target_partition_count) {
            return Ok(Arc::clone(&self.plan));
        }

        Ok(Arc::new(LocalExchangeSplitExec::try_new(
            Arc::clone(&self.plan),
            self.hash_exprs.clone(),
            self.base_partition_count,
            target_partition_count / self.owned_partition_count,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LocalExchangeSplitCandidate, default_target_partition_count_for_owned,
        derive_join_target_partition_count, insert_local_exchange_split_execs,
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
        force_partitioned_join: bool,
    }

    const FINAL_AGG_SPLIT_OPTIONS: SplitTestOptions = SplitTestOptions {
        mode: LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN,
        max_owned_partitions: 2,
        target_partitions_per_task: 4,
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
        force_partitioned_join: true,
    };

    const ALL_NARROW_WINDOW_OPTIONS: SplitTestOptions = SplitTestOptions {
        mode: LOCAL_EXCHANGE_SPLIT_MODE_ALL_NARROW_SHUFFLES,
        max_owned_partitions: 2,
        target_partitions_per_task: 4,
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

    fn shuffle_key_exec(stage_id: usize) -> Result<Arc<dyn ExecutionPlan>> {
        let repartition: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            empty_key_exec(),
            Partitioning::Hash(vec![key_expr()], 2),
        )?);
        Ok(Arc::new(NetworkShuffleExec::try_new(
            repartition,
            Uuid::new_v4(),
            stage_id,
            2,
            2,
        )?))
    }

    fn split_candidate(
        owned_partition_count: usize,
        output_partition_count: usize,
        can_insert_split: bool,
    ) -> LocalExchangeSplitCandidate {
        LocalExchangeSplitCandidate {
            plan: empty_key_exec(),
            hash_exprs: vec![key_expr()],
            base_partition_count: 2,
            owned_partition_count,
            output_partition_count,
            can_insert_split,
        }
    }

    #[test]
    fn join_target_uses_target_when_divisible() {
        let left = split_candidate(1, 1, true);
        let right = split_candidate(2, 2, true);
        assert_eq!(
            derive_join_target_partition_count(&left, &right, 8),
            Some(8)
        );
    }

    #[test]
    fn join_target_uses_common_multiple() {
        let left = split_candidate(2, 2, true);
        let right = split_candidate(3, 3, true);
        assert_eq!(
            derive_join_target_partition_count(&left, &right, 8),
            Some(12)
        );
    }

    #[test]
    fn join_target_rejects_existing_wide_side_that_cannot_be_matched() {
        let left = split_candidate(5, 5, true);
        let right = split_candidate(3, 12, false);
        assert_eq!(derive_join_target_partition_count(&left, &right, 8), None);
    }

    #[test]
    fn join_target_matches_existing_split_side() {
        let left = split_candidate(1, 1, true);
        let right = split_candidate(1, 8, false);
        assert_eq!(
            derive_join_target_partition_count(&left, &right, 8),
            Some(8)
        );
    }

    #[test]
    fn target_partition_count_uses_next_multiple_of_owned_count() {
        assert_eq!(default_target_partition_count_for_owned(2, 8), Some(8));
        assert_eq!(default_target_partition_count_for_owned(3, 8), Some(9));
        assert_eq!(default_target_partition_count_for_owned(8, 8), None);
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
    fn partitioned_join_preserves_counts_when_only_one_side_can_split() -> Result<()> {
        let left = shuffle_key_exec(1)?;
        let right = empty_key_exec();
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
            local_exchange_split_mode: LOCAL_EXCHANGE_SPLIT_MODE_ALL_NARROW_SHUFFLES.to_string(),
            local_exchange_split_max_owned_partitions: 2,
            local_exchange_split_target_partitions_per_task: 8,
            ..DistributedConfig::default()
        };
        let plan = insert_local_exchange_split_execs(plan, &d_cfg)?;
        let distributed = DistributedExec::new(plan);

        assert_snapshot!(display_plan_ascii(&distributed, false), @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(key@0, key@0)]
        │     [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=2
        │     EmptyExec
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p0..p1]
          │ RepartitionExec: partitioning=Hash([key@0], 2), input_partitions=1
          │   EmptyExec
          └──────────────────────────────────────────────────
        ");

        Ok(())
    }

    #[test]
    fn nested_partitioned_join_preserves_parent_required_count() -> Result<()> {
        let inner_join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            shuffle_key_exec(1)?,
            shuffle_key_exec(2)?,
            vec![(key_expr(), key_expr())],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?);
        let parent_join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            inner_join,
            shuffle_key_exec(3)?,
            vec![(key_expr(), key_expr())],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(parent_join));

        let d_cfg = DistributedConfig {
            local_exchange_split_mode: LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN.to_string(),
            local_exchange_split_max_owned_partitions: 2,
            local_exchange_split_target_partitions_per_task: 8,
            ..DistributedConfig::default()
        };
        let plan = insert_local_exchange_split_execs(plan, &d_cfg)?;
        let distributed = DistributedExec::new(plan);

        assert_snapshot!(display_plan_ascii(&distributed, false), @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(key@0, key@0)]
        │     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(key@0, key@0)]
        │       [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=2
        │       [Stage 2] => NetworkShuffleExec: output_partitions=1, input_tasks=2
        │     [Stage 3] => NetworkShuffleExec: output_partitions=1, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p0..p1]
          │ RepartitionExec: partitioning=Hash([key@0], 2), input_partitions=1
          │   EmptyExec
          └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p1] t1:[p0..p1]
          │ RepartitionExec: partitioning=Hash([key@0], 2), input_partitions=1
          │   EmptyExec
          └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p1] t1:[p0..p1]
          │ RepartitionExec: partitioning=Hash([key@0], 2), input_partitions=1
          │   EmptyExec
          └──────────────────────────────────────────────────
        ");

        Ok(())
    }

    #[test]
    fn partitioned_join_requirement_flows_through_collect_left_stream_child() -> Result<()> {
        let inner_join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            shuffle_key_exec(1)?,
            shuffle_key_exec(2)?,
            vec![(key_expr(), key_expr())],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?);
        let collect_left_join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            empty_key_exec(),
            inner_join,
            vec![(key_expr(), key_expr())],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?);
        let parent_join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            collect_left_join,
            shuffle_key_exec(3)?,
            vec![(key_expr(), key_expr())],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(parent_join));

        let d_cfg = DistributedConfig {
            local_exchange_split_mode: LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN.to_string(),
            local_exchange_split_max_owned_partitions: 2,
            local_exchange_split_target_partitions_per_task: 8,
            ..DistributedConfig::default()
        };
        let plan = insert_local_exchange_split_execs(plan, &d_cfg)?;
        let distributed = DistributedExec::new(plan);

        assert_snapshot!(display_plan_ascii(&distributed, false), @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(key@0, key@0)]
        │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(key@0, key@0)]
        │       EmptyExec
        │       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(key@0, key@0)]
        │         [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=2
        │         [Stage 2] => NetworkShuffleExec: output_partitions=1, input_tasks=2
        │     [Stage 3] => NetworkShuffleExec: output_partitions=1, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p0..p1]
          │ RepartitionExec: partitioning=Hash([key@0], 2), input_partitions=1
          │   EmptyExec
          └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p1] t1:[p0..p1]
          │ RepartitionExec: partitioning=Hash([key@0], 2), input_partitions=1
          │   EmptyExec
          └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p1] t1:[p0..p1]
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
}
