use crate::distributed_planner::DistributedConfig;
use crate::{LocalExchangeSplitExec, NetworkShuffleExec};
use datafusion::common::Result;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{
    Distribution, ExecutionPlan, ExecutionPlanProperties, Partitioning,
};
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
/// - [crate::distributed_planner::LOCAL_EXCHANGE_SPLIT_MODE_OFF]
///   — do not insert local exchange splits.
/// - [crate::distributed_planner::LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG]
///   — only insert above final partitioned aggregates.
/// - [crate::distributed_planner::LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN]
///   — insert above final partitioned aggregates and partitioned hash joins (default).
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
///                └─────────────┐       ┌─────────────┘
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
///                └─────────────┐       ┌─────────────┘
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
    if d_cfg.local_exchange_split_mode_is_off() {
        return Ok(plan);
    }

    rewrite_plan(plan, d_cfg, None)
}

/// Local exchange insertion rule.
///
/// Guidelines:
/// - only [AggregateExec] with [AggregateMode::FinalPartitioned] and [HashJoinExec] with
///   [PartitionMode::Partitioned] can request a split
/// - the split is inserted only above the first shuffle/split source
/// - rebuilt children must satisfy DataFusion's required input distribution
/// - partitioned joins must expose the same partition count on both sides
/// - fetch/limit/top-k nodes block insertion because their cap is partition-local
fn rewrite_plan(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    required_partition_count: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>()
        && join.partition_mode() == &PartitionMode::Partitioned
    {
        if d_cfg.local_exchange_split_mode_allows_partitioned_join() {
            return rewrite_partitioned_join(plan, d_cfg, required_partition_count);
        }

        // Even when this mode does not insert splits above joins, partitioned joins are
        // alignment boundaries. Do not let a nested consumer independently widen one side.
        return rewrite_children_preserving_current_counts(plan, d_cfg);
    }

    if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>()
        && aggregate.mode() == &AggregateMode::FinalPartitioned
        && d_cfg.local_exchange_split_mode_allows_final_agg()
    {
        return rewrite_final_partitioned_aggregate(plan, d_cfg, required_partition_count);
    }

    // A shuffle/split boundary starts an independent producer stage. A parent partition-count
    // request must not leak through it, even if that shuffle is not itself split-eligible.
    let plan = if plan.as_any().is::<NetworkShuffleExec>()
        || plan.as_any().is::<LocalExchangeSplitExec>()
    {
        rewrite_children_propagating_required_count(plan, d_cfg, None)?
    } else {
        rewrite_children_propagating_required_count(plan, d_cfg, required_partition_count)?
    };
    if let Some(target) = required_partition_count
        && let Some(rewritten) =
            try_insert_split_on_unary_path(Arc::clone(&plan), d_cfg, Some(target))?
        && rewritten.output_partitioning().partition_count() == target
    {
        return Ok(rewritten);
    }

    Ok(plan)
}

fn rewrite_children_propagating_required_count(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    required_partition_count: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }

    let plan_output_partition_count = plan.output_partitioning().partition_count();

    let new_children = children
        .into_iter()
        .map(|child| {
            let child_partition_count = child.output_partitioning().partition_count();
            let child_required_partition_count = if required_partition_count.is_some()
                && child_partition_count == plan_output_partition_count
            {
                required_partition_count
            } else {
                None
            };
            rewrite_plan(Arc::clone(child), d_cfg, child_required_partition_count)
        })
        .collect::<Result<Vec<_>>>()?;

    plan.with_new_children(new_children)
}

fn rewrite_children_preserving_current_counts(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }

    let new_children = children
        .into_iter()
        .map(|child| {
            rewrite_plan(
                Arc::clone(child),
                d_cfg,
                Some(child.output_partitioning().partition_count()),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    plan.with_new_children(new_children)
}

fn rewrite_final_partitioned_aggregate(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    required_partition_count: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(rewritten) =
        try_rewrite_final_partitioned_aggregate(Arc::clone(&plan), d_cfg, required_partition_count)?
    {
        return Ok(rewritten);
    }

    // If a parent required this aggregate to keep a specific count, do not let nested consumers
    // widen independently after this aggregate cannot use a split.
    if required_partition_count.is_some() {
        rewrite_children_preserving_current_counts(plan, d_cfg)
    } else {
        rewrite_children_propagating_required_count(plan, d_cfg, None)
    }
}

fn try_rewrite_final_partitioned_aggregate(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    required_partition_count: Option<usize>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let children = plan.children();
    let [input] = children.as_slice() else {
        return Ok(Some(plan));
    };

    let Some(required_distribution) = hash_requirement(&plan, 0) else {
        return Ok(None);
    };

    let Some(rewritten_input) = try_split_child_and_satisfy_distribution(
        Arc::clone(input),
        d_cfg,
        &required_distribution,
        required_partition_count,
    )?
    else {
        return Ok(None);
    };

    let rebuilt = Arc::clone(&plan).with_new_children(vec![rewritten_input])?;
    Ok(partition_count_matches(&rebuilt, required_partition_count).then_some(rebuilt))
}

fn rewrite_partitioned_join(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    required_partition_count: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(rewritten) =
        try_rewrite_partitioned_join(Arc::clone(&plan), d_cfg, required_partition_count)?
    {
        Ok(rewritten)
    } else {
        rewrite_children_preserving_current_counts(plan, d_cfg)
    }
}

fn try_rewrite_partitioned_join(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    required_partition_count: Option<usize>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let children = plan.children();
    let [left, right] = children.as_slice() else {
        return Ok(Some(plan));
    };
    let (left, right) = (Arc::clone(left), Arc::clone(right));

    let Some(left_distribution) = hash_requirement(&plan, 0) else {
        return Ok(None);
    };
    let Some(right_distribution) = hash_requirement(&plan, 1) else {
        return Ok(None);
    };

    // Joins must pick one count before rewriting either side; changing one side independently can
    // make the join partitioning invalid.
    let target_partition_count = match required_partition_count {
        Some(target) => target,
        None => {
            let Some(left_input) = SplitInput::find_on_unary_path(Arc::clone(&left), d_cfg)? else {
                return Ok(None);
            };
            let Some(right_input) = SplitInput::find_on_unary_path(Arc::clone(&right), d_cfg)?
            else {
                return Ok(None);
            };
            let Some(target) = left_input.aligned_target_partition_count(
                &right_input,
                d_cfg.local_exchange_split_target_partitions_per_task,
            ) else {
                return Ok(None);
            };
            target
        }
    };

    let Some(rewritten_left) = try_split_child_and_satisfy_distribution(
        Arc::clone(&left),
        d_cfg,
        &left_distribution,
        Some(target_partition_count),
    )?
    else {
        return Ok(None);
    };
    let Some(rewritten_right) = try_split_child_and_satisfy_distribution(
        Arc::clone(&right),
        d_cfg,
        &right_distribution,
        Some(target_partition_count),
    )?
    else {
        return Ok(None);
    };

    if rewritten_left.output_partitioning().partition_count() != target_partition_count
        || rewritten_right.output_partitioning().partition_count() != target_partition_count
    {
        return Ok(None);
    }

    let rewritten = Arc::clone(&plan).with_new_children(vec![rewritten_left, rewritten_right])?;
    Ok(partition_count_matches(&rewritten, required_partition_count).then_some(rewritten))
}

/// The first shuffle-side source on a consumer input path.
///
/// `NetworkShuffleExec` can be widened by inserting a `LocalExchangeSplitExec` above it. An
/// existing `LocalExchangeSplitExec` cannot be widened again, but it can satisfy a parent join that
/// already requires the same output partition count.
#[derive(Clone)]
struct SplitInput {
    plan: Arc<dyn ExecutionPlan>,
    hash_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    base_partition_count: usize,
    owned_partition_count: usize,
    output_partition_count: usize,
    already_split: bool,
}

impl SplitInput {
    /// Returns a shuffle/split boundary without recursively rewriting it.
    fn try_from_source(
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
                already_split: true,
            }));
        }

        let Some(shuffle) = plan.as_any().downcast_ref::<NetworkShuffleExec>() else {
            return Ok(None);
        };

        let layout = &shuffle.layout;
        let owned_partition_count = layout.max_partition_count_per_consumer();
        let consumer_task_count = layout.consumer_task_count();

        let Some(Partitioning::Hash(hash_exprs, base_partition_count)) =
            layout.producer_partitioning().cloned()
        else {
            return Ok(None);
        };
        if owned_partition_count == 0
            || owned_partition_count > d_cfg.local_exchange_split_max_owned_partitions
            || (consumer_task_count <= 1 && base_partition_count <= 1)
        {
            return Ok(None);
        }

        Ok(Some(Self {
            plan,
            hash_exprs,
            base_partition_count,
            output_partition_count: owned_partition_count,
            owned_partition_count,
            already_split: false,
        }))
    }

    /// Finds the first shuffle/split source on a unary path.
    fn find_on_unary_path(
        plan: Arc<dyn ExecutionPlan>,
        d_cfg: &DistributedConfig,
    ) -> Result<Option<Self>> {
        if let Some(input) = Self::try_from_source(Arc::clone(&plan), d_cfg)? {
            return Ok(Some(input));
        }

        let children = plan.children();
        let [child] = children.as_slice() else {
            return Ok(None);
        };

        Self::find_on_unary_path(Arc::clone(child), d_cfg)
    }

    /// Returns whether this boundary can expose exactly `target_partition_count` outputs.
    fn can_produce(&self, target_partition_count: usize) -> bool {
        if self.output_partition_count == target_partition_count {
            return true;
        }
        if self.already_split
            || target_partition_count == 0
            || !target_partition_count.is_multiple_of(self.owned_partition_count)
        {
            return false;
        }

        target_partition_count > self.output_partition_count
    }

    /// Picks the default widened count for an independently split consumer.
    fn inferred_target_partition_count(&self, target_per_task: usize) -> Option<usize> {
        if self.owned_partition_count == 0
            || target_per_task == 0
            || self.owned_partition_count >= target_per_task
        {
            return None;
        }

        Some(target_per_task.div_ceil(self.owned_partition_count) * self.owned_partition_count)
    }

    /// Picks the smallest shared count both join sides can produce.
    fn aligned_target_partition_count(
        &self,
        other: &Self,
        target_per_task: usize,
    ) -> Option<usize> {
        if self.base_partition_count != other.base_partition_count
            || self.owned_partition_count == 0
            || other.owned_partition_count == 0
            || target_per_task == 0
        {
            return None;
        }

        let common_multiple = lcm(self.owned_partition_count, other.owned_partition_count)?;
        let minimum_target = target_per_task
            .max(self.output_partition_count)
            .max(other.output_partition_count);
        let target = minimum_target
            .div_ceil(common_multiple)
            .checked_mul(common_multiple)?;

        (self.can_produce(target) && other.can_produce(target)).then_some(target)
    }

    /// Inserts the split, or returns the existing source if it already has the target count.
    fn insert(&self, target_partition_count: usize) -> Result<Arc<dyn ExecutionPlan>> {
        debug_assert!(
            self.can_produce(target_partition_count),
            "insert called with target {target_partition_count} that can_produce rejected"
        );

        if self.output_partition_count == target_partition_count {
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

/// Returns the hash distribution a child must satisfy, if the consumer benefits from it.
fn hash_requirement(plan: &Arc<dyn ExecutionPlan>, child_idx: usize) -> Option<Distribution> {
    if !plan
        .benefits_from_input_partitioning()
        .get(child_idx)
        .copied()
        .unwrap_or(false)
    {
        return None;
    }

    match plan.required_input_distribution().get(child_idx)? {
        requirement @ Distribution::HashPartitioned(_) => Some(requirement.clone()),
        Distribution::SinglePartition | Distribution::UnspecifiedDistribution => None,
    }
}

/// Inserts a split on a child path and validates the consumer distribution.
fn try_split_child_and_satisfy_distribution(
    child: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    required_distribution: &Distribution,
    requested_target_count: Option<usize>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(rewritten) = try_insert_split_on_unary_path(child, d_cfg, requested_target_count)?
    else {
        return Ok(None);
    };

    if !partition_count_matches(&rewritten, requested_target_count) {
        return Ok(None);
    }

    Ok(rewritten
        .output_partitioning()
        .satisfaction(
            required_distribution,
            rewritten.equivalence_properties(),
            false,
        )
        .is_satisfied()
        .then_some(rewritten))
}

/// Walks a unary path, rewrites the producer stage at the boundary, and inserts a split.
fn try_insert_split_on_unary_path(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
    requested_target_count: Option<usize>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if plan.as_any().is::<NetworkShuffleExec>() || plan.as_any().is::<LocalExchangeSplitExec>() {
        let plan = rewrite_children_propagating_required_count(plan, d_cfg, None)?;
        let Some(input) = SplitInput::try_from_source(Arc::clone(&plan), d_cfg)? else {
            return Ok(None);
        };
        let Some(target_count) = requested_target_count.or_else(|| {
            input.inferred_target_partition_count(
                d_cfg.local_exchange_split_target_partitions_per_task,
            )
        }) else {
            return Ok(None);
        };
        if !input.can_produce(target_count) {
            return Ok(None);
        }
        return input.insert(target_count).map(Some);
    }

    let children = plan.children();
    let [child] = children.as_slice() else {
        return Ok(None);
    };

    // Fetch/limit/top-k nodes apply their cap per output partition in DataFusion. Splitting below
    // one would change how many rows can reach the parent consumer.
    if plan.fetch().is_some() {
        return Ok(None);
    }

    let Some(rewritten_child) =
        try_insert_split_on_unary_path(Arc::clone(child), d_cfg, requested_target_count)?
    else {
        return Ok(None);
    };

    let Ok(rebuilt) = plan.with_new_children(vec![rewritten_child]) else {
        return Ok(None);
    };

    if !partition_count_matches(&rebuilt, requested_target_count) {
        return Ok(None);
    }

    Ok(Some(rebuilt))
}

fn partition_count_matches(plan: &Arc<dyn ExecutionPlan>, required: Option<usize>) -> bool {
    required.is_none_or(|target| plan.output_partitioning().partition_count() == target)
}

fn lcm(lhs: usize, rhs: usize) -> Option<usize> {
    fn gcd(mut lhs: usize, mut rhs: usize) -> usize {
        while rhs != 0 {
            let rem = lhs % rhs;
            lhs = rhs;
            rhs = rem;
        }
        lhs
    }

    lhs.checked_div(gcd(lhs, rhs))?.checked_mul(rhs)
}

#[cfg(test)]
mod tests {
    use super::{SplitInput, insert_local_exchange_split_execs, try_insert_split_on_unary_path};
    use crate::assert_snapshot;
    use crate::distributed_planner::insert_network_boundaries_for_test;
    use crate::test_utils::plans::{base_session_builder, context_with_query};
    use crate::{
        DistributedConfig, DistributedExec, LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN,
        LOCAL_EXCHANGE_SPLIT_MODE_OFF, NetworkShuffleExec, display_plan_ascii,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::common::Result;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::limit::LocalLimitExec;
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

    const OFF_OPTIONS: SplitTestOptions = SplitTestOptions {
        mode: LOCAL_EXCHANGE_SPLIT_MODE_OFF,
        ..FINAL_AGG_SPLIT_OPTIONS
    };

    const PARTITIONED_JOIN_SPLIT_OPTIONS: SplitTestOptions = SplitTestOptions {
        mode: LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN,
        max_owned_partitions: 2,
        target_partitions_per_task: 8,
        force_partitioned_join: true,
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

    async fn split_plan_from_ctx(
        ctx: &SessionContext,
        query: &str,
        options: SplitTestOptions,
    ) -> String {
        {
            let state_ref = ctx.state_ref();
            let mut state = state_ref.write();
            let cfg = state.config_mut().options_mut();
            if options.force_partitioned_join {
                cfg.optimizer.hash_join_single_partition_threshold = 0;
                cfg.optimizer.hash_join_single_partition_threshold_rows = 0;
            }
            let d_cfg = cfg.extensions.get_mut::<DistributedConfig>().unwrap();
            d_cfg.local_exchange_split_mode = options.mode.to_string();
            d_cfg.local_exchange_split_max_owned_partitions = options.max_owned_partitions;
            d_cfg.local_exchange_split_target_partitions_per_task =
                options.target_partitions_per_task;
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
        let split = insert_local_exchange_split_execs(distributed, d_cfg).unwrap();
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
    ) -> SplitInput {
        SplitInput {
            plan: empty_key_exec(),
            hash_exprs: vec![key_expr()],
            base_partition_count: 2,
            owned_partition_count,
            output_partition_count,
            already_split: !can_insert_split,
        }
    }

    fn split_test_config(target_partitions_per_task: usize) -> DistributedConfig {
        DistributedConfig {
            local_exchange_split_mode: LOCAL_EXCHANGE_SPLIT_MODE_FINAL_AGG_AND_JOIN.to_string(),
            local_exchange_split_max_owned_partitions: 2,
            local_exchange_split_target_partitions_per_task: target_partitions_per_task,
            ..DistributedConfig::default()
        }
    }

    #[test]
    fn join_target_uses_target_when_divisible() {
        let left = split_candidate(1, 1, true);
        let right = split_candidate(2, 2, true);
        assert_eq!(left.aligned_target_partition_count(&right, 8), Some(8));
    }

    #[test]
    fn join_target_uses_common_multiple() {
        let left = split_candidate(2, 2, true);
        let right = split_candidate(3, 3, true);
        assert_eq!(left.aligned_target_partition_count(&right, 8), Some(12));
    }

    #[test]
    fn join_target_rejects_existing_wide_side_that_cannot_be_matched() {
        let left = split_candidate(5, 5, true);
        let right = split_candidate(3, 12, false);
        assert_eq!(left.aligned_target_partition_count(&right, 8), None);
    }

    #[test]
    fn join_target_matches_existing_split_side() {
        let left = split_candidate(1, 1, true);
        let right = split_candidate(1, 8, false);
        assert_eq!(left.aligned_target_partition_count(&right, 8), Some(8));
    }

    #[test]
    fn target_partition_count_uses_next_multiple_of_owned_count() {
        assert_eq!(
            split_candidate(2, 2, true).inferred_target_partition_count(8),
            Some(8)
        );
        assert_eq!(
            split_candidate(3, 3, true).inferred_target_partition_count(8),
            Some(9)
        );
        assert_eq!(
            split_candidate(8, 8, true).inferred_target_partition_count(8),
            None
        );
    }

    #[test]
    fn split_after_shuffle_rejects_partition_local_fetch() -> Result<()> {
        let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(shuffle_key_exec(1)?, 10));
        let d_cfg = DistributedConfig {
            local_exchange_split_max_owned_partitions: 2,
            ..DistributedConfig::default()
        };
        let rewritten = try_insert_split_on_unary_path(limit, &d_cfg, Some(4))?;

        assert!(rewritten.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn final_agg_inserts_split() {
        let plan = weather_query_to_split_plan(
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#,
            FINAL_AGG_SPLIT_OPTIONS,
        )
        .await;
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
        let plan = weather_query_to_split_plan(
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#,
            FINAL_AGG_SKIP_OPTIONS,
        )
        .await;
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

    #[tokio::test]
    async fn off_mode_skips_final_agg_split() {
        let plan = weather_query_to_split_plan(
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#,
            OFF_OPTIONS,
        )
        .await;
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

        let plan = insert_local_exchange_split_execs(plan, &split_test_config(8))?;
        let plan = display_plan_ascii(&DistributedExec::new(plan), false);

        assert_snapshot!(plan, @r"
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

        let plan = insert_local_exchange_split_execs(plan, &split_test_config(8))?;
        let plan = display_plan_ascii(&DistributedExec::new(plan), false);

        assert_snapshot!(plan, @r"
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

        let plan = insert_local_exchange_split_execs(plan, &split_test_config(8))?;
        let plan = display_plan_ascii(&DistributedExec::new(plan), false);

        assert_snapshot!(plan, @r"
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
}
