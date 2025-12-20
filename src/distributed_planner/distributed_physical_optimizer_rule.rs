use crate::distributed_planner::distributed_config::DistributedConfig;
use crate::distributed_planner::distributed_plan_error::get_distribute_plan_err;
use crate::distributed_planner::task_estimator::TaskEstimator;
use crate::distributed_planner::{DistributedPlanError, NetworkBoundaryExt, non_distributable_err};
use crate::execution_plans::{DistributedExec, NetworkBroadcastExec, NetworkCoalesceExec};
use crate::stage::Stage;
use crate::{ChannelResolver, NetworkShuffleExec, PartitionIsolatorExec};
use datafusion::common::plan_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, repartition::RepartitionExec},
};
use std::sync::Arc;
use uuid::Uuid;

/// Physical optimizer rule that inspects the plan, places the appropriate network
/// boundaries and breaks it down into stages that can be executed in a distributed manner.
///
/// The rule has two steps:
///
/// 1. Inject the appropriate distributed execution nodes in the appropriate places.
///
///  This is done by looking at specific nodes in the original plan and enhancing them
///  with new additional nodes:
///  - a [DataSourceExec] is wrapped with a [PartitionIsolatorExec] for exposing just a subset
///    of the [DataSourceExec] partitions to the rest of the plan.
///  - a [CoalescePartitionsExec] is followed by a [NetworkCoalesceExec] so that all tasks in the
///    previous stage collapse into just 1 in the next stage.
///  - a [SortPreservingMergeExec] is followed by a [NetworkCoalesceExec] for the same reasons as
///    above
///  - a [RepartitionExec] with a hash partition is wrapped with a [NetworkShuffleExec] for
///    shuffling data to different tasks.
///
///
/// 2. Break down the plan into stages
///
///  Based on the network boundaries ([NetworkShuffleExec], [NetworkCoalesceExec], ...) placed in
///  the plan by the first step, the plan is divided into stages and tasks are assigned to each
///  stage.
///
///  This step might decide to not respect the amount of tasks each network boundary is requesting,
///  like when a plan is not parallelizable in different tasks (e.g. a collect left [HashJoinExec])
///  or when a [DataSourceExec] has not enough partitions to be spread across tasks.
#[derive(Debug, Default)]
pub struct DistributedPhysicalOptimizerRule;

impl PhysicalOptimizerRule for DistributedPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // We can only optimize plans that are not already distributed
        match distribute_plan(apply_network_boundaries(Arc::clone(&plan), cfg)?)? {
            None => Ok(plan),
            Some(distributed_plan) => Ok(distributed_plan),
        }
    }

    fn name(&self) -> &str {
        "DistributedPhysicalOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Places the appropriate [NetworkBoundary]s in the plan. It will look for certain nodes in the
/// provided plan and wrap them with their distributed equivalent, for example:
/// - A [RepartitionExec] will be wrapped with a [NetworkShuffleExec] for performing the
///   repartition over the network (shuffling).
/// - A [CoalescePartitionsExec] and a [SortPreservingMergeExec] both coalesce P partitions into
///   one, so a [NetworkCoalesceExec] is injected right below them to also coalesce distributed
///   tasks.
/// - A [DataSourceExec] is wrapped with a [PartitionIsolatorExec] so that each distributed task
///   only executes a certain amount of partitions.
///
/// The amount of tasks that each injected [NetworkBoundary] will spawn is calculated like this:
///
/// 1. Leaf nodes have the opportunity to provide an estimation of how many tasks should be employed
///    in the [Stage] that contains them.
///
/// 2. If a [Stage] contains multiple leaf nodes, and all provide a task count estimation, the
///    biggest is taken.
///
/// 3. When traversing the plan in a bottom to top fashion, this function looks for nodes that either
///    increase or reduce cardinality.
///     - If there's a node that increases cardinality, the next stage will spawn more tasks than the
///       current one.
///     - If there's a node that reduces cardinality, the next stage will spawn fewer tasks than the
///       current one.
///
/// 4. While traversing the plan from bottom to top, if a new [NetworkBoundary] needs to be placed,
///    it will spawn as many tasks as the previous stage multiplied by a factor determined by
///    wether the cardinality has increased or not.
///
/// 5. This is repeated until all the [NetworkBoundary]s are placed.
///
/// ## Example:
///
/// Given a plan with 3 stages:
///
/// ```text
/// ┌─────────────────┐
/// │     Stage 3     │? tasks
/// └────────▲────────┘
/// ┌────────┴────────┐
/// │     Stage 2     │? tasks
/// └────────▲────────┘
/// ┌────────┴────────┐
/// │     Stage 1     │? tasks
/// └─────────────────┘
/// ```
///
/// 1. Calculate the number of tasks for a bottom stage based on how much data the leaf nodes
///    (e.g. `DataSourceExec`s) are expected to pull.
///
/// ```text
/// ┌─────────────────┐
/// │     Stage 3     │? tasks
/// └────────▲────────┘
/// ┌────────┴────────┐
/// │     Stage 2     │? tasks
/// └────────▲────────┘
/// ┌────────┴────────┐
/// │     Stage 1     │3 tasks
/// └─────────────────┘
/// ```
///
/// 2. Based on the calculated tasks in the leaf stage (e.g. 3 tasks), calculate the amount of
///    tasks in the next stage.
///    This is done by multiplying the task count by a scale factor every time a node that
///    increments or reduces the cardinality of the data appears, which is information present in
///    the `fn cardinality_effect(&self) -> CardinalityEffect` method. For example, if "Stage 1"
///    has a partial aggregation step, and the scale factor is 1.5, it will look like this:
///
/// ```text
/// ┌─────────────────┐
/// │     Stage 3     │? tasks
/// └────────▲────────┘
/// ┌────────┴────────┐
/// │     Stage 2     │3/1.5 = 2 tasks
/// └────────▲────────┘
/// ┌────────┴────────┐
/// │     Stage 1     │3 tasks  (cardinality effect factor of 1.5)
/// └─────────────────┘
/// ```
///
///
/// 3. This is repeated recursively until all tasks have been assigned to all stages, keeping into
///    account the cardinality effect different nodes in subplans have. If there is no
///    cardinality effect (e.g. `ProjectExec` nodes), then the task count is kept across stages:
///
/// ```text
/// ┌─────────────────┐
/// │     Stage 3     │2 tasks
/// └────────▲────────┘
/// ┌────────┴────────┐
/// │     Stage 2     │2 tasks
/// └────────▲────────┘
/// ┌────────┴────────┐
/// │     Stage 1     │3 tasks
/// └─────────────────┘
/// ```
///
pub fn apply_network_boundaries(
    mut plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.output_partitioning().partition_count() > 1 {
        // Coalescing partitions here will allow us to put a NetworkCoalesceExec on top
        // of the plan, executing it in parallel.
        plan = Arc::new(CoalescePartitionsExec::new(plan))
    }
    let distributed_cfg = DistributedConfig::from_config_options(cfg)?;
    let urls = distributed_cfg.__private_channel_resolver.0.get_urls()?;
    // If there are 1 or 0 available workers, it does not make sense to distribute the query,
    // so don't.
    if urls.len() <= 1 {
        return Ok(plan);
    }
    let ctx = _apply_network_boundaries(plan, cfg, urls.len())?;
    Ok(ctx.plan)
}

/// [ApplyNetworkBoundariesCtx] helps keeping track of the stage of the task count calculations
/// while recursing through [ExecutionPlan]s.
struct ApplyNetworkBoundariesCtx {
    task_count: usize,
    this_stage_sf: f64,
    next_stage_sf: f64,
    plan: Arc<dyn ExecutionPlan>,
}

impl ApplyNetworkBoundariesCtx {
    /// Returns the task count with the calculated current scale factor, and swaps the scale
    /// factor calculated for the next stage by the current one, resetting the next stage scale
    /// factor.
    ///
    /// This is called whenever a new [NetworkBoundary] is introduced, which marks the end of
    /// one [Stage], and the beginning of the next one.
    fn scale_task_count_and_swap(&mut self) -> Result<usize, DataFusionError> {
        let task_count = (self.task_count as f64 * self.this_stage_sf).ceil() as usize;
        self.this_stage_sf = self.next_stage_sf;
        self.next_stage_sf = 1.0;
        if task_count == 0 {
            return plan_err!(
                "Attempted to assign a distributed task count of 0. This should never happen."
            );
        }
        Ok(task_count)
    }

    /// Scale the tasks count of the next stage. Note that, even if nodes in the current stage scale
    /// up or down the cardinality, that doesn't affect the task count for the current stage, but
    /// for the next one, as that's the one that will see the benefits of the current stage
    /// compressing the amount of data flowing.
    fn apply_scale_factor(&mut self, sf: f64) {
        match self.plan.cardinality_effect() {
            CardinalityEffect::LowerEqual => self.next_stage_sf /= sf,
            CardinalityEffect::GreaterEqual => self.next_stage_sf *= sf,
            _ => {}
        }
    }
}

fn _apply_network_boundaries(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
    max_tasks: usize,
) -> Result<ApplyNetworkBoundariesCtx> {
    let mut ctx = None;

    let children = plan.children();
    let mut new_children = Vec::with_capacity(children.len());
    // Recurse now in to the children so that nodes are the bottom are evaluated first.
    for child in children.iter() {
        let prev_ctx = _apply_network_boundaries(Arc::clone(child), cfg, max_tasks)?;
        new_children.push(Arc::clone(&prev_ctx.plan));
        match &mut ctx {
            None => {
                ctx.replace(ApplyNetworkBoundariesCtx {
                    task_count: prev_ctx.task_count,
                    this_stage_sf: prev_ctx.this_stage_sf,
                    next_stage_sf: prev_ctx.next_stage_sf,
                    plan: Arc::clone(&plan),
                });
            }
            Some(ctx) => {
                ctx.task_count = ctx.task_count.max(prev_ctx.task_count).min(max_tasks);
                ctx.next_stage_sf = ctx.next_stage_sf.max(prev_ctx.next_stage_sf);
            }
        }
    }

    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    let Some(mut ctx) = ctx else {
        // As there is no context, it means that children.is_empty() == true and no ctx was set, so
        // this is a leaf node, maybe a DataSourceExec, or maybe something else custom from the
        // user. We need to estimate how many tasks are needed for this leaf node, as we'll use
        // that for choosing the amount of tasks in upper stages.
        let Some(estimate) = d_cfg.__private_task_estimator.estimate_tasks(&plan, cfg) else {
            // We could not determine how many tasks this leaf node should run on, so
            // assume it cannot be distributed and employ just 1 task.
            return Ok(ApplyNetworkBoundariesCtx {
                task_count: 1,
                this_stage_sf: 1.0,
                next_stage_sf: 1.0,
                plan,
            });
        };
        return Ok(ApplyNetworkBoundariesCtx {
            task_count: estimate.task_count,
            this_stage_sf: 1.0,
            next_stage_sf: 1.0,
            plan: estimate.new_plan.unwrap_or(plan),
        });
    };

    ctx.plan = ctx.plan.with_new_children(new_children)?;

    // If this is a hash RepartitionExec, introduce a shuffle.
    if let Some(node) = ctx.plan.as_any().downcast_ref::<RepartitionExec>() {
        if !matches!(node.partitioning(), Partitioning::Hash(_, _)) {
            return Ok(ctx);
        }
        let task_count = ctx.scale_task_count_and_swap()?;
        // Network shuffles imply partitioning each data stream in a lot of different partitions,
        // which means that each resulting stream might contain tiny batches. It's important to
        // have decent sized batches here as this will ultimately be sent over the wire, and the
        // penalty there for sending many tiny batches instead of few big ones is big.
        // TODO: After https://github.com/apache/datafusion/issues/18782 is shipped, the batching
        //  will be integrated in RepartitionExec itself, so we will not need to add a
        //  CoalesceBatchesExec, we just need to tell RepartitionExec to output a
        //  `d_cfg.shuffle_batch_size` batch size.
        //  Tracked by https://github.com/datafusion-contrib/datafusion-distributed/issues/243
        if d_cfg.shuffle_batch_size > 0 {
            ctx.plan = Arc::new(CoalesceBatchesExec::new(ctx.plan, d_cfg.shuffle_batch_size));
        }
        ctx.plan = Arc::new(NetworkShuffleExec::try_new(ctx.plan, task_count)?);
        return Ok(ctx);
    } else if let Some(coalesce_batches) = ctx.plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        // If the batch coalescing is before the network boundary, remove it, as we don't
        // want it there, we want it after, and the code that adds it lives just some lines above.
        if coalesce_batches.input().is_network_boundary() {
            ctx.plan = Arc::clone(coalesce_batches.input());
        }
    }

    // If this is a CoalescePartitionsExec, it means that the original plan is trying to
    // merge all partitions into one. We need to go one step ahead and also merge all tasks
    // into one.
    if let Some(node) = ctx.plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        let input = Arc::clone(node.input());
        // If the immediate child is a PartitionIsolatorExec, it means that the rest of the
        // plan is just a couple of non-computational nodes that are probably not worth
        // distributing.
        if input.as_any().is::<PartitionIsolatorExec>() {
            return Ok(ctx);
        }

        let task_count = ctx.scale_task_count_and_swap()?;
        let new_child = NetworkCoalesceExec::new(input, task_count);
        ctx.plan = ctx.plan.with_new_children(vec![Arc::new(new_child)])?;
        return Ok(ctx);
    }

    // The SortPreservingMergeExec node will try to coalesce all partitions into just 1.
    // We need to account for it and help it by also coalescing all tasks into one, therefore
    // a NetworkCoalesceExec is introduced.
    if let Some(node) = ctx.plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
        let input = Arc::clone(node.input());

        let task_count = ctx.scale_task_count_and_swap()?;
        let new_child = NetworkCoalesceExec::new(input, task_count);
        ctx.plan = ctx.plan.with_new_children(vec![Arc::new(new_child)])?;
        return Ok(ctx);
    }

    if let Some(node) = ctx.plan.as_any().downcast_ref::<HashJoinExec>()
        && node.partition_mode() == &PartitionMode::CollectLeft
    {
        let build_side = Arc::clone(node.left());
        let probe_side = Arc::clone(node.right());
        let build_side_broadcast = Arc::new(NetworkBroadcastExec::try_new(build_side, 1)?);
        ctx.plan = ctx
            .plan
            .with_new_children(vec![build_side_broadcast, probe_side])?;
        return Ok(ctx);
    }

    // upscales or downscales the task count factor of the next stage depending on the
    // cardinality of the current plan.
    ctx.apply_scale_factor(d_cfg.cardinality_task_count_factor);

    Ok(ctx)
}

/// Takes a plan with certain network boundaries in it ([NetworkShuffleExec], [NetworkCoalesceExec], ...)
/// and breaks it down into stages.
///
/// This can be used a standalone function for distributing arbitrary plans in which users have
/// manually placed network boundaries, or as part of the [DistributedPhysicalOptimizerRule] that
/// places the network boundaries automatically as a standard [PhysicalOptimizerRule].
///
/// If there's nothing to distribute in the plan, the function returns None.
pub fn distribute_plan(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    if plan.as_any().is::<DistributedExec>() {
        return Ok(Some(plan));
    }
    let stage = match _distribute_plan_inner(Uuid::new_v4(), plan.clone(), &mut 1, 0, 1) {
        Ok(stage) => stage,
        Err(err) => {
            return match get_distribute_plan_err(&err) {
                Some(DistributedPlanError::NonDistributable(_)) => plan
                    .transform_down(|plan| {
                        // If the node cannot be distributed, rollback all the network boundaries.
                        if let Some(nb) = plan.as_network_boundary() {
                            return Ok(Transformed::yes(nb.rollback()?));
                        }
                        Ok(Transformed::no(plan))
                    })
                    .map(|v| Some(v.data)),
                _ => Err(err),
            };
        }
    };
    // After running the distributed planner, only 1 stage was created, meaning that the plan
    // was not distributed.
    if stage.num == 1 {
        return Ok(None);
    }
    let plan = stage.plan.decoded()?;
    Ok(Some(Arc::new(DistributedExec::new(Arc::clone(plan)))))
}

fn _distribute_plan_inner(
    query_id: Uuid,
    plan: Arc<dyn ExecutionPlan>,
    num: &mut usize,
    depth: usize,
    n_tasks: usize,
) -> Result<Stage, DataFusionError> {
    let mut distributed = plan.clone().transform_down(|plan| {
        // We cannot distribute [StreamingTableExec] nodes, so abort distribution.
        if plan.as_any().is::<StreamingTableExec>() {
            return Err(non_distributable_err(StreamingTableExec::static_name()))
        }

        if let Some(node) = plan.as_any().downcast_ref::<PartitionIsolatorExec>() {
            // If there's only 1 task, no need to perform any isolation.
            if n_tasks == 1 {
                return Ok(Transformed::yes(Arc::clone(plan.children().first().unwrap())));
            }
            let node = node.ready(n_tasks)?;
            // Use Continue instead of Jump to ensure any network boundaries in the
            // subtree (e.g., NetworkShuffleExec in probe side of broadcast joins)
            // are properly visited and converted from Pending to Ready state.
            return Ok(Transformed::new(Arc::new(node), true, TreeNodeRecursion::Continue));
        }

        let Some(mut dnode) = plan.as_network_boundary().map(Referenced::Borrowed) else {
            return Ok(Transformed::no(plan));
        };

        let stage = loop {
            let input_stage_info = dnode.as_ref().get_input_stage_info(n_tasks)?;
            // If the current stage has just 1 task, and the next stage is only going to have
            // 1 task, there's no point in having a network boundary in between, they can just
            // communicate in memory.
            if n_tasks == 1 && input_stage_info.task_count == 1 && plan.as_any().downcast_ref::<NetworkBroadcastExec>().is_none() {
                let mut n = dnode.as_ref().rollback()?;
                if let Some(node) = n.as_any().downcast_ref::<PartitionIsolatorExec>() {
                    // Also trim PartitionIsolatorExec out of the plan.
                    n = Arc::clone(node.children().first().unwrap());
                }
                return Ok(Transformed::yes(n));
            }
            match _distribute_plan_inner(query_id, input_stage_info.plan, num, depth + 1, input_stage_info.task_count) {
                Ok(v) => break v,
                Err(e) => match get_distribute_plan_err(&e) {
                    None => return Err(e),
                    Some(DistributedPlanError::LimitTasks(limit)) => {
                        // While attempting to build a new stage, a failure was raised stating
                        // that no more than `limit` tasks can be used for it, so we are going
                        // to limit the amount of tasks to the requested number and try building
                        // the stage again.
                        if input_stage_info.task_count == *limit {
                            return plan_err!("A node requested {limit} tasks for the stage its in, but that stage already has that many tasks");
                        }
                        dnode = Referenced::Arced(dnode.as_ref().with_input_task_count(*limit)?);
                    }
                    Some(DistributedPlanError::NonDistributable(_)) => {
                        // This full plan is non-distributable, so abort any task and stage
                        // assignation.
                        return Err(e);
                    }
                },
            }
        };
        let node = dnode.as_ref().with_input_stage(stage, n_tasks)?;
        Ok(Transformed::new(node, true, TreeNodeRecursion::Jump))
    })?;

    // The head stage is executable, and upon execution, it will lazily assign worker URLs to
    // all tasks. This must only be done once, so the executable StageExec must only be called
    // once on 1 partition.
    if depth == 0 && distributed.data.output_partitioning().partition_count() > 1 {
        distributed.data = Arc::new(CoalescePartitionsExec::new(distributed.data));
    }

    let stage = Stage::new(query_id, *num, distributed.data, n_tasks, None);
    *num += 1;
    Ok(stage)
}
/// Helper enum for storing either borrowed or owned trait object references
enum Referenced<'a, T: ?Sized> {
    Borrowed(&'a T),
    Arced(Arc<T>),
}

impl<T: ?Sized> Referenced<'_, T> {
    fn as_ref(&self) -> &T {
        match self {
            Self::Borrowed(r) => r,
            Self::Arced(arc) => arc.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::in_memory_channel_resolver::InMemoryChannelResolver;
    use crate::test_utils::parquet::register_parquet_tables;
    use crate::{DistributedExt, DistributedPhysicalOptimizerRule};
    use crate::{assert_snapshot, display_plan_ascii};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use itertools::Itertools;
    use std::sync::Arc;
    /* shema for the "weather" table

     MinTemp [type=DOUBLE] [repetitiontype=OPTIONAL]
     MaxTemp [type=DOUBLE] [repetitiontype=OPTIONAL]
     Rainfall [type=DOUBLE] [repetitiontype=OPTIONAL]
     Evaporation [type=DOUBLE] [repetitiontype=OPTIONAL]
     Sunshine [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindGustDir [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindGustSpeed [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindDir9am [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindDir3pm [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindSpeed9am [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindSpeed3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Humidity9am [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Humidity3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Pressure9am [type=DOUBLE] [repetitiontype=OPTIONAL]
     Pressure3pm [type=DOUBLE] [repetitiontype=OPTIONAL]
     Cloud9am [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Cloud3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Temp9am [type=DOUBLE] [repetitiontype=OPTIONAL]
     Temp3pm [type=DOUBLE] [repetitiontype=OPTIONAL]
     RainToday [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     RISK_MM [type=DOUBLE] [repetitiontype=OPTIONAL]
     RainTomorrow [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
    */

    #[tokio::test]
    async fn test_select_all() {
        let query = r#"
        SELECT * FROM weather
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @"DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet");
    }

    #[tokio::test]
    async fn test_aggregation() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
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
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7]
            │ CoalesceBatchesExec: target_batch_size=8192
            │   RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=4
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_fewer_workers_than_files() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(2))
        })
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
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7]
            │ CoalesceBatchesExec: target_batch_size=8192
            │   RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=4
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
            │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │         PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0]
            │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_0_workers() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(0))
        })
        .await;
        assert_snapshot!(plan, @r"
        ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
          SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
            SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
              ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
                AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  CoalesceBatchesExec: target_batch_size=8192
                    RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=4
                      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                        AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_high_cardinality_factor() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
                .with_distributed_cardinality_effect_task_scale_factor(3.0)
                .unwrap()
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
        │       ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
        │         AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        │           [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p0..p3] t2:[p0..p3]
          │ CoalesceBatchesExec: target_batch_size=8192
          │   RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=4
          │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
          │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_a_lot_of_files_per_task() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
                .with_distributed_files_per_task(3)
                .unwrap()
        })
        .await;
        assert_snapshot!(plan, @r"
        ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
          SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
            SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
              ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
                AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  CoalesceBatchesExec: target_batch_size=8192
                    RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=4
                      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                        AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_partitions_per_task() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
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
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7]
            │ CoalesceBatchesExec: target_batch_size=8192
            │   RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=4
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday"
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   CoalesceBatchesExec: target_batch_size=8192
        │     HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
        │       [Stage 2] => NetworkBroadcastExec: output_partitions=1, input_tasks=3
        │       PartitionIsolatorExec
        │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0] t1:[p1] t2:[p2]
          │ CoalescePartitionsExec
          │   CoalescePartitionsExec
          │     PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_left_join_distributed() {
        let query = r#"
        WITH a AS (
            SELECT
                AVG("MinTemp") as "MinTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'yes'
            GROUP BY "RainTomorrow"
        ), b AS (
            SELECT
                AVG("MaxTemp") as "MaxTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'no'
            GROUP BY "RainTomorrow"
        )
        SELECT
            a."MinTemp",
            b."MaxTemp"
        FROM a
        LEFT JOIN b
        ON a."RainTomorrow" = b."RainTomorrow"
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 5] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 5 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5]
          │ CoalesceBatchesExec: target_batch_size=8192
          │   HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainTomorrow@1, RainTomorrow@1)], projection=[MinTemp@0, MaxTemp@2]
          │     [Stage 3] => NetworkBroadcastExec: output_partitions=1, input_tasks=1
          │     PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,__] t2:[__,__,__,p0]
          │       ProjectionExec: expr=[avg(weather.MaxTemp)@1 as MaxTemp, RainTomorrow@0 as RainTomorrow]
          │         AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
          │           [Stage 4] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 3 ── Tasks: t0:[p0]
            │ CoalescePartitionsExec
            │   CoalescePartitionsExec
            │     [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3]
              │ ProjectionExec: expr=[avg(weather.MinTemp)@1 as MinTemp, RainTomorrow@0 as RainTomorrow]
              │   AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MinTemp)]
              │     [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
              └──────────────────────────────────────────────────
                ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7]
                │ CoalesceBatchesExec: target_batch_size=8192
                │   RepartitionExec: partitioning=Hash([RainTomorrow@0], 8), input_partitions=4
                │     AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MinTemp)]
                │       CoalesceBatchesExec: target_batch_size=8192
                │         FilterExec: RainToday@1 = yes, projection=[MinTemp@0, RainTomorrow@2]
                │           RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
                │             PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
                │               DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]
                └──────────────────────────────────────────────────
            ┌───── Stage 4 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11]
            │ CoalesceBatchesExec: target_batch_size=8192
            │   RepartitionExec: partitioning=Hash([RainTomorrow@0], 12), input_partitions=4
            │     AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
            │       CoalesceBatchesExec: target_batch_size=8192
            │         FilterExec: RainToday@1 = no, projection=[MaxTemp@0, RainTomorrow@2]
            │           RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │             PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │               DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = no, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_sort() {
        let query = r#"
        SELECT * FROM weather ORDER BY "MinTemp" DESC
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [MinTemp@0 DESC]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=3, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0] t1:[p1] t2:[p2]
          │ SortExec: expr=[MinTemp@0 DESC], preserve_partitioning=[true]
          │   PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_distinct() {
        let query = r#"
        SELECT DISTINCT "RainToday", "WindGustDir" FROM weather
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3]
          │ AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7]
            │ CoalesceBatchesExec: target_batch_size=8192
            │   RepartitionExec: partitioning=Hash([RainToday@0, WindGustDir@1], 8), input_partitions=4
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
            │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday, WindGustDir], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_show_columns() {
        let query = r#"
        SHOW COLUMNS from weather
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        CoalescePartitionsExec
          ProjectionExec: expr=[table_catalog@0 as table_catalog, table_schema@1 as table_schema, table_name@2 as table_name, column_name@3 as column_name, data_type@5 as data_type, is_nullable@4 as is_nullable]
            CoalesceBatchesExec: target_batch_size=8192
              FilterExec: table_name@2 = weather
                RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
                  StreamingTableExec: partition_sizes=1, projection=[table_catalog, table_schema, table_name, column_name, is_nullable, data_type]
        ");
    }

    #[tokio::test]
    #[ignore] // FIXME: fix this test
    async fn test_limited_by_worker() {
        let query = r#"
        SET datafusion.execution.target_partitions=2;
        SELECT 1 FROM weather
        UNION ALL
        SELECT 1 FROM flights_1m
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(2))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3]
          │ UnionExec
          │   ProjectionExec: expr=[1 as Int64(1)]
          │     PartitionIsolatorExec: t0:[p0,__] t1:[__,p0]
          │       DataSourceExec: file_groups={2 groups: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, file_type=parquet
          │   ProjectionExec: expr=[1 as Int64(1)]
          │     PartitionIsolatorExec: t0:[p0] t1:[__]
          │       DataSourceExec: file_groups={1 group: [[/testdata/flights-1m.parquet]]}, file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    async fn sql_to_explain(
        query: &str,
        f: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder,
    ) -> String {
        let config = SessionConfig::new()
            .with_target_partitions(4)
            .with_information_schema(true);

        let builder = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_config(config);

        let state = f(builder).build();

        let ctx = SessionContext::new_with_state(state);
        let mut queries = query.split(";").collect_vec();
        let last_query = queries.pop().unwrap();

        for query in queries {
            ctx.sql(query).await.unwrap();
        }

        register_parquet_tables(&ctx).await.unwrap();

        let df = ctx.sql(last_query).await.unwrap();

        let physical_plan = df.create_physical_plan().await.unwrap();
        display_plan_ascii(physical_plan.as_ref(), false)
    }
}
