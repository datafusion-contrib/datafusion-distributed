use crate::TaskCountAnnotation::{Desired, Maximum};
use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::stage::LocalStage;
use crate::{
    BroadcastExec, DistributedConfig, NetworkBoundaryExt, NetworkBroadcastExec,
    NetworkCoalesceExec, NetworkShuffleExec, TaskCountAnnotation, TaskEstimator,
};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, Result, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

/// Walks an [ExecutionPlan] and injects [NetworkShuffleExec], [NetworkBroadcastExec], and
/// [NetworkCoalesceExec] nodes wherever a stage boundary is needed. The returned plan has the
/// same shape as the input except for these inserted boundary nodes.
///
/// Per-node task counts are recorded in a side map on the [Context] (keyed by plan-pointer
/// identity) rather than mutated into the plan itself. Later passes look them up via
/// [Context::task_count].
///
/// # The three-phase loop
///
/// For every stage in the plan we run the same three phases. The bottom-up walk drives them:
/// it climbs the plan, and each time it discovers that the current node would need a network
/// boundary below itself, it pauses and runs phases 2 and 3 to "close" the stage that's just
/// been delimited, then resumes climbing into the next stage above.
///
/// ## Phase 1 — bottom-up walk, until a stage gets delimited
///
/// Starting from the leaves, we climb the plan, asking the [TaskEstimator] for a task count
/// at each leaf and merging children's task counts at each inner node (`Desired` takes the
/// max; a `Maximum(N)` anywhere in the subtree caps and dominates). We keep going until the
/// current node is one that requires a network boundary **above it** — currently a hash
/// `RepartitionExec` (→ shuffle), a `BroadcastExec` whose parent is collecting partitions
/// (→ broadcast), or any other node whose parent is `CoalescePartitionsExec` /
/// `SortPreservingMergeExec` (→ coalesce). At that moment, that node and the subtree
/// underneath it form the stage we've just delimited; the boundary will be injected above it
/// in Phase 3.
///
/// ```text
///                ▲
///                │   (climbing up...)
///                │
///                ╴   ⋯ a boundary will be injected on this edge in Phase 3 ⋯
///                │
///       ┌────────┴───────────┐   ← climb stops here.
///       │   RepartitionExec  │     This node tops the producer
///       │     (Hash, ...)    │     stage we just delimited.
///       └────────▲───────────┘
///                │
///       ┌────────┴───────────┐
///       │    AggregateExec   │
///       │      (Partial)     │
///       └────────▲───────────┘
///                │
///       ┌────────┴───────────┐
///       │    DataSourceExec  │   ← TaskEstimator returned Desired(3)
///       └────────────────────┘
///
///   children's task counts merge on the way up → reconciled value = T.
/// ```
///
/// ## Phase 2 — top-down propagation through the stage we just delimited
///
/// With the reconciled task count `T` in hand, we do a top-down walk over the producer stage
/// — starting from the input of the boundary we're about to inject — and record `T` on every
/// node we visit. Leaves go through [TaskEstimator::scale_up_leaf_node] (e.g. a
/// `DataSourceExec` may be wrapped in a `PartitionIsolatorExec`) and any wrappers introduced
/// by the scale-up also receive `T`. If the walk meets a network boundary that was already
/// injected by an earlier iteration of this loop, it does **not** descend into it — that
/// subtree belongs to a previously-formed stage and has already been finalised.
///
/// ```text
///   ┌─────────────────────────────┐
///   │     RepartitionExec(Hash)   │  ← root of the stage we just delimited
///   └──────────────┬──────────────┘
///                  │  propagate T down
///                  ▼
///   ┌─────────────────────────────┐
///   │       AggregateExec         │  ← task count := T
///   │         (Partial)           │
///   └──────────────┬──────────────┘
///                  │
///                  ▼
///   ┌─────────────────────────────┐
///   │       DataSourceExec        │  ← scale-up via TaskEstimator
///   └─────────────────────────────┘    e.g.:
///                                       ┌────────────────────┐
///                                       │ PartitionIsolator  │ ← T
///                                       │   ┌─────────────┐  │
///                                       │   │ DataSource  │  │ ← T
///                                       │   └─────────────┘  │
///                                       └────────────────────┘
/// ```
///
/// ## Phase 3 — inject the boundary and seed the next stage's starting task count
///
/// Now we wrap the producer stage in the appropriate `Network*Exec` node and decide the task
/// count above the boundary — i.e. the starting task count for the next stage up. We compute
/// a scale factor from the cardinality effects of the producer-stage nodes
/// ([calculate_scale_factor]: `LowerEqual` divides, `GreaterEqual` multiplies, by the
/// configured `cardinality_task_count_factor`) and apply it as `ceil(T_producer × sf)`. That
/// becomes the new node's recorded task count and feeds back into Phase 1 for the next stage.
///
/// ```text
///                       ▲
///                       │   bottom-up walk resumes;
///                       │   reconciled with siblings → Phase 1 for the next stage
///                       │
///         ┌─────────────┴────────────┐
///         │   NetworkShuffleExec     │  ← task count = ceil(T_producer × sf)
///         └─────────────▲────────────┘
///                       │
///         ┌─────────────┴────────────┐
///         │ producer stage as input  │  ← entire subtree, every node already
///         │      (LocalStage)        │     has its task count recorded by Phase 2
///         └──────────────────────────┘
/// ```
///
/// # Exit condition
///
/// When the bottom-up walk reaches the root, there is no parent that could trigger another
/// boundary injection, so the head stage is closed by running one final Phase 2 pass over
/// the whole plan. This guarantees every node (including head-stage nodes that never sat
/// directly above a boundary) has a task count recorded.
pub(super) async fn inject_network_boundaries(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = Context {
        parent: None,
        cfg,
        d_cfg: DistributedConfig::from_config_options(cfg)?,
        task_counts: &Mutex::new(HashMap::new()),
        query_id: Uuid::new_v4(),
        stage_id: &AtomicUsize::new(1),
    };

    _inject_network_boundaries(plan, ctx).await
}

#[derive(Clone)]
struct Context<'a> {
    parent: Option<&'a Arc<dyn ExecutionPlan>>,
    cfg: &'a ConfigOptions,
    d_cfg: &'a DistributedConfig,
    task_counts: &'a Mutex<HashMap<usize, TaskCountAnnotation>>,
    query_id: Uuid,
    stage_id: &'a AtomicUsize,
}

impl<'a> Context<'a> {
    fn with_parent(&'a self, parent: &'a Arc<dyn ExecutionPlan>) -> Self {
        Self {
            parent: Some(parent),
            cfg: self.cfg,
            d_cfg: self.d_cfg,
            task_counts: self.task_counts,
            query_id: self.query_id,
            stage_id: self.stage_id,
        }
    }

    fn max_tasks(&self) -> Result<usize> {
        Ok(match self.d_cfg.max_tasks_per_stage {
            0 => self
                .d_cfg
                .__private_worker_resolver
                .0
                .get_urls()?
                .len()
                .max(1),
            v => v,
        })
    }

    fn set_task_count(&self, plan: &Arc<dyn ExecutionPlan>, task_count: TaskCountAnnotation) {
        self.task_counts
            .lock()
            .expect("task counts mutex poisoned")
            .insert(plan_ptr_key(plan), task_count);
    }

    fn with_task_count(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        task_count: TaskCountAnnotation,
    ) -> Arc<dyn ExecutionPlan> {
        self.set_task_count(&plan, task_count);
        plan
    }

    fn task_count(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<TaskCountAnnotation> {
        let Some(task_count) = self
            .task_counts
            .lock()
            .expect("task counts mutex poisoned")
            .get(&plan_ptr_key(plan))
            .cloned()
        else {
            return plan_err!(
                "Missing task count for node {}. This is a bug in Distributed DataFusion's planner, please report it.",
                plan.name()
            );
        };
        Ok(task_count)
    }

    fn fetch_add_stage_id(&self) -> usize {
        self.stage_id.fetch_add(1, Ordering::Acquire)
    }
}

/// Identity key for a plan node. The pointer is only used as a hash-map key, never dereferenced,
/// so casting it to `usize` is safe and makes the key `Send + Sync`.
fn plan_ptr_key(plan: &Arc<dyn ExecutionPlan>) -> usize {
    Arc::as_ptr(plan) as *const () as usize
}

/// WARNING: every return statement in this function must funnel through
/// [Context::with_task_count] (or [Context::set_task_count] on the way through) so the returned
/// node has a recorded task count. Callers downstream depend on this invariant.
async fn _inject_network_boundaries(
    plan: Arc<dyn ExecutionPlan>,
    ctx: Context<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let broadcast_joins = ctx.d_cfg.broadcast_joins;
    let estimator = &ctx.d_cfg.__private_task_estimator;

    if plan.children().is_empty() {
        // This is a leaf node, maybe a DataSourceExec, or maybe something else custom from the
        // user. We need to estimate how many tasks are needed for this leaf node, and we'll take
        // this decision into account when deciding how many tasks will be actually used.
        return if let Some(estimate) = estimator.task_estimation(&plan, ctx.cfg) {
            Ok(ctx.with_task_count(plan, estimate.task_count))
        } else {
            // We could not determine how many tasks this leaf node should run on, so
            // assume it cannot be distributed and use just 1 task.
            Ok(ctx.with_task_count(plan, Maximum(1)))
        };
    }

    let mut futures = Vec::with_capacity(plan.children().len());
    for child in plan.children() {
        let child = Arc::clone(child);
        futures.push(Box::pin(_inject_network_boundaries(
            child,
            ctx.with_parent(&plan),
        )));
    }
    let processed_children = futures::future::try_join_all(futures).await?;

    let mut task_count = estimator
        .task_estimation(&plan, ctx.cfg)
        .map_or(Desired(1), |v| v.task_count);
    if ctx.d_cfg.children_isolator_unions && plan.as_any().is::<UnionExec>() {
        // Unions have the chance to decide how many tasks they should run on. If there's a union
        // with a bunch of children, the user might want to increase parallelism and increase the
        // task count for the stage running that.
        let mut count = 0;
        for processed_child in processed_children.iter() {
            count += ctx.task_count(processed_child)?.as_usize();
        }
        task_count = Desired(count);
    } else if let Some(node) = plan.as_any().downcast_ref::<HashJoinExec>()
        && node.mode == PartitionMode::CollectLeft
        && !broadcast_joins
    {
        // Only distribute CollectLeft HashJoins after we broadcast more intelligently or when it
        // is explicitly enabled.
        task_count = Maximum(1);
    } else {
        // The task count for this plan is decided by the biggest task count from the children; unless
        // a child specifies a maximum task count, in that case, the maximum is respected. Some
        // nodes can only run in one task. If there is a subplan with a single node declaring that
        // it can only run in one task, all the rest of the nodes in the stage need to respect it.
        for processed_child in processed_children.iter() {
            task_count = task_count.merge(ctx.task_count(processed_child)?)
        }
    }

    let plan = plan.with_new_children(processed_children)?;
    // Cap the reconciled task count by the configured max-per-stage budget.
    task_count = task_count.limit(ctx.max_tasks()?);

    // Upon reaching a hash repartition, we need to introduce a shuffle right above it.
    if let Some(r_exec) = plan.as_any().downcast_ref::<RepartitionExec>() {
        if matches!(r_exec.partitioning(), Partitioning::Hash(_, _)) {
            // The subtree below this point belongs to one stage. Propagate the chosen task
            // count down so every node in that stage has it recorded.
            let plan = propagate_task_count(&plan, task_count, &ctx)?;

            let f = calculate_scale_factor(&plan, &ctx);
            let input_stage = LocalStage {
                query_id: ctx.query_id,
                num: ctx.fetch_add_stage_id(),
                plan,
                tasks: task_count.as_usize(),
            };
            let plan = Arc::new(NetworkShuffleExec::from_stage(input_stage));
            let task_count = Desired((f * task_count.as_usize() as f64).ceil() as usize);
            return Ok(ctx.with_task_count(plan, task_count));
        }
    } else if let Some(parent) = ctx.parent
        // If this node is a leaf node, putting a network boundary above is a bit wasteful, so
        // we don't want to do it.
        && !plan.children().is_empty()
        // If the parent is trying to coalesce all partitions into one, we need to introduce
        // a network coalesce right below it (or in other words, above the current node)
        && (parent.as_any().is::<CoalescePartitionsExec>()
        || parent.as_any().is::<SortPreservingMergeExec>())
    {
        // A BroadcastExec underneath a coalesce parent means the build side will cross stages.
        return if plan.as_any().is::<BroadcastExec>() {
            // The subtree below this point belongs to one stage. Propagate the chosen task
            // count down so every node in that stage has it recorded.
            let plan = propagate_task_count(&plan, task_count, &ctx)?;

            let f = calculate_scale_factor(&plan, &ctx);
            let input_stage = LocalStage {
                query_id: ctx.query_id,
                num: ctx.fetch_add_stage_id(),
                plan,
                tasks: task_count.as_usize(),
            };
            let plan = Arc::new(NetworkBroadcastExec::from_stage(input_stage));
            let task_count = Desired((f * task_count.as_usize() as f64).ceil() as usize);
            Ok(ctx.with_task_count(plan, task_count))
        } else {
            // The subtree below this point belongs to one stage. Propagate the chosen task
            // count down so every node in that stage has it recorded.
            let plan = propagate_task_count(&plan, task_count, &ctx)?;
            let input_stage = LocalStage {
                query_id: ctx.query_id,
                num: ctx.fetch_add_stage_id(),
                plan,
                tasks: task_count.as_usize(),
            };
            let plan = Arc::new(NetworkCoalesceExec::from_stage(input_stage, 1));
            // The parent that triggered this branch is a `CoalescePartitionsExec` or
            // `SortPreservingMergeExec`, both of which fold all partitions into one — so the
            // stage above this boundary must run in exactly one task.
            Ok(ctx.with_task_count(plan, Maximum(1)))
        };
    }

    if ctx.parent.is_none() {
        // We've just finished walking the head stage's subplan. Run a final propagation so
        // every node in the head stage (which never crossed a stage boundary on the way up)
        // gets its task count recorded.
        propagate_task_count(&plan, task_count, &ctx)
    } else {
        // If this is not the root node, and it's also not a network boundary, then we don't need
        // to do anything else.
        Ok(ctx.with_task_count(plan, task_count))
    }
}

/// Walks `plan` top-down and records the given `task_count` for every node up until the next
/// network boundary, scaling leaves and rebuilding intermediate nodes as needed.
///
/// ```text
///       ┌────────────────────┐
///       │  RepartitionExec   │   ← top of the just-delimited stage;
///       │    (Hash, ...)     │     record T on this node
///       └────────┬───────────┘
///                │   recurse with T
///       ┌────────▼───────────┐
///       │   AggregateExec    │   ← record T, recurse
///       │     (Partial)      │
///       └────────┬───────────┘
///                │
///       ┌────────▼───────────┐
///       │   DataSourceExec   │   ← leaf: scale-up via TaskEstimator;
///       └────────────────────┘     every node in the wrapper subtree
///                                  also records T
/// ```
///
/// Per-case behaviour:
///
/// - **Leaves**: ask the [TaskEstimator] for an optional scaled-up replacement (e.g. wrapping a
///   `DataSourceExec` in a `PartitionIsolatorExec`). Every node in the returned subtree —
///   including any wrappers the estimator introduced — is recorded with `task_count`.
/// - **Network boundaries**: don't descend into the boundary's input plan (it lives in another
///   stage). Instead, rescale the boundary's input via [network_boundary_scale_input] using the
///   *consumer* partition and task counts of this side of the boundary, and stitch the rescaled
///   stage back in via [NetworkBoundary::with_input_stage].
/// - **Eligible `UnionExec`s** (when `children_isolator_unions` is on): rewrite to
///   [ChildrenIsolatorUnionExec] and recurse into each child with the per-child task count
///   chosen by [ChildrenIsolatorUnionExec::from_children_and_task_counts] — each child runs
///   isolated in its own subset of tasks.
/// - **Everything else**: recurse into children with the same `task_count`, then rebuild the
///   node with the rebuilt children.
fn propagate_task_count(
    plan: &Arc<dyn ExecutionPlan>,
    task_count: TaskCountAnnotation,
    ctx: &Context,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Handle leaf nodes.
    if plan.children().is_empty() {
        let scaled_up = ctx.d_cfg.__private_task_estimator.scale_up_leaf_node(
            plan,
            task_count.as_usize(),
            ctx.cfg,
        );
        match scaled_up {
            None => Ok(ctx.with_task_count(Arc::clone(plan), task_count)),
            Some(scaled_up) => {
                // The scaled up node might contain more than 1 node, for example, if a
                // PartitionIsolatorExec was injected.
                scaled_up.apply(|plan| {
                    ctx.set_task_count(plan, task_count);
                    Ok(TreeNodeRecursion::Continue)
                })?;
                Ok(ctx.with_task_count(scaled_up, task_count))
            }
        }

    // Handle network boundaries.
    } else if plan.is_network_boundary() {
        // Just annotate the network boundary and stop recursion here.
        Ok(ctx.with_task_count(Arc::clone(plan), task_count))

    // Handle ChildrenIsolatorUnionExec.
    } else if ctx.d_cfg.children_isolator_unions && plan.as_any().is::<UnionExec>() {
        // Propagating through ChildrenIsolatorUnionExec is not that easy, each child will
        // be executed in its own task, and therefore, they will act as if they were in executing
        // in a non-distributed context. The ChildrenIsolatorUnionExec itself will make sure to
        // determine which children to run and which to exclude depending on the task index in
        // which it's running.
        let children = plan.children();
        let c_i_union = ChildrenIsolatorUnionExec::from_children_and_task_counts(
            children.iter().map(|v| Arc::clone(*v)),
            children
                .iter()
                // TODO: rather than defaulting to 1 when no task count is recorded, it's better to error out.
                .map(|v| ctx.task_count(v).map_or(1, |v| v.as_usize())),
            task_count.as_usize(),
        )?;
        let mut new_children = Vec::with_capacity(children.len());
        for (child, task_count) in c_i_union.children_and_task_count() {
            new_children.push(propagate_task_count(child, Maximum(task_count), ctx)?);
        }
        let c_i_union = Arc::new(c_i_union).with_new_children(new_children)?;
        Ok(ctx.with_task_count(c_i_union, task_count))

    // Handle middle nodes.
    } else {
        let mut new_children = Vec::with_capacity(plan.children().len());
        for child in plan.children() {
            new_children.push(propagate_task_count(child, task_count, ctx)?);
        }
        let plan = Arc::clone(plan).with_new_children(new_children)?;
        Ok(ctx.with_task_count(plan, task_count))
    }
}

/// Returns a multiplicative factor describing how the data volume changes between the bottom of
/// `plan` (at a network boundary or a leaf) and `plan` itself. The walk descends into `plan`'s
/// children, stops at any node that is itself a network boundary (returning `1.0` there — that
/// subtree belongs to a different stage), and combines per-node cardinality effects on the way
/// back up: `LowerEqual` divides by `cardinality_task_count_factor`, `GreaterEqual` multiplies
/// by it. When a node has multiple children, their `sf`s are combined with `max` before the
/// current node's effect is applied.
///
/// Used at boundary-injection sites to scale the producer-side task count into a sensible
/// consumer-side task count for the next stage up.
///
/// ```text
///       ┌────────────────────┐
///       │  RepartitionExec   │   Equal       sf unchanged       →  0.44
///       │    (Hash, ...)     │
///       └────────▲───────────┘
///                │   combine on the way back up
///       ┌────────┴───────────┐
///       │   AggregateExec    │   LowerEqual  sf /= 1.5          →  0.44
///       │     (Partial)      │
///       └────────▲───────────┘
///                │
///       ┌────────┴───────────┐
///       │     FilterExec     │   LowerEqual  sf /= 1.5          →  0.67
///       └────────▲───────────┘
///                │
///       ┌────────┴───────────┐
///       │   DataSourceExec   │   leaf                           →  1.0 (start)
///       └────────────────────┘
/// ```
///
/// With `cardinality_task_count_factor = 1.5`, the example above yields `sf ≈ 0.44`. The
/// boundary's recorded task count above this stage will be `ceil(T_producer × sf)`.
fn calculate_scale_factor(plan: &Arc<dyn ExecutionPlan>, ctx: &Context) -> f64 {
    if plan.is_network_boundary() {
        return 1.0;
    };

    let mut sf = None;
    for plan in plan.children() {
        sf = match sf {
            None => Some(calculate_scale_factor(plan, ctx)),
            Some(sf) => Some(sf.max(calculate_scale_factor(plan, ctx))),
        }
    }

    let sf = sf.unwrap_or(1.0);
    match plan.cardinality_effect() {
        CardinalityEffect::LowerEqual => sf / ctx.d_cfg.cardinality_task_count_factor,
        CardinalityEffect::GreaterEqual => sf * ctx.d_cfg.cardinality_task_count_factor,
        _ => sf,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_planner::insert_broadcast::insert_broadcast_execs;
    use crate::test_utils::plans::{
        BuildSideOneTaskEstimator, TestPlanOptions, base_session_builder, context_with_query,
        sql_to_physical_plan,
    };
    use crate::{DistributedExt, TaskEstimation, TaskEstimator, assert_snapshot};
    use datafusion::config::ConfigOptions;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::filter::FilterExec;
    /* schema for the "weather" table

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
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @"DataSourceExec: task_count=Desired(3)")
    }

    #[tokio::test]
    async fn test_aggregation() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        ProjectionExec: task_count=Maximum(1)
          SortPreservingMergeExec: task_count=Maximum(1)
            NetworkCoalesceExec: task_count=Maximum(1)
              SortExec: task_count=Desired(2)
                ProjectionExec: task_count=Desired(2)
                  AggregateExec: task_count=Desired(2)
                    NetworkShuffleExec: task_count=Desired(2)
                      RepartitionExec: task_count=Desired(3)
                        AggregateExec: task_count=Desired(3)
                          PartitionIsolatorExec: task_count=Desired(3)
                            DataSourceExec: task_count=Desired(3)
        ")
    }

    #[tokio::test]
    async fn test_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday"
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            DataSourceExec: task_count=Maximum(1)
          DataSourceExec: task_count=Maximum(1)
        ")
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
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            NetworkCoalesceExec: task_count=Maximum(1)
              ProjectionExec: task_count=Desired(2)
                AggregateExec: task_count=Desired(2)
                  NetworkShuffleExec: task_count=Desired(2)
                    RepartitionExec: task_count=Desired(3)
                      AggregateExec: task_count=Desired(3)
                        FilterExec: task_count=Desired(3)
                          RepartitionExec: task_count=Desired(3)
                            PartitionIsolatorExec: task_count=Desired(3)
                              DataSourceExec: task_count=Desired(3)
          ProjectionExec: task_count=Maximum(1)
            AggregateExec: task_count=Maximum(1)
              NetworkShuffleExec: task_count=Maximum(1)
                RepartitionExec: task_count=Desired(3)
                  AggregateExec: task_count=Desired(3)
                    FilterExec: task_count=Desired(3)
                      RepartitionExec: task_count=Desired(3)
                        PartitionIsolatorExec: task_count=Desired(3)
                          DataSourceExec: task_count=Desired(3)
        ")
    }

    // TODO: should be changed once broadcasting is done more intelligently and not behind a
    // feature flag.
    #[tokio::test]
    async fn test_inner_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a INNER JOIN weather b ON a."RainToday" = b."RainToday"
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            DataSourceExec: task_count=Maximum(1)
          DataSourceExec: task_count=Maximum(1)
        ")
    }

    #[tokio::test]
    async fn test_distinct() {
        let query = r#"
        SELECT DISTINCT "RainToday" FROM weather
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        AggregateExec: task_count=Desired(2)
          NetworkShuffleExec: task_count=Desired(2)
            RepartitionExec: task_count=Desired(3)
              AggregateExec: task_count=Desired(3)
                PartitionIsolatorExec: task_count=Desired(3)
                  DataSourceExec: task_count=Desired(3)
        ")
    }

    #[tokio::test]
    async fn test_union_all() {
        let query = r#"
        SELECT "MinTemp" FROM weather WHERE "RainToday" = 'yes'
        UNION ALL
        SELECT "MaxTemp" FROM weather WHERE "RainToday" = 'no'
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        ChildrenIsolatorUnionExec: task_count=Desired(4)
          FilterExec: task_count=Maximum(2)
            RepartitionExec: task_count=Maximum(2)
              PartitionIsolatorExec: task_count=Maximum(2)
                DataSourceExec: task_count=Maximum(2)
          ProjectionExec: task_count=Maximum(2)
            FilterExec: task_count=Maximum(2)
              RepartitionExec: task_count=Maximum(2)
                PartitionIsolatorExec: task_count=Maximum(2)
                  DataSourceExec: task_count=Maximum(2)
        ")
    }

    #[tokio::test]
    async fn test_subquery() {
        let query = r#"
        SELECT * FROM (
            SELECT "MinTemp", "MaxTemp" FROM weather WHERE "RainToday" = 'yes'
        ) AS subquery WHERE "MinTemp" > 5
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        FilterExec: task_count=Desired(3)
          RepartitionExec: task_count=Desired(3)
            PartitionIsolatorExec: task_count=Desired(3)
              DataSourceExec: task_count=Desired(3)
        ")
    }

    #[tokio::test]
    async fn test_window_function() {
        let query = r#"
        SELECT "MinTemp", ROW_NUMBER() OVER (PARTITION BY "RainToday" ORDER BY "MinTemp") as rn
        FROM weather
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        ProjectionExec: task_count=Desired(3)
          BoundedWindowAggExec: task_count=Desired(3)
            SortExec: task_count=Desired(3)
              NetworkShuffleExec: task_count=Desired(3)
                RepartitionExec: task_count=Desired(3)
                  PartitionIsolatorExec: task_count=Desired(3)
                    DataSourceExec: task_count=Desired(3)
        ")
    }

    #[tokio::test]
    async fn test_children_isolator_union() {
        let query = r#"
        SET distributed.children_isolator_unions = true;
        SET distributed.files_per_task = 1;
        SELECT "MinTemp" FROM weather WHERE "RainToday" = 'yes'
        UNION ALL
        SELECT "MaxTemp" FROM weather WHERE "RainToday" = 'no'
        UNION ALL
        SELECT "Rainfall" FROM weather WHERE "RainTomorrow" = 'yes'
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        ChildrenIsolatorUnionExec: task_count=Desired(4)
          FilterExec: task_count=Maximum(1)
            RepartitionExec: task_count=Maximum(1)
              DataSourceExec: task_count=Maximum(1)
          ProjectionExec: task_count=Maximum(1)
            FilterExec: task_count=Maximum(1)
              RepartitionExec: task_count=Maximum(1)
                DataSourceExec: task_count=Maximum(1)
          ProjectionExec: task_count=Maximum(2)
            FilterExec: task_count=Maximum(2)
              RepartitionExec: task_count=Maximum(2)
                PartitionIsolatorExec: task_count=Maximum(2)
                  DataSourceExec: task_count=Maximum(2)
        ")
    }

    #[tokio::test]
    async fn test_intermediate_task_estimator() {
        let query = r#"
        SELECT DISTINCT "RainToday" FROM weather
        "#;
        let annotated = sql_to_annotated_with_estimator(query, |_: &RepartitionExec| {
            Some(TaskEstimation::maximum(1))
        })
        .await;
        assert_snapshot!(annotated, @r"
        AggregateExec: task_count=Desired(1)
          NetworkShuffleExec: task_count=Desired(1)
            RepartitionExec: task_count=Maximum(1)
              AggregateExec: task_count=Maximum(1)
                DataSourceExec: task_count=Maximum(1)
        ")
    }

    #[tokio::test]
    async fn test_union_all_limited_by_intermediate_estimator() {
        let query = r#"
        SELECT "MinTemp" FROM weather WHERE "RainToday" = 'yes'
        UNION ALL
        SELECT "MaxTemp" FROM weather WHERE "RainToday" = 'no'
        "#;
        let annotated = sql_to_annotated_with_estimator(query, |_: &FilterExec| {
            Some(TaskEstimation::maximum(1))
        })
        .await;
        assert_snapshot!(annotated, @r"
        ChildrenIsolatorUnionExec: task_count=Desired(2)
          FilterExec: task_count=Maximum(1)
            RepartitionExec: task_count=Maximum(1)
              DataSourceExec: task_count=Maximum(1)
          ProjectionExec: task_count=Maximum(1)
            FilterExec: task_count=Maximum(1)
              RepartitionExec: task_count=Maximum(1)
                DataSourceExec: task_count=Maximum(1)
        ")
    }

    #[tokio::test]
    async fn test_broadcast_join_annotation() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let annotated = sql_to_annotated_broadcast(query, 4, 4, true).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Desired(3)
          CoalescePartitionsExec: task_count=Desired(3)
            NetworkBroadcastExec: task_count=Desired(3)
              BroadcastExec: task_count=Desired(3)
                PartitionIsolatorExec: task_count=Desired(3)
                  DataSourceExec: task_count=Desired(3)
          PartitionIsolatorExec: task_count=Desired(3)
            DataSourceExec: task_count=Desired(3)
        ")
    }

    #[tokio::test]
    async fn test_broadcast_datasource_as_build_child() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;

        // Check physical plan before insertion, shouldn't have CoalescePartitionsExec
        let physical_plan = sql_to_physical_plan(query, 1, 4).await;
        assert_snapshot!(physical_plan, @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");

        // With target_partitions=1, there is no CoalescePartitionsExec initially
        // With broadcast, should create one and insert BroadcastExec below it
        let annotated = sql_to_annotated_broadcast(query, 1, 4, true).await;
        assert!(annotated.contains("Broadcast"));
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Desired(3)
          CoalescePartitionsExec: task_count=Desired(3)
            NetworkBroadcastExec: task_count=Desired(3)
              BroadcastExec: task_count=Desired(3)
                PartitionIsolatorExec: task_count=Desired(3)
                  DataSourceExec: task_count=Desired(3)
          PartitionIsolatorExec: task_count=Desired(3)
            DataSourceExec: task_count=Desired(3)
        ");
    }

    #[tokio::test]
    async fn test_broadcast_one_to_many() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let annotated =
            sql_to_annotated_broadcast_with_estimator(query, 3, BuildSideOneTaskEstimator).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Desired(3)
          CoalescePartitionsExec: task_count=Desired(3)
            NetworkBroadcastExec: task_count=Desired(3)
              BroadcastExec: task_count=Maximum(1)
                DataSourceExec: task_count=Maximum(1)
          PartitionIsolatorExec: task_count=Desired(3)
            DataSourceExec: task_count=Desired(3)
        ");
    }

    #[tokio::test]
    async fn test_broadcast_build_coalesce_caps_join_stage() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let annotated =
            sql_to_annotated_broadcast_with_estimator(query, 3, BroadcastBuildCoalesceMaxEstimator)
                .await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            NetworkBroadcastExec: task_count=Maximum(1)
              BroadcastExec: task_count=Desired(3)
                PartitionIsolatorExec: task_count=Desired(3)
                  DataSourceExec: task_count=Desired(3)
          DataSourceExec: task_count=Maximum(1)
        ");
    }

    #[tokio::test]
    async fn test_broadcast_disabled_default() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let annotated = sql_to_annotated_broadcast(query, 4, 4, false).await;
        // With broadcast disabled, no broadcast annotation should appear
        assert!(!annotated.contains("Broadcast"));
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            DataSourceExec: task_count=Maximum(1)
          DataSourceExec: task_count=Maximum(1)
        ")
    }

    #[tokio::test]
    async fn test_broadcast_multi_join_chain() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp", c."Rainfall"
        FROM weather a
        INNER JOIN weather b ON a."RainToday" = b."RainToday"
        INNER JOIN weather c ON b."RainToday" = c."RainToday"
        "#;
        let annotated = sql_to_annotated_broadcast(query, 4, 4, true).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Desired(3)
          CoalescePartitionsExec: task_count=Desired(3)
            NetworkBroadcastExec: task_count=Desired(3)
              BroadcastExec: task_count=Desired(3)
                HashJoinExec: task_count=Desired(3)
                  CoalescePartitionsExec: task_count=Desired(3)
                    NetworkBroadcastExec: task_count=Desired(3)
                      BroadcastExec: task_count=Desired(3)
                        PartitionIsolatorExec: task_count=Desired(3)
                          DataSourceExec: task_count=Desired(3)
                  PartitionIsolatorExec: task_count=Desired(3)
                    DataSourceExec: task_count=Desired(3)
          PartitionIsolatorExec: task_count=Desired(3)
            DataSourceExec: task_count=Desired(3)
        ")
    }

    #[tokio::test]
    async fn test_broadcast_union_children_isolator_annotation() {
        let query = r#"
        SET distributed.children_isolator_unions = true;
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        UNION ALL
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        UNION ALL
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let annotated = sql_to_annotated_broadcast(query, 4, 4, true).await;
        // With ChildrenIsolatorUnionExec, each broadcast task_count should be limited to their
        // context.
        assert_snapshot!(annotated, @r"
        ChildrenIsolatorUnionExec: task_count=Desired(4)
          HashJoinExec: task_count=Maximum(1)
            CoalescePartitionsExec: task_count=Maximum(1)
              NetworkBroadcastExec: task_count=Maximum(1)
                BroadcastExec: task_count=Desired(3)
                  PartitionIsolatorExec: task_count=Desired(3)
                    DataSourceExec: task_count=Desired(3)
            DataSourceExec: task_count=Maximum(1)
          HashJoinExec: task_count=Maximum(1)
            CoalescePartitionsExec: task_count=Maximum(1)
              NetworkBroadcastExec: task_count=Maximum(1)
                BroadcastExec: task_count=Desired(3)
                  PartitionIsolatorExec: task_count=Desired(3)
                    DataSourceExec: task_count=Desired(3)
            DataSourceExec: task_count=Maximum(1)
          HashJoinExec: task_count=Maximum(2)
            CoalescePartitionsExec: task_count=Maximum(2)
              NetworkBroadcastExec: task_count=Maximum(2)
                BroadcastExec: task_count=Desired(3)
                  PartitionIsolatorExec: task_count=Desired(3)
                    DataSourceExec: task_count=Desired(3)
            PartitionIsolatorExec: task_count=Maximum(2)
              DataSourceExec: task_count=Maximum(2)
        ");
    }

    #[allow(clippy::type_complexity)]
    struct CallbackEstimator {
        f: Arc<dyn Fn(&dyn ExecutionPlan) -> Option<TaskEstimation> + Send + Sync>,
    }

    impl CallbackEstimator {
        fn new<T: ExecutionPlan + 'static>(
            f: impl Fn(&T) -> Option<TaskEstimation> + Send + Sync + 'static,
        ) -> Self {
            let f = Arc::new(move |plan: &dyn ExecutionPlan| -> Option<TaskEstimation> {
                if let Some(plan) = plan.as_any().downcast_ref::<T>() {
                    f(plan)
                } else {
                    None
                }
            });
            Self { f }
        }
    }

    impl TaskEstimator for CallbackEstimator {
        fn task_estimation(
            &self,
            plan: &Arc<dyn ExecutionPlan>,
            _: &ConfigOptions,
        ) -> Option<TaskEstimation> {
            (self.f)(plan.as_ref())
        }

        fn scale_up_leaf_node(
            &self,
            _: &Arc<dyn ExecutionPlan>,
            _: usize,
            _: &ConfigOptions,
        ) -> Option<Arc<dyn ExecutionPlan>> {
            None
        }
    }

    #[derive(Debug)]
    struct BroadcastBuildCoalesceMaxEstimator;

    impl TaskEstimator for BroadcastBuildCoalesceMaxEstimator {
        fn task_estimation(
            &self,
            plan: &Arc<dyn ExecutionPlan>,
            _: &ConfigOptions,
        ) -> Option<TaskEstimation> {
            let coalesce = plan.as_any().downcast_ref::<CoalescePartitionsExec>()?;
            if coalesce.input().as_any().is::<BroadcastExec>() {
                Some(TaskEstimation::maximum(1))
            } else {
                None
            }
        }

        fn scale_up_leaf_node(
            &self,
            _: &Arc<dyn ExecutionPlan>,
            _: usize,
            _: &ConfigOptions,
        ) -> Option<Arc<dyn ExecutionPlan>> {
            None
        }
    }

    async fn sql_to_annotated(query: &str) -> String {
        annotate_test_plan(query, TestPlanOptions::default(), |b| b).await
    }

    async fn sql_to_annotated_broadcast(
        query: &str,
        target_partitions: usize,
        num_workers: usize,
        broadcast_enabled: bool,
    ) -> String {
        let options = TestPlanOptions {
            target_partitions,
            num_workers,
            broadcast_enabled,
        };
        annotate_test_plan(query, options, |b| b).await
    }

    async fn sql_to_annotated_with_estimator<T: ExecutionPlan + Send + Sync + 'static>(
        query: &str,
        estimator: impl Fn(&T) -> Option<TaskEstimation> + Send + Sync + 'static,
    ) -> String {
        let options = TestPlanOptions::default();
        annotate_test_plan(query, options, |b| {
            b.with_distributed_task_estimator(CallbackEstimator::new(estimator))
        })
        .await
    }

    async fn sql_to_annotated_broadcast_with_estimator(
        query: &str,
        num_workers: usize,
        estimator: impl TaskEstimator + Send + Sync + 'static,
    ) -> String {
        let options = TestPlanOptions {
            target_partitions: 4,
            num_workers,
            broadcast_enabled: true,
        };
        annotate_test_plan(query, options, |b| {
            b.with_distributed_task_estimator(estimator)
        })
        .await
    }

    async fn annotate_test_plan(
        query: &str,
        options: TestPlanOptions,
        configure: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder,
    ) -> String {
        let builder = base_session_builder(
            options.target_partitions,
            options.num_workers,
            options.broadcast_enabled,
        );
        let builder = configure(builder);
        let (ctx, query) = context_with_query(builder, query).await;
        let df = ctx.sql(&query).await.unwrap();
        let mut plan = df.create_physical_plan().await.unwrap();

        let session_config = ctx.copied_config();
        plan = insert_broadcast_execs(plan, session_config.options())
            .expect("failed to insert broadcasts");
        let cfg = session_config.options();

        let ctx = Context {
            parent: None,
            cfg,
            d_cfg: DistributedConfig::from_config_options(cfg).unwrap(),
            task_counts: &Mutex::new(HashMap::new()),
            query_id: Uuid::new_v4(),
            stage_id: &AtomicUsize::new(1),
        };

        let annotated = _inject_network_boundaries(plan, ctx.clone())
            .await
            .expect("failed to annotate plan");
        debug_annotated(&annotated, 0, &ctx)
    }

    fn debug_annotated(plan: &Arc<dyn ExecutionPlan>, indent: usize, ctx: &Context) -> String {
        let mut result = format!(
            "{}{}: task_count={:?}\n",
            "  ".repeat(indent),
            plan.name(),
            ctx.task_count(plan).unwrap()
        );
        for child in plan.children() {
            result += &debug_annotated(child, indent + 1, ctx);
        }
        result
    }
}
