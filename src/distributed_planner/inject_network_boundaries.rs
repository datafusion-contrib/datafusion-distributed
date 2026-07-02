use crate::TaskCountAnnotation::{Desired, Maximum};
use crate::execution_plans::{ChildWeight, ChildrenIsolatorUnionExec};
use crate::stage::LocalStage;
use crate::{
    BroadcastExec, DistributedConfig, NetworkBoundaryExt, NetworkBroadcastExec,
    NetworkCoalesceExec, NetworkShuffleExec, Stage, TaskCountAnnotation, TaskEstimator,
};
use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, Result, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use std::any::TypeId;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

/// Walks an [ExecutionPlan] and injects [NetworkShuffleExec], [NetworkBroadcastExec], and
/// [NetworkCoalesceExec] nodes wherever a stage boundary is needed. The returned plan has the
/// same shape as the input except for these inserted boundary nodes.
///
/// Per-node task counts are recorded in a side map on the [InjectNetworkBoundaryContext] (keyed by
/// plan-pointer identity) rather than mutated into the plan itself. Later passes look them up via
/// [InjectNetworkBoundaryContext::task_count].
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
/// - Starting from the leaves, we climb the plan, asking the [TaskEstimator] for a task count
///   at each leaf and merging children's task counts at each inner node.
/// - We keep going until the current node is one that requires a network boundary **above it**
///   (e.g. currently a hash `RepartitionExec` (→ shuffle), a `BroadcastExec` (→ broadcast), or any
///   other node whose parent is `CoalescePartitionsExec` / `SortPreservingMergeExec` (→ coalesce).
/// - At that moment, that node and the subtree underneath it form the stage we've just delimited;
///   the boundary will be injected above it in Phase 3.
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
/// - Starting from the input of the boundary we're about to inject, we do a top-down walk over the
///   just delimited stage.
/// - The `T` task count reconciled from phase 1 is assigned to every node in the stage during
///   this top-down walk.
/// - Leaves go through [TaskEstimator::scale_up_leaf_node] which is called using `T` as the
///   `task_count` argument. The default file-scan estimator wraps the leaf in a
///   [DistributedLeafExec] that holds one per-task variant for each of the `T` tasks; the
///   wrapper is transparent to network boundaries (it reports the same partition count as the
///   original) and is replaced by its per-task variant in the task spawner before serialisation.
/// - If the walk meets a network boundary that was already injected by an earlier iteration of this
///   loop, it does **not** descend into it — that subtree belongs to a previously-formed stage and
///   has already been finalised.
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
///   │    DistributedLeafExec      │  ← wraps DataSourceExec; this is replaced
///   │      (DataSourceExec)       │    with the per-task variant before sending
///   └─────────────────────────────┘    to workers
/// ```
///
/// ## Phase 3 — inject the boundary and seed the next stage's starting task count
///
/// - Now we wrap the producer stage in the appropriate `Network*Exec` node and decide the task
///   count above the boundary — i.e. the starting task count for the next stage up.
/// - We compute a scale factor from the cardinality effects of the producer-stage nodes
///   and apply it as `ceil(T_producer × sf)`.
/// - That becomes the new node's recorded task count and feeds back into Phase 1 for the next stage.
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
pub(crate) async fn inject_network_boundaries(
    plan: Arc<dyn ExecutionPlan>,
    nb_builder: impl NetworkBoundaryBuilder + Send + Sync,
    cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = InjectNetworkBoundaryContext {
        cfg,
        d_cfg: DistributedConfig::from_config_options(cfg)?,
        nb_builder: &nb_builder,
        task_counts: &Mutex::new(HashMap::new()),
        query_id: Uuid::new_v4(),
        stage_id: &AtomicUsize::new(1),
    };

    _inject_network_boundaries(plan, None, &ctx).await
}

#[derive(Clone)]
pub(crate) struct InjectNetworkBoundaryContext<'a> {
    pub(crate) d_cfg: &'a DistributedConfig,

    cfg: &'a ConfigOptions,
    nb_builder: &'a (dyn NetworkBoundaryBuilder + Send + Sync),
    task_counts: &'a Mutex<HashMap<usize, TaskCountAnnotation>>,
    query_id: Uuid,
    stage_id: &'a AtomicUsize,
}

impl<'a> InjectNetworkBoundaryContext<'a> {
    pub(crate) fn max_tasks(&self) -> Result<usize> {
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

    fn plan_with_task_count(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        task_count: TaskCountAnnotation,
    ) -> Arc<dyn ExecutionPlan> {
        self.set_task_count(&plan, task_count);
        plan
    }

    pub(crate) fn task_count(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<TaskCountAnnotation> {
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

fn collect_left_hash_join_requires_single_task(
    plan: &dyn ExecutionPlan,
    processed_children: &[Arc<dyn ExecutionPlan>],
    broadcast_joins_enabled: bool,
) -> bool {
    let Some(hash_join) = plan.downcast_ref::<HashJoinExec>() else {
        return false;
    };
    if hash_join.partition_mode() != &PartitionMode::CollectLeft {
        return false;
    }
    if !broadcast_joins_enabled {
        return true;
    }

    !processed_children
        .first()
        .is_some_and(|child| build_input_is_broadcast(child.as_ref()))
}

fn build_input_is_broadcast(plan: &dyn ExecutionPlan) -> bool {
    if plan.is::<NetworkBroadcastExec>() || plan.is::<BroadcastExec>() {
        return true;
    }

    plan.downcast_ref::<CoalescePartitionsExec>()
        .is_some_and(|coalesce| build_input_is_broadcast(coalesce.input().as_ref()))
}

/// WARNING: every return statement in this function must funnel through
/// [InjectNetworkBoundaryContext::plan_with_task_count]
/// (or [InjectNetworkBoundaryContext::set_task_count] on the way through) so the returned node has
/// a recorded task count. Callers downstream depend on this invariant.
async fn _inject_network_boundaries(
    plan: Arc<dyn ExecutionPlan>,
    parent: Option<&Arc<dyn ExecutionPlan>>,
    nb_ctx: &InjectNetworkBoundaryContext<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let broadcast_joins_enabled = nb_ctx.d_cfg.broadcast_joins;
    let estimator = &nb_ctx.d_cfg.__private_task_estimator;

    if plan.children().is_empty() {
        // This is a leaf node, maybe a DataSourceExec, or maybe something else custom from the
        // user. We need to estimate how many tasks are needed for this leaf node, and we'll take
        // this decision into account when deciding how many tasks will be actually used.
        return if let Some(estimate) = estimator.task_estimation(&plan, nb_ctx.cfg) {
            Ok(nb_ctx.plan_with_task_count(plan, estimate.task_count.limit(nb_ctx.max_tasks()?)))
        } else {
            // We could not determine how many tasks this leaf node should run on, so
            // assume it cannot be distributed and use just 1 task.
            Ok(nb_ctx.plan_with_task_count(plan, Maximum(1)))
        };
    }

    let mut futures = Vec::with_capacity(plan.children().len());
    for child in plan.children() {
        let child = Arc::clone(child);
        futures.push(Box::pin(_inject_network_boundaries(
            child,
            Some(&plan),
            nb_ctx,
        )));
    }
    let processed_children = futures::future::try_join_all(futures).await?;

    let mut task_count = estimator
        .task_estimation(&plan, nb_ctx.cfg)
        .map_or(Desired(1), |v| v.task_count);
    if nb_ctx.d_cfg.children_isolator_unions && plan.is::<UnionExec>() {
        // Unions have the chance to decide how many tasks they should run on. If there's a union
        // with a bunch of children, the user might want to increase parallelism and increase the
        // task count for the stage running that.
        let mut count = 0;
        for processed_child in processed_children.iter() {
            count += nb_ctx.task_count(processed_child)?.as_usize();
        }
        task_count = Desired(count);
    } else if collect_left_hash_join_requires_single_task(
        plan.as_ref(),
        &processed_children,
        broadcast_joins_enabled,
    ) {
        // Only distribute CollectLeft HashJoins when the build side was actually broadcast.
        // Enabling broadcast joins is not enough by itself: join types that cannot safely
        // broadcast their build side, such as LeftSemi, must keep running in one task.
        task_count = Maximum(1);
    } else {
        // The task count for this plan is decided by the biggest task count from the children; unless
        // a child specifies a maximum task count, in that case, the maximum is respected. Some
        // nodes can only run in one task. If there is a subplan with a single node declaring that
        // it can only run in one task, all the rest of the nodes in the stage need to respect it.
        for processed_child in processed_children.iter() {
            task_count = task_count.merge(nb_ctx.task_count(processed_child)?)
        }
    }

    let plan = plan.with_new_children(processed_children)?;
    // Cap the reconciled task count by the configured max-per-stage budget.
    task_count = task_count.limit(nb_ctx.max_tasks()?);

    // Upon reaching a hash repartition, we need to introduce a network shuffle right above it.
    if let Some(r_exec) = plan.downcast_ref::<RepartitionExec>()
        && matches!(r_exec.partitioning(), Partitioning::Hash(_, _))
    {
        let input_stage = LocalStage {
            query_id: nb_ctx.query_id,
            num: nb_ctx.fetch_add_stage_id(),
            plan: nb_ctx.plan_with_task_count(plan, task_count),
            tasks: task_count.as_usize(),
            metrics_set: Default::default(),
        };
        let result = nb_ctx
            .nb_builder
            .build(input_stage, TypeId::of::<NetworkShuffleExec>(), nb_ctx)
            .await?;
        let nb = Arc::new(NetworkShuffleExec::from_stage(
            result.input_stage,
            result.input_properties,
        ));
        Ok(nb_ctx.plan_with_task_count(nb, result.consumer_task_count))
    }
    // Upon reaching a broadcast, we need to introduce a network broadcast right above it.
    else if let Some(_b_exec) = plan.downcast_ref::<BroadcastExec>() {
        let input_stage = LocalStage {
            query_id: nb_ctx.query_id,
            num: nb_ctx.fetch_add_stage_id(),
            plan: nb_ctx.plan_with_task_count(plan, task_count),
            tasks: task_count.as_usize(),
            metrics_set: Default::default(),
        };
        let result = nb_ctx
            .nb_builder
            .build(input_stage, TypeId::of::<NetworkBroadcastExec>(), nb_ctx)
            .await?;
        let nb = Arc::new(NetworkBroadcastExec::from_stage(
            result.input_stage,
            result.input_properties,
        ));
        Ok(nb_ctx.plan_with_task_count(nb, result.consumer_task_count))
    }
    // If the parent of the current node is either a `CoalescePartitionsExec` or a
    // `SortPreservingMergeExec`, a network boundary below it is necessary.
    else if let Some(parent) = parent
        // If this node is a leaf node, putting a network boundary above is a bit wasteful, so
        // we don't want to do it.
        && !plan.children().is_empty()
        && (parent.is::<CoalescePartitionsExec>()
        || parent.is::<SortPreservingMergeExec>())
    {
        let input_stage = LocalStage {
            query_id: nb_ctx.query_id,
            num: nb_ctx.fetch_add_stage_id(),
            plan: nb_ctx.plan_with_task_count(plan, task_count),
            tasks: task_count.as_usize(),
            metrics_set: Default::default(),
        };
        let result = nb_ctx
            .nb_builder
            .build(input_stage, TypeId::of::<NetworkCoalesceExec>(), nb_ctx)
            .await?;
        if !matches!(result.consumer_task_count, Maximum(1)) {
            return plan_err!(
                "A NetworkCoalesceExec must return exactly a Maximum(1) annotation above"
            );
        }
        // The parent that triggered this branch is a `CoalescePartitionsExec` or
        // `SortPreservingMergeExec`, both of which fold all partitions into one — so the
        // stage above this boundary must run in exactly one task.
        let nb = Arc::new(NetworkCoalesceExec::from_stage(
            result.input_stage,
            result.input_properties,
            1,
        ));
        Ok(nb_ctx.plan_with_task_count(nb, result.consumer_task_count))
    } else if parent.is_none() {
        // We've just finished walking the head stage's subplan. Run a final propagation so
        // every node in the head stage (which never crossed a stage boundary on the way up)
        // gets its task count recorded.
        nb_ctx.propagate_task_count_until_network_boundaries(&plan, task_count)
    } else {
        // If this is not the root node, and it's also not a network boundary, then we don't need
        // to do anything else.
        Ok(nb_ctx.plan_with_task_count(plan, task_count))
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
/// - **Leaves**: ask the [TaskEstimator] for an optional scaled-up replacement (e.g. expanding a
///   `DataSourceExec`'s file groups by `task_count`). Every node in the returned subtree is
///   recorded with `task_count`.
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
impl InjectNetworkBoundaryContext<'_> {
    pub(crate) fn propagate_task_count_until_network_boundaries(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: TaskCountAnnotation,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Handle leaf nodes.
        if plan.children().is_empty() {
            let scaled_up = self.d_cfg.__private_task_estimator.scale_up_leaf_node(
                plan,
                task_count.as_usize(),
                self.cfg,
            )?;
            match scaled_up {
                None => Ok(self.plan_with_task_count(Arc::clone(plan), task_count)),
                Some(scaled_up) => {
                    // The scaled up subtree may contain more than 1 node.
                    scaled_up.apply(|plan| {
                        self.set_task_count(plan, task_count);
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    Ok(self.plan_with_task_count(scaled_up, task_count))
                }
            }

        // Handle network boundaries.
        } else if plan.is_network_boundary() {
            // Just annotate the network boundary and stop recursion here.
            Ok(self.plan_with_task_count(Arc::clone(plan), task_count))

        // Handle ChildrenIsolatorUnionExec.
        } else if self.d_cfg.children_isolator_unions && plan.is::<UnionExec>() {
            // Propagating through ChildrenIsolatorUnionExec is not that easy, each child will
            // be executed in its own task, and therefore, they will act as if they were in executing
            // in a non-distributed context. The ChildrenIsolatorUnionExec itself will make sure to
            // determine which children to run and which to exclude depending on the task index in
            // which it's running.
            //
            // Each child's bottom-up task count becomes its relative weight (children that want
            // more parallelism get a proportionally larger share of the stage's budget). A
            // `Maximum(N)` annotation maps to a hard cap so the allocator never assigns the
            // child more than `N` task slots; surplus budget is redistributed to uncapped
            // siblings, or stays empty if every child is capped.
            let children = plan.children();
            let c_i_union = ChildrenIsolatorUnionExec::from_children_and_weights(
                children.iter().map(|v| Arc::clone(v)),
                children
                    .iter()
                    .map(|v| match self.task_count(v)? {
                        Desired(n) => Ok(ChildWeight::desired(n as f64)),
                        Maximum(n) => Ok(ChildWeight::maximum(n)),
                    })
                    .collect::<Result<Vec<_>>>()?,
                task_count.as_usize(),
            )?;
            let mut new_children = Vec::with_capacity(children.len());

            let children_and_task_count = c_i_union
                .children()
                .into_iter()
                .zip(c_i_union.child_task_counts());
            for (child, task_count) in children_and_task_count {
                new_children.push(
                    self.propagate_task_count_until_network_boundaries(child, Maximum(task_count))?,
                );
            }
            let c_i_union = Arc::new(c_i_union).with_new_children(new_children)?;
            Ok(self.plan_with_task_count(c_i_union, task_count))

        // Handle middle nodes.
        } else {
            let mut new_children = Vec::with_capacity(plan.children().len());
            for child in plan.children() {
                new_children
                    .push(self.propagate_task_count_until_network_boundaries(child, task_count)?);
            }
            let plan = Arc::clone(plan).with_new_children(new_children)?;
            Ok(self.plan_with_task_count(plan, task_count))
        }
    }
}

/// Result returned by [NetworkBoundaryBuilder] implementations upon delimiting a new stage inside
/// [inject_network_boundaries].
pub(crate) struct NetworkBoundaryBuilderResult {
    /// The starting [TaskCountAnnotation] the [crate::NetworkBoundary] will be annotated with.
    /// This is just a starting point, and it might need to be reconciled with the task count
    /// annotations from other nodes.
    pub(crate) consumer_task_count: TaskCountAnnotation,
    /// The input [Stage] that will be attached to the [crate::NetworkBoundary] belonging to the
    /// stage above.
    pub(crate) input_stage: Stage,
    /// Properties (most importantly, the output partitioning) of the input stage as it will
    /// actually execute. This information might not be present in the `input_stage` field, as it
    /// might be in [Stage::Remote] state because it was already sent for execution.
    pub(crate) input_properties: Arc<PlanProperties>,
}

#[async_trait]
pub(crate) trait NetworkBoundaryBuilder {
    async fn build<'a>(
        &'a self,
        input_stage: LocalStage,
        nb_type: TypeId,
        nb_ctx: &'a InjectNetworkBoundaryContext<'a>,
    ) -> Result<NetworkBoundaryBuilderResult>;
}

#[async_trait]
impl<T, F> NetworkBoundaryBuilder for T
where
    T: Fn(LocalStage, TypeId, &InjectNetworkBoundaryContext) -> Result<F>,
    T: Send + Sync,
    F: Future<Output = Result<NetworkBoundaryBuilderResult>>,
    F: Send,
{
    async fn build<'a>(
        &'a self,
        input_stage: LocalStage,
        nb_type: TypeId,
        nb_ctx: &'a InjectNetworkBoundaryContext<'a>,
    ) -> Result<NetworkBoundaryBuilderResult> {
        self(input_stage, nb_type, nb_ctx)?.await
    }
}

/// Propagates the reconciled task count with [propagate_task_count_until_network_boundaries] and
/// returns a multiplicative factor describing how the data volume changes between the bottom of
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
pub(crate) struct CardinalityBasedNetworkBoundaryBuilder;

#[async_trait]
impl NetworkBoundaryBuilder for CardinalityBasedNetworkBoundaryBuilder {
    async fn build<'a>(
        &'a self,
        mut input_stage: LocalStage,
        nb_type: TypeId,
        nb_ctx: &'a InjectNetworkBoundaryContext<'a>,
    ) -> Result<NetworkBoundaryBuilderResult> {
        input_stage.plan = nb_ctx.propagate_task_count_until_network_boundaries(
            &input_stage.plan,
            Desired(input_stage.tasks),
        )?;
        let input_properties = Arc::clone(input_stage.plan.properties());

        if nb_type == TypeId::of::<NetworkCoalesceExec>() {
            return Ok(NetworkBoundaryBuilderResult {
                consumer_task_count: Maximum(1),
                input_stage: Stage::Local(input_stage),
                input_properties,
            });
        }

        fn calculate_scale_factor(plan: &Arc<dyn ExecutionPlan>, d_cfg: &DistributedConfig) -> f64 {
            if plan.is_network_boundary() {
                return 1.0;
            };

            let mut sf = None;
            for plan in plan.children() {
                sf = match sf {
                    None => Some(calculate_scale_factor(plan, d_cfg)),
                    Some(sf) => Some(sf.max(calculate_scale_factor(plan, d_cfg))),
                }
            }

            let sf = sf.unwrap_or(1.0);
            match plan.cardinality_effect() {
                CardinalityEffect::LowerEqual => sf / d_cfg.cardinality_task_count_factor,
                CardinalityEffect::GreaterEqual => sf * d_cfg.cardinality_task_count_factor,
                _ => sf,
            }
        }

        let f = calculate_scale_factor(&input_stage.plan, nb_ctx.d_cfg);

        Ok(NetworkBoundaryBuilderResult {
            consumer_task_count: Desired((f * input_stage.tasks as f64).ceil() as usize),
            input_stage: Stage::Local(input_stage),
            input_properties,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_planner::insert_broadcast::insert_broadcast_execs;
    use crate::test_utils::plans::{BuildSideOneTaskEstimator, TestPlanBuilder};
    use crate::{TaskEstimation, TaskEstimator, assert_snapshot};
    use datafusion::config::ConfigOptions;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
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
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @"DataSourceExec: task_count=Desired(4)")
    }

    #[tokio::test]
    async fn test_aggregation() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        SortPreservingMergeExec: task_count=Maximum(1)
          NetworkCoalesceExec: task_count=Maximum(1)
            SortExec: task_count=Desired(3)
              ProjectionExec: task_count=Desired(3)
                AggregateExec: task_count=Desired(3)
                  NetworkShuffleExec: task_count=Desired(3)
                    RepartitionExec: task_count=Desired(4)
                      AggregateExec: task_count=Desired(4)
                        DistributedLeafExec: task_count=Desired(4)
        ")
    }

    #[tokio::test]
    async fn test_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday"
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            DistributedLeafExec: task_count=Maximum(1)
          DistributedLeafExec: task_count=Maximum(1)
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
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            NetworkCoalesceExec: task_count=Maximum(1)
              ProjectionExec: task_count=Desired(2)
                AggregateExec: task_count=Desired(2)
                  NetworkShuffleExec: task_count=Desired(2)
                    RepartitionExec: task_count=Desired(4)
                      AggregateExec: task_count=Desired(4)
                        FilterExec: task_count=Desired(4)
                          RepartitionExec: task_count=Desired(4)
                            DistributedLeafExec: task_count=Desired(4)
          ProjectionExec: task_count=Maximum(1)
            AggregateExec: task_count=Maximum(1)
              NetworkShuffleExec: task_count=Maximum(1)
                RepartitionExec: task_count=Desired(4)
                  AggregateExec: task_count=Desired(4)
                    FilterExec: task_count=Desired(4)
                      RepartitionExec: task_count=Desired(4)
                        DistributedLeafExec: task_count=Desired(4)
        ")
    }

    // TODO: should be changed once broadcasting is done more intelligently and not behind a
    // feature flag.
    #[tokio::test]
    async fn test_inner_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a INNER JOIN weather b ON a."RainToday" = b."RainToday"
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            DistributedLeafExec: task_count=Maximum(1)
          DistributedLeafExec: task_count=Maximum(1)
        ")
    }

    #[tokio::test]
    async fn test_distinct() {
        let query = r#"
        SELECT DISTINCT "RainToday" FROM weather
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        AggregateExec: task_count=Desired(3)
          NetworkShuffleExec: task_count=Desired(3)
            RepartitionExec: task_count=Desired(4)
              AggregateExec: task_count=Desired(4)
                DistributedLeafExec: task_count=Desired(4)
        ")
    }

    #[tokio::test]
    async fn test_union_all() {
        let query = r#"
        SELECT "MinTemp" FROM weather WHERE "RainToday" = 'yes'
        UNION ALL
        SELECT "MaxTemp" FROM weather WHERE "RainToday" = 'no'
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        ChildrenIsolatorUnionExec: task_count=Desired(4)
          FilterExec: task_count=Maximum(2)
            RepartitionExec: task_count=Maximum(2)
              DistributedLeafExec: task_count=Maximum(2)
          ProjectionExec: task_count=Maximum(2)
            FilterExec: task_count=Maximum(2)
              RepartitionExec: task_count=Maximum(2)
                DistributedLeafExec: task_count=Maximum(2)
        ")
    }

    #[tokio::test]
    async fn test_subquery() {
        let query = r#"
        SELECT * FROM (
            SELECT "MinTemp", "MaxTemp" FROM weather WHERE "RainToday" = 'yes'
        ) AS subquery WHERE "MinTemp" > 5
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        FilterExec: task_count=Desired(4)
          RepartitionExec: task_count=Desired(4)
            DistributedLeafExec: task_count=Desired(4)
        ")
    }

    #[tokio::test]
    async fn test_window_function() {
        let query = r#"
        SELECT "MinTemp", ROW_NUMBER() OVER (PARTITION BY "RainToday" ORDER BY "MinTemp") as rn
        FROM weather
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        ProjectionExec: task_count=Desired(4)
          BoundedWindowAggExec: task_count=Desired(4)
            SortExec: task_count=Desired(4)
              NetworkShuffleExec: task_count=Desired(4)
                RepartitionExec: task_count=Desired(4)
                  DistributedLeafExec: task_count=Desired(4)
        ")
    }

    #[tokio::test]
    async fn test_children_isolator_union() {
        let query = r#"
        SELECT "MinTemp" FROM weather WHERE "RainToday" = 'yes'
        UNION ALL
        SELECT "MaxTemp" FROM weather WHERE "RainToday" = 'no'
        UNION ALL
        SELECT "Rainfall" FROM weather WHERE "RainTomorrow" = 'yes'
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        ChildrenIsolatorUnionExec: task_count=Desired(4)
          FilterExec: task_count=Maximum(2)
            RepartitionExec: task_count=Maximum(2)
              DistributedLeafExec: task_count=Maximum(2)
          ProjectionExec: task_count=Maximum(1)
            FilterExec: task_count=Maximum(1)
              RepartitionExec: task_count=Maximum(1)
                DistributedLeafExec: task_count=Maximum(1)
          ProjectionExec: task_count=Maximum(1)
            FilterExec: task_count=Maximum(1)
              RepartitionExec: task_count=Maximum(1)
                DistributedLeafExec: task_count=Maximum(1)
        ")
    }

    #[tokio::test]
    async fn test_intermediate_task_estimator() {
        let query = r#"
        SELECT DISTINCT "RainToday" FROM weather
        "#;
        let task_estimator: Arc<dyn TaskEstimator + Send + Sync + 'static> =
            Arc::new(CallbackEstimator::new(|_: &RepartitionExec| {
                Some(TaskEstimation::maximum(1))
            }));
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false)
            .distributed_task_estimator(task_estimator);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        AggregateExec: task_count=Desired(1)
          NetworkShuffleExec: task_count=Desired(1)
            RepartitionExec: task_count=Desired(1)
              AggregateExec: task_count=Desired(1)
                DistributedLeafExec: task_count=Desired(1)
        ")
    }

    #[tokio::test]
    async fn test_union_all_limited_by_intermediate_estimator() {
        let query = r#"
        SELECT "MinTemp" FROM weather WHERE "RainToday" = 'yes'
        UNION ALL
        SELECT "MaxTemp" FROM weather WHERE "RainToday" = 'no'
        "#;
        let task_estimator: Arc<dyn TaskEstimator + Send + Sync + 'static> =
            Arc::new(CallbackEstimator::new(|_: &RepartitionExec| {
                Some(TaskEstimation::maximum(1))
            }));
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false)
            .distributed_task_estimator(task_estimator);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        ChildrenIsolatorUnionExec: task_count=Desired(2)
          FilterExec: task_count=Maximum(1)
            RepartitionExec: task_count=Maximum(1)
              DistributedLeafExec: task_count=Maximum(1)
          ProjectionExec: task_count=Maximum(1)
            FilterExec: task_count=Maximum(1)
              RepartitionExec: task_count=Maximum(1)
                DistributedLeafExec: task_count=Maximum(1)
        ")
    }

    #[tokio::test]
    async fn test_broadcast_join_annotation() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan expects a single-node input plan.
            .distributed_planner(false)
            .broadcast_joins(true);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Desired(4)
          CoalescePartitionsExec: task_count=Desired(4)
            NetworkBroadcastExec: task_count=Desired(4)
              BroadcastExec: task_count=Desired(4)
                DistributedLeafExec: task_count=Desired(4)
          DistributedLeafExec: task_count=Desired(4)
        ")
    }

    #[tokio::test]
    async fn test_broadcast_datasource_as_build_child() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;

        let physical_plan_string = TestPlanBuilder::new()
            .target_partitions(1)
            .num_workers(4)
            .build()
            .await
            .physical_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_string, @"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");

        // With target_partitions=1, there is no CoalescePartitionsExec initially
        // With broadcast, should create one and insert BroadcastExec below it
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(1)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(true);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert!(annotated.contains("Broadcast"));
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Desired(4)
          CoalescePartitionsExec: task_count=Desired(4)
            NetworkBroadcastExec: task_count=Desired(4)
              BroadcastExec: task_count=Desired(4)
                DistributedLeafExec: task_count=Desired(4)
          DistributedLeafExec: task_count=Desired(4)
        ");
    }

    #[tokio::test]
    async fn test_broadcast_one_to_many() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(3)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(true)
            .distributed_task_estimator(BuildSideOneTaskEstimator);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Desired(3)
          CoalescePartitionsExec: task_count=Desired(3)
            NetworkBroadcastExec: task_count=Desired(3)
              BroadcastExec: task_count=Desired(1)
                DistributedLeafExec: task_count=Desired(1)
          DistributedLeafExec: task_count=Desired(3)
        ");
    }

    #[tokio::test]
    async fn test_broadcast_build_coalesce_caps_join_stage() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(3)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(true)
            .distributed_task_estimator(BroadcastBuildCoalesceMaxEstimator);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            NetworkBroadcastExec: task_count=Maximum(1)
              BroadcastExec: task_count=Desired(3)
                DistributedLeafExec: task_count=Desired(3)
          DistributedLeafExec: task_count=Maximum(1)
        ");
    }

    #[tokio::test]
    async fn test_broadcast_disabled_default() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(false);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        // With broadcast disabled, no broadcast annotation should appear
        assert!(!annotated.contains("Broadcast"));
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            DistributedLeafExec: task_count=Maximum(1)
          DistributedLeafExec: task_count=Maximum(1)
        ")
    }

    #[tokio::test]
    async fn test_collect_left_join_without_broadcast_stays_single_task() {
        let query = r#"
        SELECT a."MinTemp"
        FROM weather a
        WHERE EXISTS (
            SELECT 1
            FROM weather b
            WHERE a."RainToday" = b."RainToday"
        )
        "#;
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(true);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert!(!annotated.contains("Broadcast"));
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Maximum(1)
          CoalescePartitionsExec: task_count=Maximum(1)
            DistributedLeafExec: task_count=Maximum(1)
          DistributedLeafExec: task_count=Maximum(1)
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
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(true);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Desired(4)
          CoalescePartitionsExec: task_count=Desired(4)
            NetworkBroadcastExec: task_count=Desired(4)
              BroadcastExec: task_count=Desired(4)
                HashJoinExec: task_count=Desired(4)
                  CoalescePartitionsExec: task_count=Desired(4)
                    NetworkBroadcastExec: task_count=Desired(4)
                      BroadcastExec: task_count=Desired(4)
                        DistributedLeafExec: task_count=Desired(4)
                  DistributedLeafExec: task_count=Desired(4)
          DistributedLeafExec: task_count=Desired(4)
        ")
    }

    #[tokio::test]
    async fn test_broadcast_union_children_isolator_annotation() {
        let query = r#"
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
        let test_plan_builder = TestPlanBuilder::new()
            .target_partitions(4)
            .num_workers(4)
            // annotate_test_plan wants this as false so its s a single node plan
            .distributed_planner(false)
            .broadcast_joins(true)
            .distributed_children_isolator_unions(true);
        let annotated = annotate_test_plan(test_plan_builder, query).await;
        // With ChildrenIsolatorUnionExec, each broadcast task_count should be limited to their
        // context.
        assert_snapshot!(annotated, @r"
        ChildrenIsolatorUnionExec: task_count=Desired(4)
          HashJoinExec: task_count=Maximum(2)
            CoalescePartitionsExec: task_count=Maximum(2)
              NetworkBroadcastExec: task_count=Maximum(2)
                BroadcastExec: task_count=Desired(4)
                  DistributedLeafExec: task_count=Desired(4)
            DistributedLeafExec: task_count=Maximum(2)
          HashJoinExec: task_count=Maximum(1)
            CoalescePartitionsExec: task_count=Maximum(1)
              NetworkBroadcastExec: task_count=Maximum(1)
                BroadcastExec: task_count=Desired(4)
                  DistributedLeafExec: task_count=Desired(4)
            DistributedLeafExec: task_count=Maximum(1)
          HashJoinExec: task_count=Maximum(1)
            CoalescePartitionsExec: task_count=Maximum(1)
              NetworkBroadcastExec: task_count=Maximum(1)
                BroadcastExec: task_count=Desired(4)
                  DistributedLeafExec: task_count=Desired(4)
            DistributedLeafExec: task_count=Maximum(1)
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
                if let Some(plan) = plan.downcast_ref::<T>() {
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
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(None)
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
            let coalesce = plan.downcast_ref::<CoalescePartitionsExec>()?;
            if coalesce.input().is::<BroadcastExec>() {
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
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(None)
        }
    }

    async fn annotate_test_plan(test_plan_builder: TestPlanBuilder, query: &str) -> String {
        let test_plan = test_plan_builder.build().await;
        let plan = test_plan.physical_plan(query).await;
        let session_config = test_plan.get_ctx().copied_config();

        let plan_w_broadcast = insert_broadcast_execs(plan, session_config.options())
            .expect("failed to insert broadcasts");
        let network_boundaries_ctx = InjectNetworkBoundaryContext {
            cfg: session_config.options(),
            d_cfg: DistributedConfig::from_config_options(session_config.options()).unwrap(),
            task_counts: &Mutex::new(HashMap::new()),
            query_id: Uuid::new_v4(),
            stage_id: &AtomicUsize::new(1),
            nb_builder: &CardinalityBasedNetworkBoundaryBuilder,
        };

        let annotated = _inject_network_boundaries(plan_w_broadcast, None, &network_boundaries_ctx)
            .await
            .expect("failed to annotate plan");
        debug_annotated(&annotated, 0, &network_boundaries_ctx)
    }

    fn debug_annotated(
        plan: &Arc<dyn ExecutionPlan>,
        indent: usize,
        ctx: &InjectNetworkBoundaryContext,
    ) -> String {
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
