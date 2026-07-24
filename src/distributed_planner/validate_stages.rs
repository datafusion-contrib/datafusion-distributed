//! Validates the fundamental invariant of stage replication: for every stage,
//! executing its plan once per task over the per-task input assignment and unioning the
//! outputs must be equivalent to executing it once over all the data.
//!
//! DataFusion's optimizer discharges each operator's `required_input_distribution()`
//! obligations *process-locally* (a `SinglePartition` requirement is satisfied by a
//! `CoalescePartitionsExec`, a `HashPartitioned` one by a `RepartitionExec`). Replicating the
//! plan across tasks silently reinterprets "all the data" as "this task's slice", invalidating
//! those discharged obligations. Only the exchange operators — the `Network*Exec` boundaries —
//! restore properties globally:
//!
//! - `NetworkShuffleExec` → globally hash-partitioned (equal keys co-locate cluster-wide)
//! - `NetworkBroadcastExec` → a complete, identical copy in every task
//! - `NetworkCoalesceExec` → a global single partition, via a single consumer task
//!
//! The validator classifies every stage-local subtree bottom-up as either [Replicated]
//! (every task materializes the identical complete dataset) or [Partitioned] (every task
//! materializes a task-specific slice whose union is the whole), and enforces two obligations
//! along the way:
//!
//! (A) every declared input-distribution requirement must hold *cluster-globally*:
//!     `SinglePartition` may only be satisfied by a replicated subtree, and
//!     `HashPartitioned` only by a claim that (1) semantically satisfies the required
//!     expressions — checked with [Partitioning::satisfaction] against the child's claimed
//!     output partitioning and equivalence classes, exactly as DataFusion's own
//!     EnforceDistribution/SanityCheckPlan do process-locally — and (2) is globally true,
//!     i.e. established by a global exchange rather than a stage-local repartition;
//! (B) replicated data may only feed operators that never *emit* rows driven by it —
//!     N task instances would emit such rows N times, and the machinery that deduplicates
//!     them in a single process (e.g. the hash join's shared visited bitmap) does not exist
//!     across tasks. This is the one fact not derivable from any DataFusion API; see
//!     [emits_rows_driven_by].
//!
//! Ordering is a known gap: output-ordering claims weaken from global to task-local exactly
//! like distribution claims, but this validator does not model them.

use std::sync::Arc;

use datafusion::common::{Result, plan_err};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{Distribution, ExecutionPlan, ExecutionPlanProperties};

use datafusion::config::ConfigOptions;
use datafusion::prelude::SessionConfig;

use crate::execution_plans::{ChildrenIsolatorUnionExec, DistributedLeafExec};
use crate::stage::{Stage, find_all_stages};
use crate::{NetworkBoundaryExt, NetworkBroadcastExec, NetworkShuffleExec};

use super::insert_broadcast::is_left_broadcast_safe;
use super::task_estimator::{CombinedTaskEstimator, TaskEstimator};

/// How a subtree's data is laid out across the tasks of its stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DataFlow {
    /// Every task materializes the identical, complete dataset.
    Replicated,
    /// Every task materializes a task-specific subset; the union across tasks is the whole
    /// dataset. `claims_are_global` records provenance: whether the partitioning this
    /// subtree *claims* in its `PlanProperties` (e.g. `Partitioning::Hash`) was established
    /// by a global exchange (or exchange-aligned storage) and merely preserved since — as
    /// opposed to being minted task-locally by a stage-local `RepartitionExec`, where the
    /// same claim only holds within each task's slice of the data.
    Partitioned { claims_are_global: bool },
}

/// Validates every stage embedded in a fully planned distributed plan.
pub(crate) fn validate_distributed_stages(
    plan: &Arc<dyn ExecutionPlan>,
    session_cfg: &SessionConfig,
) -> Result<()> {
    let estimator = CombinedTaskEstimator::from_session_config(session_cfg);
    for stage in find_all_stages(plan) {
        if let Stage::Local(stage) = stage {
            validate_stage_plan(
                &stage.plan,
                stage.tasks,
                estimator.as_ref(),
                session_cfg.options(),
            )?;
        }
    }
    Ok(())
}

/// Validates a single stage's plan against its task count. Single-task stages are trivially
/// correct: one instance sees all the data, exactly as DataFusion's optimizer assumed.
///
/// The `estimator` is the same [TaskEstimator] machinery the planner used to decide leaf task
/// counts; the validator consults it to tell task-varying leaves from replicated ones.
pub(crate) fn validate_stage_plan(
    plan: &Arc<dyn ExecutionPlan>,
    tasks: usize,
    estimator: &dyn TaskEstimator,
    cfg: &ConfigOptions,
) -> Result<()> {
    if tasks <= 1 {
        return Ok(());
    }
    match classify(plan, tasks, estimator, cfg)? {
        DataFlow::Partitioned { .. } => Ok(()),
        DataFlow::Replicated => plan_err!(
            "stage runs {tasks} tasks but its plan is fully replicated: every task would \
             produce the identical output and the stage would emit {tasks} copies of it"
        ),
    }
}

/// Classifies `node`'s output as [DataFlow::Replicated] or [DataFlow::Partitioned] and
/// enforces obligations (A) and (B) for its inputs. Recursion stops at network boundaries:
/// their subtrees belong to child stages, which are validated against their own task counts.
fn classify(
    node: &Arc<dyn ExecutionPlan>,
    tasks: usize,
    estimator: &dyn TaskEstimator,
    cfg: &ConfigOptions,
) -> Result<DataFlow> {
    // Exchange operators re-establish global properties.
    if node.is::<NetworkBroadcastExec>() {
        return Ok(DataFlow::Replicated);
    }
    if node.is::<NetworkShuffleExec>() {
        return Ok(DataFlow::Partitioned {
            claims_are_global: true,
        });
    }
    if node.is_network_boundary() {
        // NetworkCoalesceExec (or a future boundary type): gathers all partitions into a
        // single consumer task, so it must never appear in a multi-task stage.
        return plan_err!(
            "stage runs {tasks} tasks but contains {}, which requires a single-task \
             consumer stage",
            node.name()
        );
    }
    // A DistributedLeafExec resolves to a different slice of the underlying source in every
    // task. (Its per-task variants are children in the plan tree, so check before the leaf
    // case below.) If the scan preserves a hash partitioning from the storage layout
    // (hive-style pre-partitioned files with `preserve_file_partitions`), the per-task
    // slicing follows those same partitions, so equal keys still co-locate cluster-wide.
    if node.is::<DistributedLeafExec>() {
        let claims_are_global = matches!(node.output_partitioning(), Partitioning::Hash(..));
        return Ok(DataFlow::Partitioned { claims_are_global });
    }
    // A ChildrenIsolatorUnionExec divides the stage's tasks among its children: child `i`
    // executes only in the tasks its `task_idx_map` assigns to it. Each child subtree must
    // therefore be validated against its own effective task count, not the stage's. A child
    // allotted a single task behaves like a single-task stage — trivially correct, including
    // any NetworkCoalesceExec it contains.
    if let Some(union) = node.downcast_ref::<ChildrenIsolatorUnionExec>() {
        for (child_idx, child) in union.children.iter().enumerate() {
            let child_tasks = union
                .task_idx_map
                .iter()
                .filter(|entries| entries.iter().any(|(child, _)| *child == child_idx))
                .count();
            if child_tasks > 1
                && classify(child, child_tasks, estimator, cfg)? == DataFlow::Replicated
            {
                return plan_err!(
                    "input {child_idx} of {} is replicated but allotted {child_tasks} tasks; \
                     each task would emit an identical copy of its data",
                    node.name()
                );
            }
        }
        // Children occupy disjoint task allotments, so across the stage's tasks the union
        // emits each child's data exactly once.
        return Ok(DataFlow::Partitioned {
            claims_are_global: false,
        });
    }
    let children = node.children();
    if children.is_empty() {
        // A leaf that some TaskEstimator knows how to scale is task-varying: each task
        // executes it over its own slice or work assignment (this mirrors how
        // `inject_network_boundaries` decides leaf task counts, and covers custom sources
        // like work-unit-feed leaves). Any other leaf (in-memory table, literal values) is
        // embedded verbatim in every task's serialized plan: identical, complete data.
        // NOTE: a volatile leaf (e.g. one backed by a random or time-dependent source)
        // would break the replication assumption; nothing in the ExecutionPlan API exposes
        // that.
        return Ok(if estimator.task_estimation(node, cfg).is_some() {
            DataFlow::Partitioned {
                claims_are_global: matches!(node.output_partitioning(), Partitioning::Hash(..)),
            }
        } else {
            DataFlow::Replicated
        });
    }

    let child_flows = children
        .iter()
        .map(|child| classify(child, tasks, estimator, cfg))
        .collect::<Result<Vec<_>>>()?;

    // Obligation (A): declared input-distribution requirements must hold cluster-globally.
    let requirements = node.required_input_distribution();
    for (idx, ((child, flow), requirement)) in children
        .iter()
        .zip(&child_flows)
        .zip(&requirements)
        .enumerate()
    {
        match requirement {
            Distribution::UnspecifiedDistribution => {}
            Distribution::SinglePartition => {
                if let DataFlow::Partitioned { .. } = flow {
                    return plan_err!(
                        "{} requires a single partition from its input {idx} ({}), but in a \
                         {tasks}-task stage that input only delivers the task's own slice of \
                         the data; each task would silently compute on partial data. The \
                         input must be replicated (broadcast) or the stage must run in a \
                         single task",
                        node.name(),
                        child.name()
                    );
                }
            }
            Distribution::HashPartitioned(_) => {
                // `Replicated` inputs pass both checks below: every task computes over the
                // complete data, and whether the resulting duplication is legal is decided
                // where it mixes into partitioned flow, or at the stage root.
                if let DataFlow::Partitioned { claims_are_global } = flow {
                    // Semantic check, delegated to DataFusion: does the child's claimed
                    // output partitioning satisfy the required expressions? Equivalence
                    // classes make renamed keys pass; dropped or wrong keys fail. Subset
                    // satisfaction — partitioned on a subset of the required keys — still
                    // co-locates equal keys and is accepted, matching what
                    // EnforceDistribution accepts process-locally (e.g. an aggregate
                    // grouping on a superset of its input's join keys).
                    // DataFusion 54 does not export the type `satisfaction` returns, so a
                    // reference NotSatisfied value (an unknown partitioning can never
                    // satisfy a hash requirement) stands in for naming the variant.
                    let eq_properties = child.equivalence_properties();
                    let not_satisfied = Partitioning::UnknownPartitioning(2).satisfaction(
                        requirement,
                        eq_properties,
                        true,
                    );
                    let satisfaction =
                        child
                            .output_partitioning()
                            .satisfaction(requirement, eq_properties, true);
                    if satisfaction == not_satisfied {
                        return plan_err!(
                            "{} requires its input {idx} ({}) to be hash-partitioned on \
                             specific keys, but that input's claimed partitioning ({}) does \
                             not satisfy the requirement",
                            node.name(),
                            child.name(),
                            child.output_partitioning()
                        );
                    }
                    // Provenance check: the claim must be globally true. A stage-local
                    // RepartitionExec mints the same claim, but it runs once per task over
                    // only that task's slice, so equal keys living in different tasks would
                    // never meet.
                    if !claims_are_global {
                        return plan_err!(
                            "{} requires its input {idx} ({}) to be hash-partitioned, but \
                             in a {tasks}-task stage that partitioning was established \
                             task-locally: equal keys living in different tasks would never \
                             meet. The input must come through a NetworkShuffleExec",
                            node.name(),
                            child.name()
                        );
                    }
                }
            }
        }
    }

    // Partitioned consumers zip partition `i` of every hash-required input within a task,
    // so all such inputs must agree on partition count — otherwise equal keys land at
    // different partition indices and never meet.
    let mut hash_partitioned_counts = children
        .iter()
        .zip(&child_flows)
        .zip(&requirements)
        .enumerate()
        .filter(|(_, ((_, flow), requirement))| {
            matches!(requirement, Distribution::HashPartitioned(_))
                && matches!(flow, DataFlow::Partitioned { .. })
        })
        .map(|(idx, ((child, _), _))| (idx, child.output_partitioning().partition_count()));
    if let Some((first_idx, first_count)) = hash_partitioned_counts.next() {
        for (idx, count) in hash_partitioned_counts {
            if count != first_count {
                return plan_err!(
                    "{} requires co-partitioned inputs, but input {first_idx} claims \
                     {first_count} partitions while input {idx} claims {count}: equal keys \
                     would land at different partition indices and never meet",
                    node.name()
                );
            }
        }
    }

    let any_partitioned = child_flows
        .iter()
        .any(|flow| matches!(flow, DataFlow::Partitioned { .. }));
    if !any_partitioned {
        // A deterministic operator over exclusively replicated inputs produces replicated
        // output; legality is deferred to the consumer.
        return Ok(DataFlow::Replicated);
    }

    // Obligation (B): this node mixes replicated inputs into partitioned flow, so its output
    // rows must be driven exclusively by the partitioned inputs.
    for (idx, flow) in child_flows.iter().enumerate() {
        if matches!(flow, DataFlow::Replicated) && emits_rows_driven_by(node, idx) {
            return plan_err!(
                "{} emits rows driven by its replicated input {idx}; each of the stage's \
                 {tasks} task instances would emit those rows, duplicating them in the \
                 stage output",
                node.name()
            );
        }
    }

    // Provenance propagation. DataFusion's PlanProperties already carry each node's claimed
    // partitioning through the operators — remapped through equivalence classes on renames,
    // degraded when keys are projected away — so the *content* of the claim needs no
    // tracking here; consumers check it with [Partitioning::satisfy] above. The one thing
    // PlanProperties cannot express is whether a claim is globally true. A stage-local
    // RepartitionExec is the only stage-local operator that mints new claims, and it runs
    // once per task over only that task's slice, so its claims hold task-locally only.
    // Every other operator inherits its claim from its children, preserving provenance.
    let claims_are_global = !node.is::<RepartitionExec>()
        && child_flows.iter().all(|flow| match flow {
            DataFlow::Partitioned { claims_are_global } => *claims_are_global,
            DataFlow::Replicated => true,
        });
    Ok(DataFlow::Partitioned { claims_are_global })
}

/// The one fact about an operator that no DataFusion API exposes: does it emit output rows
/// *driven by* the data of its `child_idx` input (as opposed to merely probing it)? An input
/// may be replicated across task instances only when the answer is no.
///
/// Unknown operators default to `true`: a `UnionExec`, a limit, a window — anything that
/// forwards or produces rows from a replicated input — would duplicate them, so the
/// conservative answer is the correct default. New operators must opt in here explicitly.
fn emits_rows_driven_by(node: &Arc<dyn ExecutionPlan>, child_idx: usize) -> bool {
    if let Some(join) = node.downcast_ref::<HashJoinExec>() {
        return child_idx == 0 && !is_left_broadcast_safe(join.join_type());
    }
    if let Some(join) = node.downcast_ref::<NestedLoopJoinExec>() {
        return child_idx == 0 && !is_left_broadcast_safe(join.join_type());
    }
    if node.is::<CrossJoinExec>() {
        // Every output row pairs a build row with a probe row, so output is probe-driven:
        // with a partitioned probe side, each pair is produced exactly once.
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::plans::TestPlanBuilder;

    /// Plans `query` through the full distributed planner. Since [validate_distributed_stages]
    /// is wired into `create_physical_plan`, an invalid stage surfaces here as a planning
    /// error rather than as a returned plan. Also returns the session config so tests can
    /// re-run validation directly.
    async fn try_plan_distributed(
        query: &str,
        broadcast_joins: bool,
    ) -> (Result<Arc<dyn ExecutionPlan>>, SessionConfig) {
        let test_plan = TestPlanBuilder::new()
            .target_partitions(3)
            .num_workers(4)
            .distributed_planner(true)
            .broadcast_joins(broadcast_joins)
            .build()
            .await;
        let ctx = test_plan.get_ctx();
        let session_cfg = ctx.copied_config();
        let plan = match ctx.sql(query).await {
            Ok(df) => df.create_physical_plan().await,
            Err(err) => Err(err),
        };
        (plan, session_cfg)
    }

    #[tokio::test]
    async fn planner_rewrites_collect_left_join_to_partitioned() {
        // LEFT is not broadcast-safe, so `insert_broadcast_execs` never broadcasts its build
        // side — instead `normalize_collect_joins` rewrites the join to
        // PartitionMode::Partitioned, which distributes correctly through shuffles.
        let (plan, session_cfg) = try_plan_distributed(
            r#"SELECT a."MinTemp", b."MaxTemp"
               FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday""#,
            true,
        )
        .await;
        let plan = plan.expect("expected planning to succeed");
        validate_distributed_stages(&plan, &session_cfg).expect("expected validation to pass");
    }

    #[tokio::test]
    async fn planner_swaps_unbroadcast_nested_loop_left_join() {
        // A Left NLJ is not broadcast-safe either; `normalize_collect_joins` swaps its
        // inputs (Left becomes Right) so the emitting side becomes the partitioned probe
        // side and the build side can be broadcast as usual.
        let (plan, session_cfg) = try_plan_distributed(
            r#"SELECT a."MinTemp", b."MaxTemp"
               FROM weather a LEFT JOIN weather b ON a."MinTemp" < b."MaxTemp""#,
            true,
        )
        .await;
        let plan = plan.expect("expected planning to succeed");
        validate_distributed_stages(&plan, &session_cfg).expect("expected validation to pass");
    }

    #[test]
    fn rejects_collect_left_join_with_sliced_build_side() {
        // The planner's task-count gate prevents this shape from ever being produced, so
        // build it by hand to keep direct validator coverage: a CollectLeft join whose
        // collected build side is a task-varying leaf behind a plain CoalescePartitionsExec.
        // Each task would collect only its own slice of the build data.
        let leaf = || -> Arc<dyn ExecutionPlan> { Arc::new(EmptyExec::new(test_schema())) };
        let build: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(leaf()));
        let on = vec![(column("a", &build.schema()), column("a", &test_schema()))];
        let join: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                build,
                leaf(),
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );
        let err = validate(&join).expect_err("expected validation to fail");
        assert!(
            err.to_string().contains("requires a single partition"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn planner_caps_cross_join_with_broadcast_disabled() {
        // Cross joins are always broadcast-safe, but with broadcast joins disabled there is
        // no broadcast at all; the task-count gate caps the join's stage to a single task,
        // producing a plan that validates cleanly.
        let (plan, session_cfg) = try_plan_distributed(
            r#"SELECT sum(a."MinTemp" + b."MaxTemp")
               FROM weather a CROSS JOIN weather b"#,
            false,
        )
        .await;
        let plan = plan.expect("expected planning to succeed");
        validate_distributed_stages(&plan, &session_cfg).expect("expected validation to pass");
    }

    #[tokio::test]
    async fn accepts_broadcast_inner_join() {
        // Inner is broadcast-safe: the build side arrives through a NetworkBroadcastExec
        // (replicated), the probe side is a sliced leaf (partitioned), and an inner join
        // only emits probe-driven rows.
        let (plan, session_cfg) = try_plan_distributed(
            r#"SELECT a."MinTemp", b."MaxTemp"
               FROM weather a INNER JOIN weather b ON a."RainToday" = b."RainToday""#,
            true,
        )
        .await;
        let plan = plan.expect("expected planning to succeed");
        validate_distributed_stages(&plan, &session_cfg).expect("expected validation to pass");
    }

    #[tokio::test]
    async fn accepts_plan_with_broadcast_disabled() {
        // With broadcast joins disabled the planner caps CollectLeft joins to a single task,
        // so whatever stages remain must validate cleanly.
        let (plan, session_cfg) = try_plan_distributed(
            r#"SELECT a."MinTemp", b."MaxTemp"
               FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday""#,
            false,
        )
        .await;
        let plan = plan.expect("expected planning to succeed");
        validate_distributed_stages(&plan, &session_cfg).expect("expected validation to pass");
    }

    // ---- Hand-built plans below: shapes the real planner never produces, exercising the
    // ---- claim-satisfaction, provenance, and co-partitioning checks directly. The stub
    // ---- estimator (`&4usize`) claims every childless leaf, making it task-varying.

    use crate::stage::LocalStage;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
    use datafusion::physical_plan::PlanProperties;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::projection::ProjectionExec;
    use uuid::Uuid;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]))
    }

    fn column(name: &str, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new_with_schema(name, schema).unwrap())
    }

    /// A stand-in for a real shuffle: a [NetworkShuffleExec] whose stage claims
    /// `Hash([key], partitions)`. Classified as globally partitioned by construction.
    fn fake_shuffle(key: &str, partitions: usize) -> Arc<dyn ExecutionPlan> {
        let schema = test_schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::Hash(vec![column(key, &schema)], partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(NetworkShuffleExec::from_stage(
            Stage::Local(LocalStage {
                query_id: Uuid::default(),
                num: 1,
                plan: Arc::new(EmptyExec::new(schema)),
                tasks: 4,
                metrics_set: Default::default(),
            }),
            properties,
        ))
    }

    /// A Partitioned hash join between `left` and `right` on `key` = `key`.
    fn partitioned_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        key: &str,
    ) -> Arc<dyn ExecutionPlan> {
        let on = vec![(column(key, &left.schema()), column(key, &right.schema()))];
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn validate(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        validate_stage_plan(plan, 4, &4usize, &ConfigOptions::default())
    }

    #[test]
    fn accepts_partitioned_join_over_matching_global_shuffles() {
        let join = partitioned_join(fake_shuffle("a", 4), fake_shuffle("a", 4), "a");
        validate(&join).expect("expected validation to pass");
    }

    #[test]
    fn rejects_partitioned_join_on_wrong_key() {
        // Both sides are globally partitioned — but the left one on `b`, while the join
        // requires partitioning on `a`. The provenance bit alone cannot see this; the
        // claim-satisfaction check must.
        let join = partitioned_join(fake_shuffle("b", 4), fake_shuffle("a", 4), "a");
        let err = validate(&join).expect_err("expected validation to fail");
        assert!(
            err.to_string().contains("does not satisfy"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_partitioned_join_over_stage_local_repartition() {
        // The repartition claims exactly the required Hash([a], 4) — the claim satisfies,
        // but it was minted inside the stage over the task's own slice of the leaf.
        let leaf: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(test_schema()));
        let hash = Partitioning::Hash(vec![column("a", &test_schema())], 4);
        let repartition: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(leaf, hash).unwrap());
        let join = partitioned_join(repartition, fake_shuffle("a", 4), "a");
        let err = validate(&join).expect_err("expected validation to fail");
        assert!(
            err.to_string().contains("established task-locally"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_partitioned_join_with_mismatched_partition_counts() {
        let join = partitioned_join(fake_shuffle("a", 4), fake_shuffle("a", 8), "a");
        let err = validate(&join).expect_err("expected validation to fail");
        assert!(
            err.to_string().contains("co-partitioned"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accepts_renamed_key_through_projection() {
        // Each side renames `a` to `x` above a shuffle hashed on `a`; the join keys
        // reference `x`. DataFusion remaps the claimed Hash([a]) through the projection's
        // equivalence mapping, so the satisfaction check passes under the new name.
        let renamed = || -> Arc<dyn ExecutionPlan> {
            let shuffle = fake_shuffle("a", 4);
            let exprs = vec![(column("a", &shuffle.schema()), "x".to_string())];
            Arc::new(ProjectionExec::try_new(exprs, shuffle).unwrap())
        };
        let join = partitioned_join(renamed(), renamed(), "x");
        validate(&join).expect("expected validation to pass");
    }
}
