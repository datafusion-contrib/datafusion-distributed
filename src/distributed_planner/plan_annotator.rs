use crate::TaskCountAnnotation::{Desired, Maximum};
use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::{BroadcastExec, DistributedConfig, TaskCountAnnotation, TaskEstimator};
use datafusion::common::{DataFusionError, plan_datafusion_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Annotation attached to a single [ExecutionPlan] that determines the kind of network boundary
/// needed just below itself.
pub(super) enum PlanOrNetworkBoundary {
    Plan(Arc<dyn ExecutionPlan>),
    Shuffle,
    Coalesce,
    Broadcast,
}

impl Debug for PlanOrNetworkBoundary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plan(plan) => write!(f, "{}", plan.name()),
            Self::Shuffle => write!(f, "[NetworkBoundary] Shuffle"),
            Self::Coalesce => write!(f, "[NetworkBoundary] Coalesce"),
            Self::Broadcast => write!(f, "[NetworkBoundary] Broadcast"),
        }
    }
}

impl PlanOrNetworkBoundary {
    fn is_network_boundary(&self) -> bool {
        matches!(self, Self::Shuffle | Self::Coalesce | Self::Broadcast)
    }
}

/// Wraps an [ExecutionPlan] and annotates it with information about how many distributed tasks
/// it should run on, and whether it needs a network boundary below or not.
pub(super) struct AnnotatedPlan {
    /// The annotated [ExecutionPlan].
    pub(super) plan_or_nb: PlanOrNetworkBoundary,
    /// The annotated children of this [ExecutionPlan]. This will always hold the same nodes as
    /// `self.plan.children()` but annotated.
    pub(super) children: Vec<AnnotatedPlan>,

    // annotation fields
    /// How many distributed tasks this plan should run on.
    pub(super) task_count: TaskCountAnnotation,
}

impl Debug for AnnotatedPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn fmt_dbg(f: &mut Formatter<'_>, plan: &AnnotatedPlan, depth: usize) -> std::fmt::Result {
            write!(
                f,
                "{}{:?}: task_count={:?}",
                " ".repeat(depth * 2),
                plan.plan_or_nb,
                plan.task_count
            )?;
            writeln!(f)?;
            for child in plan.children.iter() {
                fmt_dbg(f, child, depth + 1)?;
            }
            Ok(())
        }

        fmt_dbg(f, self, 0)
    }
}

/// Annotates recursively an [ExecutionPlan] and its children with information about how many
/// distributed tasks it should run on, and whether it needs a network boundary below it or not.
///
/// This is the first step of the distribution process, where the plan structure is still left
/// untouched and the existing nodes are just annotated for future steps to perform the distribution.
///
/// The plans are annotated in a bottom-to-top manner, starting with the leaf nodes all the way
/// to the head of the plan:
///
/// 1. Leaf nodes have the opportunity to provide an estimation of how many distributed tasks should
///    be used for the whole stage that will execute them.
///
/// 2. If a stage contains multiple leaf nodes, and all provide a task count estimation, the
///    biggest is taken.
///
/// 3. When traversing the plan in a bottom-to-top fashion, this function looks for nodes that
///    either increase or reduce cardinality:
///     - If there's a node that increases cardinality, the next stage will spawn more tasks than
///       the current one.
///     - If there's a node that reduces cardinality, the next stage will spawn fewer tasks than the
///       current one.
///
/// 4. At a certain point, the function will reach a node that needs a network boundary below; in
///    that case, the node is annotated with a [PlanOrNetworkBoundary] value. At this point, all
///    the nodes below must reach a consensus about the final task count for the stage below the
///    network boundary.
///
/// 5. This process is repeated recursively until all nodes are annotated.
///
/// ## Example:
///
/// Following the process above, an annotated plan will look like this:
///
/// ```text
///
///                                                                                           ┌──────────────────────┐ task_count: Desired(3) (inherited from child)
///                                                                                           │   CoalesceBatches    │ network_boundary: None
///                                                                                           └───────────▲──────────┘
///                                                                                                       │
///                                                                                           ┌───────────┴──────────┐
///                                                                                           │       HashJoin       │ task_count: Desired(3) (inherits from probe child)
///                                                                                           │ (CollectLeft Inner)  │ network_boundary: None
///                                                                                           └────▲────────────▲────┘
///                                                                                                │            │
///                                                                                      ┌─────────┘            └──────────┐
///                                                                                 Build Side                        Probe Side
///                                                                                      │                                 │
///                                                                          ┌───────────┴──────────┐          ┌───────────┴──────────┐ task_count: Desired(3) (inherited from child)
///  task_count: Desired(3) (inherits from the probe side)                   │  CoalescePartitions  │          │      Projection      │ network_boundary: None
///  network_boundary: Broadcast                                             └───────────▲──────────┘          └───────────▲──────────┘
///                                                                                      │                                 │
///                                                                          ┌───────────┴──────────┐          ┌───────────┴──────────┐ task_count: Desired(3) (as this node requires a network boundary below,
///  task_count: Desired(2) (inherited from child)                           │    BroadcastExec     │          │     Aggregation      │ and the stage below reduces the cardinality of the data because of the
///  network_boundary: None                                                  └───────────▲──────────┘          │       (Final)        │ partial aggregation, we can choose a smaller amount of tasks)
///                                                                                      │                     └───────────▲──────────┘ network_boundary: Some(Shuffle) (because the child is a repartition)
///  task_count: Desired(2) (this was set by a TaskEstimator implementation) ┌───────────┴──────────┐                      │
///  network_boundary: None                                                  │      DataSource      │          ┌───────────┴──────────┐ task_count: Desired(4) (inherited from child)
///                                                                          └──────────────────────┘          │     Repartition      │ network_boundary: None
///                                                                                                            └───────────▲──────────┘
///                                                                                                                        │
///                                                                                                            ┌───────────┴──────────┐
///                                                                                                            │     Aggregation      │ task_count: Desired(4) (inherited from child)
///                                                                                                            │      (Partial)       │ network_boundary: None
///                                                                                                            └───────────▲──────────┘
///                                                                                                                        │
///                                                                                                            ┌───────────┴──────────┐ task_count: Desired(4) (this was set by a TaskEstimator implementation)
///                                                                                                            │      DataSource      │ network_boundary: None
///                                                                                                            └──────────────────────┘
/// ```
///
/// ```
pub(super) fn annotate_plan(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<AnnotatedPlan, DataFusionError> {
    _annotate_plan(plan, None, cfg, true)
}

fn _annotate_plan(
    plan: Arc<dyn ExecutionPlan>,
    parent: Option<&Arc<dyn ExecutionPlan>>,
    cfg: &ConfigOptions,
    root: bool,
) -> Result<AnnotatedPlan, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    let broadcast_joins = d_cfg.broadcast_joins;
    let estimator = &d_cfg.__private_task_estimator;
    let n_workers = d_cfg.__private_worker_resolver.0.get_urls()?.len().max(1);

    let annotated_children = plan
        .children()
        .iter()
        .map(|child| _annotate_plan(Arc::clone(child), Some(&plan), cfg, false))
        .collect::<Result<Vec<_>, _>>()?;

    if plan.children().is_empty() {
        // This is a leaf node, maybe a DataSourceExec, or maybe something else custom from the
        // user. We need to estimate how many tasks are needed for this leaf node, and we'll take
        // this decision into account when deciding how many tasks will be actually used.
        return if let Some(estimate) = estimator.task_estimation(&plan, cfg) {
            Ok(AnnotatedPlan {
                plan_or_nb: PlanOrNetworkBoundary::Plan(plan),
                children: Vec::new(),
                task_count: estimate.task_count.limit(n_workers),
            })
        } else {
            // We could not determine how many tasks this leaf node should run on, so
            // assume it cannot be distributed and use just 1 task.
            Ok(AnnotatedPlan {
                plan_or_nb: PlanOrNetworkBoundary::Plan(plan),
                children: Vec::new(),
                task_count: Maximum(1),
            })
        };
    }

    // Detect broadcast on the build side so we can keep build/probe stages decoupled.
    let has_broadcast_child = plan
        .children()
        .first()
        .is_some_and(|child| child.as_any().is::<BroadcastExec>());
    let mut task_count = estimator
        .task_estimation(&plan, cfg)
        .map_or(Desired(1), |v| v.task_count);
    if d_cfg.children_isolator_unions && plan.as_any().is::<UnionExec>() {
        // Unions have the chance to decide how many tasks they should run on. If there's a union
        // with a bunch of children, the user might want to increase parallelism and increase the
        // task count for the stage running that.
        let mut count = 0;
        for annotated_child in annotated_children.iter() {
            count += annotated_child.task_count.as_usize();
        }
        task_count = Desired(count);
    } else if let Some(node) = plan.as_any().downcast_ref::<HashJoinExec>()
        && node.mode == PartitionMode::CollectLeft
        && !broadcast_joins
    {
        // Only distriubte CollectLeft HashJoins after we broadcast more intelligently or when it
        // is explicitly enabled.
        task_count = Maximum(1);
    } else if !has_broadcast_child {
        // For Shuffle/Coalesce we inherit parallelism from children in the same stage.
        // For Broadcast we intentionally skip this so the build-side task count does not
        // drive the probe-side stage size.
        // The task count for this plan is decided by the biggest task count from the children unless
        // a child specifies a maximum task count, in that case, the maximum is respected. Some
        // nodes can only run in one task. If there is a subplan with a single node declaring that
        // it can only run in one task, all the rest of the nodes in the stage need to respect it.
        for annotated_child in annotated_children.iter() {
            task_count = match (task_count, &annotated_child.task_count) {
                (Desired(desired), Desired(child)) => Desired(desired.max(*child)),
                (Maximum(max), Desired(_)) => Maximum(max),
                (Desired(_), Maximum(max)) => Maximum(*max),
                (Maximum(max_1), Maximum(max_2)) => Maximum(max_1.min(*max_2)),
            };
        }
    }

    task_count = task_count.limit(n_workers);

    // Wrap the node with a boundary node if the parent marks it.
    let mut annotation = AnnotatedPlan {
        plan_or_nb: PlanOrNetworkBoundary::Plan(Arc::clone(&plan)),
        children: annotated_children,
        task_count: task_count.clone(),
    };

    // Upon reaching a hash repartition, we need to introduce a shuffle right above it.
    if let Some(r_exec) = plan.as_any().downcast_ref::<RepartitionExec>() {
        if matches!(r_exec.partitioning(), Partitioning::Hash(_, _)) {
            annotation = AnnotatedPlan {
                plan_or_nb: PlanOrNetworkBoundary::Shuffle,
                children: vec![annotation],
                task_count,
            };
        }
    } else if let Some(parent) = parent
        // If this node is a leaf node, putting a network boundary above is a bit wasteful, so
        // we don't want to do it.
        && !plan.children().is_empty()
        // If the parent is trying to coalesce all partitions into one, we need to introduce
        // a network coalesce right below it (or in other words, above the current node)
        && (parent.as_any().is::<CoalescePartitionsExec>()
            || parent.as_any().is::<SortPreservingMergeExec>())
    {
        // A BroadcastExec underneath a coalesce parent means the build side will cross stages.
        if plan.as_any().is::<BroadcastExec>() {
            annotation = AnnotatedPlan {
                plan_or_nb: PlanOrNetworkBoundary::Broadcast,
                children: vec![annotation],
                task_count,
            };
        } else {
            annotation = AnnotatedPlan {
                plan_or_nb: PlanOrNetworkBoundary::Coalesce,
                children: vec![annotation],
                task_count,
            };
        }
    }

    // The plan needs a NetworkBoundary. At this point we have all the info we need for choosing
    // the right size for the stage below, so what we need to do is take the calculated final
    // task count and propagate to all the children that will eventually be part of the stage.
    fn propagate_task_count(
        annotation: &mut AnnotatedPlan,
        task_count: &TaskCountAnnotation,
        d_cfg: &DistributedConfig,
    ) -> Result<(), DataFusionError> {
        annotation.task_count = task_count.clone();
        match &annotation.plan_or_nb {
            PlanOrNetworkBoundary::Plan(plan) => {
                if d_cfg.children_isolator_unions && plan.as_any().is::<UnionExec>() {
                    // Propagating through ChildrenIsolatorUnionExec is not that easy, each child will
                    // be executed in its own task, and therefore, they will act as if they were in executing
                    // in a non-distributed context. The ChildrenIsolatorUnionExec itself will make sure to
                    // determine which children to run and which to exclude depending on the task index in
                    // which it's running.
                    let c_i_union = ChildrenIsolatorUnionExec::from_children_and_task_counts(
                        plan.children().into_iter().cloned(),
                        annotation.children.iter().map(|v| v.task_count.as_usize()),
                        task_count.as_usize(),
                    )?;
                    for children_and_tasks in c_i_union.task_idx_map.iter() {
                        for (child_i, task_ctx) in children_and_tasks {
                            if let Some(child) = annotation.children.get_mut(*child_i) {
                                propagate_task_count(child, &Maximum(task_ctx.task_count), d_cfg)?
                            };
                        }
                    }
                    annotation.plan_or_nb = PlanOrNetworkBoundary::Plan(Arc::new(c_i_union));
                } else {
                    for child in &mut annotation.children {
                        propagate_task_count(child, task_count, d_cfg)?;
                    }
                }
                Ok(())
            }
            PlanOrNetworkBoundary::Broadcast => {
                // Broadcast is a stage split only propagate a Maximum cap into the build stage.
                if let Maximum(max) = task_count {
                    for child in &mut annotation.children {
                        let child_task_count = child.task_count.clone().limit(*max);
                        propagate_task_count(child, &child_task_count, d_cfg)?;
                    }
                }
                Ok(())
            }
            _ => {
                // This is a network boundary.
                //
                // Nothing to propagate here, all the nodes below the network boundary were already
                // assigned a task count, we do not want to overwrite it.
                Ok(())
            }
        }
    }

    if annotation.plan_or_nb.is_network_boundary() {
        // The plan is a network boundary, so everything below it belongs to the same stage. This
        // means that we need to propagate the task count to all the nodes in that stage.
        if matches!(annotation.plan_or_nb, PlanOrNetworkBoundary::Broadcast) {
            // Broadcast splits build/probe stages only cap the build stage if the probe is Maximum.
            if let Maximum(max) = annotation.task_count {
                for annotated_child in annotation.children.iter_mut() {
                    let child_task_count = annotated_child.task_count.clone().limit(max);
                    propagate_task_count(annotated_child, &child_task_count, d_cfg)?;
                }
            }
            return Ok(annotation);
        }

        for annotated_child in annotation.children.iter_mut() {
            propagate_task_count(annotated_child, &annotation.task_count, d_cfg)?;
        }

        // If the current plan that needs a NetworkBoundary boundary below is either a
        // CoalescePartitionsExec or a SortPreservingMergeExec, then we are sure that all the stage
        // that they are going to be part of needs to run in exactly one task.
        if matches!(annotation.plan_or_nb, PlanOrNetworkBoundary::Coalesce) {
            annotation.task_count = Maximum(1);
            return Ok(annotation);
        }

        // From now and up in the plan, a new task count needs to be calculated for the next stage.
        // Depending on the number of nodes that reduce/increase cardinality, the task count will be
        // calculated based on the previous task count multiplied by a factor.
        fn calculate_scale_factor(annotation: &AnnotatedPlan, f: f64) -> f64 {
            let PlanOrNetworkBoundary::Plan(plan) = &annotation.plan_or_nb else {
                return 1.0;
            };

            let mut sf = None;
            for plan in &annotation.children {
                sf = match sf {
                    None => Some(calculate_scale_factor(plan, f)),
                    Some(sf) => Some(sf.max(calculate_scale_factor(plan, f))),
                }
            }

            let sf = sf.unwrap_or(1.0);
            match plan.cardinality_effect() {
                CardinalityEffect::LowerEqual => sf / f,
                CardinalityEffect::GreaterEqual => sf * f,
                _ => sf,
            }
        }
        let sf = calculate_scale_factor(
            annotation.children.first().ok_or_else(|| {
                plan_datafusion_err!("missing child in a plan annotated with a network boundary")
            })?,
            d_cfg.cardinality_task_count_factor,
        );
        let prev_task_count = annotation.task_count.as_usize() as f64;
        annotation.task_count = Desired((prev_task_count * sf).ceil() as usize);
        Ok(annotation)
    } else if root {
        // If this is the root node, it means that we have just finished annotating nodes for the
        // subplan belonging to the head stage, so propagate the task count to all children.
        let task_count = annotation.task_count.clone();
        propagate_task_count(&mut annotation, &task_count, d_cfg)?;
        Ok(annotation)
    } else {
        // If this is not the root node, and it's also not a network boundary, then we don't need
        // to do anything else.
        Ok(annotation)
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
            [NetworkBoundary] Coalesce: task_count=Maximum(1)
              SortExec: task_count=Desired(2)
                ProjectionExec: task_count=Desired(2)
                  AggregateExec: task_count=Desired(2)
                    [NetworkBoundary] Shuffle: task_count=Desired(2)
                      RepartitionExec: task_count=Desired(3)
                        AggregateExec: task_count=Desired(3)
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
            [NetworkBoundary] Coalesce: task_count=Maximum(1)
              ProjectionExec: task_count=Desired(2)
                AggregateExec: task_count=Desired(2)
                  [NetworkBoundary] Shuffle: task_count=Desired(2)
                    RepartitionExec: task_count=Desired(3)
                      AggregateExec: task_count=Desired(3)
                        FilterExec: task_count=Desired(3)
                          RepartitionExec: task_count=Desired(3)
                            DataSourceExec: task_count=Desired(3)
          ProjectionExec: task_count=Maximum(1)
            AggregateExec: task_count=Maximum(1)
              [NetworkBoundary] Shuffle: task_count=Maximum(1)
                RepartitionExec: task_count=Desired(3)
                  AggregateExec: task_count=Desired(3)
                    FilterExec: task_count=Desired(3)
                      RepartitionExec: task_count=Desired(3)
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
          [NetworkBoundary] Shuffle: task_count=Desired(2)
            RepartitionExec: task_count=Desired(3)
              AggregateExec: task_count=Desired(3)
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
              DataSourceExec: task_count=Maximum(2)
          ProjectionExec: task_count=Maximum(2)
            FilterExec: task_count=Maximum(2)
              RepartitionExec: task_count=Maximum(2)
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
              [NetworkBoundary] Shuffle: task_count=Desired(3)
                RepartitionExec: task_count=Desired(3)
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
          [NetworkBoundary] Shuffle: task_count=Desired(1)
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
            [NetworkBoundary] Broadcast: task_count=Desired(3)
              BroadcastExec: task_count=Desired(3)
                DataSourceExec: task_count=Desired(3)
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
            [NetworkBoundary] Broadcast: task_count=Desired(3)
              BroadcastExec: task_count=Desired(3)
                DataSourceExec: task_count=Desired(3)
          DataSourceExec: task_count=Desired(3)
        ");
    }

    #[tokio::test]
    async fn test_broadcast_one_to_many_annotation() {
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
            [NetworkBoundary] Broadcast: task_count=Desired(3)
              BroadcastExec: task_count=Maximum(1)
                DataSourceExec: task_count=Maximum(1)
          DataSourceExec: task_count=Desired(3)
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
            [NetworkBoundary] Broadcast: task_count=Desired(3)
              BroadcastExec: task_count=Desired(3)
                HashJoinExec: task_count=Desired(3)
                  CoalescePartitionsExec: task_count=Desired(1)
                    [NetworkBoundary] Broadcast: task_count=Desired(3)
                      BroadcastExec: task_count=Desired(3)
                        DataSourceExec: task_count=Desired(3)
                  DataSourceExec: task_count=Desired(3)
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
              [NetworkBoundary] Broadcast: task_count=Maximum(1)
                BroadcastExec: task_count=Desired(1)
                  DataSourceExec: task_count=Desired(1)
            DataSourceExec: task_count=Maximum(1)
          HashJoinExec: task_count=Maximum(1)
            CoalescePartitionsExec: task_count=Maximum(1)
              [NetworkBoundary] Broadcast: task_count=Maximum(1)
                BroadcastExec: task_count=Desired(1)
                  DataSourceExec: task_count=Desired(1)
            DataSourceExec: task_count=Maximum(1)
          HashJoinExec: task_count=Maximum(2)
            CoalescePartitionsExec: task_count=Maximum(2)
              [NetworkBoundary] Broadcast: task_count=Maximum(2)
                BroadcastExec: task_count=Desired(2)
                  DataSourceExec: task_count=Desired(2)
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

        plan = insert_broadcast_execs(plan, ctx.state_ref().read().config_options().as_ref())
            .expect("failed to insert broadcasts");

        let annotated = annotate_plan(plan, ctx.state_ref().read().config_options().as_ref())
            .expect("failed to annotate plan");
        format!("{annotated:?}")
    }
}
