use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::{DistributedConfig, TaskCountAnnotation, TaskEstimator};
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
#[derive(Debug, PartialEq)]
pub(super) enum RequiredNetworkBoundary {
    Shuffle,
    Coalesce,
}

/// Wraps an [ExecutionPlan] and annotates it with information about how many distributed tasks
/// it should run on, and whether it needs a network boundary below or not.
pub(super) struct AnnotatedPlan {
    /// The annotated [ExecutionPlan].
    pub(super) plan: Arc<dyn ExecutionPlan>,
    /// The annotated children of this [ExecutionPlan]. This will always hold the same nodes as
    /// `self.plan.children()` but annotated.
    pub(super) children: Vec<AnnotatedPlan>,

    // annotation fields
    /// How many distributed tasks this plan should run on.
    pub(super) task_count: TaskCountAnnotation,
    /// Whether this [ExecutionPlan] needs a network boundary below it or not. Even if this is set
    /// to `Some()`, a later step can still decide to not place the network boundary under certain
    /// situations, like if both sides of the boundary have a task count equal to 1.
    pub(super) required_network_boundary: Option<RequiredNetworkBoundary>,
}

impl Debug for AnnotatedPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn fmt_dbg(f: &mut Formatter<'_>, plan: &AnnotatedPlan, depth: usize) -> std::fmt::Result {
            write!(
                f,
                "{}{}: task_count={:?}",
                " ".repeat(depth * 2),
                plan.plan.name(),
                plan.task_count
            )?;
            if let Some(nb) = &plan.required_network_boundary {
                write!(f, ", required_network_boundary={nb:?}")?;
            }
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
///    that case, the node is annotated with a [RequiredNetworkBoundary] value. At this point, all
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
/// ┌────────────────────┐ task_count: Maximum(1) (because we try to coalesce all partitions into 1)
/// │ CoalescePartitions │ network_boundary: Some(Coalesce)
/// └──────────▲─────────┘
///            │
/// ┌──────────┴─────────┐ task_count: Desired(3) (inherited from the child)
/// │     Projection     │ network_boundary: None
/// └──────────▲─────────┘
///            │
/// ┌──────────┴─────────┐ task_count: Desired(3) (as this node requires a network boundary below,
/// │    Aggregation     │    and the stage below reduces the cardinality of the data because of the
/// │       (final)      │    partial aggregation, we can choose a smaller amount of tasks)
/// └──────────▲─────────┘ network_boundary: Some(Shuffle) (because the child is a repartition)
///            │
/// ┌──────────┴─────────┐ task_count: Desired(4) (inherited from the child)
/// │    Repartition     │ network_boundary: None
/// └──────────▲─────────┘
///            │
/// ┌──────────┴─────────┐ task_count: Desired(4) (inherited from the child)
/// │    Aggregation     │ network_boundary: None
/// │     (partial)      │
/// └──────────▲─────────┘
///            │
/// ┌──────────┴─────────┐ task_count: Desired(4) (this was set by a TaskEstimator implementation)
/// │   DataSourceExec   │ network_boundary: None
/// └────────────────────┘
/// ```
///
/// ```
pub(super) fn annotate_plan(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<AnnotatedPlan, DataFusionError> {
    _annotate_plan(plan, cfg, true)
}
fn _annotate_plan(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
    root: bool,
) -> Result<AnnotatedPlan, DataFusionError> {
    use TaskCountAnnotation::*;
    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    let estimator = &d_cfg.__private_task_estimator;
    let n_workers = d_cfg.__private_worker_resolver.0.get_urls()?.len().max(1);

    let annotated_children = plan
        .children()
        .iter()
        .map(|child| _annotate_plan(Arc::clone(child), cfg, false))
        .collect::<Result<Vec<_>, _>>()?;

    if plan.children().is_empty() {
        // This is a leaf node, maybe a DataSourceExec, or maybe something else custom from the
        // user. We need to estimate how many tasks are needed for this leaf node, and we'll take
        // this decision into account when deciding how many tasks will be actually used.
        if let Some(estimate) = estimator.task_estimation(&plan, cfg) {
            return Ok(AnnotatedPlan {
                plan,
                children: Vec::new(),
                task_count: estimate.task_count.limit(n_workers),
                required_network_boundary: None,
            });
        } else {
            // We could not determine how many tasks this leaf node should run on, so
            // assume it cannot be distributed and used just 1 task.
            return Ok(AnnotatedPlan {
                plan,
                children: Vec::new(),
                task_count: Maximum(1),
                required_network_boundary: None,
            });
        }
    }

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
    {
        // We cannot distribute CollectLeft HashJoinExec nodes yet. Once
        // https://github.com/datafusion-contrib/datafusion-distributed/pull/229 lands,
        // we can remove this check.
        task_count = Maximum(1);
    } else {
        // The task count for this plan is decided by the biggest task count from the children; unless
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

    // The plan does not need a NetworkBoundary, so just take the biggest task count from
    // the children and annotate the plan with that.
    let mut annotated_plan = AnnotatedPlan {
        required_network_boundary: required_network_boundary_below(plan.as_ref()),
        children: annotated_children,
        task_count,
        plan,
    };

    // The plan needs a NetworkBoundary. At this point we have all the info we need for choosing
    // the right size for the stage below, so what we need to do is take the calculated final
    // task count and propagate to all the children that will eventually be part of the stage.
    fn propagate_task_count(
        plan: &mut AnnotatedPlan,
        task_count: &TaskCountAnnotation,
        d_cfg: &DistributedConfig,
    ) -> Result<(), DataFusionError> {
        plan.task_count = task_count.clone();
        if plan.required_network_boundary.is_some() {
            // nothing to propagate here, all the nodes below the network boundary were already
            // assigned a task count, we do not want to overwrite it.
        } else if d_cfg.children_isolator_unions && plan.plan.as_any().is::<UnionExec>() {
            // Propagating through ChildrenIsolatorUnionExec is not that easy, each child will
            // be executed in its own task, and therefore, they will act as if they were in executing
            // in a non-distributed context. The ChildrenIsolatorUnionExec itself will make sure to
            // determine which children to run and which to exclude depending on the task index in
            // which it's running.
            let c_i_union = ChildrenIsolatorUnionExec::from_children_and_task_counts(
                plan.children.iter().map(|v| v.plan.clone()),
                plan.children.iter().map(|v| v.task_count.as_usize()),
                task_count.as_usize(),
            )?;
            for children_and_tasks in c_i_union.task_idx_map.iter() {
                for (child_i, task_ctx) in children_and_tasks {
                    if let Some(child) = plan.children.get_mut(*child_i) {
                        propagate_task_count(child, &Maximum(task_ctx.task_count), d_cfg)?
                    };
                }
            }
            plan.plan = Arc::new(c_i_union);
        } else {
            for child in &mut plan.children {
                propagate_task_count(child, task_count, d_cfg)?;
            }
        }
        Ok(())
    }

    if let Some(nb) = &annotated_plan.required_network_boundary {
        // The plan is a network boundary, so everything below it belongs to the same stage. This
        // means that we need to propagate the task count to all the nodes in that stage.
        for annotated_child in annotated_plan.children.iter_mut() {
            propagate_task_count(annotated_child, &annotated_plan.task_count, d_cfg)?;
        }

        // If the current plan that needs a NetworkBoundary boundary below is either a
        // CoalescePartitionsExec or a SortPreservingMergeExec, then we are sure that all the stage
        // that they are going to be part of needs to run in exactly one task.
        if nb == &RequiredNetworkBoundary::Coalesce {
            annotated_plan.task_count = Maximum(1);
            return Ok(annotated_plan);
        }

        // From now and up in the plan, a new task count needs to be calculated for the next stage.
        // Depending on the number of nodes that reduce/increase cardinality, the task count will be
        // calculated based on the previous task count multiplied by a factor.
        fn calculate_scale_factor(plan: &AnnotatedPlan, f: f64) -> f64 {
            let mut sf = None;

            if plan.required_network_boundary.is_none() {
                for plan in plan.children.iter() {
                    sf = match sf {
                        None => Some(calculate_scale_factor(plan, f)),
                        Some(sf) => Some(sf.max(calculate_scale_factor(plan, f))),
                    }
                }
            }

            let sf = sf.unwrap_or(1.0);
            match plan.plan.cardinality_effect() {
                CardinalityEffect::LowerEqual => sf / f,
                CardinalityEffect::GreaterEqual => sf * f,
                _ => sf,
            }
        }
        let sf = calculate_scale_factor(
            annotated_plan.children.first().ok_or_else(|| {
                plan_datafusion_err!("missing child in a plan annotated with a network boundary")
            })?,
            d_cfg.cardinality_task_count_factor,
        );
        let prev_task_count = annotated_plan.task_count.as_usize() as f64;
        annotated_plan.task_count = Desired((prev_task_count * sf).ceil() as usize);
        Ok(annotated_plan)
    } else if root {
        // If this is the root node, it means that we have just finished annotating nodes for the
        // subplan belonging to the head stage, so propagate the task count to all children.
        let task_count = annotated_plan.task_count.clone();
        propagate_task_count(&mut annotated_plan, &task_count, d_cfg)?;
        Ok(annotated_plan)
    } else {
        // If this is not the root node, and it's also not a network boundary, then we don't need
        // to do anything else.
        Ok(annotated_plan)
    }
}

/// Returns if the [ExecutionPlan] requires a network boundary below it, and if it does, the kind
/// of network boundary ([RequiredNetworkBoundary]).
fn required_network_boundary_below(parent: &dyn ExecutionPlan) -> Option<RequiredNetworkBoundary> {
    let children = parent.children();
    let first_child = children.first()?;

    if let Some(r_exec) = first_child.as_any().downcast_ref::<RepartitionExec>()
        && matches!(r_exec.partitioning(), Partitioning::Hash(_, _))
    {
        return Some(RequiredNetworkBoundary::Shuffle);
    }
    if parent.as_any().is::<CoalescePartitionsExec>()
        || parent.as_any().is::<SortPreservingMergeExec>()
    {
        // If the next node is a leaf node, distributing this is going to be a bit wasteful, so
        // we don't want to do it.
        if first_child.children().is_empty() {
            return None;
        }
        return Some(RequiredNetworkBoundary::Coalesce);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::test_utils::parquet::register_parquet_tables;
    use crate::{DistributedExt, TaskEstimation, assert_snapshot};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use itertools::Itertools;
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
          SortPreservingMergeExec: task_count=Maximum(1), required_network_boundary=Coalesce
            SortExec: task_count=Desired(2)
              ProjectionExec: task_count=Desired(2)
                AggregateExec: task_count=Desired(2)
                  CoalesceBatchesExec: task_count=Desired(2), required_network_boundary=Shuffle
                    RepartitionExec: task_count=Desired(3)
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
        CoalesceBatchesExec: task_count=Maximum(1)
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
        CoalesceBatchesExec: task_count=Maximum(1)
          HashJoinExec: task_count=Maximum(1)
            CoalescePartitionsExec: task_count=Maximum(1), required_network_boundary=Coalesce
              ProjectionExec: task_count=Desired(2)
                AggregateExec: task_count=Desired(2)
                  CoalesceBatchesExec: task_count=Desired(2), required_network_boundary=Shuffle
                    RepartitionExec: task_count=Desired(3)
                      AggregateExec: task_count=Desired(3)
                        CoalesceBatchesExec: task_count=Desired(3)
                          FilterExec: task_count=Desired(3)
                            RepartitionExec: task_count=Desired(3)
                              DataSourceExec: task_count=Desired(3)
            ProjectionExec: task_count=Maximum(1)
              AggregateExec: task_count=Maximum(1)
                CoalesceBatchesExec: task_count=Maximum(1), required_network_boundary=Shuffle
                  RepartitionExec: task_count=Desired(3)
                    AggregateExec: task_count=Desired(3)
                      CoalesceBatchesExec: task_count=Desired(3)
                        FilterExec: task_count=Desired(3)
                          RepartitionExec: task_count=Desired(3)
                            DataSourceExec: task_count=Desired(3)
        ")
    }

    #[tokio::test]
    async fn test_inner_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a INNER JOIN weather b ON a."RainToday" = b."RainToday"
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        CoalesceBatchesExec: task_count=Maximum(1)
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
          CoalesceBatchesExec: task_count=Desired(2), required_network_boundary=Shuffle
            RepartitionExec: task_count=Desired(3)
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
          CoalesceBatchesExec: task_count=Maximum(2)
            FilterExec: task_count=Maximum(2)
              RepartitionExec: task_count=Maximum(2)
                DataSourceExec: task_count=Maximum(2)
          ProjectionExec: task_count=Maximum(2)
            CoalesceBatchesExec: task_count=Maximum(2)
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
        CoalesceBatchesExec: task_count=Desired(3)
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
              CoalesceBatchesExec: task_count=Desired(3), required_network_boundary=Shuffle
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
          CoalesceBatchesExec: task_count=Maximum(1)
            FilterExec: task_count=Maximum(1)
              RepartitionExec: task_count=Maximum(1)
                DataSourceExec: task_count=Maximum(1)
          ProjectionExec: task_count=Maximum(1)
            CoalesceBatchesExec: task_count=Maximum(1)
              FilterExec: task_count=Maximum(1)
                RepartitionExec: task_count=Maximum(1)
                  DataSourceExec: task_count=Maximum(1)
          ProjectionExec: task_count=Maximum(2)
            CoalesceBatchesExec: task_count=Maximum(2)
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
          CoalesceBatchesExec: task_count=Desired(1), required_network_boundary=Shuffle
            RepartitionExec: task_count=Maximum(1)
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
          CoalesceBatchesExec: task_count=Maximum(1)
            FilterExec: task_count=Maximum(1)
              RepartitionExec: task_count=Maximum(1)
                DataSourceExec: task_count=Maximum(1)
          ProjectionExec: task_count=Maximum(1)
            CoalesceBatchesExec: task_count=Maximum(1)
              FilterExec: task_count=Maximum(1)
                RepartitionExec: task_count=Maximum(1)
                  DataSourceExec: task_count=Maximum(1)
        ")
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
        sql_to_annotated_with_options(query, move |b| b).await
    }

    async fn sql_to_annotated_with_estimator<T: ExecutionPlan + Send + Sync + 'static>(
        query: &str,
        estimator: impl Fn(&T) -> Option<TaskEstimation> + Send + Sync + 'static,
    ) -> String {
        sql_to_annotated_with_options(query, move |b| {
            b.with_distributed_task_estimator(CallbackEstimator::new(estimator))
        })
        .await
    }

    async fn sql_to_annotated_with_options(
        query: &str,
        f: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder,
    ) -> String {
        let config = SessionConfig::new()
            .with_target_partitions(4)
            .with_information_schema(true);

        let state = f(SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(4)))
        .build();

        let ctx = SessionContext::new_with_state(state);
        let mut queries = query.split(";").collect_vec();
        let last_query = queries.pop().unwrap();

        for query in queries {
            ctx.sql(query).await.unwrap();
        }

        register_parquet_tables(&ctx).await.unwrap();

        let df = ctx.sql(last_query).await.unwrap();

        let annotated = annotate_plan(
            df.create_physical_plan().await.unwrap(),
            ctx.state_ref().read().config_options().as_ref(),
        )
        .expect("failed to annotate plan");
        format!("{annotated:?}")
    }
}
