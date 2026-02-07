use crate::distributed_planner::children_isolator_union_split::children_isolator_union_split;
use crate::distributed_planner::statistics::{
    ComputeCostClass, calculate_compute_cost, calculate_row_stats,
};
use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::{BroadcastExec, DistributedConfig, DistributedPlannerExtension};
use datafusion::common::{DataFusionError, plan_datafusion_err, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use itertools::Itertools;
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

const NETWORK_BOUNDARY_COST_CLASS: ComputeCostClass = ComputeCostClass::S;

/// Wraps an [ExecutionPlan] and annotates it with information about how many distributed tasks
/// it should run on, and whether it needs a network boundary below or not.
pub(super) struct AnnotatedPlan {
    /// The annotated [ExecutionPlan].
    pub(super) plan_or_nb: PlanOrNetworkBoundary,
    /// The annotated children of this [ExecutionPlan]. This will always hold the same nodes as
    /// `self.plan.children()` but annotated.
    pub(super) children: Vec<AnnotatedPlan>,

    // annotation fields
    /// This node can only run in exactly 1 task.
    pub(super) task_count: Option<usize>,

    /// Determines how compute intensive this plan is.
    pub(super) cost_class: ComputeCostClass,

    /// The maximum amount of tasks in which this node is allowed to run.
    pub(super) max_task_count_restriction: Option<usize>,

    /// Stats about how many rows will this node return.
    pub(super) stats: Statistics,
}

impl AnnotatedPlan {
    fn cost(&self) -> Result<usize, DataFusionError> {
        let mut bytes_to_compute = 0;
        if self.children.is_empty() {
            bytes_to_compute = *self.stats.total_byte_size.get_value().unwrap_or(&0);
        } else {
            for input_child in &self.children {
                bytes_to_compute += *input_child.stats.total_byte_size.get_value().unwrap_or(&0);
            }
        }

        Ok((self.cost_class.factor() * bytes_to_compute as f64) as usize)
    }

    pub(super) fn cost_aggregated_until_network_boundary(&self) -> Result<usize, DataFusionError> {
        let mut accumulated_compute_cost = self.cost()?;
        for input_child in &self.children {
            accumulated_compute_cost += if input_child.plan_or_nb.is_network_boundary() {
                input_child.cost()?
            } else {
                input_child.cost_aggregated_until_network_boundary()?
            }
        }
        Ok(accumulated_compute_cost)
    }

    pub(super) fn task_count(&self) -> Result<usize, DataFusionError> {
        self.task_count.ok_or_else(|| {
            plan_datafusion_err!(
                "AnnotatedPlan {:?} does not have a task count assigned",
                self.plan_or_nb
            )
        })
    }

    // The plan needs a NetworkBoundary. At this point we have all the info we need for choosing
    // the right size for the stage below, so what we need to do is take the calculated final
    // task count and propagate to all the children that will eventually be part of the stage.
    fn propagate_task_count_until_network_boundary(
        &mut self,
        task_count: usize,
        d_cfg: &DistributedConfig,
    ) -> Result<(), DataFusionError> {
        self.task_count = Some(task_count);
        let plan = match &self.plan_or_nb {
            // If it's a normal plan, continue with the propagation.
            PlanOrNetworkBoundary::Plan(plan) => plan,
            // This is a network boundary.
            //
            // Nothing to propagate here, all the nodes below the network boundary were already
            // assigned a task count, we do not want to overwrite it.
            PlanOrNetworkBoundary::Broadcast => return Ok(()),
            PlanOrNetworkBoundary::Shuffle => return Ok(()),
            PlanOrNetworkBoundary::Coalesce => return Ok(()),
        };

        if d_cfg.children_isolator_unions && plan.as_any().is::<UnionExec>() {
            // Propagating through ChildrenIsolatorUnionExec is not that easy, each child will
            // be executed in its own task, and therefore, they will act as if they were in executing
            // in a non-distributed context. The ChildrenIsolatorUnionExec itself will make sure to
            // determine which children to run and which to exclude depending on the task index in
            // which it's running.
            let task_idx_map = children_isolator_union_split(&self.children, task_count)?;

            for children_and_tasks in &task_idx_map {
                for (child_i, task_ctx) in children_and_tasks {
                    let Some(child) = self.children.get_mut(*child_i) else {
                        return plan_err!(
                            "Error propagating task count from ChildrenIsolatorUnionExec: {child_i} index out of range for {} children.",
                            children_and_tasks.len()
                        );
                    };

                    child.propagate_task_count_until_network_boundary(task_ctx.task_count, d_cfg)?
                }
            }
            let c_i_union = ChildrenIsolatorUnionExec::from_children_and_task_counts(
                plan.children().into_iter().cloned().collect(),
                task_idx_map,
            )?;
            self.plan_or_nb = PlanOrNetworkBoundary::Plan(Arc::new(c_i_union));
        } else {
            for child in &mut self.children {
                child.propagate_task_count_until_network_boundary(task_count, d_cfg)?;
            }
        }
        Ok(())
    }
}

impl Debug for AnnotatedPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn fmt_dbg(
            f: &mut Formatter<'_>,
            annotation: &AnnotatedPlan,
            depth: usize,
        ) -> std::fmt::Result {
            write!(
                f,
                "{}{:?}: task_count={:?} output_rows={:?}",
                " ".repeat(depth * 2),
                annotation.plan_or_nb,
                annotation.task_count,
                annotation.stats.num_rows.get_value().unwrap_or(&0),
            )?;
            if let PlanOrNetworkBoundary::Plan(plan) = &annotation.plan_or_nb {
                write!(f, " cost_class={:?}", calculate_compute_cost(plan))?;
                write!(
                    f,
                    " accumulated_cost={:?}",
                    annotation
                        .cost_aggregated_until_network_boundary()
                        .unwrap_or(0)
                )?;
                let output_bytes = annotation.stats.total_byte_size.get_value().unwrap_or(&0);
                write!(f, " output_bytes={output_bytes:?}")?;
            }
            writeln!(f)?;
            for child in annotation.children.iter() {
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
/// The plans are annotated in a bottom-up manner, starting with the leaf nodes all the way
/// to the head of the plan:
///
/// 1. Leaf nodes have the opportunity to declare some statistics about how much data is going to
///    be pulled by them, along with other statistics like the number of distinct values per column.
///
/// 2. The function recurses up propagating statistics, until a network boundary is reached.
///
/// 3. Upon reaching a network boundary, the function looks at the nodes below and calculates an
///    estimation of the compute cost of the plan below. The compute estimation is based on the
///    amount of rows that are expected to flow through it, the estimated size of each row, and how
///    compute hungry each node is.
///
/// 4. Based on the total aggregated compute cost for the plan below, a specific number of tasks
///    is assigned to all those nodes, as they all will run in the same stage.
///
/// 5. This process is repeated recursively until all nodes are annotated.
///
/// ## Example:
///
/// Following the process above, an annotated plan will look like this:
///
/// TODO: create a graphical example
pub(super) fn annotate_plan(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<AnnotatedPlan, DataFusionError> {
    let mut annotation = _annotate_plan(plan, None, cfg)?;
    let d_cfg = DistributedConfig::from_config_options(cfg)?;

    let stage_below_task_count = if let Some(n) = annotation.max_task_count_restriction {
        n
    } else {
        let compute_cost = annotation.cost_aggregated_until_network_boundary()?;
        cost_based_task_count(compute_cost, cfg, d_cfg)?
    };
    // This is the root node, it means that we have just finished annotating nodes for the
    // subplan belonging to the head stage, so propagate the task count to all children.
    annotation.propagate_task_count_until_network_boundary(stage_below_task_count, d_cfg)?;
    Ok(annotation)
}

fn _annotate_plan(
    plan: Arc<dyn ExecutionPlan>,
    parent: Option<&Arc<dyn ExecutionPlan>>,
    cfg: &ConfigOptions,
) -> Result<AnnotatedPlan, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    let broadcast_joins = d_cfg.broadcast_joins;
    let estimator = &d_cfg.__private_distributed_planner_extension;

    let annotated_children = plan
        .children()
        .iter()
        .map(|child| _annotate_plan(Arc::clone(child), Some(&plan), cfg))
        .collect::<Result<Vec<_>, _>>()?;

    let cost_class = d_cfg
        .__private_distributed_planner_extension
        .compute_cost(&plan, cfg)
        .unwrap_or_else(|| calculate_compute_cost(&plan));

    let mut max_task_count_restriction = estimator.max_tasks(&plan, cfg);
    if annotated_children.is_empty() {
        // This is a leaf node, maybe a DataSourceExec, or maybe something else custom from the
        // user. We need to estimate how many tasks are needed for this leaf node, and we'll take
        // this decision into account when deciding how many tasks will be actually used.
        // We could not determine how many tasks this leaf node should run on, so
        // assume it cannot be distributed and use just 1 task.
        return Ok(match calculate_row_stats(&plan, &[], d_cfg) {
            Ok(output_row_stats) => AnnotatedPlan {
                stats: output_row_stats,
                plan_or_nb: PlanOrNetworkBoundary::Plan(plan),
                children: Vec::new(),

                max_task_count_restriction,
                cost_class,
                task_count: None,
            },
            Err(_) => {
                // We know nothing about this plan. It has no row statistics, so we cannot assume
                // that is fine to distribute it.
                AnnotatedPlan {
                    stats: Statistics::new_unknown(&plan.schema()),
                    plan_or_nb: PlanOrNetworkBoundary::Plan(plan),
                    children: Vec::new(),

                    max_task_count_restriction: Some(1),
                    cost_class,
                    task_count: None,
                }
            }
        });
    }

    if d_cfg.children_isolator_unions && plan.as_any().is::<UnionExec>() {
        // A UNION might have some children declaring a max_task_count_restriction, but if it's
        // going to be converted into a ChildrenIsolatorUnionExec, it might be able to satisfy
        // those restrictions while still distributing the stage, so we can skip the restriction
        // propagation.
    } else if let Some(node) = plan.as_any().downcast_ref::<HashJoinExec>()
        && node.mode == PartitionMode::CollectLeft
        && !broadcast_joins
    {
        // Only distribute CollectLeft HashJoins after we broadcast more intelligently or when it
        // is explicitly enabled.
        max_task_count_restriction = Some(1)
    } else {
        // The task count for this plan is decided by the biggest task count from the children; unless
        // a child specifies a maximum task count, in that case, the maximum is respected. Some
        // nodes can only run in one task. If there is a subplan with a single node declaring that
        // it can only run in one task, all the rest of the nodes in the stage need to respect it.
        for annotated_child in annotated_children.iter() {
            max_task_count_restriction = match (
                max_task_count_restriction,
                annotated_child.max_task_count_restriction,
            ) {
                (None, None) => None,
                (Some(max), None) => Some(max),
                (None, Some(max)) => Some(max),
                (Some(max_1), Some(max_2)) => Some(max_1.min(max_2)),
            };
        }
    }

    let input_row_stats = annotated_children.iter().map(|v| &v.stats).collect_vec();
    let mut annotation = AnnotatedPlan {
        plan_or_nb: PlanOrNetworkBoundary::Plan(Arc::clone(&plan)),
        stats: calculate_row_stats(&plan, &input_row_stats, d_cfg)?,
        children: annotated_children,

        max_task_count_restriction,
        cost_class,
        task_count: None,
    };

    // Upon reaching a hash repartition, we need to introduce a shuffle right above it.
    if let Some(r_exec) = plan.as_any().downcast_ref::<RepartitionExec>() {
        if matches!(r_exec.partitioning(), Partitioning::Hash(_, _)) {
            annotation = AnnotatedPlan {
                plan_or_nb: PlanOrNetworkBoundary::Shuffle,
                stats: annotation.stats.clone(),
                children: vec![annotation],

                max_task_count_restriction: None,
                cost_class: NETWORK_BOUNDARY_COST_CLASS,
                task_count: None,
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
                stats: annotation.stats.clone(),
                children: vec![annotation],

                max_task_count_restriction: None,
                cost_class: NETWORK_BOUNDARY_COST_CLASS,
                task_count: None,
            };
        } else {
            annotation = AnnotatedPlan {
                plan_or_nb: PlanOrNetworkBoundary::Coalesce,
                stats: annotation.stats.clone(),
                children: vec![annotation],

                max_task_count_restriction: None,
                cost_class: NETWORK_BOUNDARY_COST_CLASS,
                task_count: None,
            };
        }
    }

    if annotation.plan_or_nb.is_network_boundary() {
        let Some(input) = annotation.children.first_mut() else {
            return plan_err!("Found a network boundary without  input");
        };
        let stage_below_task_count = if let Some(n) = input.max_task_count_restriction {
            n
        } else {
            let compute_cost = input.cost_aggregated_until_network_boundary()?;
            cost_based_task_count(compute_cost, cfg, d_cfg)?
        };

        // The plan is a network boundary, so everything below belongs to the same stage. This
        // means that we need to propagate the task count to all the nodes in that stage.
        input.propagate_task_count_until_network_boundary(stage_below_task_count, d_cfg)?;

        // If the current plan that needs a NetworkBoundary boundary below is either a
        // CoalescePartitionsExec or a SortPreservingMergeExec, then we are sure that all the stage
        // that they are going to be part of needs to run in exactly one task.
        if matches!(annotation.plan_or_nb, PlanOrNetworkBoundary::Coalesce) {
            annotation.max_task_count_restriction = Some(1);
        }

        Ok(annotation)
    } else {
        // If this is not the root node, and it's also not a network boundary, then we don't need
        // to do anything else.
        Ok(annotation)
    }
}

fn cost_based_task_count(
    cost: usize,
    cfg: &ConfigOptions,
    d_cfg: &DistributedConfig,
) -> Result<usize, DataFusionError> {
    if cost == 0 {
        return Ok(1);
    }

    let partitions_needed =
        (cost as f64 / d_cfg.bytes_processed_per_partition as f64).ceil() as usize;
    let partitions_available = cfg.execution.target_partitions;
    let workers_needed = (partitions_needed as f64 / partitions_available as f64).ceil() as usize;
    let workers_available = d_cfg.__private_worker_resolver.0.get_urls()?.len().max(1);
    Ok(std::cmp::min(workers_needed, workers_available))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_planner::insert_broadcast::insert_broadcast_execs;
    use crate::test_utils::plans::{
        BuildSideOneDistributedPlannerExtension, TestPlanOptions, base_session_builder,
        context_with_query, sql_to_physical_plan,
    };
    use crate::{DistributedExt, DistributedPlannerExtension, assert_snapshot};
    use datafusion::config::ConfigOptions;
    use datafusion::execution::SessionStateBuilder;
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
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @"DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=95892 output_bytes=95892")
    }

    #[tokio::test]
    async fn test_aggregation() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        ProjectionExec: task_count=Some(1) output_rows=366 cost_class=XS accumulated_cost=15554 output_bytes=6222
          SortPreservingMergeExec: task_count=Some(1) output_rows=366 cost_class=L accumulated_cost=12443 output_bytes=6222
            [NetworkBoundary] Coalesce: task_count=Some(1) output_rows=366
              SortExec: task_count=Some(4) output_rows=366 cost_class=XL accumulated_cost=25509 output_bytes=6222
                ProjectionExec: task_count=Some(4) output_rows=366 cost_class=XS accumulated_cost=15554 output_bytes=6222
                  AggregateExec: task_count=Some(4) output_rows=366 cost_class=L accumulated_cost=12443 output_bytes=6222
                    [NetworkBoundary] Shuffle: task_count=Some(4) output_rows=366
                      RepartitionExec: task_count=Some(4) output_rows=366 cost_class=S accumulated_cost=18665 output_bytes=6222
                        AggregateExec: task_count=Some(4) output_rows=366 cost_class=L accumulated_cost=14310 output_bytes=6222
                          DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=6222 output_bytes=6222
        ")
    }

    #[tokio::test]
    async fn test_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday"
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Some(1) output_rows=366 cost_class=XL accumulated_cost=52337 output_bytes=6588
          CoalescePartitionsExec: task_count=Some(1) output_rows=366 cost_class=XXS accumulated_cost=12370 output_bytes=9516
            DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
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
        HashJoinExec: task_count=Some(1) output_rows=74 cost_class=XL accumulated_cost=21089 output_bytes=1332
          CoalescePartitionsExec: task_count=Some(1) output_rows=74 cost_class=XXS accumulated_cost=3147 output_bytes=3148
            [NetworkBoundary] Coalesce: task_count=Some(1) output_rows=74
              ProjectionExec: task_count=Some(2) output_rows=74 cost_class=XS accumulated_cost=7869 output_bytes=3148
                AggregateExec: task_count=Some(2) output_rows=74 cost_class=L accumulated_cost=6295 output_bytes=3148
                  [NetworkBoundary] Shuffle: task_count=Some(2) output_rows=74
                    RepartitionExec: task_count=Some(4) output_rows=74 cost_class=S accumulated_cost=45640 output_bytes=3148
                      AggregateExec: task_count=Some(4) output_rows=74 cost_class=L accumulated_cost=43437 output_bytes=3148
                        FilterExec: task_count=Some(4) output_rows=74 cost_class=M accumulated_cost=39345 output_bytes=3148
                          RepartitionExec: task_count=Some(4) output_rows=366 cost_class=XS accumulated_cost=23607 output_bytes=15738
                            DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=15738 output_bytes=15738
          ProjectionExec: task_count=Some(1) output_rows=74 cost_class=XS accumulated_cost=7869 output_bytes=3148
            AggregateExec: task_count=Some(1) output_rows=74 cost_class=L accumulated_cost=6295 output_bytes=3148
              [NetworkBoundary] Shuffle: task_count=Some(1) output_rows=74
                RepartitionExec: task_count=Some(4) output_rows=74 cost_class=S accumulated_cost=45640 output_bytes=3148
                  AggregateExec: task_count=Some(4) output_rows=74 cost_class=L accumulated_cost=43437 output_bytes=3148
                    FilterExec: task_count=Some(4) output_rows=74 cost_class=M accumulated_cost=39345 output_bytes=3148
                      RepartitionExec: task_count=Some(4) output_rows=366 cost_class=XS accumulated_cost=23607 output_bytes=15738
                        DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=15738 output_bytes=15738
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
        HashJoinExec: task_count=Some(1) output_rows=366 cost_class=XL accumulated_cost=52337 output_bytes=6588
          CoalescePartitionsExec: task_count=Some(1) output_rows=366 cost_class=XXS accumulated_cost=12370 output_bytes=9516
            DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
        ")
    }

    #[tokio::test]
    async fn test_distinct() {
        let query = r#"
        SELECT DISTINCT "RainToday" FROM weather
        "#;
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        AggregateExec: task_count=Some(4) output_rows=366 cost_class=L accumulated_cost=12443 output_bytes=6222
          [NetworkBoundary] Shuffle: task_count=Some(4) output_rows=366
            RepartitionExec: task_count=Some(4) output_rows=366 cost_class=S accumulated_cost=18665 output_bytes=6222
              AggregateExec: task_count=Some(4) output_rows=366 cost_class=L accumulated_cost=14310 output_bytes=6222
                DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=6222 output_bytes=6222
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
        ChildrenIsolatorUnionExec: task_count=Some(4) output_rows=148 cost_class=M accumulated_cost=48532 output_bytes=2496
          FilterExec: task_count=Some(2) output_rows=74 cost_class=M accumulated_cost=23790 output_bytes=1904
            RepartitionExec: task_count=Some(2) output_rows=366 cost_class=XS accumulated_cost=14274 output_bytes=9516
              DataSourceExec: task_count=Some(2) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          ProjectionExec: task_count=Some(2) output_rows=74 cost_class=XS accumulated_cost=24742 output_bytes=592
            FilterExec: task_count=Some(2) output_rows=74 cost_class=M accumulated_cost=23790 output_bytes=1904
              RepartitionExec: task_count=Some(2) output_rows=366 cost_class=XS accumulated_cost=14274 output_bytes=9516
                DataSourceExec: task_count=Some(2) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
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
        FilterExec: task_count=Some(4) output_rows=74 cost_class=M accumulated_cost=32025 output_bytes=2562
          RepartitionExec: task_count=Some(4) output_rows=366 cost_class=XS accumulated_cost=19215 output_bytes=12810
            DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=12810 output_bytes=12810
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
        ProjectionExec: task_count=Some(4) output_rows=366 cost_class=XS accumulated_cost=43516 output_bytes=5856
          BoundedWindowAggExec: task_count=Some(4) output_rows=366 cost_class=XL accumulated_cost=37111 output_bytes=12810
            SortExec: task_count=Some(4) output_rows=366 cost_class=XL accumulated_cost=21886 output_bytes=9516
              [NetworkBoundary] Shuffle: task_count=Some(4) output_rows=366
                RepartitionExec: task_count=Some(4) output_rows=366 cost_class=S accumulated_cost=16177 output_bytes=9516
                  DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
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
        let annotated = sql_to_annotated(query).await;
        assert_snapshot!(annotated, @r"
        ChildrenIsolatorUnionExec: task_count=Some(4) output_rows=222 cost_class=M accumulated_cost=73274 output_bytes=3088
          FilterExec: task_count=Some(1) output_rows=74 cost_class=M accumulated_cost=23790 output_bytes=1904
            RepartitionExec: task_count=Some(1) output_rows=366 cost_class=XS accumulated_cost=14274 output_bytes=9516
              DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          ProjectionExec: task_count=Some(1) output_rows=74 cost_class=XS accumulated_cost=24742 output_bytes=592
            FilterExec: task_count=Some(1) output_rows=74 cost_class=M accumulated_cost=23790 output_bytes=1904
              RepartitionExec: task_count=Some(1) output_rows=366 cost_class=XS accumulated_cost=14274 output_bytes=9516
                DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          ProjectionExec: task_count=Some(2) output_rows=74 cost_class=XS accumulated_cost=24742 output_bytes=592
            FilterExec: task_count=Some(2) output_rows=74 cost_class=M accumulated_cost=23790 output_bytes=1904
              RepartitionExec: task_count=Some(2) output_rows=366 cost_class=XS accumulated_cost=14274 output_bytes=9516
                DataSourceExec: task_count=Some(2) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
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
        HashJoinExec: task_count=Some(4) output_rows=366 cost_class=XL accumulated_cost=49482 output_bytes=6588
          CoalescePartitionsExec: task_count=Some(4) output_rows=366 cost_class=XXS accumulated_cost=9515 output_bytes=9516
            [NetworkBoundary] Broadcast: task_count=Some(4) output_rows=366
              BroadcastExec: task_count=Some(3) output_rows=366 cost_class=Zero accumulated_cost=9516 output_bytes=9516
                DataSourceExec: task_count=Some(3) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
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
        HashJoinExec: task_count=Some(4) output_rows=366 cost_class=XL accumulated_cost=49482 output_bytes=6588
          CoalescePartitionsExec: task_count=Some(4) output_rows=366 cost_class=XXS accumulated_cost=9515 output_bytes=9516
            [NetworkBoundary] Broadcast: task_count=Some(4) output_rows=366
              BroadcastExec: task_count=Some(4) output_rows=366 cost_class=Zero accumulated_cost=9516 output_bytes=9516
                DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
        ");
    }

    #[tokio::test]
    async fn test_broadcast_one_to_many() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let annotated = sql_to_annotated_broadcast_with_estimator(
            query,
            3,
            BuildSideOneDistributedPlannerExtension,
        )
        .await;
        assert_snapshot!(annotated, @r"
        HashJoinExec: task_count=Some(3) output_rows=366 cost_class=XL accumulated_cost=49482 output_bytes=6588
          CoalescePartitionsExec: task_count=Some(3) output_rows=366 cost_class=XXS accumulated_cost=9515 output_bytes=9516
            [NetworkBoundary] Broadcast: task_count=Some(3) output_rows=366
              BroadcastExec: task_count=Some(1) output_rows=366 cost_class=Zero accumulated_cost=9516 output_bytes=9516
                DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          DataSourceExec: task_count=Some(3) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
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
        HashJoinExec: task_count=Some(1) output_rows=366 cost_class=XL accumulated_cost=49482 output_bytes=6588
          CoalescePartitionsExec: task_count=Some(1) output_rows=366 cost_class=XXS accumulated_cost=9515 output_bytes=9516
            [NetworkBoundary] Broadcast: task_count=Some(1) output_rows=366
              BroadcastExec: task_count=Some(3) output_rows=366 cost_class=Zero accumulated_cost=9516 output_bytes=9516
                DataSourceExec: task_count=Some(3) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
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
        HashJoinExec: task_count=Some(1) output_rows=366 cost_class=XL accumulated_cost=52337 output_bytes=6588
          CoalescePartitionsExec: task_count=Some(1) output_rows=366 cost_class=XXS accumulated_cost=12370 output_bytes=9516
            DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
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
        HashJoinExec: task_count=Some(4) output_rows=366 cost_class=XL accumulated_cost=58047 output_bytes=9882
          CoalescePartitionsExec: task_count=Some(4) output_rows=366 cost_class=XXS accumulated_cost=12810 output_bytes=12810
            [NetworkBoundary] Broadcast: task_count=Some(4) output_rows=366
              BroadcastExec: task_count=Some(4) output_rows=366 cost_class=Zero accumulated_cost=49482 output_bytes=12810
                HashJoinExec: task_count=Some(4) output_rows=366 cost_class=XL accumulated_cost=49482 output_bytes=12810
                  CoalescePartitionsExec: task_count=Some(4) output_rows=366 cost_class=XXS accumulated_cost=9515 output_bytes=9516
                    [NetworkBoundary] Broadcast: task_count=Some(4) output_rows=366
                      BroadcastExec: task_count=Some(3) output_rows=366 cost_class=Zero accumulated_cost=9516 output_bytes=9516
                        DataSourceExec: task_count=Some(3) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
                  DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          DataSourceExec: task_count=Some(4) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
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
        ChildrenIsolatorUnionExec: task_count=Some(4) output_rows=1098 cost_class=M accumulated_cost=148446 output_bytes=19764
          HashJoinExec: task_count=Some(1) output_rows=366 cost_class=XL accumulated_cost=49482 output_bytes=6588
            CoalescePartitionsExec: task_count=Some(1) output_rows=366 cost_class=XXS accumulated_cost=9515 output_bytes=9516
              [NetworkBoundary] Broadcast: task_count=Some(1) output_rows=366
                BroadcastExec: task_count=Some(3) output_rows=366 cost_class=Zero accumulated_cost=9516 output_bytes=9516
                  DataSourceExec: task_count=Some(3) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
            DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          HashJoinExec: task_count=Some(1) output_rows=366 cost_class=XL accumulated_cost=49482 output_bytes=6588
            CoalescePartitionsExec: task_count=Some(1) output_rows=366 cost_class=XXS accumulated_cost=9515 output_bytes=9516
              [NetworkBoundary] Broadcast: task_count=Some(1) output_rows=366
                BroadcastExec: task_count=Some(3) output_rows=366 cost_class=Zero accumulated_cost=9516 output_bytes=9516
                  DataSourceExec: task_count=Some(3) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
            DataSourceExec: task_count=Some(1) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
          HashJoinExec: task_count=Some(2) output_rows=366 cost_class=XL accumulated_cost=49482 output_bytes=6588
            CoalescePartitionsExec: task_count=Some(2) output_rows=366 cost_class=XXS accumulated_cost=9515 output_bytes=9516
              [NetworkBoundary] Broadcast: task_count=Some(2) output_rows=366
                BroadcastExec: task_count=Some(3) output_rows=366 cost_class=Zero accumulated_cost=9516 output_bytes=9516
                  DataSourceExec: task_count=Some(3) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
            DataSourceExec: task_count=Some(2) output_rows=366 cost_class=M accumulated_cost=9516 output_bytes=9516
        ");
    }

    #[derive(Debug)]
    struct BroadcastBuildCoalesceMaxEstimator;

    impl DistributedPlannerExtension for BroadcastBuildCoalesceMaxEstimator {
        fn max_tasks(&self, plan: &Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Option<usize> {
            let coalesce = plan.as_any().downcast_ref::<CoalescePartitionsExec>()?;
            if coalesce.input().as_any().is::<BroadcastExec>() {
                Some(1)
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

    async fn sql_to_annotated_broadcast_with_estimator(
        query: &str,
        num_workers: usize,
        estimator: impl DistributedPlannerExtension + Send + Sync + 'static,
    ) -> String {
        let options = TestPlanOptions {
            target_partitions: 4,
            num_workers,
            broadcast_enabled: true,
        };
        annotate_test_plan(query, options, |b| {
            b.with_distributed_planner_extension(estimator)
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
        let (mut ctx, query) = context_with_query(builder, query).await;
        ctx.set_distributed_bytes_processed_per_partition(1000)
            .unwrap();
        let df = ctx.sql(&query).await.unwrap();
        let mut plan = df.create_physical_plan().await.unwrap();

        plan = insert_broadcast_execs(plan, ctx.state_ref().read().config_options().as_ref())
            .expect("failed to insert broadcasts");

        let annotated = annotate_plan(plan, ctx.state_ref().read().config_options().as_ref())
            .expect("failed to annotate plan");
        format!("{annotated:?}")
    }
}
