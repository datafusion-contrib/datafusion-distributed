use crate::{DistributedConfig, TaskEstimator};
use datafusion::common::DataFusionError;
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(super) enum TaskCountAnnotation {
    Desired(usize),
    Maximum(usize),
}

impl TaskCountAnnotation {
    pub(super) fn as_usize(&self) -> usize {
        match self {
            Self::Desired(desired) => *desired,
            Self::Maximum(maximum) => *maximum,
        }
    }
}

pub(super) struct AnnotatedPlan {
    pub(super) plan: Arc<dyn ExecutionPlan>,
    pub(super) children: Vec<AnnotatedPlan>,
    // annotation fields
    pub(super) task_count: TaskCountAnnotation,
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

pub(super) fn annotate_plan(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<AnnotatedPlan, DataFusionError> {
    use TaskCountAnnotation::*;
    let d_cfg = DistributedConfig::from_config_options(cfg)?;

    let annotated_children = plan
        .children()
        .iter()
        .map(|child| annotate_plan(Arc::clone(child), cfg))
        .collect::<Result<Vec<_>, _>>()?;

    if plan.children().is_empty() {
        // This is a leaf node, maybe a DataSourceExec, or maybe something else custom from the
        // user. We need to estimate how many tasks are needed for this leaf node, and we'll take
        // this decision into account when deciding how many tasks will be actually used.
        let estimator = &d_cfg.__private_task_estimator;
        if let Some(estimate) = estimator.tasks_for_leaf_node(&plan, cfg) {
            return Ok(AnnotatedPlan {
                plan,
                children: Vec::new(),
                task_count: Desired(estimate.task_count),
                required_network_boundary: None,
            });
        } else {
            // We could not determine how many tasks this leaf node should run on, so
            // assume it cannot be distributed and used just 1 task.
            return Ok(AnnotatedPlan {
                plan,
                children: Vec::new(),
                task_count: Desired(1),
                required_network_boundary: None,
            });
        }
    }

    // The task count for this plan is decided by the biggest task count from the children; unless
    // a child specifies a maximum task count, in that case, the maximum is respected. Some
    // nodes can only run in one task. If there is a subplan with a single node declaring that
    // it can only run in one task, all the rest of the nodes in the stage need to respect it.
    let mut task_count = Desired(1);
    let n_workers = d_cfg.__private_channel_resolver.0.get_urls()?.len().max(1);
    for annotated_child in annotated_children.iter() {
        task_count = match (task_count, &annotated_child.task_count) {
            (Desired(desired), Desired(child)) => Desired(desired.max(*child).min(n_workers)),
            (Maximum(max), Desired(_)) => Maximum(max.min(n_workers)),
            (Desired(_), Maximum(max)) => Maximum((*max).min(n_workers)),
            (Maximum(max_1), Maximum(max_2)) => Maximum(max_1.min(*max_2).min(n_workers)),
        }
    }

    // We cannot distribute CollectLeft HashJoinExec nodes yet. Once
    // https://github.com/datafusion-contrib/datafusion-distributed/pull/229 lands,
    // we can remove this check.
    if let Some(node) = plan.as_any().downcast_ref::<HashJoinExec>() {
        if node.mode == PartitionMode::CollectLeft {
            task_count = Maximum(1);
        }
    }

    // The plan does not need a NetworkBoundary, so just take the biggest task count from
    // the children and annotate the plan with that.
    let mut annotated_plan = AnnotatedPlan {
        required_network_boundary: required_network_boundary_below(plan.as_ref()),
        children: annotated_children,
        task_count,
        plan,
    };
    if annotated_plan.required_network_boundary.is_none() {
        return Ok(annotated_plan);
    };

    // The plan needs a NetworkBoundary. At this point we have all the info we need for choosing
    // the right size for the stage below, so what we need to do is take the calculated final
    // task count and propagate to all the children that will eventually be part of the stage.
    fn propagate_task_count(plan: &mut AnnotatedPlan, task_count: &TaskCountAnnotation) {
        plan.task_count = task_count.clone();
        if plan.required_network_boundary.is_none() {
            for child in &mut plan.children {
                propagate_task_count(child, task_count);
            }
        }
    }
    for annotated_child in annotated_plan.children.iter_mut() {
        propagate_task_count(annotated_child, &annotated_plan.task_count);
    }

    // If the current plan that needs a NetworkBoundary boundary below is either a
    // CoalescePartitionsExec or a SortPreservingMergeExec, then we are sure that all the stage
    // that they are going to be part of needs to run in exactly one task.
    if annotated_plan.required_network_boundary == Some(RequiredNetworkBoundary::Coalesce) {
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
        annotated_plan.children.first().expect("missing child"),
        d_cfg.cardinality_task_count_factor,
    );
    let task_count = annotated_plan.task_count.as_usize() as f64;
    annotated_plan.task_count = Desired((task_count * sf).ceil() as usize);

    Ok(annotated_plan)
}

#[derive(Debug, PartialEq)]
pub(super) enum RequiredNetworkBoundary {
    Shuffle,
    Coalesce,
}

pub(super) fn required_network_boundary_below(
    parent: &dyn ExecutionPlan,
) -> Option<RequiredNetworkBoundary> {
    let children = parent.children();
    let first_child = children.first()?;

    if let Some(r_exec) = first_child.as_any().downcast_ref::<RepartitionExec>() {
        if matches!(r_exec.partitioning(), Partitioning::Hash(_, _)) {
            return Some(RequiredNetworkBoundary::Shuffle);
        }
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
