use crate::distributed_planner::statistics::complexity_cpu::complexity_cpu;
use crate::distributed_planner::statistics::plan_statistics::partition_statistics_with_children_override;
use datafusion::common::Result;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use std::sync::Arc;

pub(crate) fn calculate_cost(plan: &Arc<dyn ExecutionPlan>) -> Result<usize> {
    f(plan).map(|(cost, _stats)| cost)
}

fn f(plan: &Arc<dyn ExecutionPlan>) -> Result<(usize, Arc<Statistics>)> {
    let children = plan.children();
    let mut child_stats = Vec::with_capacity(children.len());
    let mut acc_cost = 0;
    for child in children {
        let (cost, child_stat) = f(child)?;
        acc_cost += cost;
        child_stats.push(child_stat);
    }

    let stats = Arc::new(partition_statistics_with_children_override(
        plan,
        None,
        &child_stats,
    )?);
    let complexity = complexity_cpu(plan);
    acc_cost += complexity.cost(&stats, &child_stats).unwrap_or(0);
    Ok((acc_cost, stats))
}
