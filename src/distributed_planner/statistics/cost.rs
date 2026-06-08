use crate::DistributedConfig;
use crate::distributed_planner::statistics::compute_per_node::calculate_compute_complexity;
use crate::distributed_planner::statistics::plan_statistics::plan_statistics;
use datafusion::common::Result;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use std::sync::Arc;

pub(crate) fn calculate_cost(
    plan: &Arc<dyn ExecutionPlan>,
    cfg: &DistributedConfig,
) -> Result<usize> {
    f(plan, cfg).map(|(cost, _stats)| cost)
}

fn f(plan: &Arc<dyn ExecutionPlan>, d_cfg: &DistributedConfig) -> Result<(usize, Arc<Statistics>)> {
    let children = plan.children();
    let mut child_stats = Vec::with_capacity(children.len());
    let mut acc_cost = 0;
    for child in children {
        let (cost, child_stat) = f(child, d_cfg)?;
        acc_cost += cost;
        child_stats.push(child_stat);
    }

    let stats = plan_statistics(plan, &child_stats, d_cfg)?;
    let complexity = calculate_compute_complexity(plan);
    acc_cost += complexity.cost(&stats, &child_stats).unwrap_or(0);
    Ok((acc_cost, stats))
}
