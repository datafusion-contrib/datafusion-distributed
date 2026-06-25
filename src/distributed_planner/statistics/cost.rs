use crate::distributed_planner::statistics::complexity_cpu::complexity_cpu;
use crate::distributed_planner::statistics::complexity_memory::complexity_memory;
use crate::distributed_planner::statistics::complexity_network::complexity_network;
use crate::distributed_planner::statistics::plan_statistics::partition_statistics_with_children_override;
use datafusion::common::Result;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use std::ops::AddAssign;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct Cost {
    pub(crate) cpu: usize,
    pub(crate) memory: usize,
    pub(crate) network: usize,
}

impl AddAssign for Cost {
    fn add_assign(&mut self, rhs: Self) {
        self.cpu += rhs.cpu;
        self.memory += rhs.memory;
        self.network += rhs.network;
    }
}

pub(crate) fn calculate_cost(plan: &Arc<dyn ExecutionPlan>) -> Result<Cost> {
    f(plan).map(|(cost, _stats)| cost)
}

fn f(plan: &Arc<dyn ExecutionPlan>) -> Result<(Cost, Arc<Statistics>)> {
    let children = plan.children();
    let mut child_stats = Vec::with_capacity(children.len());
    let mut acc_cost = Cost::default();
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
    let cpu = complexity_cpu(plan);
    acc_cost.cpu += cpu.cost(&stats, &child_stats).unwrap_or(0);
    let memory = complexity_memory(plan);
    acc_cost.memory += memory.cost(&stats, &child_stats).unwrap_or(0);
    let network = complexity_network(plan);
    acc_cost.network += network.cost(&stats, &child_stats).unwrap_or(0);
    Ok((acc_cost, stats))
}
