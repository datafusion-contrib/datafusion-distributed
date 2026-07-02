use crate::distributed_planner::statistics::complexity_cpu::complexity_cpu;
use crate::distributed_planner::statistics::complexity_memory::complexity_memory;
use crate::distributed_planner::statistics::complexity_network::complexity_network;
use crate::distributed_planner::statistics::plan_statistics::plan_statistics;
use datafusion::common::Result;
use datafusion::common::stats::Precision;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use std::ops::AddAssign;
use std::sync::Arc;

#[derive(Default, Debug)]
pub(crate) struct Cost {
    pub(crate) cpu: Precision<usize>,
    pub(crate) memory: Precision<usize>,
    pub(crate) network: Precision<usize>,
}

impl AddAssign for Cost {
    fn add_assign(&mut self, rhs: Self) {
        self.cpu = sum_precision(self.cpu, rhs.cpu);
        self.memory = sum_precision(self.memory, rhs.memory);
        self.network = sum_precision(self.network, rhs.network);
    }
}

fn sum_precision(one: Precision<usize>, other: Precision<usize>) -> Precision<usize> {
    match (one.get_value(), other.get_value()) {
        (Some(one), Some(other)) => Precision::Inexact(one + other),
        (Some(one), None) => Precision::Inexact(*one),
        (None, Some(other)) => Precision::Inexact(*other),
        (None, None) => Precision::Absent,
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

    let stats = plan_statistics(plan, &child_stats)?;
    let new_cost = Cost {
        cpu: inexact_or_absent(complexity_cpu(plan).cost(&stats, &child_stats)),
        memory: inexact_or_absent(complexity_memory(plan).cost(&stats, &child_stats)),
        network: inexact_or_absent(complexity_network(plan).cost(&stats, &child_stats)),
    };
    acc_cost += new_cost;
    Ok((acc_cost, stats))
}

fn inexact_or_absent(value: Option<usize>) -> Precision<usize> {
    match value {
        None => Precision::Absent,
        Some(v) => Precision::Inexact(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_snapshot;
    use crate::test_utils::plans::TestPlanBuilder;

    #[tokio::test]
    async fn smoke_test() -> Result<()> {
        let plan = TestPlanBuilder::new()
            .target_partitions(4)
            .physical_plan(r#"SELECT * FROM weather WHERE "MinTemp" > 5"#)
            .await;
        let cost = calculate_cost(&plan)?;
        assert_snapshot!(format!("{cost:#?}"), @r"
        Cost {
            cpu: Inexact(154841),
            memory: Inexact(0),
            network: Inexact(0),
        }
        ");
        Ok(())
    }
}
