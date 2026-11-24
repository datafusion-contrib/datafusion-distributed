use crate::complexity_cpu::complexity_cpu;
use crate::complexity_memory::complexity_memory;
use crate::plan_statistics::plan_statistics;
use crate::{Complexity, CostModelExtension};
use datafusion::common::Result;
use datafusion::common::stats::Precision;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use std::ops::AddAssign;
use std::sync::Arc;

/// Bytes processed, held in memory and moved across the network by a plan.
#[derive(Default, Debug)]
pub struct Cost {
    pub cpu: Precision<usize>,
    pub memory: Precision<usize>,
    pub network: Precision<usize>,
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

/// Options for customizing the cost model.
#[derive(Default)]
pub struct CalculateCostOptions {
    /// Extensions to the cost model that account foreign [ExecutionPlan] implementations.
    extensions: Vec<Box<dyn CostModelExtension>>,
}

impl CalculateCostOptions {
    /// Adds a [CostModelExtension].
    pub fn with_extension(mut self, extension: impl CostModelExtension + 'static) -> Self {
        self.extensions.push(Box::new(extension));
        self
    }
}

/// Calculates the accumulated cost of `plan` and all its descendants.
///
/// Each node is described by the first of `opts.extensions` that handles it, falling back to the
/// built-in model when none does.
pub fn calculate_cost(plan: &Arc<dyn ExecutionPlan>, opts: &CalculateCostOptions) -> Result<Cost> {
    f(plan, opts).map(|(cost, _stats)| cost)
}

fn f(
    plan: &Arc<dyn ExecutionPlan>,
    opts: &CalculateCostOptions,
) -> Result<(Cost, Arc<Statistics>)> {
    let children = plan.children();
    let mut child_stats = Vec::with_capacity(children.len());
    let mut acc_cost = Cost::default();
    for child in children {
        let (cost, child_stat) = f(child, opts)?;
        acc_cost += cost;
        child_stats.push(child_stat);
    }

    let stats = plan_statistics(plan, &child_stats)?;
    let ext = &opts.extensions;
    let cpu = ext
        .iter()
        .find_map(|e| e.complexity_cpu(plan))
        .unwrap_or_else(|| complexity_cpu(plan));
    let memory = ext
        .iter()
        .find_map(|e| e.complexity_memory(plan))
        .unwrap_or_else(|| complexity_memory(plan));
    // Upstream DataFusion nodes never move data across the network.
    let network = ext
        .iter()
        .find_map(|e| e.complexity_network(plan))
        .unwrap_or(Complexity::Constant(0.));
    let new_cost = Cost {
        cpu: inexact_or_absent(cpu.cost(&stats, &child_stats)),
        memory: inexact_or_absent(memory.cost(&stats, &child_stats)),
        network: inexact_or_absent(network.cost(&stats, &child_stats)),
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
    use crate::test_utils::TestPlanBuilder;

    #[tokio::test]
    async fn smoke_test() -> Result<()> {
        let plan = TestPlanBuilder::new()
            .target_partitions(4)
            .physical_plan(r#"SELECT * FROM weather WHERE "MinTemp" > 5"#)
            .await;
        let cost = calculate_cost(&plan, &CalculateCostOptions::default())?;
        assert_snapshot!(format!("{cost:#?}"), @r"
        Cost {
            cpu: Inexact(154841),
            memory: Inexact(1),
            network: Inexact(0),
        }
        ");
        Ok(())
    }
}
