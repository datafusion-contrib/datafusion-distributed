use crate::{DistributedMetricsFormat, plan_statistics, rewrite_distributed_plan_with_metrics};
use datafusion::common::{DataFusionError, Statistics};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricValue;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Debug, Default, Copy, Clone)]
pub struct StatsVsMetricsDisplayOptions {
    display_output_rows: bool,
    display_output_bytes: bool,
}

impl StatsVsMetricsDisplayOptions {
    pub fn with_display_output_rows(mut self) -> Self {
        self.display_output_rows = true;
        self
    }

    pub fn with_display_output_bytes(mut self) -> Self {
        self.display_output_bytes = true;
        self
    }
}

pub fn stats_vs_metrics_display(
    plan: Arc<dyn ExecutionPlan>,
    opts: StatsVsMetricsDisplayOptions,
) -> Result<String, DataFusionError> {
    let plan = rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)?;
    let node = Node::from_plan(plan, opts)?;
    Ok(format!("{node:?}"))
}

struct Node {
    name: String,
    stats: Statistics,
    output_rows: Option<usize>,
    output_bytes: Option<usize>,
    children: Vec<Node>,
    opts: StatsVsMetricsDisplayOptions,
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn fmt(f: &mut Formatter<'_>, node: &Node, depth: usize) -> std::fmt::Result {
            for _ in 0..depth {
                write!(f, "  ")?;
            }
            write!(f, "{}:", node.name)?;
            if let Some(actual) = &node.output_rows
                && node.opts.display_output_rows
            {
                let estimated = *node.stats.num_rows.get_value().unwrap_or(&0);
                let err = (100 * estimated.abs_diff(*actual)) / actual.max(&estimated).max(&1);
                write!(f, " output_rows={estimated} vs {actual} ({err}%)")?;
            }
            if let Some(actual) = &node.output_bytes
                && node.opts.display_output_bytes
            {
                let estimated = *node.stats.total_byte_size.get_value().unwrap_or(&0);
                let err = (100 * estimated.abs_diff(*actual)) / actual.max(&estimated).max(&1);
                write!(f, " output_bytes_err={estimated} vs {actual} ({err}%)")?;
            }
            writeln!(f)?;
            for c in &node.children {
                fmt(f, c, depth + 1)?;
            }
            Ok(())
        }

        fmt(f, self, 0)
    }
}

impl Node {
    fn from_plan(
        plan: Arc<dyn ExecutionPlan>,
        opts: StatsVsMetricsDisplayOptions,
    ) -> Result<Self, DataFusionError> {
        let mut children = vec![];
        for child in plan.children() {
            children.push(Node::from_plan(Arc::clone(child), opts)?);
        }

        let mut node = Node {
            name: plan.name().to_string(),
            stats: plan_statistics(
                &plan,
                &children.iter().map(|v| &v.stats).collect::<Vec<_>>(),
                Default::default(),
            )?,
            output_rows: None,
            output_bytes: None,
            children,
            opts,
        };
        if let Some(metrics) = plan.metrics() {
            node.output_rows = metrics.output_rows();
            node.output_bytes = metrics
                .sum(|v| matches!(v.value(), MetricValue::OutputBytes(_)))
                .map(|v| v.as_usize());
        }

        Ok(node)
    }
}
