use datafusion::common::{Statistics, not_impl_err, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use delegate::delegate;
use std::fmt::Formatter;
use std::sync::Arc;

// FIXME: because of limitations the the statistics API on DataFusion, we need to resource to
//  this sketchy way of overriding child statistics, as we cannot just provide our own.
//  If we don't do this:
//   1. we cannot tell nodes to compute statistics based on the ones we provide.
//   2. we recompute statistics unnecessarily across the plan
//  This is tracked by https://github.com/apache/datafusion/issues/20184 upstream, and until
//  that one is solved, we need to resource to this wrapper.
pub(crate) fn partition_statistics_with_children_override(
    node: &Arc<dyn ExecutionPlan>,
    partition: Option<usize>,
    child_stats: &[Arc<Statistics>],
) -> Result<Statistics> {
    // DataFusion stats system is not very mature yet. This override layer brings in changes
    // that might not have already been released or informed overrides.
    let statistics_wrapped_children = child_stats
        .iter()
        .zip(node.children())
        .map(|(stats, child)| StatisticsWrapper {
            inner: Arc::clone(child),
            stats: Arc::clone(stats),
        })
        .map(|v| Arc::new(v) as _)
        .collect();

    let stats = Arc::clone(node)
        .with_new_children(statistics_wrapped_children)?
        .partition_statistics(partition)?;

    Ok(stats.as_ref().clone())
}

#[derive(Debug)]
struct StatisticsWrapper {
    stats: Arc<Statistics>,
    inner: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for StatisticsWrapper {
    delegate! {
        to self.inner {
            fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result;
        }
    }
}

impl ExecutionPlan for StatisticsWrapper {
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        if partition.is_some() {
            return plan_err!("StatisticsWrapper not prepared for partition-specific stats");
        }
        Ok(Arc::clone(&self.stats))
    }

    delegate! {
        to self.inner {
            fn name(&self) -> &str;
            fn properties(&self) -> &Arc<PlanProperties>;
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn repartitioned(&self, _target_partitions: usize, _config: &ConfigOptions) -> Result<Option<Arc<dyn ExecutionPlan>>>;
            fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream>;
            fn supports_limit_pushdown(&self) -> bool;
            fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>>;
            fn fetch(&self) -> Option<usize>;
            fn cardinality_effect(&self) -> CardinalityEffect;
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("with_new_children not implemented")
    }
}
