use std::sync::Arc;

use datafusion::common::HashMap;
use datafusion::physical_expr::{PhysicalExpr, conjunction};
use datafusion::physical_expr_common::physical_expr::{
    snapshot_generation, snapshot_physical_expr,
};
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricType, PruningMetrics,
};
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;

use crate::common::dynamic_filters::partition_filter::PartitionFilter;
use crate::common::dynamic_filters::physical_predicate::convert_physical_filter_to_predicate;

/// Uses the scan's dynamic filters on each file just before the reader starts it. Skips files
/// whose partition value can't match the filters.
pub(crate) struct DynamicFilterPruner {
    /// The scan's dynamic filters, combined with AND.
    dynamic_filters: Arc<dyn PhysicalExpr>,

    /// The generation of `dynamic_filters` that `predicate` was built from.
    cached_snapshot_generation: u64,

    /// The filters as an Iceberg predicate. `None` when no part of them converts.
    predicate: Option<Predicate>,

    /// A partition filter for each partition spec, by spec id. `None` keeps every file written
    /// with that spec, because its partition values can't be checked. Cleared when `predicate`
    /// changes.
    partition_filters: HashMap<i32, Option<PartitionFilter>>,

    /// The files checked. Pruned files were skipped and matched files were kept.
    files: PruningMetrics,
}

impl DynamicFilterPruner {
    pub(crate) fn new(
        dynamic_filters: Vec<Arc<dyn PhysicalExpr>>,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        Self {
            dynamic_filters: conjunction(dynamic_filters),
            cached_snapshot_generation: 0,
            predicate: None,
            partition_filters: HashMap::new(),
            files: MetricBuilder::new(metrics)
                .with_type(MetricType::Summary)
                .pruning_metrics("files_pruned_by_partition", partition),
        }
    }

    /// The task to read, or `None` to skip it. Any error keeps the task.
    pub(crate) fn prune(&mut self, task: FileScanTask) -> Option<FileScanTask> {
        if self.can_skip(&task) {
            self.files.add_pruned(1);
            return None;
        }
        self.files.add_matched(1);
        Some(task)
    }

    /// Whether no row in the task's partition can match the filters.
    fn can_skip(&mut self, task: &FileScanTask) -> bool {
        let (Some(spec), Some(partition)) = (task.partition_spec(), task.partition()) else {
            return false;
        };
        if spec.is_unpartitioned() {
            return false;
        }
        self.refresh();
        let Some(predicate) = &self.predicate else {
            return false;
        };
        // Every task in a scan has the same table schema, so the spec id is enough as a key.
        let partition_filter = self
            .partition_filters
            .entry(spec.spec_id())
            .or_insert_with(|| {
                PartitionFilter::try_new(
                    predicate.clone(),
                    task.schema_ref(),
                    spec,
                    task.case_sensitive(),
                )
                .ok()
            });
        let Some(filter) = partition_filter.as_ref() else {
            return false;
        };
        match filter.can_match(partition) {
            Ok(matches) => !matches,
            // Files with the same spec would most likely fail the same way, so stop checking
            // them until the filters change.
            Err(_) => {
                *partition_filter = None;
                false
            }
        }
    }

    /// Rebuilds the predicate when any of the filters has changed.
    fn refresh(&mut self) {
        // Read the generation before taking the snapshot. If a filter changes in between, the
        // next file sees a newer generation and rebuilds.
        let generation = snapshot_generation(&self.dynamic_filters);
        if self.cached_snapshot_generation == generation {
            return;
        }
        self.cached_snapshot_generation = generation;
        self.partition_filters.clear();
        self.predicate = snapshot_physical_expr(Arc::clone(&self.dynamic_filters))
            .ok()
            .and_then(|snapshot| convert_physical_filter_to_predicate(&snapshot));
    }
}
