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
use crate::common::dynamic_filters::row_group_filter::reader_filter;
use crate::common::dynamic_filters::task_filter::and_predicate;

/// Applies the scan's dynamic filters to each file just before iceberg-rust's `ArrowReader` opens
/// it. It skips files whose partition value can't match the filters. For the other files, it can
/// add the filters to the file's own filter, so `ArrowReader` can skip row groups.
pub(crate) struct DynamicFilterPruner {
    /// The scan's dynamic filters, combined with AND.
    dynamic_filters: Arc<dyn PhysicalExpr>,

    /// The generation of `dynamic_filters` that `predicate` was built from.
    cached_snapshot_generation: u64,

    /// The dynamic filters are stored as iceberg Predicates after being converted
    /// `None` if the filters can't be converted
    predicate: Option<Predicate>,

    /// A map of partition spec IDs to their corresponding partition filter executors
    partition_filters: HashMap<i32, Option<PartitionFilter>>,

    /// Metrics to track pruning
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

    /// Whether the filters rule out every row in the task's partition. Counts the task as pruned
    /// or matched
    pub(crate) fn should_prune(&mut self, task: &FileScanTask) -> bool {
        let prune = self.can_skip(task);
        if prune {
            self.files.add_pruned(1);
        } else {
            self.files.add_matched(1);
        }
        prune
    }

    /// Adds the dynamic filters to the task's own filter, so iceberg-rust's `ArrowReader` can use
    /// them to skip row groups. Returns the task unchanged if adding them fails.
    pub(crate) fn with_reader_filter(&mut self, task: FileScanTask) -> FileScanTask {
        self.refresh();
        let Some(predicate) = &self.predicate else {
            return task;
        };

        // Some data files may not contain identity partition columns in their rows.
        // `ArrowReader` reads a column the file doesn't store as NULL, so a filter on it drops
        // every row in the file. The same goes for columns added later with a default value.
        // We accept this until iceberg-rust fixes it.
        //
        // We AND onto the task's existing filter, so `ArrowReader` still applies the query's
        // WHERE clause.
        and_predicate(&task, reader_filter(predicate)).unwrap_or(task)
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

        // Every `FileScanTask` within a scan has the same Iceberg schema, taken from the selected
        // Iceberg snapshot.
        // The only difference in partition filters can come from the partition spec being different
        // between data files.
        // The PartitionFilter struct -
        // 1. Binds the `Predicate`, which just contains column names and values, to the actual
        //    schema, resolving column names to Iceberg field IDs etc.
        // 2. Projects the column filter into a partition filter. A partition spec is not limited
        //    to just direct column filters; it can have expressions like `bucket(2, x)` or
        //    `day(ts)`. A column filter like `ts >= '2024-03-15 10:00'` becomes
        //    `ts_day >= 2024-03-15` which can be checked against every data file's partition value.
        // These steps can be expensive, so we maintain a cache of a `PartitionFilter` per spec ID.
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

        // Check whether the file can be dropped, based on the partition values.
        // If there's an error while evaluating the filter,
        // we return `false` so that the file is not skipped
        filter.can_match(partition).is_ok_and(|matches| !matches)
    }

    /// Rebuilds the predicate when any of the filters has changed.
    fn refresh(&mut self) {
        // Read the current generation before snapshotting the filters.
        // The two reads aren't atomic, so a filter can change between them.
        //
        // In this order, the snapshot can be newer than the generation we have stored
        // which is okay - because we're still doing the most 'efficient' pruning
        //
        // In the opposite order, we could store a newer generation but an older filter,
        // which would prune less until the filter updated.
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
