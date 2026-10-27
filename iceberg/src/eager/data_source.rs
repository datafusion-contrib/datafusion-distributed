use crate::common::file_partition;
use crate::common::scan_config::ScanConfig;
use crate::eager::planned_files::PlannedFiles;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Statistics, exec_err};
use datafusion::datasource::source::DataSource;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::StreamExt;
use iceberg::scan::FileScanTask;
use std::sync::Arc;

/// An Iceberg scan whose complete file assignment is available before execution.
/// Routing handlers inspect these files through the task-specialized plan; the
/// codec transports the same partition groups to the worker's Iceberg reader.
#[derive(Debug, Clone)]
pub struct EagerIcebergDataSource {
    pub(crate) scan: ScanConfig,
    pub(crate) statistics: Arc<Statistics>,
    pub(crate) files: Arc<PlannedFiles>,
    pub(crate) partitions: Arc<[Vec<usize>]>,
}
impl EagerIcebergDataSource {
    /// Borrow this scan's files for placement or inspection, without I/O or cloning.
    /// File iteration order does not promise row ordering.
    pub fn planned_files(&self) -> impl Iterator<Item = &FileScanTask> {
        self.partitions
            .iter()
            .flatten()
            .map(|&index| &self.files.tasks()[index])
    }

    /// Borrow the files that execution will read for one local partition.
    ///
    /// # Errors
    /// Returns an error for an out-of-bounds partition.
    pub fn planned_partition_tasks(
        &self,
        partition: usize,
    ) -> Result<impl ExactSizeIterator<Item = &FileScanTask>> {
        let Some(indices) = self.partitions.get(partition) else {
            return exec_err!("Invalid Iceberg scan partition {partition}");
        };
        Ok(indices.iter().map(|&index| &self.files.tasks()[index]))
    }

    /// Distribute this scan's files across tasks, then across their local partitions.
    /// Like FileScanConfig, the resulting source is the assignment used by routing
    /// and execution. The descriptor storage and its reservation remain shared.
    pub(crate) fn specialize(&self, task_count: usize) -> Result<Vec<Self>> {
        let partitions_per_task = self.partitions.len();
        if task_count
            .checked_mul(partitions_per_task)
            .is_none_or(|n| n == 0)
        {
            return exec_err!("Invalid Iceberg assignment partition count");
        }
        let mut indices: Vec<_> = self.partitions.iter().flatten().copied().collect();
        // Keep discovery order, including when specializing an existing subset.
        indices.sort_unstable();
        let mut groups = vec![vec![Vec::new(); partitions_per_task]; task_count];
        for (i, index) in indices.into_iter().enumerate() {
            let partition = file_partition(i, task_count, partitions_per_task);
            groups[partition / partitions_per_task][partition % partitions_per_task].push(index);
        }
        Ok(groups
            .into_iter()
            .map(|partitions| {
                let mut source = self.clone();
                source.partitions = partitions.into();
                source.scan.metrics = Default::default();
                // Whole-snapshot statistics do not describe a task's subset.
                source.statistics = Arc::new(Statistics::new_unknown(&source.scan.schema));
                source
            })
            .collect())
    }
}
impl DataSource for EagerIcebergDataSource {
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let count = self.planned_partition_tasks(partition)?.len();
        let source = self.clone();
        let tasks = futures::stream::iter(0..count)
            .map(move |offset| {
                Ok(source.files.tasks()[source.partitions[partition][offset]].clone())
            })
            .boxed();
        self.scan.open(tasks, partition, context)
    }
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "format=iceberg, planned_files={}",
            self.partitions.iter().map(Vec::len).sum::<usize>()
        )?;
        if let Some(fetch) = self.scan.fetch {
            write!(f, ", fetch={fetch}")?;
        }
        Ok(())
    }
    fn output_partitioning(&self) -> Partitioning {
        self.scan.partitioning.clone()
    }
    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(Arc::clone(&self.scan.schema))
    }
    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&self.statistics))
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let mut source = self.clone();
        source.scan.fetch = fetch;
        Some(Arc::new(source))
    }
    fn fetch(&self) -> Option<usize> {
        self.scan.fetch
    }
    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }
    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.scan.metrics.clone()
    }
}
