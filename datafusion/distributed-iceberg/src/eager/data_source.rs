use crate::common::scan_config::ScanConfig;
use crate::eager::planned_files::PlannedFiles;
use datafusion::arrow::datatypes::SchemaRef;
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
use iceberg::io::FileIO;
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
    /// Construct a scan from already selected files grouped by local output partition.
    /// Preserves group order without discovering, pruning, or assigning files.
    /// Descriptors are charged to `context`'s memory pool; statistics are unknown.
    /// Reader settings are captured from `context` and shared across local partitions.
    ///
    /// # Errors
    /// Returns an error if there are no partition groups or memory reservation fails.
    /// Empty groups are valid, including for an empty scan.
    pub fn from_file_groups(
        schema: SchemaRef,
        groups: Vec<Vec<FileScanTask>>,
        file_io: FileIO,
        runtime: iceberg::Runtime,
        fetch: Option<usize>,
        context: &TaskContext,
    ) -> Result<Self> {
        if groups.is_empty() {
            return exec_err!("Iceberg scan requires at least one file group");
        }
        let mut tasks = Vec::new();
        let mut partitions = Vec::with_capacity(groups.len());
        for group in groups {
            let start = tasks.len();
            tasks.extend(group);
            partitions.push((start..tasks.len()).collect());
        }
        Ok(Self {
            statistics: Arc::new(Statistics::new_unknown(&schema)),
            scan: ScanConfig::new(
                schema,
                Partitioning::UnknownPartitioning(partitions.len()),
                fetch,
                file_io,
                runtime,
                context,
            ),
            files: Arc::new(PlannedFiles::from_tasks(tasks, context)?),
            partitions: partitions.into(),
        })
    }

    /// Output schema, including the scan's projection.
    pub fn schema(&self) -> &SchemaRef {
        &self.scan.schema
    }

    /// File I/O configured for this process. A codec transports its properties,
    /// then supplies worker-local storage and runtime resources on reconstruction.
    pub fn file_io(&self) -> &FileIO {
        &self.scan.iceberg_file_io
    }

    /// Borrow this scan's files in retained-file order, independent of partition groups.
    /// Positions in this iterator are the inputs to [`Self::with_file_groups`].
    /// This sorts file indices, without cloning descriptors or performing I/O.
    /// File iteration order does not promise row ordering.
    pub fn planned_files(&self) -> impl ExactSizeIterator<Item = &FileScanTask> {
        self.file_indices()
            .into_iter()
            .map(|index| &self.files.tasks()[index])
    }

    fn file_indices(&self) -> Vec<usize> {
        let mut indices: Vec<_> = self.partitions.iter().flatten().copied().collect();
        indices.sort_unstable();
        indices
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

    /// Derive a scan using positions in this source's [`Self::planned_files`] iterator.
    /// Each group is one local output partition; supplied order is preserved.
    /// Files may be omitted, but each position may appear at most once.
    ///
    /// The result shares descriptors and their memory reservation with this source.
    /// It has fresh metrics and unknown statistics because its file set may differ.
    /// No file discovery, assignment policy, or file I/O is performed.
    ///
    /// # Errors
    /// Returns an error for zero groups, out-of-bounds positions, or repeated positions.
    pub fn with_file_groups(&self, mut groups: Vec<Vec<usize>>) -> Result<Self> {
        if groups.is_empty() {
            return exec_err!("Iceberg scan requires at least one file group");
        }
        let indices = self.file_indices();
        let mut used = vec![false; indices.len()];
        for position in groups.iter_mut().flatten() {
            let Some(&index) = indices.get(*position) else {
                return exec_err!("Invalid Iceberg file position {}", *position);
            };
            if std::mem::replace(&mut used[*position], true) {
                return exec_err!("Repeated Iceberg file position {}", *position);
            }
            *position = index;
        }
        let mut source = self.clone();
        source.scan.partitioning = Partitioning::UnknownPartitioning(groups.len());
        source.partitions = groups.into();
        source.scan.metrics = Default::default();
        source.statistics = Arc::new(Statistics::new_unknown(&source.scan.schema));
        Ok(source)
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let count = self.planned_partition_tasks(partition)?.len();
        let source = self.clone();
        let tasks = futures::stream::iter(0..count)
            .map(move |offset| {
                Ok(source.files.tasks()[source.partitions[partition][offset]].clone())
            })
            .boxed();
        self.scan.open(tasks, partition)
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
