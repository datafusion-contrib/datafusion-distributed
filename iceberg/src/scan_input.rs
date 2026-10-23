use std::sync::Arc;

use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion_distributed::WorkUnitFeed;
use iceberg::scan::FileScanTask;

use crate::IcebergWorkUnitFeed;
use crate::file_partitions::FilePartitions;
use crate::planned_files::PlannedFiles;

/// How a scan obtains file tasks. Assignment policy is resolved before execution
/// for retained files; lazy scans receive their work from the coordinator's feed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ScanInput {
    /// Immutable files assigned to this scan, grouped by local output partition.
    AssignedFiles(AssignedFiles),
    /// File tasks discovered and delivered during execution.
    WorkUnitFeed(Box<WorkUnitFeed<IcebergWorkUnitFeed>>),
}

/// A concrete assignment, shared by routing inspection, serialization and reading.
/// File descriptors stay shared on the coordinator; only this scan's groups are
/// serialized when sending a task to a worker.
#[derive(Debug, Clone)]
pub struct AssignedFiles {
    pub(crate) planned: Arc<PlannedFiles>,
    partitions: Arc<[Vec<usize>]>,
}

impl AssignedFiles {
    /// Number of local output partitions, including empty partitions.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// The files owned by a local output partition, in delivery order.
    pub fn partition(
        &self,
        partition: usize,
    ) -> Result<impl ExactSizeIterator<Item = &FileScanTask>> {
        let Some(indices) = self.partitions.get(partition) else {
            return exec_err!("Invalid Iceberg scan partition {partition}");
        };
        Ok(indices.iter().map(|&index| &self.planned.tasks()[index]))
    }

    /// All files owned by this scan. This does not promise row ordering.
    pub fn iter(&self) -> impl Iterator<Item = &FileScanTask> {
        self.partitions
            .iter()
            .flatten()
            .map(|&index| &self.planned.tasks()[index])
    }

    pub(crate) fn assign(
        planned: Arc<PlannedFiles>,
        task_count: usize,
        partitions_per_task: usize,
        greedy: bool,
    ) -> Result<Vec<Self>> {
        if task_count
            .checked_mul(partitions_per_task)
            .is_none_or(|n| n == 0)
        {
            return exec_err!("Invalid Iceberg assignment partition count");
        }
        let destinations = if greedy {
            FilePartitions::greedy(planned.tasks(), task_count, partitions_per_task)
        } else {
            FilePartitions::RoundRobin {
                next_file: 0,
                task_count,
                partitions_per_task,
            }
        };
        let mut groups = vec![vec![Vec::new(); partitions_per_task]; task_count];
        for (index, partition) in (0..planned.tasks().len()).zip(destinations) {
            groups[partition / partitions_per_task][partition % partitions_per_task].push(index);
        }
        Ok(groups
            .into_iter()
            .map(|partitions| Self {
                planned: Arc::clone(&planned),
                partitions: partitions.into(),
            })
            .collect())
    }

    pub(crate) fn from_partitions(
        groups: Vec<Vec<FileScanTask>>,
        context: &TaskContext,
        may_filter_rows: bool,
    ) -> Result<Self> {
        let mut tasks = Vec::new();
        let mut partitions = Vec::new();
        for group in groups {
            let start = tasks.len();
            tasks.extend(group);
            partitions.push((start..tasks.len()).collect());
        }
        Ok(Self {
            planned: Arc::new(PlannedFiles::from_tasks(tasks, context, may_filter_rows)?),
            partitions: partitions.into(),
        })
    }

    pub(crate) fn task(&self, partition: usize, offset: usize) -> &FileScanTask {
        &self.planned.tasks()[self.partitions[partition][offset]]
    }

    /// Number of files owned by this scan.
    pub fn len(&self) -> usize {
        self.partitions.iter().map(Vec::len).sum()
    }

    /// Whether this scan has no assigned files.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn bytes(&self) -> usize {
        if self.len() == self.planned.tasks().len() {
            return self.planned.bytes();
        }
        self.iter().fold(0usize, |total, file| {
            total.saturating_add(file.length() as usize)
        })
    }

    pub(crate) fn rows(&self) -> Option<usize> {
        if self.len() == self.planned.tasks().len() {
            return self.planned.rows();
        }
        self.iter().try_fold(0usize, |total, file| {
            total.checked_add(usize::try_from(file.record_count()?).ok()?)
        })
    }
}
