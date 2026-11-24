use std::mem::{size_of, size_of_val};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::TryStreamExt;
use iceberg::scan::{FileScanTask, TableScan};

use crate::common::arrow_bytes::scan_arrow_bytes;
use crate::common::df_err;

/// File work retained by a query after manifest planning and before execution.
/// Shared by task-local sources so their descriptors and reservation have one owner.
#[derive(Debug)]
pub(crate) struct PlannedFiles {
    tasks: Vec<FileScanTask>,
    _reservation: MemoryReservation,
}

impl PlannedFiles {
    pub(crate) fn tasks(&self) -> &[FileScanTask] {
        &self.tasks
    }

    /// Summarize selected file work, with `column_statistics` when they were read; predicates and
    /// deletes can reduce output rows. The total size is an in-memory estimate from
    /// [scan_arrow_bytes].
    pub(crate) fn statistics(
        &self,
        schema: &SchemaRef,
        may_filter_rows: bool,
        column_statistics: Option<Vec<ColumnStatistics>>,
    ) -> Statistics {
        let rows = self.tasks.iter().try_fold(0usize, |rows, task| {
            rows.checked_add(usize::try_from(task.record_count()?).ok()?)
        });
        let num_rows = match rows {
            Some(0) if self.tasks.is_empty() => Precision::Exact(0),
            Some(rows) if !may_filter_rows => Precision::Exact(rows),
            Some(rows) => Precision::Inexact(rows),
            None => Precision::Absent,
        };
        let column_statistics =
            column_statistics.unwrap_or_else(|| Statistics::unknown_column(schema));
        Statistics {
            num_rows,
            total_byte_size: scan_arrow_bytes(schema, num_rows, &column_statistics),
            column_statistics,
        }
    }

    /// On-disk bytes the selected file work reads.
    pub(crate) fn file_bytes(&self) -> usize {
        self.tasks.iter().fold(0usize, |bytes, task| {
            bytes.saturating_add(task.length() as usize)
        })
    }

    fn new(context: &TaskContext) -> Self {
        Self {
            tasks: Vec::new(),
            _reservation: MemoryConsumer::new("Iceberg file planning")
                .register(context.memory_pool()),
        }
    }

    fn push(&mut self, task: FileScanTask) -> Result<()> {
        // Include owned paths/delete references and allow room for Vec capacity.
        self._reservation
            .try_grow(retained_size(&task).saturating_mul(2))?;
        self.tasks.push(task);
        Ok(())
    }

    pub(crate) fn from_tasks(tasks: Vec<FileScanTask>, context: &TaskContext) -> Result<Self> {
        let mut planned = Self::new(context);
        for task in tasks {
            planned.push(task)?;
        }
        Ok(planned)
    }

    pub(crate) async fn collect(scan: TableScan, context: &TaskContext) -> Result<Self> {
        let mut planned = Self::new(context);
        let mut stream = scan.plan_files().await.map_err(df_err)?;
        while let Some(task) = stream.try_next().await.map_err(df_err)? {
            planned.push(task)?;
        }
        Ok(planned)
    }
}

fn retained_size(task: &FileScanTask) -> usize {
    let mut bytes = size_of::<FileScanTask>()
        + task.data_file_path().len()
        + size_of_val(task.project_field_ids())
        + size_of_val(task.deletes())
        + task.key_metadata().map_or(0, <[u8]>::len);
    for delete in task.deletes() {
        bytes = bytes
            .saturating_add(delete.file_path().len())
            .saturating_add(delete.referenced_data_file().map_or(0, str::len))
            .saturating_add(delete.equality_ids().map_or(0, size_of_val))
            .saturating_add(delete.key_metadata().map_or(0, <[u8]>::len));
    }
    bytes
}
