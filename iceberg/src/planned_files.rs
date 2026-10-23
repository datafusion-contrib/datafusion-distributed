use std::mem::{size_of, size_of_val};

use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::TryStreamExt;
use iceberg::scan::{FileScanTask, TableScan};
use iceberg::spec::{Datum, PrimitiveLiteral};

use crate::common::df_err;

/// File work retained by a query after manifest planning and before execution.
/// The task list cannot be mutated after collection.
#[derive(Debug)]
pub(crate) struct PlannedFiles {
    tasks: Vec<FileScanTask>,
    may_filter_rows: bool,
    _reservation: MemoryReservation,
}

impl PlannedFiles {
    pub(crate) fn tasks(&self) -> &[FileScanTask] {
        &self.tasks
    }

    /// Predicates or delete files may remove rows counted in file metadata.
    pub(crate) fn may_filter_rows(&self) -> bool {
        self.may_filter_rows
    }

    fn new(context: &TaskContext, may_filter_rows: bool) -> Self {
        Self {
            tasks: Vec::new(),
            may_filter_rows,
            _reservation: MemoryConsumer::new("Iceberg file planning")
                .register(context.memory_pool()),
        }
    }

    fn push(&mut self, task: FileScanTask) -> Result<()> {
        // Include owned paths/delete references and allow room for Vec capacity.
        self._reservation
            .try_grow(retained_size(&task).saturating_mul(2))?;
        self.may_filter_rows |= !task.deletes().is_empty();
        self.tasks.push(task);
        Ok(())
    }

    pub(crate) fn from_tasks(
        tasks: Vec<FileScanTask>,
        context: &TaskContext,
        may_filter_rows: bool,
    ) -> Result<Self> {
        let mut planned = Self::new(context, may_filter_rows);
        for task in tasks {
            planned.push(task)?;
        }
        Ok(planned)
    }

    pub(crate) async fn collect(
        scan: TableScan,
        context: &TaskContext,
        may_filter_rows: bool,
    ) -> Result<Self> {
        let mut planned = Self::new(context, may_filter_rows);
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
    if let Some(stats) = task.column_stats() {
        for counts in [
            stats.column_sizes(),
            stats.value_counts(),
            stats.null_value_counts(),
            stats.nan_value_counts(),
        ] {
            bytes = bytes.saturating_add(counts.capacity().saturating_mul(size_of::<(i32, u64)>()));
        }
        for bounds in [stats.lower_bounds(), stats.upper_bounds()] {
            bytes =
                bytes.saturating_add(bounds.capacity().saturating_mul(size_of::<(i32, Datum)>()));
            for datum in bounds.values() {
                let owned = match datum.literal() {
                    PrimitiveLiteral::String(value) => value.capacity(),
                    PrimitiveLiteral::Binary(value) => value.capacity(),
                    _ => 0,
                };
                bytes = bytes.saturating_add(owned);
            }
        }
    }
    bytes
}
