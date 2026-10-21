use std::mem::{size_of, size_of_val};

use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::TryStreamExt;
use iceberg::scan::{FileScanTask, TableScan};

use crate::common::df_err;

/// File work retained by a query after manifest planning and before execution.
/// The task list and its summaries are built together and cannot be mutated after collection.
#[derive(Debug)]
pub(crate) struct PlannedFiles {
    tasks: Vec<FileScanTask>,
    bytes: usize,
    rows: Option<usize>,
    may_filter_rows: bool,
    _reservation: MemoryReservation,
}

impl PlannedFiles {
    pub(crate) fn tasks(&self) -> &[FileScanTask] {
        &self.tasks
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn rows(&self) -> Option<usize> {
        self.rows
    }

    /// Predicates or delete files may remove rows counted in file metadata.
    pub(crate) fn may_filter_rows(&self) -> bool {
        self.may_filter_rows
    }

    pub(crate) async fn collect(
        scan: TableScan,
        context: &TaskContext,
        may_filter_rows: bool,
    ) -> Result<Self> {
        let reservation =
            MemoryConsumer::new("Iceberg file planning").register(context.memory_pool());
        let mut planned = Self {
            tasks: Vec::new(),
            bytes: 0,
            rows: Some(0),
            may_filter_rows,
            _reservation: reservation,
        };
        let mut stream = scan.plan_files().await.map_err(df_err)?;
        while let Some(task) = stream.try_next().await.map_err(df_err)? {
            // Account for owned paths and delete references, and allow room for Vec capacity.
            planned
                ._reservation
                .try_grow(retained_size(&task).saturating_mul(2))?;
            planned.bytes = planned.bytes.saturating_add(task.length() as usize);
            planned.rows = planned.rows.and_then(|rows| {
                task.record_count()
                    .and_then(|count| rows.checked_add(count as usize))
            });
            planned.may_filter_rows |= !task.deletes().is_empty();
            planned.tasks.push(task);
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
