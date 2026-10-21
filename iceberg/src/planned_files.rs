use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::TryStreamExt;
use iceberg::scan::{FileScanTask, TableScan};

use crate::common::df_err;

/// File work retained by a query after manifest planning and before execution.
#[derive(Debug)]
pub(crate) struct PlannedFiles {
    pub(crate) tasks: Vec<Arc<FileScanTask>>,
    _reservation: MemoryReservation,
}

impl PlannedFiles {
    pub(crate) async fn collect(
        scan: TableScan,
        context: &TaskContext,
        max_files: usize,
    ) -> Result<Self> {
        let reservation =
            MemoryConsumer::new("Iceberg file planning").register(context.memory_pool());
        let mut planned = Self {
            tasks: Vec::new(),
            _reservation: reservation,
        };
        let mut stream = scan.plan_files().await.map_err(df_err)?;
        while let Some(task) = stream.try_next().await.map_err(df_err)? {
            if planned.tasks.len() >= max_files {
                return exec_err!(
                    "Iceberg file planning exceeded iceberg.planning_max_files={max_files}"
                );
            }
            // Account for owned paths and delete references, and allow room for Vec capacity.
            planned
                ._reservation
                .try_grow(retained_size(&task).saturating_mul(2))?;
            planned.tasks.push(Arc::new(task));
        }
        Ok(planned)
    }
}

fn retained_size(task: &FileScanTask) -> usize {
    let mut bytes = size_of::<FileScanTask>()
        + size_of::<Arc<FileScanTask>>()
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
