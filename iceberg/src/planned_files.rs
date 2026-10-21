use std::collections::HashMap;
use std::mem::{size_of, size_of_val};
use std::sync::{Arc, Mutex};

use datafusion::common::{Result, config_err, exec_err, internal_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_distributed::greedy_work_unit_assignment;
use futures::TryStreamExt;
use iceberg::scan::{FileScanTask, TableScan};

use crate::common::df_err;

/// File work retained by a query after manifest planning and before execution.
#[derive(Debug)]
pub(crate) struct PlannedFiles {
    pub(crate) tasks: Vec<Arc<FileScanTask>>,
    pub(crate) bytes: usize,
    pub(crate) rows: Option<usize>,
    pub(crate) filtered: bool,
    _reservation: MemoryReservation,
    assignments: Mutex<HashMap<usize, Arc<FileAssignment>>>,
}

/// Immutable file indexes assigned to distributed tasks, without choosing workers.
#[derive(Debug)]
pub(crate) struct FileAssignment {
    pub(crate) task_files: Vec<Vec<usize>>,
    _reservation: MemoryReservation,
}

impl PlannedFiles {
    pub(crate) fn assignment(
        &self,
        task_count: usize,
        context: &TaskContext,
    ) -> Result<Arc<FileAssignment>> {
        if task_count == 0 {
            return config_err!("Iceberg file assignment requires at least one task");
        }
        let mut assignments = self
            .assignments
            .lock()
            .map_err(|_| internal_datafusion_err!("Iceberg file assignment lock poisoned"))?;
        if let Some(assignment) = assignments.get(&task_count) {
            return Ok(Arc::clone(assignment));
        }
        let reservation =
            MemoryConsumer::new("Iceberg file assignment").register(context.memory_pool());
        let bytes = self
            .tasks
            .len()
            .saturating_mul(size_of::<usize>())
            .saturating_add(task_count.saturating_mul(size_of::<Vec<usize>>()));
        // Charge retained indexes and vector capacity before constructing the groups.
        reservation.try_grow(bytes.saturating_mul(2))?;
        // Construction holds an extra order array, candidate costs, the allocator's
        // order array/load heap, and spare space for output-vector growth.
        // Declare scratch before the vectors so it outlives them on success or failure.
        let scratch =
            MemoryConsumer::new("Iceberg file assignment scratch").register(context.memory_pool());
        let scratch_bytes = self
            .tasks
            .len()
            .saturating_mul(3 * size_of::<usize>() + size_of::<u64>())
            .saturating_add(task_count.saturating_mul(size_of::<(u128, usize, usize)>()));
        scratch.try_grow(scratch_bytes)?;
        // Manifest discovery order is not stable; use object identity/range for ties.
        let mut order: Vec<_> = (0..self.tasks.len()).collect();
        order.sort_unstable_by_key(|&i| {
            let task = &self.tasks[i];
            (task.data_file_path(), task.start(), task.length(), i)
        });
        let costs: Vec<_> = order.iter().map(|&i| self.tasks[i].length()).collect();
        let mut task_files = greedy_work_unit_assignment(&costs, task_count)?;
        for group in &mut task_files {
            for index in group {
                *index = order[*index];
            }
        }
        let assignment = Arc::new(FileAssignment {
            task_files,
            _reservation: reservation,
        });
        assignments.insert(task_count, Arc::clone(&assignment));
        Ok(assignment)
    }

    pub(crate) async fn collect(
        scan: TableScan,
        context: &TaskContext,
        max_files: usize,
        filtered: bool,
    ) -> Result<Self> {
        let reservation =
            MemoryConsumer::new("Iceberg file planning").register(context.memory_pool());
        let mut planned = Self {
            tasks: Vec::new(),
            bytes: 0,
            rows: Some(0),
            filtered,
            _reservation: reservation,
            assignments: Mutex::new(HashMap::new()),
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
            planned.bytes = planned.bytes.saturating_add(task.length() as usize);
            planned.rows = planned.rows.and_then(|rows| {
                task.record_count()
                    .and_then(|count| rows.checked_add(count as usize))
            });
            planned.filtered |= !task.deletes().is_empty();
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
