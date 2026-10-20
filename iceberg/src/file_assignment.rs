use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::mem::size_of;

use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use iceberg::scan::FileScanTask;

// Largest file first; object identity breaks ties independently of manifest order.
type FilePriority<'a> = (u64, Reverse<&'a str>, Reverse<u64>, Reverse<usize>);
type TaskLoad = Reverse<(u128, usize, usize)>;

/// Execution-local destinations, indexed by the retained file's position.
#[derive(Debug)]
pub(crate) struct FileAssignment {
    partitions: Vec<usize>,
    _reservation: MemoryReservation,
}

impl FileAssignment {
    /// The dispatcher validates nonzero task/partition counts and their product.
    pub(crate) async fn build(
        files: &[FileScanTask],
        task_count: usize,
        partitions_per_task: usize,
        context: &TaskContext,
    ) -> Result<Self> {
        let reservation =
            MemoryConsumer::new("Iceberg file assignment").register(context.memory_pool());
        reservation.try_grow(files.len().saturating_mul(size_of::<usize>()))?;
        // Declare before the heaps so their storage is dropped before its reservation.
        let scratch =
            MemoryConsumer::new("Iceberg file assignment scratch").register(context.memory_pool());
        scratch.try_grow(
            files
                .len()
                .saturating_mul(size_of::<FilePriority<'_>>())
                .saturating_add(task_count.saturating_mul(size_of::<TaskLoad>())),
        )?;
        let mut order = BinaryHeap::with_capacity(files.len());
        let mut partitions = Vec::with_capacity(files.len());
        for (index, file) in files.iter().enumerate() {
            order.push((
                file.length(),
                Reverse(file.data_file_path()),
                Reverse(file.start()),
                Reverse(index),
            ));
            partitions.push(0);
            if index % 256 == 0 {
                tokio::task::yield_now().await;
            }
        }
        // Prefer fewer files, then task index, when byte loads tie.
        let mut loads = BinaryHeap::with_capacity(task_count);
        for task in 0..task_count {
            loads.push(Reverse((0_u128, 0_usize, task)));
            if task % 256 == 0 {
                tokio::task::yield_now().await;
            }
        }
        let mut assigned = 0;
        while let Some((bytes, _, _, Reverse(index))) = order.pop() {
            {
                let mut least_loaded = loads.peek_mut().expect("validated nonzero task count");
                let Reverse((load, count, task)) = &mut *least_loaded;
                partitions[index] = *task * partitions_per_task + *count % partitions_per_task;
                *load += u128::from(bytes);
                *count += 1;
            }
            assigned += 1;
            if assigned % 256 == 0 {
                tokio::task::yield_now().await;
            }
        }
        Ok(Self {
            partitions,
            _reservation: reservation,
        })
    }

    pub(crate) fn partition(&self, index: usize) -> usize {
        self.partitions[index]
    }
}
