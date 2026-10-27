/// Visit distributed tasks before their local partitions. Counts must be nonzero.
pub(crate) fn file_partition(index: usize, task_count: usize, partitions_per_task: usize) -> usize {
    (index % task_count) * partitions_per_task + (index / task_count) % partitions_per_task
}
