use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::vec::IntoIter;

use iceberg::scan::FileScanTask;

// Largest file first; object identity breaks ties independently of manifest order.
type FilePriority<'a> = (u64, Reverse<&'a str>, Reverse<u64>, Reverse<usize>);
type TaskLoad = Reverse<(u128, usize, usize)>;

/// Destination partitions in file-stream order. The dispatcher validates nonzero
/// counts and their product before constructing either routing policy.
#[derive(Debug)]
pub(crate) enum FilePartitions {
    RoundRobin {
        next_file: usize,
        task_count: usize,
        partitions_per_task: usize,
    },
    Greedy(IntoIter<usize>),
}

impl FilePartitions {
    /// Build destinations in the original retained-file order, without reordering files.
    pub(crate) fn greedy(
        files: &[FileScanTask],
        task_count: usize,
        partitions_per_task: usize,
    ) -> Self {
        let mut order: BinaryHeap<FilePriority<'_>> = BinaryHeap::with_capacity(files.len());
        let mut partitions = Vec::with_capacity(files.len());
        for (index, file) in files.iter().enumerate() {
            order.push((
                file.length(),
                Reverse(file.data_file_path()),
                Reverse(file.start()),
                Reverse(index),
            ));
            partitions.push(0);
        }
        // Prefer fewer files, then task index, when byte loads tie.
        let mut loads: BinaryHeap<TaskLoad> = BinaryHeap::with_capacity(task_count);
        for task in 0..task_count {
            loads.push(Reverse((0_u128, 0_usize, task)));
        }
        while let Some((bytes, _, _, Reverse(index))) = order.pop() {
            {
                let mut least_loaded = loads.peek_mut().expect("validated nonzero task count");
                let Reverse((load, count, task)) = &mut *least_loaded;
                partitions[index] = *task * partitions_per_task + *count % partitions_per_task;
                *load += u128::from(bytes);
                *count += 1;
            }
        }
        Self::Greedy(partitions.into_iter())
    }
}

impl Iterator for FilePartitions {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::RoundRobin {
                next_file,
                task_count,
                partitions_per_task,
            } => {
                let i = *next_file;
                let partition = (i % *task_count) * *partitions_per_task
                    + (i / *task_count) % *partitions_per_task;
                *next_file += 1;
                if *next_file == *task_count * *partitions_per_task {
                    *next_file = 0;
                }
                Some(partition)
            }
            Self::Greedy(partitions) => partitions.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FilePartitions;

    #[test]
    fn round_robin_repeats_task_first_without_counter_overflow() {
        let partitions = FilePartitions::RoundRobin {
            next_file: 0,
            task_count: 3,
            partitions_per_task: 2,
        };
        assert_eq!(
            partitions.take(12).collect::<Vec<_>>(),
            [0, 2, 4, 1, 3, 5, 0, 2, 4, 1, 3, 5]
        );
        let mut partitions = FilePartitions::RoundRobin {
            next_file: usize::MAX - 1,
            task_count: 1,
            partitions_per_task: usize::MAX,
        };
        assert_eq!(partitions.next(), Some(usize::MAX - 1));
        assert_eq!(partitions.next(), Some(0));
    }
}
