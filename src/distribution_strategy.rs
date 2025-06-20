// A Grouper is used to group partitions.
pub trait Grouper {
    // group groups the number of partitions into a vec of groups.
    fn group(&self, num_partitions: usize) -> Vec<PartitionGroup>;
}

// PartitionGroup is a struct that represents a range of partitions from [start, end). This is
// more space efficient than a vector of u64s.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PartitionGroup {
    start: usize,
    end: usize,
}

impl PartitionGroup {
    // new creates a new PartitionGroup containing partitions in the range [start..end).
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    // start is the first in the range
    pub fn start(&self) -> usize {
        self.start
    }

    // end is the exclusive end partition in the range
    pub fn end(&self) -> usize {
        self.end
    }
}

// PartitionGrouper groups a number partitions together depending on a partition_group_size.
// Ex. 10 partitions with a group size of 3 will yield groups [(0..3), (3..6), (6..9), (9)].
// - A partition_group_size of 0 will panic
// - Grouping 0 partitions will result an empty vec
// - It's possible for bad groupings to exist. Ex. if the group size is 99 and there are 100
//   partitions, then you will get unbalanced partitions [(0..99), (99)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct PartitionGrouper {
    partition_group_size: usize,
}

impl PartitionGrouper {
    pub fn new(partition_group_size: usize) -> Self {
        assert!(
            partition_group_size > 0,
            "partition groups cannot be size 0"
        );
        PartitionGrouper {
            partition_group_size,
        }
    }
}

impl Grouper for PartitionGrouper {
    // group implements the Grouper trait
    fn group(&self, num_partitions: usize) -> Vec<PartitionGroup> {
        (0..num_partitions)
            .step_by(self.partition_group_size)
            .map(|start| {
                let end = std::cmp::min(start + self.partition_group_size, num_partitions);
                PartitionGroup { start, end }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_grouper_basic() {
        let grouper = PartitionGrouper::new(4);
        let groups = grouper.group(10);

        let expected = vec![
            PartitionGroup { start: 0, end: 4 },
            PartitionGroup { start: 4, end: 8 },
            PartitionGroup { start: 8, end: 10 },
        ];

        assert_eq!(groups, expected);
    }

    #[test]
    fn test_partition_grouper_uneven() {
        let grouper = PartitionGrouper::new(2);
        let groups = grouper.group(5);

        let expected = vec![
            PartitionGroup { start: 0, end: 2 },
            PartitionGroup { start: 2, end: 4 },
            PartitionGroup { start: 4, end: 5 },
        ];

        assert_eq!(groups, expected);
    }

    #[test]
    #[should_panic]
    fn test_invalid_group_size() {
        PartitionGrouper::new(0);
    }

    #[test]
    fn test_num_partitions_smaller_than_group_size() {
        let g = PartitionGrouper::new(2);
        let groups = g.group(1);
        let expected = vec![PartitionGroup { start: 0, end: 1 }];
        assert_eq!(groups, expected);
    }

    #[test]
    fn test_num_partitions_equal_to_group_size() {
        let g = PartitionGrouper::new(2);
        let groups = g.group(2);
        let expected = vec![PartitionGroup { start: 0, end: 2 }];
        assert_eq!(groups, expected);
    }

    #[test]
    fn test_zero_partitions_to_group() {
        let g = PartitionGrouper::new(2);
        let groups = g.group(0);
        let expected = vec![];
        assert_eq!(groups, expected);
    }
}
