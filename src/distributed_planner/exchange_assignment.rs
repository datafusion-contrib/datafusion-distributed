//! Assignment rules for one network exchange boundary.
//!
//! Boundary planning decides where the boundary belongs and how many tasks run on each side.
//! Assignment then decides which producer task/partition each consumer-local output slot should
//! read. This module only owns the read-assignment decision. Static planning can build it in
//! `prepare_network_boundaries`, while adaptive planning can build the same assignment later after
//! it has selected the consumer task count.

use datafusion::common::{Result, plan_err};
use std::ops::Range;
use std::sync::Arc;

/// Upstream read target for one consumer-local output partition.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SlotReadPlan {
    /// Read the same partition from each producer task.
    Fanout {
        producer_tasks: Range<usize>,
        producer_partition: usize,
    },
    /// Read one partition from one producer task.
    Single {
        producer_task: usize,
        producer_partition: usize,
    },
}

/// Every consumer reads its assigned logical partition range from every producer task.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ShuffleExchangeAssignment {
    producer_task_count: usize,
    consumer_task_count: usize,
    partitions_per_consumer: usize,
}

impl ShuffleExchangeAssignment {
    fn resolve_slot(
        &self,
        consumer_task_idx: usize,
        local_partition_idx: usize,
    ) -> Option<SlotReadPlan> {
        if consumer_task_idx >= self.consumer_task_count
            || local_partition_idx >= self.partitions_per_consumer
        {
            return None;
        }

        Some(SlotReadPlan::Fanout {
            producer_tasks: 0..self.producer_task_count,
            producer_partition: consumer_task_idx * self.partitions_per_consumer
                + local_partition_idx,
        })
    }
}

/// Consumers divide producer tasks into contiguous groups and pad uneven groups with empty slots.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoalesceExchangeAssignment {
    producer_task_count: usize,
    consumer_task_count: usize,
    partitions_per_producer_task: usize,
    producer_task_ranges: Vec<Range<usize>>,
}

impl CoalesceExchangeAssignment {
    /// Maximum number of producer tasks assigned to any one consumer.
    fn max_input_task_count_per_consumer(&self) -> usize {
        self.producer_task_ranges
            .iter()
            .map(|range| range.len())
            .max()
            .unwrap_or(0)
    }

    /// Output partitions needed by every consumer, including padded empty slots.
    fn max_partition_count_per_consumer(&self) -> usize {
        self.max_input_task_count_per_consumer() * self.partitions_per_producer_task
    }

    fn resolve_slot(
        &self,
        consumer_task_idx: usize,
        local_partition_idx: usize,
    ) -> Option<SlotReadPlan> {
        let producer_task_range = self.producer_task_ranges.get(consumer_task_idx)?;
        let producer_task_offset = local_partition_idx / self.partitions_per_producer_task;
        let producer_partition = local_partition_idx % self.partitions_per_producer_task;
        let producer_task = producer_task_range.clone().nth(producer_task_offset)?;

        Some(SlotReadPlan::Single {
            producer_task,
            producer_partition,
        })
    }
}

/// Broadcast uses the same fanout shape as shuffle, but over broadcast-expanded partitions.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BroadcastExchangeAssignment {
    producer_task_count: usize,
    consumer_task_count: usize,
    partitions_per_consumer: usize,
}

impl BroadcastExchangeAssignment {
    fn resolve_slot(
        &self,
        consumer_task_idx: usize,
        local_partition_idx: usize,
    ) -> Option<SlotReadPlan> {
        if consumer_task_idx >= self.consumer_task_count
            || local_partition_idx >= self.partitions_per_consumer
        {
            return None;
        }

        Some(SlotReadPlan::Fanout {
            producer_tasks: 0..self.producer_task_count,
            producer_partition: consumer_task_idx * self.partitions_per_consumer
                + local_partition_idx,
        })
    }
}

/// Concrete read assignment for one prepared network boundary.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ExchangeAssignment {
    Shuffle(ShuffleExchangeAssignment),
    Coalesce(CoalesceExchangeAssignment),
    Broadcast(BroadcastExchangeAssignment),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExchangeAssignmentKind {
    Shuffle,
    Coalesce,
    Broadcast,
}

impl ExchangeAssignment {
    pub(crate) fn try_shuffle(
        producer_task_count: usize,
        consumer_task_count: usize,
        partitions_per_consumer: usize,
    ) -> Result<Arc<Self>> {
        if producer_task_count == 0 {
            return plan_err!("shuffle exchange requires producer_task_count > 0");
        }
        if consumer_task_count == 0 {
            return plan_err!("shuffle exchange requires consumer_task_count > 0");
        }
        if partitions_per_consumer == 0 {
            return plan_err!("shuffle exchange requires partitions_per_consumer > 0");
        }

        Ok(Arc::new(Self::Shuffle(ShuffleExchangeAssignment {
            producer_task_count,
            consumer_task_count,
            partitions_per_consumer,
        })))
    }

    pub(crate) fn try_coalesce(
        producer_task_count: usize,
        consumer_task_count: usize,
        partitions_per_producer_task: usize,
    ) -> Result<Arc<Self>> {
        if consumer_task_count == 0 {
            return plan_err!("coalesce exchange requires consumer_task_count > 0");
        }
        if partitions_per_producer_task == 0 {
            return plan_err!("coalesce exchange requires partitions_per_producer_task > 0");
        }

        Ok(Arc::new(Self::Coalesce(CoalesceExchangeAssignment {
            producer_task_count,
            consumer_task_count,
            partitions_per_producer_task,
            producer_task_ranges: split_ranges(producer_task_count, consumer_task_count),
        })))
    }

    pub(crate) fn try_broadcast(
        producer_task_count: usize,
        consumer_task_count: usize,
        partitions_per_consumer: usize,
    ) -> Result<Arc<Self>> {
        if producer_task_count == 0 {
            return plan_err!("broadcast exchange requires producer_task_count > 0");
        }
        if consumer_task_count == 0 {
            return plan_err!("broadcast exchange requires consumer_task_count > 0");
        }
        if partitions_per_consumer == 0 {
            return plan_err!("broadcast exchange requires partitions_per_consumer > 0");
        }

        Ok(Arc::new(Self::Broadcast(BroadcastExchangeAssignment {
            producer_task_count,
            consumer_task_count,
            partitions_per_consumer,
        })))
    }

    pub(crate) fn producer_task_count(&self) -> usize {
        match self {
            Self::Shuffle(assignment) => assignment.producer_task_count,
            Self::Coalesce(assignment) => assignment.producer_task_count,
            Self::Broadcast(assignment) => assignment.producer_task_count,
        }
    }

    pub(crate) fn kind(&self) -> ExchangeAssignmentKind {
        match self {
            Self::Shuffle(_) => ExchangeAssignmentKind::Shuffle,
            Self::Coalesce(_) => ExchangeAssignmentKind::Coalesce,
            Self::Broadcast(_) => ExchangeAssignmentKind::Broadcast,
        }
    }

    pub(crate) fn consumer_task_count(&self) -> usize {
        match self {
            Self::Shuffle(assignment) => assignment.consumer_task_count,
            Self::Coalesce(assignment) => assignment.consumer_task_count,
            Self::Broadcast(assignment) => assignment.consumer_task_count,
        }
    }

    pub(crate) fn max_partition_count_per_consumer(&self) -> usize {
        match self {
            Self::Shuffle(assignment) => assignment.partitions_per_consumer,
            Self::Coalesce(assignment) => assignment.max_partition_count_per_consumer(),
            Self::Broadcast(assignment) => assignment.partitions_per_consumer,
        }
    }

    pub(crate) fn partitions_per_producer_task(&self) -> usize {
        match self {
            Self::Shuffle(assignment) => {
                assignment.partitions_per_consumer * assignment.consumer_task_count
            }
            Self::Coalesce(assignment) => assignment.partitions_per_producer_task,
            Self::Broadcast(assignment) => {
                assignment.partitions_per_consumer * assignment.consumer_task_count
            }
        }
    }

    /// Returns the per-kind partition count needed to reconstruct this assignment.
    pub(crate) fn assignment_partition_count(&self) -> usize {
        match self {
            Self::Shuffle(assignment) => assignment.partitions_per_consumer,
            Self::Coalesce(assignment) => assignment.partitions_per_producer_task,
            Self::Broadcast(assignment) => assignment.partitions_per_consumer,
        }
    }

    /// Returns the advertised output partition IDs owned by one consumer task.
    pub(crate) fn partition_range_for_consumer(
        &self,
        consumer_task_idx: usize,
    ) -> Option<Range<usize>> {
        if consumer_task_idx >= self.consumer_task_count() {
            return None;
        }

        match self {
            Self::Shuffle(assignment) => {
                let start = consumer_task_idx * assignment.partitions_per_consumer;
                Some(start..start + assignment.partitions_per_consumer)
            }
            Self::Coalesce(_) => Some(0..self.partitions_per_producer_task()),
            Self::Broadcast(assignment) => {
                let start = consumer_task_idx * assignment.partitions_per_consumer;
                Some(start..start + assignment.partitions_per_consumer)
            }
        }
    }

    /// Maps a consumer-local output partition to the upstream data it must read.
    pub(crate) fn resolve_slot(
        &self,
        consumer_task_idx: usize,
        local_partition_idx: usize,
    ) -> Option<SlotReadPlan> {
        match self {
            Self::Shuffle(assignment) => {
                assignment.resolve_slot(consumer_task_idx, local_partition_idx)
            }
            Self::Coalesce(assignment) => {
                assignment.resolve_slot(consumer_task_idx, local_partition_idx)
            }
            Self::Broadcast(assignment) => {
                assignment.resolve_slot(consumer_task_idx, local_partition_idx)
            }
        }
    }
}

/// Splits producer task ids into contiguous consumer groups as evenly as possible.
fn split_ranges(total: usize, groups: usize) -> Vec<Range<usize>> {
    if groups == 0 {
        return Vec::new();
    }

    let base = total / groups;
    let extra = total % groups;
    let mut ranges = Vec::with_capacity(groups);
    let mut start = 0;

    for idx in 0..groups {
        let len = base + usize::from(idx < extra);
        ranges.push(start..start + len);
        start += len;
    }

    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shuffle_assignment_preserves_scaled_fanout() {
        let assignment = ExchangeAssignment::try_shuffle(3, 2, 4).unwrap();

        assert_eq!(assignment.producer_task_count(), 3);
        assert_eq!(assignment.consumer_task_count(), 2);
        assert_eq!(assignment.max_partition_count_per_consumer(), 4);
        assert_eq!(assignment.partitions_per_producer_task(), 8);
        assert_eq!(assignment.partition_range_for_consumer(1), Some(4..8));
        assert_eq!(
            assignment.resolve_slot(1, 2),
            Some(SlotReadPlan::Fanout {
                producer_tasks: 0..3,
                producer_partition: 6,
            })
        );
        assert_eq!(assignment.resolve_slot(1, 4), None);
    }

    #[test]
    fn coalesce_assignment_assigns_contiguous_task_groups() {
        let assignment = ExchangeAssignment::try_coalesce(3, 2, 4).unwrap();

        assert_eq!(assignment.producer_task_count(), 3);
        assert_eq!(assignment.consumer_task_count(), 2);
        assert_eq!(assignment.max_partition_count_per_consumer(), 8);
        assert_eq!(assignment.partitions_per_producer_task(), 4);
        assert_eq!(assignment.partition_range_for_consumer(0), Some(0..4));
        assert_eq!(
            assignment.resolve_slot(0, 4),
            Some(SlotReadPlan::Single {
                producer_task: 1,
                producer_partition: 0,
            })
        );
        assert_eq!(
            assignment.resolve_slot(1, 3),
            Some(SlotReadPlan::Single {
                producer_task: 2,
                producer_partition: 3,
            })
        );
        assert_eq!(assignment.resolve_slot(1, 4), None);
    }

    #[test]
    fn broadcast_assignment_preserves_scaled_fanout() {
        let assignment = ExchangeAssignment::try_broadcast(2, 3, 4).unwrap();

        assert_eq!(assignment.producer_task_count(), 2);
        assert_eq!(assignment.consumer_task_count(), 3);
        assert_eq!(assignment.max_partition_count_per_consumer(), 4);
        assert_eq!(assignment.partitions_per_producer_task(), 12);
        assert_eq!(assignment.partition_range_for_consumer(2), Some(8..12));
        assert_eq!(
            assignment.resolve_slot(2, 1),
            Some(SlotReadPlan::Fanout {
                producer_tasks: 0..2,
                producer_partition: 9,
            })
        );
        assert_eq!(assignment.resolve_slot(3, 0), None);
    }
}
