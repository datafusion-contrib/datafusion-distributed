use datafusion::common::{Result, plan_err};
use datafusion::physical_expr::Partitioning;
use std::ops::Range;
use std::sync::Arc;

/// Runtime read plan for one consumer-local output slot.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SlotReadPlan {
    /// Read one partition from every producer task in the range.
    Fanout {
        producer_tasks: Range<usize>,
        producer_partition: usize,
    },
    /// Read one concrete partition from one concrete producer task.
    Single {
        producer_task: usize,
        producer_partition: usize,
    },
}

/// Planner-owned metadata for a shuffle exchange.
#[derive(Debug, Clone, PartialEq)]
pub struct ShuffleExchangeLayout {
    pub(crate) producer_task_count: usize,
    pub(crate) consumer_task_count: usize,
    pub(crate) producer_partitioning: Partitioning,
    pub(crate) consumer_partition_ranges: Vec<Range<usize>>,
}

impl ShuffleExchangeLayout {
    fn try_new(
        producer_partitioning: Partitioning,
        producer_task_count: usize,
        consumer_task_count: usize,
    ) -> Result<Self> {
        let logical_partition_count = producer_partitioning.partition_count();
        if consumer_task_count == 0 {
            return plan_err!("shuffle exchange requires consumer_task_count > 0");
        }
        if logical_partition_count > 0 && consumer_task_count > logical_partition_count {
            return plan_err!(
                "shuffle exchange requires consumer_task_count <= logical_partition_count, got {} > {}",
                consumer_task_count,
                logical_partition_count
            );
        }

        Ok(Self {
            producer_task_count,
            consumer_task_count,
            producer_partitioning,
            consumer_partition_ranges: split_ranges(logical_partition_count, consumer_task_count),
        })
    }

    fn partitions_per_producer_task(&self) -> usize {
        self.producer_partitioning.partition_count()
    }

    fn max_partition_count_per_consumer(&self) -> usize {
        self.consumer_partition_ranges
            .iter()
            .map(|range| range.len())
            .max()
            .unwrap_or(0)
    }

    fn logical_slot_count(&self) -> usize {
        self.producer_partitioning.partition_count()
    }

    fn producer_task_range(&self, _consumer_task_idx: usize) -> Range<usize> {
        0..self.producer_task_count
    }

    fn resolve_slot(
        &self,
        consumer_task_idx: usize,
        local_partition_idx: usize,
    ) -> Option<SlotReadPlan> {
        let producer_partition = self
            .consumer_partition_ranges
            .get(consumer_task_idx)?
            .clone()
            .nth(local_partition_idx)?;
        Some(SlotReadPlan::Fanout {
            producer_tasks: self.producer_task_range(consumer_task_idx),
            producer_partition,
        })
    }
}

/// Planner-owned metadata for a coalesce exchange.
#[derive(Debug, Clone, PartialEq)]
pub struct CoalesceExchangeLayout {
    pub(crate) producer_task_count: usize,
    pub(crate) consumer_task_count: usize,
    pub(crate) partitions_per_producer_task: usize,
    pub(crate) producer_task_ranges: Vec<Range<usize>>,
    pub(crate) consumer_slot_ranges: Vec<Range<usize>>,
}

impl CoalesceExchangeLayout {
    fn try_new(
        producer_task_count: usize,
        consumer_task_count: usize,
        partitions_per_producer_task: usize,
    ) -> Result<Self> {
        if consumer_task_count == 0 {
            return plan_err!("coalesce exchange requires consumer_task_count > 0");
        }
        if partitions_per_producer_task == 0 {
            return plan_err!("coalesce exchange requires partitions_per_producer_task > 0");
        }

        let producer_task_ranges = split_ranges(producer_task_count, consumer_task_count);
        let consumer_slot_ranges = producer_task_ranges
            .iter()
            .map(|task_range| {
                let start = task_range.start * partitions_per_producer_task;
                let end = task_range.end * partitions_per_producer_task;
                start..end
            })
            .collect();

        Ok(Self {
            producer_task_count,
            consumer_task_count,
            partitions_per_producer_task,
            producer_task_ranges,
            consumer_slot_ranges,
        })
    }

    fn max_partition_count_per_consumer(&self) -> usize {
        self.consumer_slot_ranges
            .iter()
            .map(|range| range.len())
            .max()
            .unwrap_or(0)
    }

    fn logical_slot_count(&self) -> usize {
        self.consumer_slot_ranges
            .last()
            .map(|range| range.end)
            .unwrap_or(0)
    }

    fn producer_task_range(&self, consumer_task_idx: usize) -> Range<usize> {
        self.producer_task_ranges[consumer_task_idx].clone()
    }

    fn resolve_slot(
        &self,
        consumer_task_idx: usize,
        local_partition_idx: usize,
    ) -> Option<SlotReadPlan> {
        let global_slot = self
            .consumer_slot_ranges
            .get(consumer_task_idx)?
            .clone()
            .nth(local_partition_idx)?;
        Some(SlotReadPlan::Single {
            producer_task: global_slot / self.partitions_per_producer_task,
            producer_partition: global_slot % self.partitions_per_producer_task,
        })
    }
}

/// Planner-owned metadata for a broadcast exchange.
#[derive(Debug, Clone, PartialEq)]
pub struct BroadcastExchangeLayout {
    pub(crate) producer_task_count: usize,
    pub(crate) consumer_task_count: usize,
    pub(crate) partitions_per_producer_task: usize,
    pub(crate) consumer_partition_ranges: Vec<Range<usize>>,
}

impl BroadcastExchangeLayout {
    fn try_new(
        producer_task_count: usize,
        consumer_task_count: usize,
        partitions_per_producer_task: usize,
    ) -> Result<Self> {
        if consumer_task_count == 0 {
            return plan_err!("broadcast exchange requires consumer_task_count > 0");
        }
        if partitions_per_producer_task == 0 {
            return plan_err!("broadcast exchange requires partitions_per_producer_task > 0");
        }
        if partitions_per_producer_task % consumer_task_count != 0 {
            return plan_err!(
                "broadcast exchange requires partitions_per_producer_task to divide consumer_task_count evenly, got {} and {}",
                partitions_per_producer_task,
                consumer_task_count
            );
        }

        Ok(Self {
            producer_task_count,
            consumer_task_count,
            partitions_per_producer_task,
            consumer_partition_ranges: split_ranges(
                partitions_per_producer_task,
                consumer_task_count,
            ),
        })
    }

    fn max_partition_count_per_consumer(&self) -> usize {
        self.consumer_partition_ranges
            .iter()
            .map(|range| range.len())
            .max()
            .unwrap_or(0)
    }

    fn logical_slot_count(&self) -> usize {
        self.partitions_per_producer_task
    }

    fn producer_task_range(&self, _consumer_task_idx: usize) -> Range<usize> {
        0..self.producer_task_count
    }

    fn resolve_slot(
        &self,
        consumer_task_idx: usize,
        local_partition_idx: usize,
    ) -> Option<SlotReadPlan> {
        let producer_partition = self
            .consumer_partition_ranges
            .get(consumer_task_idx)?
            .clone()
            .nth(local_partition_idx)?;
        Some(SlotReadPlan::Fanout {
            producer_tasks: self.producer_task_range(consumer_task_idx),
            producer_partition,
        })
    }
}

/// Durable network-boundary metadata chosen by the planner.
///
/// Layouts are serialized, rebuilt when children change partitioning, and shared across
/// planning and decoding. They do not answer execution-time routing questions directly.
#[derive(Debug, Clone, PartialEq)]
pub enum ExchangeLayout {
    Shuffle(ShuffleExchangeLayout),
    Coalesce(CoalesceExchangeLayout),
    Broadcast(BroadcastExchangeLayout),
}

impl ExchangeLayout {
    pub fn try_shuffle(
        producer_partitioning: Partitioning,
        producer_task_count: usize,
        consumer_task_count: usize,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self::Shuffle(ShuffleExchangeLayout::try_new(
            producer_partitioning,
            producer_task_count,
            consumer_task_count,
        )?)))
    }

    pub fn try_coalesce(
        producer_task_count: usize,
        consumer_task_count: usize,
        partitions_per_producer_task: usize,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self::Coalesce(CoalesceExchangeLayout::try_new(
            producer_task_count,
            consumer_task_count,
            partitions_per_producer_task,
        )?)))
    }

    pub fn try_broadcast(
        producer_task_count: usize,
        consumer_task_count: usize,
        partitions_per_producer_task: usize,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self::Broadcast(BroadcastExchangeLayout::try_new(
            producer_task_count,
            consumer_task_count,
            partitions_per_producer_task,
        )?)))
    }

    pub fn producer_task_count(&self) -> usize {
        match self {
            Self::Shuffle(layout) => layout.producer_task_count,
            Self::Coalesce(layout) => layout.producer_task_count,
            Self::Broadcast(layout) => layout.producer_task_count,
        }
    }

    pub fn consumer_task_count(&self) -> usize {
        match self {
            Self::Shuffle(layout) => layout.consumer_task_count,
            Self::Coalesce(layout) => layout.consumer_task_count,
            Self::Broadcast(layout) => layout.consumer_task_count,
        }
    }

    pub fn producer_partitioning(&self) -> Option<&Partitioning> {
        match self {
            Self::Shuffle(layout) => Some(&layout.producer_partitioning),
            Self::Coalesce(_) | Self::Broadcast(_) => None,
        }
    }

    pub fn partitions_per_producer_task(&self) -> usize {
        match self {
            Self::Shuffle(layout) => layout.partitions_per_producer_task(),
            Self::Coalesce(layout) => layout.partitions_per_producer_task,
            Self::Broadcast(layout) => layout.partitions_per_producer_task,
        }
    }

    pub fn consumer_partition_range(&self, consumer_task_idx: usize) -> &Range<usize> {
        match self {
            Self::Shuffle(layout) => &layout.consumer_partition_ranges[consumer_task_idx],
            Self::Coalesce(layout) => &layout.consumer_slot_ranges[consumer_task_idx],
            Self::Broadcast(layout) => &layout.consumer_partition_ranges[consumer_task_idx],
        }
    }

    pub fn max_partition_count_per_consumer(&self) -> usize {
        match self {
            Self::Shuffle(layout) => layout.max_partition_count_per_consumer(),
            Self::Coalesce(layout) => layout.max_partition_count_per_consumer(),
            Self::Broadcast(layout) => layout.max_partition_count_per_consumer(),
        }
    }

    pub fn logical_slot_count(&self) -> usize {
        match self {
            Self::Shuffle(layout) => layout.logical_slot_count(),
            Self::Coalesce(layout) => layout.logical_slot_count(),
            Self::Broadcast(layout) => layout.logical_slot_count(),
        }
    }

    /// Creates the runtime resolver view for this layout.
    pub(crate) fn resolver(&self) -> ExchangeResolver<'_> {
        ExchangeResolver { layout: self }
    }
}

/// Execution-time resolver derived from an [ExchangeLayout].
///
/// Resolvers answer routing questions such as “which producer task(s) and partition(s) should
/// this consumer-local slot read from?” without becoming part of the serialized plan.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExchangeResolver<'a> {
    layout: &'a ExchangeLayout,
}

impl ExchangeResolver<'_> {
    pub(crate) fn consumer_task_count(&self) -> usize {
        self.layout.consumer_task_count()
    }

    pub(crate) fn partitions_per_producer_task(&self) -> usize {
        self.layout.partitions_per_producer_task()
    }

    pub(crate) fn consumer_partition_range(&self, consumer_task_idx: usize) -> &Range<usize> {
        self.layout.consumer_partition_range(consumer_task_idx)
    }

    pub(crate) fn producer_task_range(&self, consumer_task_idx: usize) -> Range<usize> {
        match self.layout {
            ExchangeLayout::Shuffle(layout) => layout.producer_task_range(consumer_task_idx),
            ExchangeLayout::Coalesce(layout) => layout.producer_task_range(consumer_task_idx),
            ExchangeLayout::Broadcast(layout) => layout.producer_task_range(consumer_task_idx),
        }
    }

    /// Resolves one consumer-local slot to the concrete upstream read plan.
    pub(crate) fn resolve_slot(
        &self,
        consumer_task_idx: usize,
        local_partition_idx: usize,
    ) -> Option<SlotReadPlan> {
        match self.layout {
            ExchangeLayout::Shuffle(layout) => {
                layout.resolve_slot(consumer_task_idx, local_partition_idx)
            }
            ExchangeLayout::Coalesce(layout) => {
                layout.resolve_slot(consumer_task_idx, local_partition_idx)
            }
            ExchangeLayout::Broadcast(layout) => {
                layout.resolve_slot(consumer_task_idx, local_partition_idx)
            }
        }
    }
}

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
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::Column;
    use std::sync::Arc as StdArc;

    fn col(name: &str, idx: usize) -> StdArc<dyn PhysicalExpr> {
        StdArc::new(Column::new(name, idx))
    }

    #[test]
    fn shuffle_layout_splits_hash_partition_space() {
        let layout =
            ExchangeLayout::try_shuffle(Partitioning::Hash(vec![col("a", 0)], 17), 4, 8).unwrap();
        let resolver = layout.resolver();
        assert_eq!(layout.producer_task_count(), 4);
        assert_eq!(layout.consumer_task_count(), 8);
        assert_eq!(layout.partitions_per_producer_task(), 17);
        assert_eq!(layout.logical_slot_count(), 17);
        assert_eq!(layout.consumer_partition_range(0), &(0..3));
        assert_eq!(layout.consumer_partition_range(7), &(15..17));
        assert_eq!(resolver.producer_task_range(3), 0..4);
        assert_eq!(layout.max_partition_count_per_consumer(), 3);
        assert_eq!(
            resolver.resolve_slot(0, 0),
            Some(SlotReadPlan::Fanout {
                producer_tasks: 0..4,
                producer_partition: 0,
            })
        );
        assert_eq!(
            resolver.resolve_slot(0, 2),
            Some(SlotReadPlan::Fanout {
                producer_tasks: 0..4,
                producer_partition: 2,
            })
        );
        assert_eq!(
            resolver.resolve_slot(7, 1),
            Some(SlotReadPlan::Fanout {
                producer_tasks: 0..4,
                producer_partition: 16,
            })
        );
        assert_eq!(resolver.resolve_slot(7, 2), None);
    }

    #[test]
    fn shuffle_layout_rejects_more_consumers_than_hash_partitions() {
        let err = ExchangeLayout::try_shuffle(Partitioning::Hash(vec![col("a", 0)], 2), 2, 5)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("consumer_task_count <= logical_partition_count")
        );
    }

    #[test]
    fn coalesce_layout_tracks_task_groups_and_flattened_slots() {
        let layout = ExchangeLayout::try_coalesce(3, 2, 4).unwrap();
        let resolver = layout.resolver();
        assert_eq!(layout.producer_task_count(), 3);
        assert_eq!(layout.consumer_task_count(), 2);
        assert_eq!(layout.partitions_per_producer_task(), 4);
        assert_eq!(layout.logical_slot_count(), 12);
        assert_eq!(resolver.producer_task_range(0), 0..2);
        assert_eq!(resolver.producer_task_range(1), 2..3);
        assert_eq!(layout.consumer_partition_range(0), &(0..8));
        assert_eq!(layout.consumer_partition_range(1), &(8..12));
        assert_eq!(layout.max_partition_count_per_consumer(), 8);
        assert_eq!(
            resolver.resolve_slot(0, 0),
            Some(SlotReadPlan::Single {
                producer_task: 0,
                producer_partition: 0,
            })
        );
        assert_eq!(
            resolver.resolve_slot(0, 3),
            Some(SlotReadPlan::Single {
                producer_task: 0,
                producer_partition: 3,
            })
        );
        assert_eq!(
            resolver.resolve_slot(0, 4),
            Some(SlotReadPlan::Single {
                producer_task: 1,
                producer_partition: 0,
            })
        );
        assert_eq!(
            resolver.resolve_slot(1, 3),
            Some(SlotReadPlan::Single {
                producer_task: 2,
                producer_partition: 3,
            })
        );
        assert_eq!(resolver.resolve_slot(1, 4), None);
    }

    #[test]
    fn broadcast_layout_splits_shared_partition_space_across_consumers() {
        let layout = ExchangeLayout::try_broadcast(2, 3, 6).unwrap();
        let resolver = layout.resolver();
        assert_eq!(layout.producer_task_count(), 2);
        assert_eq!(layout.consumer_task_count(), 3);
        assert_eq!(layout.partitions_per_producer_task(), 6);
        assert_eq!(layout.logical_slot_count(), 6);
        assert_eq!(resolver.producer_task_range(0), 0..2);
        assert_eq!(layout.consumer_partition_range(0), &(0..2));
        assert_eq!(layout.consumer_partition_range(1), &(2..4));
        assert_eq!(layout.consumer_partition_range(2), &(4..6));
        assert_eq!(layout.max_partition_count_per_consumer(), 2);
        assert_eq!(
            resolver.resolve_slot(0, 0),
            Some(SlotReadPlan::Fanout {
                producer_tasks: 0..2,
                producer_partition: 0,
            })
        );
        assert_eq!(
            resolver.resolve_slot(1, 1),
            Some(SlotReadPlan::Fanout {
                producer_tasks: 0..2,
                producer_partition: 3,
            })
        );
        assert_eq!(resolver.resolve_slot(2, 2), None);
    }
}
