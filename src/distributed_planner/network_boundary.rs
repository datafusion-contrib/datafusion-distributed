use crate::{BroadcastExec, NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec, Stage};
use datafusion::common::{Result, internal_err};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// Where a producer's output partition should be sent: which consumer task, and the local partition
/// index within that task's slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionRoute {
    pub consumer_task: usize,
    pub consumer_partition: usize,
}

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
    /// Called when a [Stage] is correctly formed. The [NetworkBoundary] can use this
    /// information to perform any internal transformations necessary for distributed execution.
    ///
    /// Typically, [NetworkBoundary]s will use this call for transitioning from "Pending" to "ready".
    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>>;

    /// Returns the assigned input [Stage], if any.
    fn input_stage(&self) -> &Stage;

    /// Defines what head node should the producer stage feeding this [NetworkBoundary]
    /// implementation have. This information is used during planning an executing for ensuring
    /// the head of a stage has the appropriate shape for consumption.
    fn producer_head(&self, consumer_tasks: usize) -> ProducerHead;

    /// `P_c`: how many partitions each consumer task reads in the sliced layout
    /// (`global = P_c * consumer_task + local`) that shuffle and broadcast reads use. Surfaced so a
    /// transport that has to place a produced partition does not re-derive it from node properties.
    /// Meaningless for [NetworkCoalesceExec], whose consumers read whole per-producer-task groups.
    fn partitions_per_consumer_task(&self) -> usize {
        self.properties().partitioning.partition_count()
    }

    /// Maps a producer output partition to the consumer task and the local partition within that
    /// task that reads it, for the `global = P_c * consumer_task + local` layout.
    ///
    /// Boundaries whose consumers do not read that layout must override this with an error; the
    /// default would silently misroute them. A zero-partition boundary is a planner bug, so it
    /// errors instead of routing everything to task `0`.
    fn route_partition(&self, output_partition: usize) -> Result<PartitionRoute> {
        let p_c = self.partitions_per_consumer_task();
        if p_c == 0 {
            return internal_err!(
                "cannot route output partition {output_partition}: the boundary reports 0 \
                 partitions per consumer task"
            );
        }
        Ok(PartitionRoute {
            consumer_task: output_partition / p_c,
            consumer_partition: output_partition % p_c,
        })
    }
}

/// Defines what shape should the head node of a stage have upon getting executed. Depending
/// on the [NetworkBoundary] implementation, the stage below should have different head nodes.
pub enum ProducerHead {
    /// No specific head node is necessary.
    None,
    /// The head node should be a [BroadcastExec].
    BroadcastExec { output_partitions: usize },
    /// The head node should be a [RepartitionExec].
    RepartitionExec { partitioning: Partitioning },
}

/// Extension trait for downcasting dynamic types to [NetworkBoundary].
pub trait NetworkBoundaryExt {
    /// Downcasts self to a [NetworkBoundary] if possible.
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary>;
    /// Returns whether self is a [NetworkBoundary] or not.
    fn is_network_boundary(&self) -> bool {
        self.as_network_boundary().is_some()
    }
}

impl NetworkBoundaryExt for dyn ExecutionPlan {
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary> {
        if let Some(node) = self.downcast_ref::<NetworkShuffleExec>() {
            Some(node)
        } else if let Some(node) = self.downcast_ref::<NetworkCoalesceExec>() {
            Some(node)
        } else if let Some(node) = self.downcast_ref::<NetworkBroadcastExec>() {
            Some(node)
        } else {
            None
        }
    }
}

/// Ensures the head of the provided plan complies with the passed [ProducerHead] definition. This
/// can be called both during planning and lazily at runtime.
pub(crate) fn insert_producer_head(
    input: Arc<dyn ExecutionPlan>,
    head: ProducerHead,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input = if let Some(r_exec) = input.downcast_ref::<RepartitionExec>() {
        Arc::clone(r_exec.input())
    } else if let Some(b_exec) = input.downcast_ref::<BroadcastExec>() {
        Arc::clone(b_exec.input())
    } else {
        input
    };
    let plan = match head {
        ProducerHead::None => input,
        ProducerHead::BroadcastExec { output_partitions } => {
            let partitions = input.output_partitioning().partition_count();
            Arc::new(BroadcastExec::new(input, output_partitions / partitions))
        }
        ProducerHead::RepartitionExec { partitioning } => {
            Arc::new(RepartitionExec::try_new(input, partitioning)?)
        }
    };
    Ok(plan)
}
