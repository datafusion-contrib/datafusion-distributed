use crate::execution_plans::SamplerExec;
use crate::{
    BroadcastExec, MaybeEncoded, NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec,
    Stage,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use std::sync::Arc;

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
    /// Called when a [Stage] is correctly formed. The [NetworkBoundary] can use this
    /// information to perform any internal transformations necessary for distributed execution.
    ///
    /// Typically, [NetworkBoundary]s will use this call for transitioning from "Pending" to "ready".
    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn NetworkBoundary>>;

    /// Returns the assigned input [Stage], if any.
    fn input_stage(&self) -> &Stage;

    /// Defines what head node should the producer stage feeding this [NetworkBoundary]
    /// implementation have. This information is used during planning an executing for ensuring
    /// the head of a stage has the appropriate shape for consumption. Returns an error when that
    /// shape cannot be constructed from the boundary's partitioning.
    fn producer_head(&self, consumer_tasks: usize) -> Result<ProducerHead>;
}

/// Defines what shape should the head node of a stage have upon getting executed. Depending
/// on the [NetworkBoundary] implementation, the stage below should have different head nodes.
#[derive(Clone)]
pub enum ProducerHead {
    /// No specific head node is necessary.
    None,
    /// The head node should be a [BroadcastExec] over a [CoalescePartitionsExec].
    BroadcastExec {
        output_partitions: usize,
        /// Limit applied by the [CoalescePartitionsExec] right below the [BroadcastExec]. A
        /// broadcast stage with a fetch always runs in a single task, so the limit stays global.
        fetch: Option<usize>,
    },
    /// The head node should be a [RepartitionExec].
    RepartitionExec {
        partitioning: MaybeEncoded<Partitioning>,
        /// Whether the producer must preserve its input's output ordering.
        preserve_order: bool,
    },
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

impl ProducerHead {
    pub(crate) fn ensure_decoded(self, schema: SchemaRef, ctx: &TaskContext) -> Result<Self> {
        Ok(match self {
            Self::RepartitionExec {
                partitioning,
                preserve_order,
            } => Self::RepartitionExec {
                partitioning: MaybeEncoded::Decoded(partitioning.decode(schema, ctx)?),
                preserve_order,
            },
            v => v,
        })
    }

    /// Ensures the head of the provided plan complies with the passed [ProducerHead] definition. This
    /// can be called both during planning and lazily at runtime.
    pub(crate) fn insert(self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let input = if let Some(r_exec) = input.downcast_ref::<RepartitionExec>() {
            Arc::clone(r_exec.input())
        } else if let Some(b_exec) = input.downcast_ref::<BroadcastExec>() {
            match b_exec.input().downcast_ref::<CoalescePartitionsExec>() {
                Some(cp_exec) => Arc::clone(cp_exec.input()),
                None => Arc::clone(b_exec.input()),
            }
        } else {
            input
        };
        let plan = match self {
            ProducerHead::None => input,
            ProducerHead::BroadcastExec {
                output_partitions,
                fetch,
            } => {
                let input = Arc::new(CoalescePartitionsExec::new(input).with_fetch(fetch));
                let partitions = input.properties().partitioning.partition_count();
                Arc::new(BroadcastExec::new(input, output_partitions / partitions))
            }
            ProducerHead::RepartitionExec {
                partitioning,
                preserve_order,
            } => {
                let repartition = RepartitionExec::try_new(input, partitioning.try_decoded()?)?;
                Arc::new(if preserve_order {
                    repartition.with_preserve_order()
                } else {
                    repartition
                })
            }
        };
        Ok(plan)
    }

    /// Injects a [SamplerExec] right below a [RepartitionExec] or [BroadcastExec].
    pub(crate) fn insert_sampler(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(r_exec) = input.downcast_ref::<RepartitionExec>() {
            let child = Arc::clone(r_exec.input());
            input.replace_children(
                vec![Arc::new(SamplerExec::new(child))],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
        } else if let Some(b_exec) = input.downcast_ref::<BroadcastExec>() {
            let b_input = Arc::clone(b_exec.input());
            let new_b_input = match b_input.downcast_ref::<CoalescePartitionsExec>() {
                Some(cp_exec) => {
                    let child = Arc::clone(cp_exec.input());
                    Arc::clone(&b_input).replace_children(
                        vec![Arc::new(SamplerExec::new(child))],
                        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                    )?
                }
                None => Arc::new(SamplerExec::new(b_input)),
            };
            input.replace_children(
                vec![new_b_input],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
        } else {
            Ok(input)
        }
    }
}
