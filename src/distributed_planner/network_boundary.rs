use crate::execution_plans::SamplerExec;
use crate::protocol::ProducerHeadSpec;
use crate::{
    BroadcastExec, DistributedCodec, NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec,
    Stage,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{DefaultPhysicalProtoConverter, PhysicalPlanDecodeContext};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
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
    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn NetworkBoundary>>;

    /// Returns the assigned input [Stage], if any.
    fn input_stage(&self) -> &Stage;

    /// Defines what head node should the producer stage feeding this [NetworkBoundary]
    /// implementation have. This information is used during planning an executing for ensuring
    /// the head of a stage has the appropriate shape for consumption.
    fn producer_head(&self, consumer_tasks: usize) -> ProducerHead;

    /// Maps a producer output partition to the consumer task and the local partition within that
    /// task that reads it, for the sliced layout shuffle and broadcast reads use
    /// (`global = P_c * consumer_task + local`, where `P_c` is the boundary's own per-task output
    /// partition count). A pull-based transport never needs this: its consumers compute their own
    /// slice inside the boundary's `execute`. A push-based transport places every produced
    /// partition before any consumer asks, so it reads the layout here instead of re-deriving it
    /// and drifting when the layout changes.
    ///
    /// Boundaries whose consumers do not read that layout must override this with an error; the
    /// default would silently misroute them. A zero-partition boundary is a planner bug, so it
    /// errors instead of routing everything to task `0`.
    fn route_partition(&self, output_partition: usize) -> Result<PartitionRoute> {
        let p_c = self.properties().partitioning.partition_count();
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

impl ProducerHead {
    pub(crate) fn to_spec(&self, cfg: &SessionConfig) -> Result<ProducerHeadSpec> {
        match self {
            Self::None => Ok(ProducerHeadSpec::None),
            Self::BroadcastExec { output_partitions } => Ok(ProducerHeadSpec::BroadcastExec {
                output_partitions: *output_partitions,
            }),
            Self::RepartitionExec { partitioning } => {
                let partitioning = serialize_partitioning(
                    partitioning,
                    &DistributedCodec::new_combined_with_user(cfg),
                    &DefaultPhysicalProtoConverter {},
                )
                .map(|v| v.encode_to_vec())?;
                Ok(ProducerHeadSpec::RepartitionExec { partitioning })
            }
        }
    }

    pub(crate) fn from_spec(
        spec: &ProducerHeadSpec,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Self> {
        match spec {
            ProducerHeadSpec::None => Ok(Self::None),
            ProducerHeadSpec::BroadcastExec { output_partitions } => Ok(Self::BroadcastExec {
                output_partitions: *output_partitions,
            }),
            ProducerHeadSpec::RepartitionExec { partitioning } => {
                let proto_partitioning = protobuf::Partitioning::decode(partitioning.as_slice())
                    .map_err(|e| proto_error(e.to_string()))?;
                let codec = DistributedCodec::new_combined_with_user(ctx.session_config());
                let decode_ctx = PhysicalPlanDecodeContext::new(ctx, &codec);
                let partitioning = parse_protobuf_partitioning(
                    Some(&proto_partitioning),
                    &decode_ctx,
                    &schema,
                    &DefaultPhysicalProtoConverter {},
                )?
                .ok_or_else(|| proto_error("Could not parse partitioning"))?;
                Ok(Self::RepartitionExec { partitioning })
            }
        }
    }

    /// Ensures the head of the provided plan complies with the passed [ProducerHead] definition. This
    /// can be called both during planning and lazily at runtime.
    pub(crate) fn insert(self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let input = if let Some(r_exec) = input.downcast_ref::<RepartitionExec>() {
            Arc::clone(r_exec.input())
        } else if let Some(b_exec) = input.downcast_ref::<BroadcastExec>() {
            Arc::clone(b_exec.input())
        } else {
            input
        };
        let plan = match self {
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

    /// Injects a [SamplerExec] right below a [RepartitionExec] or [BroadcastExec].
    pub(crate) fn insert_sampler(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(r_exec) = input.downcast_ref::<RepartitionExec>() {
            let child = Arc::clone(r_exec.input());
            input.with_new_children(vec![Arc::new(SamplerExec::new(child))])
        } else if let Some(b_exec) = input.downcast_ref::<BroadcastExec>() {
            let child = Arc::clone(b_exec.input());
            input.with_new_children(vec![Arc::new(SamplerExec::new(child))])
        } else {
            Ok(input)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::InProcessChannelResolver;
    use crate::{DistributedExt, NetworkBoundaryExt, SessionStateBuilderExt, WorkerResolver};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::error::DataFusionError;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use std::io::Write;
    use url::Url;

    struct Workers(usize);

    impl WorkerResolver for Workers {
        fn get_urls(&self) -> Result<Vec<Url>> {
            (0..self.0)
                .map(|i| Url::parse(&format!("http://worker-{i}")))
                .collect::<Result<_, _>>()
                .map_err(|err| DataFusionError::External(Box::new(err)))
        }
    }

    /// Pins the sliced routing (`global = P_c * consumer_task + local`) on boundaries the
    /// planner actually built, with `P_c` read off the boundary's own properties, so an override
    /// or a change in what `properties()` reports fails here first. The data-level guarantee
    /// that the slicing matches what consumers read comes from the in-process transport's
    /// end-to-end test. `NetworkCoalesceExec` must refuse to route: its consumers read whole
    /// per-producer-task groups, and the sliced formula would misroute them.
    #[tokio::test]
    async fn route_partition_matches_the_consumer_slicing() -> Result<()> {
        let path = std::env::temp_dir().join(format!("dfd_routing_{}.csv", std::process::id()));
        let mut file =
            std::fs::File::create(&path).map_err(|e| DataFusionError::External(Box::new(e)))?;
        writeln!(file, "k,v").unwrap();
        for i in 0..200 {
            writeln!(file, "{},{}", ["a", "b", "c", "d"][i % 4], i).unwrap();
        }
        drop(file);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_distributed_planner()
            .with_distributed_worker_resolver(Workers(4))
            .with_distributed_channel_resolver(InProcessChannelResolver::default())
            .with_distributed_file_scan_config_bytes_per_partition(1)
            .unwrap()
            .build();
        let ctx = SessionContext::from(state);
        ctx.register_csv("t", path.to_str().unwrap(), CsvReadOptions::new())
            .await?;
        let physical = ctx
            .sql("SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k")
            .await?
            .create_physical_plan()
            .await?;

        let mut sliced = 0usize;
        let mut grouped = 0usize;
        physical.apply(|node| {
            let Some(nb) = node.as_ref().as_network_boundary() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            if node
                .as_ref()
                .downcast_ref::<NetworkCoalesceExec>()
                .is_some()
            {
                assert!(
                    nb.route_partition(0).is_err(),
                    "a per-task-group boundary must refuse the sliced routing"
                );
                grouped += 1;
                return Ok(TreeNodeRecursion::Continue);
            }
            let p_c = nb.properties().partitioning.partition_count();
            assert!(p_c > 0);
            for consumer_task in 0..3 {
                for local in 0..p_c {
                    let route = nb.route_partition(p_c * consumer_task + local)?;
                    assert_eq!(route.consumer_task, consumer_task);
                    assert_eq!(route.consumer_partition, local);
                }
            }
            sliced += 1;
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(sliced > 0, "the plan grew no sliced-layout boundary");
        assert!(grouped > 0, "the plan grew no per-task-group boundary");
        std::fs::remove_file(&path).ok();
        Ok(())
    }
}
