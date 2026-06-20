use crate::common::{TreeNodeExt, task_ctx_with_extension};
use crate::work_unit_feed::remote_work_unit_feed::build_work_unit;
use crate::worker::generated::worker as pb;
use crate::{DistributedConfig, DistributedTaskContext, DistributedWorkUnitFeedContext};
use datafusion::common::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::sync::Arc;

/// Drives every registered [crate::WorkUnitFeed] in `plan` for one task and returns its
/// per-partition work-unit streams, already encoded.
///
/// Transport-neutral: the Flight dispatch wraps each [pb::WorkUnit] in its envelope and pushes it
/// over the coordinator-to-worker gRPC stream; an in-process transport hands them straight to the
/// worker-side channels. Each unit carries its `(feed id, task-local partition)`; two tasks emit
/// identical pairs, so routing to the right task stays the transport's job, as with Flight's
/// per-task stream. A task owns a non-overlapping window of partition feeds, offset by its task
/// index, and each partition feed can be consumed once.
pub fn collect_task_work_unit_feeds(
    plan: &Arc<dyn ExecutionPlan>,
    ctx: &Arc<TaskContext>,
    task_index: usize,
    task_count: usize,
) -> Result<Vec<BoxStream<'static, Result<pb::WorkUnit>>>> {
    let d_cfg = DistributedConfig::from_config_options(ctx.session_config().options())?;
    let registry = &d_cfg.__private_work_unit_feed_registry;

    let d_ctx = DistributedTaskContext {
        task_index,
        task_count,
    };
    let mut streams = vec![];
    plan.apply_with_dt_ctx(d_ctx, |plan, d_ctx| {
        let Some(wuf) = registry.get_work_unit_feed(plan) else {
            return Ok(TreeNodeRecursion::Continue);
        };

        let partitions = plan.properties().partitioning.partition_count();
        let start_partition = partitions * d_ctx.task_index;
        let end_partition = start_partition + partitions;

        let dist_feed_ctx = DistributedWorkUnitFeedContext {
            fan_out_tasks: d_ctx.task_count,
        };
        let t_ctx = Arc::new(task_ctx_with_extension(ctx, dist_feed_ctx));
        let id = wuf.id();

        for (partition, feed_idx) in (start_partition..end_partition).enumerate() {
            let stream = wuf
                .feed(feed_idx, Arc::clone(&t_ctx))?
                .map(move |res| res.map(|wu| build_work_unit(&id, partition, wu)))
                .boxed();
            streams.push(stream);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(streams)
}
