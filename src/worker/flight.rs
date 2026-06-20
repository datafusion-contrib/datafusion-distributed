use crate::coordinator::FlightWorkerDispatch;
use crate::distributed_planner::ProducerHead;
use crate::stage::RemoteStage;
use crate::worker::transport::{WorkerConnection, WorkerDispatch, WorkerTransport};
use crate::worker::worker_connection_pool::{
    LocalWorkerConnection, LocalWorkerContext, RemoteWorkerConnection,
};
use datafusion::common::{Result, internal_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use std::ops::Range;
use std::sync::Arc;

/// The Arrow-Flight gRPC transport, used by default. The read side opens one bidirectional gRPC
/// stream per worker task and demultiplexes it into per-partition streams; the write side ships
/// plans over the coordinator-to-worker channel via [FlightWorkerDispatch].
#[derive(Default)]
pub struct FlightWorkerTransport;

impl WorkerTransport for FlightWorkerTransport {
    fn open(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        producer_head: ProducerHead,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn WorkerConnection>> {
        let Some(target_url) = input_stage.workers.get(target_task) else {
            return Err(internal_datafusion_err!(
                "input_stage.workers[{target_task}] out of range."
            ));
        };
        if let Some(lw_ctx) = ctx.session_config().get_extension::<LocalWorkerContext>()
            && &lw_ctx.self_url == target_url
        {
            // Reaching ourselves: pull from the local task registry instead of a gRPC round-trip.
            Ok(Box::new(LocalWorkerConnection::init(
                input_stage,
                target_partitions,
                target_task,
                producer_head,
                ctx,
                metrics,
            )?))
        } else {
            Ok(Box::new(RemoteWorkerConnection::init(
                input_stage,
                target_partitions,
                target_task,
                producer_head,
                ctx,
                metrics,
            )?))
        }
    }

    fn dispatcher(&self) -> Box<dyn WorkerDispatch> {
        Box::new(FlightWorkerDispatch::default())
    }
}
