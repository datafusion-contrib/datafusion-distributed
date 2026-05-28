use crate::common::{now_nanos, serialize_uuid};
use crate::worker::generated::worker as pb;
use crate::{BytesMetricExt, LatencyMetricExt, WorkUnit};
use datafusion::common::{HashMap, Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::MetricBuilder;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_proto::protobuf::proto_error;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

pub(crate) type WorkUnitTx = UnboundedSender<Result<pb::WorkUnit>>;
pub(crate) type WorkUnitRx = UnboundedReceiver<Result<pb::WorkUnit>>;
pub(crate) type RemoteWorkUnitFeedRxs = HashMap<(Uuid, usize), Mutex<Option<WorkUnitRx>>>;
pub(crate) type RemoteWorkUnitFeedTxs = HashMap<(Uuid, usize), WorkUnitTx>;

/// Bridge between the worker's gRPC layer and the remote-variant
/// [`crate::WorkUnitFeed`]s installed in the deserialized plan.
///
/// One (sender, receiver) pair is created per `(feed id, partition)` when a new plan is
/// set on the worker:
/// - The **senders** are used by the [`crate::Worker`] gRPC handler to push the serialized
///   [`crate::WorkUnit`]s that arrive over the coordinator channel into the right queue.
/// - The **receivers** are consumed by the worker-side [`RemoteFeedProvider`] (the remote
///   variant of [`crate::WorkUnitFeed`]), which decodes the bytes back into the leaf plan's
///   concrete `T::WorkUnit` type so the leaf sees the same typed stream as it would in a
///   single-node execution.
#[derive(Default)]
pub(crate) struct RemoteWorkUnitFeedRegistry {
    pub(crate) receivers: RemoteWorkUnitFeedRxs,
    pub(crate) senders: RemoteWorkUnitFeedTxs,
}

impl RemoteWorkUnitFeedRegistry {
    /// Creates all the receivers and senders for a specific [WorkUnit] Feed id. One feed per
    /// partition is created.
    pub(crate) fn add(&mut self, id: Uuid, partitions: usize) {
        for partition in 0..partitions {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            self.receivers.insert((id, partition), Mutex::new(Some(rx)));
            self.senders.insert((id, partition), tx);
        }
    }
}

pub(crate) fn build_work_unit_msg(
    id: &Uuid,
    partition: usize,
    work_unit: Box<dyn WorkUnit>,
) -> pb::CoordinatorToWorkerMsg {
    pb::CoordinatorToWorkerMsg {
        inner: Some(pb::coordinator_to_worker_msg::Inner::WorkUnit(
            pb::WorkUnit {
                id: serialize_uuid(id),
                partition: partition as u64,
                body: work_unit.encode_to_bytes(),
                created_timestamp_unix_nanos: now_nanos(),
                sent_timestamp_unix_nanos: 0,
                received_timestamp_unix_nanos: 0,
                processed_timestamp_unix_nanos: 0,
            },
        )),
    }
}

pub(crate) fn set_work_unit_send_time(
    mut msg: pb::CoordinatorToWorkerMsg,
) -> pb::CoordinatorToWorkerMsg {
    if let pb::CoordinatorToWorkerMsg {
        inner: Some(pb::coordinator_to_worker_msg::Inner::WorkUnit(work_unit)),
    } = &mut msg
    {
        work_unit.sent_timestamp_unix_nanos = now_nanos();
    }
    msg
}

pub(crate) fn set_work_unit_received_time(
    mut msg: pb::CoordinatorToWorkerMsg,
) -> pb::CoordinatorToWorkerMsg {
    if let pb::CoordinatorToWorkerMsg {
        inner: Some(pb::coordinator_to_worker_msg::Inner::WorkUnit(work_unit)),
    } = &mut msg
    {
        work_unit.received_timestamp_unix_nanos = now_nanos();
    }
    msg
}

/// Remove implementation of a [WorkUnitFeedProvider] that pulls [crate::WorkUnit]s coming over
/// the wire from a [RemoteWorkUnitFeedRegistry].
///
/// Deserializing a [crate::WorkUnitFeed] with [crate::WorkUnitFeed::from_proto] always returns a
/// [crate::WorkUnitFeed<RemoteFeedProvider>] that will receive messages over the network, rather
/// than executing the original [WorkUnitFeedProvider] locally.
///
/// There's a diagram about how this works in [crate::WorkUnitFeed].
#[derive(Debug, Clone)]
pub(crate) struct RemoteFeedProvider {
    pub(crate) id: Uuid,
    pub(crate) metrics: ExecutionPlanMetricsSet,
}

impl RemoteFeedProvider {
    pub(crate) fn feed<T: WorkUnit + Default>(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<T>>> {
        let bdr = || MetricBuilder::new(&self.metrics);

        let bytes_transferred = bdr().bytes_counter("work_unit_bytes");
        let msg_count = bdr().global_counter("work_unit_count");
        // Track end-to-end network latency distribution for all work units.
        let send_latency_max = bdr().max_latency("work_unit_send_latency_max");
        let send_latency_p50 = bdr().p50_latency("work_unit_send_latency_p50");

        let received_latency_max = bdr().max_latency("work_unit_received_latency_max");
        let received_latency_p50 = bdr().p50_latency("work_unit_received_latency_p50");

        let processed_latency_max = bdr().max_latency("work_unit_processed_latency_max");
        let processed_latency_p50 = bdr().p50_latency("work_unit_processed_latency_p50");

        let elapsed_compute = bdr().elapsed_compute(partition);

        let Some(rxs) = ctx
            .session_config()
            .get_extension::<RemoteWorkUnitFeedRxs>()
        else {
            return exec_err!("Missing RemoteWorkUnitFeedRegistry in context");
        };

        let id = self.id;
        let Some(remote_feed) = rxs.get(&(id, partition)) else {
            return exec_err!(
                "Missing WorkUnit feed for id {id} and partition {partition}. Was the WorkUnitFeed registered with DistributedExt::with_distributed_work_unit_feed?"
            );
        };

        let Some(receiver) = std::mem::take(&mut *remote_feed.lock().unwrap()) else {
            return exec_err!(
                "WorkUnit feed for id {id} and partition {partition} was already consumed"
            );
        };

        Ok(UnboundedReceiverStream::new(receiver)
            .map(move |work_unit_or_err| {
                let mut work_unit = work_unit_or_err?;
                let timer = elapsed_compute.timer();
                let result = T::decode(work_unit.body.as_slice())
                    .map_err(|err| proto_error(format!("{err}")));
                timer.done();
                work_unit.processed_timestamp_unix_nanos = now_nanos();

                let pb::WorkUnit {
                    created_timestamp_unix_nanos: base,
                    sent_timestamp_unix_nanos,
                    received_timestamp_unix_nanos,
                    processed_timestamp_unix_nanos,
                    body,
                    ..
                } = work_unit;

                bytes_transferred.add_bytes(body.len());
                msg_count.add(1);

                send_latency_max.add_nanos((sent_timestamp_unix_nanos - base) as usize);
                send_latency_p50.add_nanos((sent_timestamp_unix_nanos - base) as usize);

                received_latency_max.add_nanos((received_timestamp_unix_nanos - base) as usize);
                received_latency_p50.add_nanos((received_timestamp_unix_nanos - base) as usize);

                processed_latency_max.add_nanos((processed_timestamp_unix_nanos - base) as usize);
                processed_latency_p50.add_nanos((processed_timestamp_unix_nanos - base) as usize);

                result
            })
            .boxed())
    }

    pub(crate) fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.metrics.clone()
    }
}
