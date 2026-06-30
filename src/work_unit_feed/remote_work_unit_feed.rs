use crate::common::{now_ns, serialize_uuid};
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

pub type WorkUnitTx = UnboundedSender<Result<pb::WorkUnit>>;
pub type WorkUnitRx = UnboundedReceiver<Result<pb::WorkUnit>>;
pub type RemoteWorkUnitFeedRxs = HashMap<(Uuid, usize), Mutex<Option<WorkUnitRx>>>;
pub type RemoteWorkUnitFeedTxs = HashMap<(Uuid, usize), WorkUnitTx>;

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
pub struct RemoteWorkUnitFeedRegistry {
    pub receivers: RemoteWorkUnitFeedRxs,
    pub senders: RemoteWorkUnitFeedTxs,
}

impl RemoteWorkUnitFeedRegistry {
    /// Creates all the receivers and senders for a specific [WorkUnit] Feed id. One feed per
    /// partition is created.
    pub fn add(&mut self, id: Uuid, partitions: usize) {
        for partition in 0..partitions {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            self.receivers.insert((id, partition), Mutex::new(Some(rx)));
            self.senders.insert((id, partition), tx);
        }
    }
}

/// Encodes one feed element into a [pb::WorkUnit] for `partition` of feed `id`. The send/receive
/// stamps stay zero for the transport to fill in right before delivery.
pub(crate) fn build_work_unit(
    id: &Uuid,
    partition: usize,
    work_unit: Box<dyn WorkUnit>,
) -> pb::WorkUnit {
    pb::WorkUnit {
        id: serialize_uuid(id),
        partition: partition as u64,
        body: work_unit.encode_to_bytes(),
        created_timestamp_unix_nanos: now_ns(),
        sent_timestamp_unix_nanos: 0,
        received_timestamp_unix_nanos: 0,
        processed_timestamp_unix_nanos: 0,
    }
}

#[cfg(feature = "flight")]
pub(crate) fn build_work_unit_batch_msg(
    id: &Uuid,
    work_unit_batch: Vec<(usize, Result<Box<dyn WorkUnit>>)>,
) -> Result<pb::CoordinatorToWorkerMsg> {
    Ok(pb::CoordinatorToWorkerMsg {
        inner: Some(pb::coordinator_to_worker_msg::Inner::WorkUnitBatch(
            pb::WorkUnitBatch {
                batch: work_unit_batch
                    .into_iter()
                    .map(|(partition, work_unit)| Ok(build_work_unit(id, partition, work_unit?)))
                    .collect::<Result<_>>()?,
            },
        )),
    })
}

/// Stamps the send time on a bare unit. Any transport can stamp before delivery; the worker-side
/// latency math treats a missing stamp as zero latency.
pub fn set_sent_time(work_unit: &mut pb::WorkUnit) {
    work_unit.sent_timestamp_unix_nanos = now_ns();
}

/// Stamps the receive time on a bare unit. See [set_sent_time].
pub fn set_received_time(work_unit: &mut pb::WorkUnit) {
    work_unit.received_timestamp_unix_nanos = now_ns();
}

#[cfg(feature = "flight")]
pub(crate) fn set_work_unit_send_time(
    mut msg: pb::CoordinatorToWorkerMsg,
) -> pb::CoordinatorToWorkerMsg {
    if let pb::CoordinatorToWorkerMsg {
        inner: Some(pb::coordinator_to_worker_msg::Inner::WorkUnitBatch(work_unit_batch)),
    } = &mut msg
    {
        for work_unit in &mut work_unit_batch.batch {
            work_unit.sent_timestamp_unix_nanos = now_ns();
        }
    }
    msg
}

#[cfg(feature = "flight")]
pub(crate) fn set_work_unit_received_time(
    mut msg: pb::CoordinatorToWorkerMsg,
) -> pb::CoordinatorToWorkerMsg {
    if let pb::CoordinatorToWorkerMsg {
        inner: Some(pb::coordinator_to_worker_msg::Inner::WorkUnitBatch(work_unit_batch)),
    } = &mut msg
    {
        for work_unit in &mut work_unit_batch.batch {
            work_unit.received_timestamp_unix_nanos = now_ns();
        }
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
                work_unit.processed_timestamp_unix_nanos = now_ns();

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
