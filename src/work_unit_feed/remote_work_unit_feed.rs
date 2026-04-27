use crate::WorkUnitFeedProvider;
use datafusion::common::exec_err;
use datafusion::execution::TaskContext;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

pub(crate) type WorkUnitTx = UnboundedSender<datafusion::common::Result<Vec<u8>>>;
pub(crate) type WorkUnitRx = UnboundedReceiver<datafusion::common::Result<Vec<u8>>>;
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
}

impl WorkUnitFeedProvider for RemoteFeedProvider {
    type WorkUnit = Vec<u8>;

    fn feed(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> datafusion::common::Result<BoxStream<'static, datafusion::common::Result<Self::WorkUnit>>>
    {
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

        Ok(UnboundedReceiverStream::new(receiver).boxed())
    }
}
