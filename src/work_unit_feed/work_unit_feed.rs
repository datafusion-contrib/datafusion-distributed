use crate::WorkUnitFeedProvider;
use crate::common::{deserialize_uuid, serialize_uuid};
use crate::work_unit_feed::remote_work_unit_feed::RemoteFeedProvider;
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::stream::BoxStream;
use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

/// The [WorkUnitFeed] is created with a user-provided [WorkUnitFeedProvider] and is embedded
/// in any custom [datafusion::physical_plan::ExecutionPlan] implementation as a field.
///
/// It exposes the [WorkUnitFeed::feed] method that users are expected to call in their
/// [datafusion::physical_plan::ExecutionPlan::execute] implementation, which provides a stream
/// of [crate::WorkUnit]s, representing individual units of work (e.g., file addresses) at runtime.
/// This is useful for when these units of work cannot be known at planning time, and are
/// expected to be discovered streamed at execution time instead, as the query makes progress.
///
/// The special thing about this structure, is that it automatically works under distributed
/// scenarios:
/// - The feeds are streamed from coordinator to workers, so the [WorkUnitFeedProvider::feed] method
///   is never called from a remote worker.
/// - When deserializing a plan containing a [WorkUnitFeed] in a remote worker, a gRPC remote
///   streaming version of the [WorkUnitFeed] is deserialized instead, streaming back the contents
///   from the original [WorkUnitFeed].
///
/// For the distributed layer to find the feed inside a leaf plan, register a getter
/// closure via [`crate::DistributedExt::set_distributed_work_unit_feed`].
///
/// Keep in mind that, while interacting with [WorkUnitFeed] within a node, there's no compile-time
/// guarantee that it will not be in "remote" mode, although it's guaranteed that this mode only
/// applies after the [datafusion::physical_plan::ExecutionPlan] has been deserialized.
///
/// Upon serializing or de-serializing a plan containing a [WorkUnitFeed], use the
/// [WorkUnitFeed::from_proto] and [WorkUnitFeed::to_proto] methods.
///
/// # Example of [WorkUnitFeed] in single-node
///
/// ```text
/// ┌──────────────────────────────────────────────────────┐
/// │                    ExecutionPlan                     │
/// │                                                      │
/// │                                                      │
/// │┌────────────────────────────────────────────────────┐│
/// ││                    WorkUnitFeed                    ││
/// ││ ┌───────────┐     ┌───────────┐     ┌───────────┐  ││
/// ││ │ .feed(0)  │     │ .feed(1)  │     │ .feed(2)  │  ││
/// ││ └────┬──────┘     └──┬────────┘     └──┬────────┘  ││
/// │└──────┼───────────────┼─────────────────┼───────────┘│  .─.
/// │┌──────┼─────────┐┌────┼───────────┐┌───.▼.──────────┐│ (   ) WorkUnit
/// ││      │P0       ││   .▼. P1       ││  (   )P2       ││  `─'  (e.g., a file address)
/// ││     .▼.        ││  (   )         ││   `┬'          ││
/// ││    (   )       ││   `┬'          ││    │           ││
/// ││     `─'        ││    │           ││   .▼.          ││
/// ││      │         ││   .▼.          ││  (   )         ││
/// ││      │         ││  (   )         ││   `┬'          ││
/// ││      │         ││   `─'          ││    │           ││
/// ││      ▼         ││    ▼           ││    ▼           ││
/// ││  processing... ││  processing... ││  processing... ││
/// ││      │         ││    │           ││    │           ││
/// ││      │         ││    │           ││    │           ││
/// │└──────┼─────────┘└────┼───────────┘└────┼───────────┘│
/// └───────┼───────────────┼─────────────────┼────────────┘
///   ┌─────▼─────┐     ┌───▼───────┐      ┌──▼────────┐
///   │RecordBatch│     │RecordBatch│      │RecordBatch│
///   └───────────┘     └───────────┘      └───────────┘
/// ```
///
///
/// # Example of [WorkUnitFeed] during distributed execution
///
/// ```text
///                                                                                                     ┌──────────────────┐
///                                                                                                     │Coordinating Stage│
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┴──────────────────┘
///                                                                                                                        │
/// │
///  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐│
/// ││                                                    WorkUnitFeed                                                    │
///  │  ┌───────────┐     ┌───────────┐     ┌───────────┐            ┌───────────┐      ┌───────────┐    ┌───────────┐    ││
/// ││  │ .feed(0)  │     │ .feed(1)  │     │ .feed(2)  │            │ .feed(3)  │      │ .feed(4)  │    │ .feed(5)  │    │
///  │  └────┬──────┘     └──┬────────┘     └──┬────────┘            └─────┬─────┘      └──┬────────┘    └───┬───────┘    ││
/// │└───────┼───────────────┼─────────────────┼───────────────────────────┼───────────────┼─────────────────┼────────────┘
///          │               │                 │                           │               │                .┴.            │
/// └ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ .┴. ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─(   )─ ─ ─ ─ ─ ─
///          │              .┴.                │                         (   )             │                `┬'
///          │             (   )               │                          `┬'              │                .┴.
///         .┴.             `┬'               .┴.                          │               │               (   )
///        (   )             │               (   )                         │              .┴.               `┬'
///         `┬'             .┴.               `┬'────────────┐             │             (   )               │┌────────────┐
///          │             (   )               ││  Worker 1  │             │              `┬'                ││  Worker 2  │
/// ┌ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ `┬' ─ ─ ─ ─ ─ ─ ─ ─│┴────────────┘    ┌ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─│┴────────────┘
///  ┌───────┼───────────────┼─────────────────┼────────────┐│     ┌───────┼───────────────┼─────────────────┼────────────┐│
/// ││       │            Exe│utionPlan        │            │     ││       │            Exe│utionPlan        │            │
///  │       │               │                 │            ││     │       │               │                 │            ││
/// ││┌──────┼───────────────┼─────────────────┼───────────┐│     ││┌──────┼───────────────┼─────────────────┼───────────┐│
///  ││      │          Remot│WorkUnitFeed     │           │││     ││      │          Remot│WorkUnitFeed     │           │││
/// │││ ┌────▼──────┐     ┌──▼────────┐     ┌──▼────────┐  ││     │││ ┌────▼──────┐     ┌──▼────────┐     ┌──▼────────┐  ││
///  ││ │ .feed(0)  │     │ .feed(1)  │     │ .feed(2)  │  │││     ││ │ .feed(0)  │     │ .feed(1)  │     │ .feed(2)  │  │││
/// │││ └────┬──────┘     └──┬────────┘     └──┬────────┘  ││     │││ └────┬──────┘     └──┬────────┘     └──┬────────┘  ││
///  │└──────┼───────────────┼─────────────────┼───────────┘││     │└──────┼───────────────┼─────────────────┼───────────┘││
/// ││       │               │                 │            │     ││       │               │                 │            │
///  │┌──────┼─────────┐┌────┼───────────┐┌───.▼.──────────┐││     │┌──────┼─────────┐┌────┼───────────┐┌───.▼.──────────┐││
/// │││      │P0       ││   .▼. P1       ││  (   )P2       ││     │││      │P0       ││   .▼. P1       ││  (   )P2       ││
///  ││     .▼.        ││  (   )         ││   `┬'          │││     ││     .▼.        ││  (   )         ││   `┬'          │││
/// │││    (   )       ││   `┬'          ││    │           ││     │││    (   )       ││   `┬'          ││    │           ││
///  ││     `─'        ││    ┼           ││   .▼.          │││     ││     `─'        ││    ┼           ││   .▼.          │││
/// │││      │         ││   .▼.          ││  (   )         ││     │││      │         ││   .▼.          ││  (   )         ││
///  ││      │         ││  (   )         ││   `┬'          │││     ││      │         ││  (   )         ││   `┬'          │││
/// │││      │         ││   `─'          ││    │           ││     │││      │         ││   `─'          ││    │           ││
///  ││      ▼         ││                ││    ▼           │││     ││      ▼         ││                ││    ▼           │││
/// │││  processing... ││  processing... ││  processing... ││     │││  processing... ││  processing... ││  processing... ││
///  ││      │         ││    │           ││    │           │││     ││      │         ││    │           ││    │           │││
/// │││      │         ││    │           ││    │           ││     │││      │         ││    │           ││    │           ││
///  │└──────┼─────────┘└────┼───────────┘└────┼───────────┘││     │└──────┼─────────┘└────┼───────────┘└────┼───────────┘││
/// │└───────┼───────────────┼─────────────────┼────────────┘     │└───────┼───────────────┼─────────────────┼────────────┘
///    ┌─────▼─────┐     ┌───▼───────┐      ┌──▼────────┐    │       ┌─────▼─────┐     ┌───▼───────┐      ┌──▼────────┐    │
/// │  │RecordBatch│     │RecordBatch│      │RecordBatch│         │  │RecordBatch│     │RecordBatch│      │RecordBatch│
///    └───────────┘     └───────────┘      └───────────┘    │       └───────────┘     └───────────┘      └───────────┘    │
/// └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─     └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// ```
#[derive(Debug, Clone)]
pub struct WorkUnitFeed<T: WorkUnitFeedProvider> {
    pub(crate) id: Uuid,
    pub(crate) provider: RemoteOrLocalProvider<T>,
}

#[derive(Debug, Clone)]
pub enum RemoteOrLocalProvider<T: WorkUnitFeedProvider> {
    Local(T),
    Remote(RemoteFeedProvider),
}

impl<T: WorkUnitFeedProvider> RemoteOrLocalProvider<T> {
    fn feed(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<T::WorkUnit>>> {
        match self {
            Self::Local(local) => local.feed(partition, ctx),
            Self::Remote(remote) => Ok(remote.feed::<T::WorkUnit>(partition, ctx)?),
        }
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        match self {
            Self::Local(local) => local.metrics(),
            Self::Remote(remote) => remote.metrics(),
        }
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WorkUnitFeedProto {
    #[prost(bytes, tag = "1")]
    pub id: Vec<u8>,
}

impl<T: WorkUnitFeedProvider> WorkUnitFeed<T> {
    /// Builds a new local [`WorkUnitFeed`] backed by the given `provider`. Store the
    /// resulting feed as a field of your leaf [`datafusion::physical_plan::ExecutionPlan`]
    /// and register a getter with [`crate::DistributedExt::set_distributed_work_unit_feed`]
    /// so the distributed layer can find it.
    pub fn new(provider: T) -> Self {
        Self {
            id: Uuid::new_v4(),
            provider: RemoteOrLocalProvider::Local(provider),
        }
    }

    /// Reconstructs a [`WorkUnitFeed`] from its serialized form. The resulting feed is in
    /// the **remote** variant — it will read work units off the network using the
    /// `RemoteWorkUnitFeedRegistry` installed in the worker's session config. Used by
    /// physical plan codecs when deserializing a plan on a worker.
    pub fn from_proto(proto: WorkUnitFeedProto) -> Result<Self> {
        let id = deserialize_uuid(&proto.id)?;
        Ok(WorkUnitFeed {
            id,
            provider: RemoteOrLocalProvider::Remote(RemoteFeedProvider {
                id,
                metrics: ExecutionPlanMetricsSet::new(),
            }),
        })
    }

    /// Serializes just the feed's identifier. The concrete provider is never sent over the
    /// wire — the coordinator keeps the local provider to produce work units, and the
    /// worker rebuilds a remote-variant feed via [`WorkUnitFeed::from_proto`] that reads
    /// from the network.
    pub fn to_proto(&self) -> WorkUnitFeedProto {
        WorkUnitFeedProto {
            id: serialize_uuid(&self.id),
        }
    }

    /// Consumes the feed and returns the user-provided [`WorkUnitFeedProvider`] if this
    /// feed is in the local variant. Returns an error if the feed is remote (i.e. we're on
    /// a worker and there is no local provider to extract).
    pub fn try_into_inner(self) -> Result<T> {
        match self.provider {
            RemoteOrLocalProvider::Local(local) => Ok(local),
            RemoteOrLocalProvider::Remote(_) => {
                internal_err!(
                    "Cannot get the inner local provider, as the remote variant was already set"
                )
            }
        }
    }

    /// Consumes the feed and returns the user-provided [`WorkUnitFeedProvider`] if this
    /// feed is in the local variant. Returns None otherwise.
    pub fn into_inner(self) -> Option<T> {
        match self.provider {
            RemoteOrLocalProvider::Local(local) => Some(local),
            RemoteOrLocalProvider::Remote(_) => None,
        }
    }

    /// Returns a reference to the inner [`WorkUnitFeedProvider`] if this feed is
    /// in the local variant. Returns None otherwise
    pub fn inner(&self) -> Option<&T> {
        match &self.provider {
            RemoteOrLocalProvider::Local(local) => Some(local),
            RemoteOrLocalProvider::Remote(_) => None,
        }
    }

    /// Returns a mutable reference to the inner [`WorkUnitFeedProvider`] if this feed is
    /// in the local variant. Returns None otherwise
    pub fn inner_mut(&mut self) -> Option<&mut T> {
        match &mut self.provider {
            RemoteOrLocalProvider::Local(local) => Some(local),
            RemoteOrLocalProvider::Remote(_) => None,
        }
    }

    /// Returns a reference to the inner [`WorkUnitFeedProvider`] if this feed is
    /// in the local variant. Returns an error if the feed is remote (i.e. we're on
    /// a worker and there is no local provider to extract).
    pub fn try_inner(&self) -> Result<&T> {
        match &self.provider {
            RemoteOrLocalProvider::Local(local) => Ok(local),
            RemoteOrLocalProvider::Remote(_) => {
                internal_err!(
                    "Cannot get the inner local provider, as the remote variant was already set"
                )
            }
        }
    }

    /// Returns a mutable reference to the inner [`WorkUnitFeedProvider`] if this feed is
    /// in the local variant. Returns an error if the feed is remote (i.e. we're on
    /// a worker and there is no local provider to extract).
    pub fn try_inner_mut(&mut self) -> Result<&mut T> {
        match &mut self.provider {
            RemoteOrLocalProvider::Local(local) => Ok(local),
            RemoteOrLocalProvider::Remote(_) => {
                internal_err!(
                    "Cannot get the inner local provider, as the remote variant was already set"
                )
            }
        }
    }

    /// Returns the per-partition stream of [`WorkUnit`]s for `partition`. Refer to the
    /// [WorkUnitFeed] docs for more details about how this works.
    pub fn feed(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<T::WorkUnit>>> {
        self.provider.feed(partition, ctx)
    }

    /// DataFusion metrics collected at runtime while streaming [WorkUnit]s through [Self::feed].
    pub fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.provider.metrics()
    }
}
