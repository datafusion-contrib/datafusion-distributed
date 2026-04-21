use crate::common::{require_one_child, task_ctx_with_extension};
use datafusion::common::{Result, Statistics, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SortOrderPushdownResult,
};
use datafusion_proto::protobuf::proto_error;
use futures::StreamExt;
use futures::stream::BoxStream;
use prost::Message;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

/// A [WorkUnit] represents a piece of metadata necessary for an [ExecutionPlan] (typically a
/// [datafusion::datasource::source::DataSourceExec]) to perform its execution.
///
/// It can be anything:
/// - A Parquet file address in S3 that must be read.
/// - An HTTP query that should be issued to an external API.
/// - An external database query that should be executed externally.
/// - Etc...
///
/// Any protobuf message that implements `prost`'s [Message] trait is a valid [WorkUnit]
/// automatically, and no explicit `impl` block is needed for casting any protobuf message to a
/// [WorkUnit].
///
/// # Example
///
/// ```rust
/// # use datafusion_distributed::WorkUnit;
///
/// #[derive(Clone, PartialEq, ::prost::Message)]
/// struct FileAddressWorkUnit {
///     #[prost(string, tag = "1")]
///     url: String,
/// }
///
/// let file_address = FileAddressWorkUnit {
///     url: "s3://my-bucket/file.format".into()
/// };
///
/// let work_unit = Box::new(file_address) as Box<dyn WorkUnit>;
/// ```
pub trait WorkUnit: Message {
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn encode_to_bytes(&self) -> Vec<u8>;
}

impl<T: Message + 'static> WorkUnit for T {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
    fn encode_to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }
}

pub type WorkUnitFeed = BoxStream<'static, Result<Box<dyn WorkUnit>>>;
pub(crate) type WorkUnitTx = UnboundedSender<Result<Box<dyn WorkUnit>>>;
pub(crate) type WorkUnitRx = UnboundedReceiver<Result<Box<dyn WorkUnit>>>;
pub(crate) type RemoteWorkUnitFeedRxs = HashMap<Uuid, Mutex<Vec<WorkUnitRx>>>;
pub(crate) type RemoteWorkUnitFeedTxs = HashMap<(Uuid, usize), WorkUnitTx>;
type WorkUnitFeedsCtx = Vec<Mutex<Option<WorkUnitFeed>>>;

/// This struct is used by [RemoteWorkUnitFeedProvider] for consuming remote feeds that
/// come over the wire from the coordinating stage.
/// - The senders are used in the [crate::Worker] gRPC layer that listens to serialized plans over
///   the network, which also receives streams of serialized [WorkUnit]s coming from the
///   coordinating stage.
/// - The receivers are consumed by [RemoteWorkUnitFeedProvider], which will place the [WorkUnit]s
///   in the existing [WorkUnitFeedExec] as if nothing happened.
#[derive(Default)]
pub(crate) struct RemoteWorkUnitFeedRegistry {
    pub(crate) receivers: RemoteWorkUnitFeedRxs,
    pub(crate) senders: RemoteWorkUnitFeedTxs,
}

impl RemoteWorkUnitFeedRegistry {
    /// Creates all the receivers and senders for a specific [WorkUnit] Feed id. One feed per
    /// partition is created.
    pub(crate) fn add(&mut self, id: Uuid, partitions: usize) {
        let mut rxs = Vec::with_capacity(partitions);
        for partition in 0..partitions {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            rxs.push(rx);
            self.senders.insert((id, partition), tx);
        }

        self.receivers.insert(id, Mutex::new(rxs));
    }
}

/// Gets a partition feed from the [TaskContext] corresponding to the current partition.
///
/// A partition feeds is a per-partition stream of arbitrary user-defined messages relevant for
/// a certain [ExecutionPlan] during execution (e.g., a file address).
///
/// Typically, all the details relevant for executing a node are discovered and resolved at
/// planning time:
///
/// ### Example: typical case without [WorkUnitFeedExec]
///
/// ```text
/// ┌─────────────────────────────────────────────────────┐
/// │                                                     │
/// │                                                     │
/// │              ... rest of the plan ...               │
/// │                                                     │
/// │                                                     │
/// └───────▲────────────▲────────────▲────────────▲──────┘
///         │            │            │            │
///         │            │            │            │
///         │            │            │            │
/// ┌───────┼────────────┼────────────┼────────────┼──────┐
/// │       │            │MyDataSource│            │      │
/// │ ┌─────┴────┐ ┌─────┴────┐ ┌─────┴────┐ ┌─────┴────┐ │
/// │ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │ ( ) WorkUnit (e.g., file address)
/// │ │( )( )    │ │( )       │ │( )       │ │( )( )    │ │     Known at planning time and
/// │ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │     backend into the data source
/// └─────────────────────────────────────────────────────┘
/// ```
///
/// However, in certain situations, these pieces of information (like file addresses) necessary
/// during execution are not fully known at planning time, and they are discovered at runtime as
/// the query makes progress. [WorkUnitFeedExec] and [work_unit_feed()] cover this case:
///
/// ```text
/// ┌─────────────────────────────────────────────────────┐
/// │                                                     │
/// │                                                     │
/// │              ... rest of the plan ...               │
/// │                                                     │
/// │                                                     │
/// └─────────▲────────────▲────────────▲────────────▲────┘
///           │            │            │            │
///           │            │            │            │
/// ┌─────────┼────────────┼────────────┼────────────┼────┐
/// │         │        WorkUnitFeedExec │            │    │
/// │ ┌───────┴──┐ ┌───────┴──┐ ┌───────┴──┐ ┌───────┴──┐ │
/// │ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │
/// │ │          │ │          │ │          │ │          │ │
/// │ └──┬────▲──┘ └──┬────▲──┘ └──┬────▲──┘ └──┬────▲──┘ │
/// └────┼────┼──────( )───┼───────┼────┼──────( )───┼────┘
///     ( )   │       │    │       │    │       │    │      ( ) WorkUnit (e.g., file address)
///      │    │       │    │      ( )   │       │    │          Discovered at runtime and fed
/// ┌───( )───┼───────┼────┼───────┼────┼──────( )───┼────┐     into the data source
/// │    │    │       │   MyDataSource  │       │    │    │
/// │ ┌──▼────┴──┐ ┌──▼────┴──┐ ┌──▼────┴──┐ ┌──▼────┴──┐ │
/// │ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │
/// │ │          │ │          │ │          │ │          │ │
/// │ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
/// └─────────────────────────────────────────────────────┘
/// ```
///
/// A partition feed is available in an [ExecutionPlan] if a [WorkUnitFeedExec] exists right on
/// top of the node in which [work_unit_feed] is called. The elements in the stream are expected
/// to be user-defined pieces of data necessary for executing a specific [ExecutionPlan].
pub fn work_unit_feed<T: Any + Message + Default + 'static>(
    partition: usize,
    ctx: &TaskContext,
) -> Option<BoxStream<'static, Result<T>>> {
    let work_unit_feed = work_unit_feed_boxed(partition, ctx)?.map(|work_unit| {
        let any_work_unit = work_unit?.into_any();
        if any_work_unit.is::<T>() {
            let work_unit = any_work_unit.downcast::<T>().expect("cannot downcast to T");
            Ok(*work_unit)
        } else if any_work_unit.is::<Vec<u8>>() {
            let bytes = any_work_unit
                .downcast::<Vec<u8>>()
                .expect("cannot downcast to Vec<u8>");
            T::decode(bytes.as_slice()).map_err(|err| proto_error(format!("{err}")))
        } else {
            exec_err!(
                "Expected WorkUnit feed to be of type {}, but it was something else",
                std::any::type_name::<T>()
            )
        }
    });
    Some(work_unit_feed.boxed())
}

pub(crate) fn work_unit_feed_boxed(
    partition: usize,
    ctx: &TaskContext,
) -> Option<BoxStream<'static, Result<Box<dyn WorkUnit>>>> {
    let work_unit_feeds = ctx.session_config().get_extension::<WorkUnitFeedsCtx>()?;
    let work_unit_feed = work_unit_feeds.get(partition)?;
    std::mem::take(&mut *work_unit_feed.lock().unwrap())
}

/// Extension point for the [WorkUnitFeedExec] node that allows user to define their own
/// [WorkUnit] feeds.
///
/// An implementation of this trait is necessary for building a [WorkUnitFeedExec] execution plan,
/// and is meant to be used in conjunction with the [work_unit_feed] function for retrieving the
/// per-partition work unit streams in the node immediately below.
///
/// In a distributed context, the [WorkUnitFeedProvider] is always called from the "coordinating"
/// stage that initiates the query, and [WorkUnit] feeds are streamed from that "coordinating" stage
/// to the different respective workers over the network. This means that the [WorkUnit] feed will
/// be available to the leaf node whether they get distributed or not.
///
/// See [WorkUnitFeedProvider::feed] for more details.
pub trait WorkUnitFeedProvider: Send + Sync + Debug {
    /// Returns the [WorkUnitFeedProvider] as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Format this [WorkUnitFeedProvider] for display in explain plans.
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result;

    /// Builds a [WorkUnit] stream for the provided `partition` that will be passed down to
    /// the node below, and made available there through the [work_unit_feed] function.
    ///
    /// This method can be called in two different situations:
    ///
    /// ### Single-node context
    ///
    /// In a single node context, this method is called directly from [WorkUnitFeedExec::execute],
    /// and the returned [WorkUnit] feed is passed down to child of [WorkUnitFeedExec] through
    /// the [TaskContext].
    ///
    /// In a single node context, this method will be called `P` times, where `P` is the number of
    /// partitions of the node below.
    ///
    /// ```text
    /// ┌─────────────────────────────────────────────────────┐
    /// │        WorkUnitFeedExec(UserDefinedProvider)        │
    /// │ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
    /// │ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │
    /// │ │          │ │          │ │          │ │          │ │
    /// │ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ │
    /// └──────┼────────────┼────────────┼────────────┼───────┘
    ///        │            │            │            │
    ///    .feed(0)     .feed(1)      .feed()      .feed()
    /// ┌──────┼────────────┼────────────┼────────────┼───────┐
    /// │      │            │ MyPlanExec │            │       │
    /// │ ┌────▼─────┐ ┌────▼─────┐ ┌────▼─────┐ ┌────▼─────┐ │
    /// │ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │
    /// │ │          │ │          │ │          │ │          │ │
    /// │ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
    /// └─────────────────────────────────────────────────────┘
    /// ```
    ///
    /// ### Distributed context
    ///
    /// In a distributed context, this method is never called remotely inside of workers, it's
    /// always called inside the coordinating stage (inside [crate::DistributedExec]), which
    /// reaches out to the appropriate workers and streams the [WorkUnit] feed over the network.
    ///
    /// In a distributed context this method is called `P*T` times, where `P` is the number of
    /// partitions of the node below, and `T` is the number of distributed tasks.
    ///
    /// The [crate::TaskEstimator::scale_up_leaf_node] method can be used to intercept a
    /// [WorkUnitFeedExec] and tweak its [WorkUnitFeedProvider] implementation so that it returns
    /// `P*T` [WorkUnit] streams.
    ///
    /// ```text
    ///                                                                              ┌─────────────────────┐
    ///                                                                              │ Coordinating stage  │
    ///                 ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─└─────────────────────┘
    ///                  ┌────────────────────────────────────────────────────────────────────────────────┐│
    ///                 ││                                DistributedExec                                 │
    ///                  │                                                                                ││
    ///                 ││┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐│
    ///                  ││.feed(0)││.feed(1)││.feed(2)││.feed(3)││.feed(4)││.feed(5)││.feed(6)││.feed(7)│││
    ///                 ││└────┬───┘└───┬────┘└────┬───┘└────┬───┘└───┬────┘└────┬───┘└───┬────┘└───┬────┘│
    ///                  └─────┼────────┼──────────┼─────────┼────────┼──────────┼────────┼─────────┼─────┘│
    ///                 └ ─ ─ ─│─ ─ ─ ─ ┼ ─ ─ ─ ─ ─│─ ─ ─ ─ ─│─ ─ ─ ─ ┼ ─ ─ ─ ─ ─│─ ─ ─ ─ ┼ ─ ─ ─ ─ ┼ ─ ─ ─
    ///          ┌─────────────┘        │          │         │        │          │        │         └─────────────┐
    ///          │           ┌──────────┘          │         │        │          │        └──────────┐            │
    ///          │           │             ┌───────┘         │        │          └───────┐           │            │
    ///          │           │             │           ┌─────┘        └─────┐            │           │            │
    ///          │           │             │           │┌────────┐          │            │           │            │┌────────┐
    ///          │           │             │           ││Worker 1│          │            │           │            ││Worker 2│
    /// ┌ ─ ─ ─ ─│─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─│┴────────┘ ┌ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─│┴────────┘
    ///  ┌───────┼───────────┼─────────────┼───────────┼───────┐ │  ┌───────┼────────────┼───────────┼────────────┼───────┐ │
    /// ││       │  WorkUnitFeedExec(RemoteProvider)   │       │   ││       │  WorkUnitFeedExec(RemoteProvider)   │       │
    ///  │ ┌─────▼────┐ ┌────▼─────┐ ┌─────▼────┐ ┌────▼─────┐ │ │  │ ┌─────▼────┐ ┌─────▼────┐ ┌────▼─────┐ ┌────▼─────┐ │ │
    /// ││ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │   ││ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │
    ///  │ │          │ │          │ │          │ │          │ │ │  │ │          │ │          │ │          │ │          │ │ │
    /// ││ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ │   ││ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ │
    ///  └──────┼────────────┼────────────┼────────────┼───────┘ │  └──────┼────────────┼────────────┼────────────┼───────┘ │
    /// │       │            │            │            │           │       │            │            │            │
    ///     .feed(0)     .feed(1)     .feed(2)     .feed(3)      │     .feed(0)     .feed(1)     .feed(2)     .feed(3)      │
    /// │┌──────┼────────────┼────────────┼────────────┼───────┐   │┌──────┼────────────┼────────────┼────────────┼───────┐
    ///  │      │            │ MyPlanExec │            │       │ │  │      │            │ MyPlanExec │            │       │ │
    /// ││ ┌────▼─────┐ ┌────▼─────┐ ┌────▼─────┐ ┌────▼─────┐ │   ││ ┌────▼─────┐ ┌────▼─────┐ ┌────▼─────┐ ┌────▼─────┐ │
    ///  │ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │ │  │ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │ │
    /// ││ │          │ │          │ │          │ │          │ │   ││ │          │ │          │ │          │ │          │ │
    ///  │ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │ │  │ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │ │
    /// │└─────────────────────────────────────────────────────┘   │└─────────────────────────────────────────────────────┘
    ///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
    /// ```
    ///
    fn feeds(&self, feed_count: usize, ctx: Arc<TaskContext>) -> Result<Vec<WorkUnitFeed>>;

    /// Runtime metrics gathered from the process of streaming [WorkUnit]s.
    fn metrics(&self) -> ExecutionPlanMetricsSet {
        ExecutionPlanMetricsSet::new()
    }

    /// Injects arbitrary run-time state into this DataSource, returning a new instance
    /// that incorporates that state *if* it is relevant to the concrete DataSource implementation.
    ///
    /// This is a generic entry point: the `state` can be any type wrapped in
    /// `Arc<dyn Any + Send + Sync>`.  A work unit exec that cares about the state should
    /// down-cast it to the concrete type it expects and, if successful, return a
    /// modified copy of itself that captures the provided value.  If the state is
    /// not applicable, the default behaviour is to return `None` so that parent
    /// nodes can continue propagating the attempt further down the plan tree.
    fn with_new_state(
        &self,
        _state: Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn WorkUnitFeedProvider>> {
        None
    }
}

#[derive(Debug)]
pub(crate) struct RemoteWorkUnitFeedProvider {
    pub(crate) id: Uuid,
}

impl WorkUnitFeedProvider for RemoteWorkUnitFeedProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "RemoteWorkUnitFeedProvider")
    }

    fn feeds(
        &self,
        feed_count: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<Vec<BoxStream<'static, Result<Box<dyn WorkUnit>>>>> {
        let Some(rxs) = ctx
            .session_config()
            .get_extension::<RemoteWorkUnitFeedRxs>()
        else {
            return exec_err!("Missing RemoteWorkUnitFeedRegistry in context");
        };

        let id = self.id;
        let Some(remote_feeds) = rxs.get(&id) else {
            return exec_err!("Missing WorkUnit feeds for id {id}");
        };

        let receivers = std::mem::take(&mut *remote_feeds.lock().unwrap());
        if receivers.is_empty() {
            return exec_err!("WorkUnit feeds for id {id} where already consumed");
        };

        if receivers.len() != feed_count {
            return exec_err!(
                "Expected {feed_count} for RemoteWorkUnitFeedProvider, but the current context contains {} remote feeds",
                receivers.len()
            );
        }

        Ok(receivers
            .into_iter()
            .map(|x| UnboundedReceiverStream::new(x).boxed())
            .collect())
    }
}

/// [ExecutionPlan] implementation that sits on top of a leaf node and provides it with a stream
/// of [WorkUnit]s at runtime per partition.
///
/// ```text
/// ┌─────────────────────────────────────────────────────┐
/// │                                                     │
/// │                                                     │
/// │              ... rest of the plan ...               │
/// │                                                     │
/// │                                                     │
/// └─────────▲────────────▲────────────▲────────────▲────┘
///           │            │            │            │
///           │            │            │            │
/// ┌─────────┼────────────┼────────────┼────────────┼────┐
/// │         │        WorkUnitFeedExec │            │    │
/// │ ┌───────┴──┐ ┌───────┴──┐ ┌───────┴──┐ ┌───────┴──┐ │
/// │ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │
/// │ │          │ │          │ │          │ │          │ │
/// │ └──┬────▲──┘ └──┬────▲──┘ └──┬────▲──┘ └──┬────▲──┘ │
/// └────┼────┼──────( )───┼───────┼────┼──────( )───┼────┘
///     ( )   │       │    │       │    │       │    │      ( ) WorkUnit (e.g., file address)
///      │    │       │    │      ( )   │       │    │          Discovered at runtime and fed
/// ┌───( )───┼───────┼────┼───────┼────┼──────( )───┼────┐     into a leaf node.
/// │    │    │       │   MyPlanExec    │       │    │    │
/// │ ┌──▼────┴──┐ ┌──▼────┴──┐ ┌──▼────┴──┐ ┌──▼────┴──┐ │
/// │ │    P0    │ │    P1    │ │    P2    │ │    P3    │ │
/// │ │          │ │          │ │          │ │          │ │
/// │ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
/// └─────────────────────────────────────────────────────┘
/// ```
///
/// The generation of per-partition [WorkUnit] feeds is controlled by the user with the
/// [WorkUnitFeedProvider] trait.
///
/// An [ExecutionPlan] right below a [WorkUnitFeedExec] can use the  [work_unit_feed] function
/// for accessing the stream of user-defined data at runtime in the `.execute()` method.
#[derive(Debug, Clone)]
pub struct WorkUnitFeedExec {
    pub(crate) id: Uuid,
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) provider: Arc<dyn WorkUnitFeedProvider>,
    pub(crate) task_ctx: Arc<OnceLock<Result<Arc<TaskContext>, Arc<DataFusionError>>>>,
}

impl DisplayAs for WorkUnitFeedExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "WorkUnitFeedExec: ")?;
        self.provider.fmt_as(t, f)
    }
}

impl WorkUnitFeedExec {
    /// Builds a new [WorkUnitFeedExec] that will feed [WorkUnit]s return by `provider` into
    /// `input`, and will pass through the record batches return by `input` to upstream.
    pub fn new(input: Arc<dyn ExecutionPlan>, provider: Arc<dyn WorkUnitFeedProvider>) -> Self {
        Self {
            id: Uuid::new_v4(),
            input,
            provider,
            task_ctx: Default::default(),
        }
    }

    /// Returns the [ExecutionPlan] to which [WorkUnit]s are feed, and from which record batches
    /// are streamed to upstream nodes.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Returns the [WorkUnitFeedProvider] implementation that powers this [WorkUnitFeedExec] and
    /// streams [WorkUnit]s to the input [ExecutionPlan].
    pub fn provider(&self) -> &Arc<dyn WorkUnitFeedProvider> {
        &self.provider
    }

    /// Replaces the existing [WorkUnitFeedProvider] with a new one, maintaining the same input
    /// and unique identifier.
    pub fn with_new_provider(&self, provider: Arc<dyn WorkUnitFeedProvider>) -> Self {
        Self {
            id: self.id,
            input: Arc::clone(&self.input),
            provider,
            task_ctx: Default::default(),
        }
    }

    /// Lazily inits a new [TaskContext] based on the provided one with a [WorkUnitFeedsCtx]
    /// embedded as an extension:
    /// - The first calls to this function starts the [WorkUnit] feeds, places them into a clone
    ///   of the provided [TaskContext], which then gets stored in a [OnceLock].
    /// - Subsequent calls just return the previous already instantiated [TaskContext]s with all
    ///   the [WorkUnit] feeds added.
    pub(crate) fn get_or_init_task_ctx_with_feeds(
        &self,
        feed_count: usize,
        context: &Arc<TaskContext>,
    ) -> Result<Arc<TaskContext>> {
        let context = self.task_ctx.get_or_init(|| {
            let feeds = self.provider.feeds(feed_count, context.clone())?;
            if feeds.len() != feed_count {
                exec_err!("Expected {feed_count} feeds, but got {}", feeds.len())?;
            }
            let feeds: WorkUnitFeedsCtx = feeds.into_iter().map(Some).map(Mutex::new).collect();
            let task_ctx = task_ctx_with_extension(context, feeds);
            Ok(Arc::new(task_ctx))
        });
        let context = match context {
            Ok(context) => Arc::clone(context),
            Err(err) => return Err(DataFusionError::Shared(Arc::clone(err))),
        };
        Ok(context)
    }
}

impl ExecutionPlan for WorkUnitFeedExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            id: self.id,
            input: require_one_child(children)?,
            provider: Arc::clone(&self.provider),
            task_ctx: Default::default(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partition_count = self.input.output_partitioning().partition_count();
        let context = self.get_or_init_task_ctx_with_feeds(partition_count, &context)?;
        self.input.execute(partition, context)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        match self.input.try_swapping_with_projection(projection)? {
            Some(new_input) => Ok(Some(
                Arc::new(self.clone()).with_new_children(vec![new_input])?,
            )),
            None => Ok(None),
        }
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let child = self.input();

        match child.try_pushdown_sort(order)? {
            SortOrderPushdownResult::Exact { inner } => {
                let new_exec = Arc::new(self.clone()).with_new_children(vec![inner])?;
                Ok(SortOrderPushdownResult::Exact { inner: new_exec })
            }
            SortOrderPushdownResult::Inexact { inner } => {
                let new_exec = Arc::new(self.clone()).with_new_children(vec![inner])?;
                Ok(SortOrderPushdownResult::Inexact { inner: new_exec })
            }
            SortOrderPushdownResult::Unsupported => Ok(SortOrderPushdownResult::Unsupported),
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.provider.metrics().clone_inner())
    }

    fn with_new_state(&self, state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        self.provider.with_new_state(state).map(|new_data_source| {
            Arc::new(self.with_new_provider(new_data_source)) as Arc<dyn ExecutionPlan>
        })
    }
}
