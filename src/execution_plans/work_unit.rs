use crate::common::{require_one_child, task_ctx_with_extension};
use datafusion::common::{Result, exec_err, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::InvariantLevel;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_proto::protobuf::proto_error;
use futures::StreamExt;
use futures::stream::BoxStream;
use prost::Message;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Trait that represents a protobuf [Message] that can be upcast to [Any].
///
/// This is just a Rust compiler trick for allowing [Message] to be downcast to a specific
/// implementation without any serialization/deserialization.
pub(crate) trait AnyMessage: Message {
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn encode_to_bytes(&self) -> Vec<u8>;
}

impl<T: Message + 'static> AnyMessage for T {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
    fn encode_to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }
}

/// List of [WorkUnitFeed], typically one per partition in an [ExecutionPlan].
///
/// As this struct is expected to be threaded across [ExecutionPlan] implementations and
/// other structs with non-mutable access, they need to be hidden behind a [Mutex].
///
/// The [WorkUnitFeed] contains a reference to a stream that is only meant to be consumed by one
/// consumer, therefore, it's not clonable, and an owned reference is needed for consuming it. This
/// means that moving around this structure requires "taking" the stream instance and moving them
/// to a new place, leaving an empty un-consumable placeholder in the original reference.
pub(crate) struct WorkUnitFeeds {
    feeds: Vec<Mutex<WorkUnitFeed>>,
}

impl WorkUnitFeeds {
    pub(crate) fn from_vec(feeds: Vec<WorkUnitFeed>) -> Self {
        Self {
            feeds: feeds.into_iter().map(Mutex::new).collect(),
        }
    }

    /// Takes all the [WorkUnitFeed]s and moves them to the returning [WorkUnitFeeds], leaving
    /// `self` with completely empty streams.
    pub(crate) fn take_all(&self) -> Result<WorkUnitFeeds> {
        let mut feeds = Vec::with_capacity(self.len());
        for idx in 0..self.len() {
            feeds.push(Mutex::new(self.take(idx)?));
        }
        Ok(Self { feeds })
    }

    /// Returns the amount of partition feeds in [Self], typically matching the amount of partitions
    /// in an [ExecutionPlan].
    pub(crate) fn len(&self) -> usize {
        self.feeds.len()
    }

    /// Takes a [WorkUnitFeed] for the provided index, returning it, and leaving an empty
    /// un-consumable placeholder in the respected `partition` of `self`.
    pub(crate) fn take(&self, partition: usize) -> Result<WorkUnitFeed> {
        let Some(stream) = self.feeds.get(partition) else {
            return internal_err!(
                "Invalid partition feed index {partition}, there are only {} partition feeds",
                self.feeds.len()
            );
        };
        let stream = std::mem::take(&mut *stream.lock().unwrap());
        if matches!(stream, WorkUnitFeed::AlreadyConsumed) {
            return internal_err!("Partition feed already consumed for partition {partition}");
        };
        Ok(stream)
    }
}

/// A stream of arbitrary protobuf messages that can be either encoded or decoded.
pub(crate) enum WorkUnitFeed {
    /// A stream of already decoded protobuf messages, ready to be downcast to a specific type.
    Decoded(BoxStream<'static, Result<Box<dyn AnyMessage>>>),
    /// A stream of bytes representing serialized protobuf messages.
    Encoded(BoxStream<'static, Result<Vec<u8>>>),
    /// Placeholder used for marking this [WorkUnitFeed] as already consumed.
    AlreadyConsumed,
}

impl Default for WorkUnitFeed {
    fn default() -> Self {
        Self::AlreadyConsumed
    }
}

impl WorkUnitFeed {
    pub(crate) fn into_encoded(self) -> Result<BoxStream<'static, Result<Vec<u8>>>> {
        match self {
            WorkUnitFeed::Decoded(stream) => Ok(stream
                .map(|msg_or_err| msg_or_err.map(|msg| msg.encode_to_bytes()))
                .boxed()),
            WorkUnitFeed::Encoded(stream) => Ok(stream),
            WorkUnitFeed::AlreadyConsumed => internal_err!("Partition feed already consumed"),
        }
    }

    fn into_decoded<T: Any + Message + Default + 'static>(
        self,
    ) -> Result<BoxStream<'static, Result<T>>> {
        Ok(match self {
            WorkUnitFeed::Decoded(s) => s
                .map(|msg| match msg?.into_any().downcast::<T>() {
                    Ok(v) => Ok(*v),
                    Err(_) => internal_err!("Cannot downcast metadata message"),
                })
                .boxed(),
            WorkUnitFeed::Encoded(s) => s
                .map(|buff| {
                    T::decode(buff?.as_slice()).map_err(|err| proto_error(format!("{err}")))
                })
                .boxed(),
            WorkUnitFeed::AlreadyConsumed => return exec_err!("Partition feed already consumed"),
        })
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
/// └──────────────────────────┬──────────────────────────┘
///                            │
///                            │
///                            │
/// ┌──────────────────────────▼──────────────────────────┐
/// │                     MyPlanExec                      │
/// │ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │  .
/// │ │ . .P1    │ │ .  P2    │ │ .  P3    │ │ . .P4    │ │ ( ) File address
/// │ │( ( )     │ │( )       │ │( )       │ │( ( )     │ │  '   or similar
/// │ └─'─'──────┘ └─'────────┘ └─'────────┘ └─'─'──────┘ │
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
/// └──────────────────────────┬──────────────────────────┘
///                            │
///                            │
/// ┌──────────────────────────▼──────────────────────────┐
/// │                  WorkUnitFeedExec                   │
/// │ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
/// │ │    P1    │ │    P2    │ │    P3    │ │    P4    │ │
/// │ │          │ │          │ │          │ │          │ │
/// │ └─────┬────┘ └─────.────┘ └─────┬────┘ └─────┬────┘ │
/// └───────.───────────( )───────────┼────────────.──────┘  .
///        ( )           '            .           ( )       ( ) File address
///         '            │           ( )           '         '   or similar
/// ┌──────( )───────────┼────────────'────────────.──────┐
/// │       '            │MyPlanExec  │           ( )     │
/// │ ┌─────▼────┐ ┌─────▼────┐ ┌─────▼────┐ ┌─────'────┐ │
/// │ │    P1    │ │    P2    │ │    P3    │ │    P4    │ │
/// │ │          │ │          │ │          │ │          │ │
/// │ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
/// └─────────────────────────────────────────────────────┘
/// ```
///
/// A partition feed is available in an [ExecutionPlan] if a [WorkUnitFeedExec] exists right on
/// top of the node in which [work_unit_feed] is called. The elements in the stream are expected
/// to be user-defined pieces of data necessary for executing a specific [ExecutionPlan].
pub fn work_unit_feed<T: Any + Message + Default + 'static>(
    ctx: &TaskContext,
) -> Result<Option<BoxStream<'static, Result<T>>>> {
    let Some(this) = ctx.session_config().get_extension::<Mutex<WorkUnitFeed>>() else {
        return Ok(None);
    };
    Ok(Some(
        std::mem::take(&mut *this.lock().unwrap()).into_decoded()?,
    ))
}

/// [ExecutionPlan] implementation that sits on top of a leaf node and provides it with execution
/// information at runtime.
///
/// ```text
/// ┌─────────────────────────────────────────────────────┐
/// │                                                     │
/// │                                                     │
/// │              ... rest of the plan ...               │
/// │                                                     │
/// │                                                     │
/// └──────────────────────────┬──────────────────────────┘
///                            │
///                            │
/// ┌──────────────────────────▼──────────────────────────┐
/// │                  WorkUnitFeedExec                   │
/// │ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
/// │ │    P1    │ │    P2    │ │    P3    │ │    P4    │ │
/// │ │          │ │          │ │          │ │          │ │
/// │ └─────┬────┘ └─────.────┘ └─────┬────┘ └─────┬────┘ │
/// └───────.───────────( )───────────┼────────────.──────┘  .
///        ( )           '            .           ( )       ( ) File address
///         '            │           ( )           '         '   or similar
/// ┌──────( )───────────┼────────────'────────────.──────┐
/// │       '            │MyPlanExec  │           ( )     │
/// │ ┌─────▼────┐ ┌─────▼────┐ ┌─────▼────┐ ┌─────'────┐ │
/// │ │    P1    │ │    P2    │ │    P3    │ │    P4    │ │
/// │ │          │ │          │ │          │ │          │ │
/// │ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
/// └─────────────────────────────────────────────────────┘
/// ```
///
/// An [ExecutionPlan] right below a [WorkUnitFeedExec] can use the  [work_unit_feed()] function
/// for accessing the stream of user-defined data at runtime in the `.execute()` method.
pub struct WorkUnitFeedExec {
    pub(crate) id: Uuid,
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) work_unit_feeds: WorkUnitFeeds,
}

impl Debug for WorkUnitFeedExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkUnitFeedExec")
    }
}

impl DisplayAs for WorkUnitFeedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "WorkUnitFeedExec: feeds={}", self.work_unit_feeds.len())
    }
}

impl WorkUnitFeedExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        streams: Vec<BoxStream<'static, Result<Box<dyn AnyMessage>>>>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            input,
            work_unit_feeds: WorkUnitFeeds {
                feeds: streams
                    .into_iter()
                    .map(WorkUnitFeed::Decoded)
                    .map(Mutex::new)
                    .collect(),
            },
        }
    }
}

impl ExecutionPlan for WorkUnitFeedExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn check_invariants(&self, _: InvariantLevel) -> Result<()> {
        let output_partitions = self.properties().partitioning.partition_count();
        let work_unit_feeds = self.work_unit_feeds.feeds.len();
        if output_partitions != work_unit_feeds {
            return internal_err!(
                "Expected {output_partitions} partition feeds, but got {work_unit_feeds}"
            );
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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
            work_unit_feeds: self.work_unit_feeds.take_all()?,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let work_unit_feed = self.work_unit_feeds.take(partition)?;
        let context = Arc::new(task_ctx_with_extension(
            &context,
            Mutex::new(work_unit_feed),
        ));
        self.input.execute(partition, context)
    }
}
