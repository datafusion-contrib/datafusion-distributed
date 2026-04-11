use crate::common::{require_one_child, task_ctx_with_extension};
use datafusion::common::{Result, internal_err};
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

pub(crate) struct PartitionFeeds {
    feeds: Vec<Mutex<PartitionFeed>>,
}

impl PartitionFeeds {
    pub(crate) fn from_vec(feeds: Vec<PartitionFeed>) -> Self {
        Self {
            feeds: feeds.into_iter().map(Mutex::new).collect(),
        }
    }

    pub(crate) fn take_all(&self) -> Result<PartitionFeeds> {
        let mut feeds = Vec::with_capacity(self.len());
        for idx in 0..self.len() {
            feeds.push(Mutex::new(self.take(idx)?));
        }
        Ok(Self { feeds })
    }

    pub(crate) fn len(&self) -> usize {
        self.feeds.len()
    }

    pub(crate) fn take(&self, partition: usize) -> Result<PartitionFeed> {
        let Some(stream) = self.feeds.get(partition) else {
            return internal_err!(
                "Invalid partition feed index {partition}, there are only {} partition feeds",
                self.feeds.len()
            );
        };
        let stream = std::mem::take(&mut *stream.lock().unwrap());
        if matches!(stream, PartitionFeed::None) {
            return internal_err!("Partition feed not available for partition {partition}");
        };
        Ok(stream)
    }
}

pub(crate) enum PartitionFeed {
    Local(BoxStream<'static, Result<Box<dyn AnyMessage>>>),
    Remote(BoxStream<'static, Result<Vec<u8>>>),
    None,
}

impl Default for PartitionFeed {
    fn default() -> Self {
        Self::None
    }
}

impl PartitionFeed {
    pub(crate) fn into_remote(self) -> Result<BoxStream<'static, Result<Vec<u8>>>> {
        match self {
            PartitionFeed::Local(stream) => Ok(stream
                .map(|msg_or_err| msg_or_err.map(|msg| msg.encode_to_bytes()))
                .boxed()),
            PartitionFeed::Remote(stream) => Ok(stream),
            PartitionFeed::None => internal_err!("Partition feed not available"),
        }
    }
}

pub fn partition_feed<T: Any + Message + Default + 'static>(
    ctx: &TaskContext,
) -> Option<BoxStream<'static, Result<T>>> {
    let this = ctx
        .session_config()
        .get_extension::<Mutex<PartitionFeed>>()?;
    let metadata_stream = std::mem::take(&mut *this.lock().unwrap());
    match metadata_stream {
        PartitionFeed::Local(s) => Some(
            s.map(|item| match item {
                Ok(msg) => match msg.into_any().downcast::<T>() {
                    Ok(v) => Ok(*v),
                    Err(_) => internal_err!("Cannot downcast metadata message"),
                },
                Err(e) => Err(e),
            })
            .boxed(),
        ),
        PartitionFeed::Remote(s) => Some(
            s.map(|buff| T::decode(buff?.as_slice()).map_err(|err| proto_error(format!("{err}"))))
                .boxed(),
        ),
        PartitionFeed::None => None,
    }
}

pub struct PartitionFeedExec {
    pub(crate) id: Uuid,
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) partition_feeds: PartitionFeeds,
}

impl Debug for PartitionFeedExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionFeedExec")
    }
}

impl DisplayAs for PartitionFeedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PartitionFeedExec: feeds={}", self.partition_feeds.len())
    }
}

impl PartitionFeedExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        streams: Vec<BoxStream<'static, Result<Box<dyn AnyMessage>>>>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            input,
            partition_feeds: PartitionFeeds {
                feeds: streams
                    .into_iter()
                    .map(PartitionFeed::Local)
                    .map(Mutex::new)
                    .collect(),
            },
        }
    }
}

impl ExecutionPlan for PartitionFeedExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn check_invariants(&self, _: InvariantLevel) -> Result<()> {
        let output_partitions = self.properties().partitioning.partition_count();
        let partition_feeds = self.partition_feeds.feeds.len();
        if output_partitions != partition_feeds {
            return internal_err!(
                "Expected {output_partitions} partition feeds, but got {partition_feeds}"
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
            partition_feeds: self.partition_feeds.take_all()?,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partition_feed = self.partition_feeds.take(partition)?;
        let context = Arc::new(task_ctx_with_extension(
            &context,
            Mutex::new(partition_feed),
        ));
        self.input.execute(partition, context)
    }
}
