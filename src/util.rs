use std::{
    collections::HashMap,
    fmt::Display,
    future::Future,
    io::Cursor,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use anyhow::{anyhow, Context as anyhowctx};
use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    error::ArrowError,
    ipc::{
        convert::fb_to_schema,
        reader::StreamReader,
        root_as_message,
        writer::{IpcWriteOptions, StreamWriter},
        MetadataVersion,
    },
};
use arrow_flight::{decode::FlightRecordBatchStream, FlightClient, FlightData, Ticket};
use async_stream::stream;
use bytes::Bytes;
use datafusion::{
    common::{
        internal_datafusion_err,
        tree_node::{Transformed, TreeNode},
    },
    datasource::{physical_plan::FileScanConfig, source::DataSourceExec},
    error::DataFusionError,
    execution::{object_store::ObjectStoreUrl, RecordBatchStream, SendableRecordBatchStream},
    physical_plan::{
        displayable, stream::RecordBatchStreamAdapter, ExecutionPlan, ExecutionPlanProperties,
    },
    prelude::SessionContext,
};
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::{stream::BoxStream, Stream, StreamExt};
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, http::HttpBuilder, ObjectStore,
};
use parking_lot::RwLock;
use prost::Message;
use tokio::{
    macros::support::thread_rng_n,
    net::TcpListener,
    runtime::{Handle, Runtime},
};
use tonic::transport::Channel;
use url::Url;

use crate::{
    codec::DDCodec,
    logging::{debug, error, trace},
    protobuf::StageAddrs,
    result::Result,
    stage_reader::DDStageReaderExec,
    vocab::{Addrs, Host},
};

struct Spawner {
    runtime: Arc<Runtime>,
}

impl Spawner {
    fn new() -> Self {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("can build runtime"),
        );
        Self { runtime }
    }

    fn wait_for_future<F>(&self, f: F, name: &str) -> Result<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let name_c = name.to_owned();
        trace!("Spawner::wait_for {name_c}");
        let (tx, rx) = std::sync::mpsc::channel::<F::Output>();

        let func = move || {
            trace!("spawned fut start {name_c}");

            let out = Handle::current().block_on(f);
            trace!("spawned fut stop {name_c}");
            tx.send(out).inspect_err(|e| {
                error!("ERROR sending future reesult over channel!!!! {e:?}");
            })
        };

        {
            let _guard = self.runtime.enter();
            let handle = Handle::current();

            trace!("Spawner spawning {name}");
            handle.spawn_blocking(func);
            trace!("Spawner spawned {name}");
        }

        let out = rx
            .recv_timeout(Duration::from_secs(5))
            .inspect_err(|e| {
                error!("Spawner::wait_for {name} timed out waiting for future result: {e:?}");
            })
            .context("Spawner::wait_for failed to receive future result")?;

        debug!("Spawner::wait_for {name} returning");
        Ok(out)
    }
}

static SPAWNER: OnceLock<Spawner> = OnceLock::new();

pub fn wait_for<F>(f: F, name: &str) -> Result<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    let spawner = SPAWNER.get_or_init(Spawner::new);

    trace!("waiting for future: {name}");
    let name = name.to_owned();
    let out = spawner.wait_for_future(f, &name);
    trace!("done waiting for future: {name}");
    out
}

pub fn physical_plan_to_bytes(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<u8>, DataFusionError> {
    trace!(
        "serializing plan to bytes. plan:\n{}",
        display_plan_with_partition_counts(&plan)
    );
    let codec = DDCodec {};
    let proto = datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
    let bytes = proto.encode_to_vec();

    Ok(bytes)
}

pub fn bytes_to_physical_plan(
    ctx: &SessionContext,
    plan_bytes: &[u8],
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let proto_plan = datafusion_proto::protobuf::PhysicalPlanNode::try_decode(plan_bytes)?;

    let codec = DDCodec {};
    let plan = proto_plan.try_into_physical_plan(ctx, ctx.runtime_env().as_ref(), &codec)?;
    Ok(plan)
}

pub fn get_addrs(stage_addrs: &StageAddrs) -> Result<Addrs> {
    let mut addrs = Addrs::new();

    for (stage_id, partition_addrs) in stage_addrs.stage_addrs.iter() {
        let mut stage_addrs = HashMap::new();
        for (partition, hosts) in partition_addrs.partition_addrs.iter() {
            stage_addrs.insert(*partition, hosts.hosts.clone());
        }
        addrs.insert(*stage_id, stage_addrs);
    }

    Ok(addrs)
}

pub fn flight_data_to_schema(flight_data: &FlightData) -> anyhow::Result<SchemaRef> {
    let message = root_as_message(&flight_data.data_header[..])
        .map_err(|_| ArrowError::CastError("Cannot get root as message".to_string()))?;

    let ipc_schema: arrow::ipc::Schema = message
        .header_as_schema()
        .ok_or_else(|| ArrowError::CastError("Cannot get header as Schema".to_string()))?;
    let schema = fb_to_schema(ipc_schema);
    let schema = Arc::new(schema);
    Ok(schema)
}

pub fn batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let schema = batch.schema();
    let buffer: Vec<u8> = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)
        .map_err(|e| internal_datafusion_err!("Cannot create ipcwriteoptions {e}"))?;

    let mut stream_writer = StreamWriter::try_new_with_options(buffer, &schema, options)?;
    stream_writer.write(batch)?;
    let bytes = stream_writer.into_inner()?;
    Ok(bytes)
}

pub fn ipc_to_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let mut stream_reader = StreamReader::try_new_buffered(Cursor::new(bytes), None)?;

    match stream_reader.next() {
        Some(Ok(batch_res)) => Ok(batch_res),
        Some(Err(e)) => Err(e.into()),
        None => Err(anyhow!("Expected a valid batch").into()),
    }
}

/// produce a new SendableRecordBatchStream that will respect the rows
/// limit in the batches that it produces.  
///
/// It does this in a naive way, but it does honor the limit.  It will
///
/// For example, if the stream produces batches with length 8,
/// and the max row limit is 5, then this new stream will yield
/// batches with length 5, then 3, then 5, then 3 etc.  Simply
/// slicing on the max rows
pub fn max_rows_stream(
    mut in_stream: SendableRecordBatchStream,
    max_rows: usize,
) -> SendableRecordBatchStream
where
{
    let schema = in_stream.schema();
    let fixed_stream = stream! {
        while let Some(batch_res) = in_stream.next().await {
            match batch_res {
                Ok(batch) => {
                    if batch.num_rows() > max_rows {
                        let mut rows_remaining = batch.num_rows();
                        let mut offset = 0;
                        while rows_remaining > max_rows {
                            let s = batch.slice(offset, max_rows);

                            offset += max_rows;
                            rows_remaining -= max_rows;
                            yield Ok(s);
                        }
                        // yield remainder of the batch
                        yield Ok(batch.slice(offset, rows_remaining));
                    } else {
                        yield Ok(batch);
                    }
                },
                Err(e) => yield Err(e)
            }
        }
    };
    let adapter = RecordBatchStreamAdapter::new(schema, fixed_stream);

    Box::pin(adapter)
}

pub fn input_stage_ids(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<u64>, DataFusionError> {
    let mut result = vec![];
    plan.clone()
        .transform_down(|node: Arc<dyn ExecutionPlan>| {
            if let Some(reader) = node.as_any().downcast_ref::<DDStageReaderExec>() {
                result.push(reader.stage_id);
            }
            Ok(Transformed::no(node))
        })?;
    Ok(result)
}

pub async fn report_on_lag<F, T>(name: &str, fut: F) -> T
where
    F: Future<Output = T>,
{
    let name = name.to_owned();
    let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
    let expire = Duration::from_secs(2);

    let report = async move {
        tokio::time::sleep(expire).await;
        while rx.try_recv().is_err() {
            println!("{name} waiting to complete");
            tokio::time::sleep(expire).await;
        }
    };
    tokio::spawn(report);

    let out = fut.await;
    tx.send(()).unwrap();
    out
}

/// A utility wrapper for a stream that will print a message if it has been over
/// 2 seconds since receiving data.  Useful for debugging which streams are
/// stuck
pub fn lag_reporting_stream<S, T>(name: &str, in_stream: S) -> impl Stream<Item = T> + Send
where
    S: Stream<Item = T> + Send,
    T: Send,
{
    let mut stream = Box::pin(in_stream);
    let name = name.to_owned();

    let out_stream = async_stream::stream! {
        while let Some(item) = report_on_lag(&name, stream.next()).await {
            yield item;
        };
    };

    Box::pin(out_stream)
}

pub fn reporting_stream(
    name: &str,
    in_stream: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    let schema = in_stream.schema();
    let mut stream = Box::pin(in_stream);
    let name = name.to_owned();

    let out_stream = async_stream::stream! {
        trace!("stream:{name}: attempting to read");
        while let Some(batch) = stream.next().await {
            match batch {
                Ok(ref b) => trace!("stream:{name}: got batch of {} rows", b.num_rows()),
                Err(ref e) => trace!("stream:{name}: got error {e}"),
            };
            yield batch;
        };
    };

    Box::pin(RecordBatchStreamAdapter::new(schema, out_stream)) as SendableRecordBatchStream
}

pub struct ProcessorClient {
    /// the host we are connecting to
    pub(crate) host: Host,
    /// The flight client to the processor
    inner: FlightClient,
    /// the channel cache in the factory
    channels: Arc<RwLock<HashMap<String, Channel>>>,
}

impl ProcessorClient {
    pub fn new(
        host: Host,
        inner: FlightClient,
        channels: Arc<RwLock<HashMap<String, Channel>>>,
    ) -> Self {
        Self {
            host,
            inner,
            channels,
        }
    }

    pub async fn do_get(
        &mut self,
        ticket: Ticket,
    ) -> arrow_flight::error::Result<FlightRecordBatchStream> {
        let stream = self.inner.do_get(ticket).await
        .inspect_err(|e| {
            error!("Error in do_get for processor {}: {e:?}. 
                Considering this channel poisoned and removing it from ProcessorClientFactory cache", self.host);
            self.channels.write().remove(&self.host.addr);
        })?;

        Ok(stream)
    }

    pub async fn do_action(
        &mut self,
        action: arrow_flight::Action,
    ) -> arrow_flight::error::Result<BoxStream<'static, arrow_flight::error::Result<Bytes>>> {
        let result = self.inner.do_action(action).await.inspect_err(|e| {
            error!(
                "Error in do_action for processor {}: {e:?}. 
                    Considering this channel poisoned and removing it from ProcessorClientFactory \
                 cache",
                self.host
            );
            self.channels.write().remove(&self.host.addr);
        })?;

        Ok(result)
    }
}

struct ProcessorClientFactory {
    channels: Arc<RwLock<HashMap<String, Channel>>>,
}

impl ProcessorClientFactory {
    fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_client(&self, host: &Host) -> Result<ProcessorClient, DataFusionError> {
        let url = format!("http://{}", host.addr);

        let maybe_chan = self.channels.read().get(&host.addr).cloned();
        let chan = match maybe_chan {
            Some(chan) => {
                debug!("ProcessorFactory using cached channel for {host}");
                chan
            }
            None => {
                let host_str = host.to_string();
                let fut = async move {
                    trace!("ProcessorFactory connecting to {host_str}");
                    Channel::from_shared(url.clone())
                        .map_err(|e| {
                            internal_datafusion_err!("ProcessorFactory invalid url {e:#?}")
                        })?
                        // FIXME: update timeout value to not be a magic number
                        .connect_timeout(Duration::from_secs(2))
                        .connect()
                        .await
                        .map_err(|e| {
                            internal_datafusion_err!("ProcessorFactory cannot connect {e:#?}")
                        })
                };

                let chan = wait_for(fut, "ProcessorFactory::get_client").map_err(|e| {
                    internal_datafusion_err!(
                        "ProcessorFactory Cannot wait for channel connect future {e:#?}"
                    )
                })??;
                trace!("ProcessorFactory connected to {host}");
                self.channels
                    .write()
                    .insert(host.addr.to_string(), chan.clone());

                chan
            }
        };
        debug!("ProcessorFactory have channel now for {host}");

        let flight_client = FlightClient::new(chan);
        debug!("ProcessorFactory made flight client for {host}");
        Ok(ProcessorClient::new(
            host.clone(),
            flight_client,
            self.channels.clone(),
        ))
    }
}

static FACTORY: OnceLock<ProcessorClientFactory> = OnceLock::new();

pub fn get_client(host: &Host) -> Result<ProcessorClient, DataFusionError> {
    let factory = FACTORY.get_or_init(ProcessorClientFactory::new);
    factory.get_client(host)
}

/// Copied from datafusion_physical_plan::union as its useful and not public
pub struct CombinedRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
    /// Stream entries
    entries: Vec<SendableRecordBatchStream>,
}

impl CombinedRecordBatchStream {
    /// Create an CombinedRecordBatchStream
    pub fn new(schema: SchemaRef, entries: Vec<SendableRecordBatchStream>) -> Self {
        Self { schema, entries }
    }
}

impl RecordBatchStream for CombinedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for CombinedRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;

        let start = thread_rng_n(self.entries.len() as u32) as usize;
        let mut idx = start;

        for _ in 0..self.entries.len() {
            let stream = self.entries.get_mut(idx).unwrap();

            match Pin::new(stream).poll_next(cx) {
                Ready(Some(val)) => {
                    trace!("Combined stream got {:?}", val);
                    return Ready(Some(val));
                }
                Ready(None) => {
                    // Remove the entry
                    self.entries.swap_remove(idx);

                    // Check if this was the last entry, if so the cursor needs
                    // to wrap
                    if idx == self.entries.len() {
                        idx = 0;
                    } else if idx < start && start <= self.entries.len() {
                        // The stream being swapped into the current index has
                        // already been polled, so skip it.
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
                Pending => {
                    idx = idx.wrapping_add(1) % self.entries.len();
                }
            }
        }

        // If the map is empty, then the stream is complete.
        if self.entries.is_empty() {
            Ready(None)
        } else {
            Pending
        }
    }
}

pub fn display_plan_with_partition_counts(plan: &Arc<dyn ExecutionPlan>) -> impl Display {
    let mut output = String::with_capacity(1000);

    print_node(plan, 0, &mut output);
    output
}

fn print_node(plan: &Arc<dyn ExecutionPlan>, indent: usize, output: &mut String) {
    output.push_str(&format!(
        "[ output_partitions: {}]{:>indent$}{}",
        plan.output_partitioning().partition_count(),
        "",
        displayable(plan.as_ref()).set_show_schema(true).one_line(),
        indent = indent
    ));

    for child in plan.children() {
        print_node(child, indent + 2, output);
    }
}

pub fn register_object_store_for_paths_in_plan(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<(), DataFusionError> {
    let check_plan = |plan: Arc<dyn ExecutionPlan>| -> Result<_, DataFusionError> {
        for input in plan.children().into_iter() {
            if let Some(node) = input.as_any().downcast_ref::<DataSourceExec>() {
                if let Some(config) = node.data_source().as_any().downcast_ref::<FileScanConfig>() {
                    let url = &config.object_store_url;
                    maybe_register_object_store(ctx, url.as_ref())?
                }
            }
        }
        Ok(Transformed::no(plan))
    };

    plan.transform_down(check_plan)?;

    Ok(())
}

/// Registers an object store with the given session context based on the
/// provided path.
///
/// # Arguments
///
/// * `ctx` - A reference to the `SessionContext` where the object store will be
///   registered.
/// * `path` - A string slice that holds the path or URL of the object store.
pub fn maybe_register_object_store(ctx: &SessionContext, url: &Url) -> Result<(), DataFusionError> {
    let (ob_url, object_store) = if url.as_str().starts_with("s3://") {
        let bucket = url
            .host_str()
            .ok_or(internal_datafusion_err!("missing bucket name in s3:// url"))?;

        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()?;
        (
            ObjectStoreUrl::parse(format!("s3://{bucket}"))?,
            Arc::new(s3) as Arc<dyn ObjectStore>,
        )
    } else if url.as_str().starts_with("gs://") || url.as_str().starts_with("gcs://") {
        let bucket = url
            .host_str()
            .ok_or(internal_datafusion_err!("missing bucket name in gs:// url"))?;

        let gs = GoogleCloudStorageBuilder::new()
            .with_bucket_name(bucket)
            .build()?;

        (
            ObjectStoreUrl::parse(format!("gs://{bucket}"))?,
            Arc::new(gs) as Arc<dyn ObjectStore>,
        )
    } else if url.as_str().starts_with("http://") || url.as_str().starts_with("https://") {
        let scheme = url.scheme();

        let host = url.host_str().ok_or(internal_datafusion_err!(
            "missing host name in {}:// url",
            scheme
        ))?;

        let http = HttpBuilder::new()
            .with_url(format!("{scheme}://{host}"))
            .build()?;

        (
            ObjectStoreUrl::parse(format!("{scheme}://{host}"))?,
            Arc::new(http) as Arc<dyn ObjectStore>,
        )
    } else {
        let local = object_store::local::LocalFileSystem::new();
        (
            ObjectStoreUrl::parse("file://")?,
            Arc::new(local) as Arc<dyn ObjectStore>,
        )
    };

    debug!("Registering object store for {}", ob_url);

    ctx.register_object_store(ob_url.as_ref(), object_store);
    Ok(())
}

pub async fn start_up(port: usize) -> Result<TcpListener> {
    let my_host_str = format!("0.0.0.0:{}", port);

    let listener = TcpListener::bind(&my_host_str)
        .await
        .context("Could not bind socket to {my_host_str}")?;

    Ok(listener)
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, vec};

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };
    use futures::stream;
    use test_log::test;

    use super::*;

    #[test]
    fn test_wait_for() {
        let fut = async || 5;
        let out = wait_for(fut(), "my_future").unwrap();
        assert_eq!(out, 5);
    }

    #[test]
    fn test_wait_for_nested() {
        println!("test_wait_for_nested");
        let fut = async || {
            println!("in outter fut");
            let fut5 = async || {
                println!("in inner fut");
                5
            };
            wait_for(fut5(), "inner").unwrap()
        };

        let out = wait_for(fut(), "outer").unwrap();
        assert_eq!(out, 5);
    }

    #[test(tokio::test)]
    async fn test_max_rows_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap();

        // 24 total rows
        let batches = (0..3).map(|_| Ok(batch.clone())).collect::<Vec<_>>();

        let in_stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream::iter(batches)));

        let out_stream = max_rows_stream(in_stream, 3);
        let batches: Vec<_> = out_stream.collect().await;

        println!("got {} batches", batches.len());
        for batch in batches.iter() {
            println!("batch length: {}", batch.as_ref().unwrap().num_rows());
        }

        assert_eq!(batches.len(), 9);
        assert_eq!(batches[0].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[1].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[2].as_ref().unwrap().num_rows(), 2);
        assert_eq!(batches[3].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[4].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[5].as_ref().unwrap().num_rows(), 2);
        assert_eq!(batches[6].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[7].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[8].as_ref().unwrap().num_rows(), 2);
    }
}
