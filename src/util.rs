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
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
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

/// Blocks until a future completes, enabling async operations in sync contexts
///
/// This function provides a way to execute async futures in synchronous code by using
/// a dedicated thread pool. It's particularly useful during startup and initialization
/// phases where async operations need to complete before proceeding.
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

/// Serializes a DataFusion execution plan to bytes for network transmission
///
/// This function converts a physical execution plan into a protobuf representation
/// and then encodes it as bytes. This is essential for distributing execution plans
/// from the proxy to worker nodes in the distributed system.
pub fn physical_plan_to_bytes(
    plan: Arc<dyn ExecutionPlan>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<u8>, DataFusionError> {
    trace!(
        "serializing plan to bytes. plan:\n{}",
        display_plan_with_partition_counts(&plan)
    );
    // Convert DataFusion plan to protobuf representation
    let proto = datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(plan, codec)?;
    // Encode protobuf to bytes for network transmission
    let bytes = proto.encode_to_vec();

    Ok(bytes)
}

/// Deserializes bytes back to a DataFusion execution plan on worker nodes
///
/// This function reconstructs a physical execution plan from protobuf bytes received
/// from the proxy. Workers use this to recreate the execution plan they need to execute
/// for their assigned stage and partition group.
pub fn bytes_to_physical_plan(
    ctx: &SessionContext,
    plan_bytes: &[u8],
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    // Decode protobuf from bytes
    let proto_plan = datafusion_proto::protobuf::PhysicalPlanNode::try_decode(plan_bytes)?;

    // Convert protobuf back to DataFusion execution plan
    let plan = proto_plan.try_into_physical_plan(ctx, ctx.runtime_env().as_ref(), codec)?;
    Ok(plan)
}

/// Converts protobuf StageAddrs to internal Addrs format for easier access
///
/// This function transforms the protobuf address structure into a simplified HashMap
/// format that's more convenient for internal use. The conversion flattens the nested
/// protobuf structure into direct stage_id -> partition_id -> [hosts] mapping.
pub fn get_addrs(stage_addrs: &StageAddrs) -> Result<Addrs> {
    let mut addrs = Addrs::new();

    // Convert nested protobuf structure to flat HashMap
    for (stage_id, partition_addrs) in stage_addrs.stage_addrs.iter() {
        let mut stage_addrs = HashMap::new();
        for (partition, hosts) in partition_addrs.partition_addrs.iter() {
            stage_addrs.insert(*partition, hosts.hosts.clone());
        }
        addrs.insert(*stage_id, stage_addrs);
    }

    Ok(addrs)
}

/// Extracts schema information from Arrow Flight data messages
///
/// This function parses the header of a Flight data message to extract the Arrow schema.
/// It's used when establishing connections with workers to understand the structure of
/// data that will be transmitted over the Flight protocol.
pub fn flight_data_to_schema(flight_data: &FlightData) -> anyhow::Result<SchemaRef> {
    // Parse the Flight message header to extract schema information
    let message = root_as_message(&flight_data.data_header[..])
        .map_err(|_| ArrowError::CastError("Cannot get root as message".to_string()))?;

    // Extract schema from the message header
    let ipc_schema: arrow::ipc::Schema = message
        .header_as_schema()
        .ok_or_else(|| ArrowError::CastError("Cannot get header as Schema".to_string()))?;
    let schema = fb_to_schema(ipc_schema);
    let schema = Arc::new(schema);
    Ok(schema)
}

/// Converts a RecordBatch to IPC (Inter-Process Communication) bytes format
///
/// This function serializes a RecordBatch using Arrow's IPC format, which is efficient
/// for network transmission and inter-process communication. Used when workers need to
/// send data to other components or for intermediate result storage.
pub fn batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let schema = batch.schema();
    let buffer: Vec<u8> = Vec::new();
    // Configure IPC writer with specific options for consistency
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)
        .map_err(|e| internal_datafusion_err!("Cannot create ipcwriteoptions {e}"))?;

    // Write RecordBatch to IPC format
    let mut stream_writer = StreamWriter::try_new_with_options(buffer, &schema, options)?;
    stream_writer.write(batch)?;
    let bytes = stream_writer.into_inner()?;
    Ok(bytes)
}

/// Converts IPC bytes back to a RecordBatch for processing
///
/// This function deserializes IPC format bytes back into a RecordBatch that can be
/// processed by DataFusion. Used when receiving serialized data from other components
/// or when reading intermediate results from storage.
pub fn ipc_to_batch(bytes: &[u8]) -> Result<RecordBatch> {
    // Create a buffered reader from the IPC bytes
    let mut stream_reader = StreamReader::try_new_buffered(Cursor::new(bytes), None)?;

    // Extract the first (and expected only) batch from the stream
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

/// Extracts stage IDs from DDStageReaderExec nodes in an execution plan tree
///
/// This function traverses an execution plan to find all DDStageReaderExec nodes and
/// collects their stage IDs. Used to understand which stages a plan depends on for
/// proper execution ordering and dependency resolution.
pub fn input_stage_ids(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<u64>, DataFusionError> {
    let mut result = vec![];
    // Walk the execution plan tree to find stage reader nodes
    plan.clone()
        .transform_down(|node: Arc<dyn ExecutionPlan>| {
            if let Some(reader) = node.as_any().downcast_ref::<DDStageReaderExec>() {
                result.push(reader.stage_id);
            }
            Ok(Transformed::no(node))
        })?;
    Ok(result)
}

/// Monitors and reports on slow-running async operations
///
/// This function executes a future while monitoring its execution time. If the operation
/// takes longer than expected, it logs warnings to help identify performance bottlenecks
/// in the distributed system.
pub async fn report_on_lag<F, T>(name: &str, fut: F) -> T
where
    F: Future<Output = T>,
{
    let name = name.to_owned();
    let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
    let expire = Duration::from_secs(2);

    // Spawn a background task to monitor execution time
    let report = async move {
        tokio::time::sleep(expire).await;
        while rx.try_recv().is_err() {
            println!("{name} waiting to complete");
            tokio::time::sleep(expire).await;
        }
    };
    tokio::spawn(report);

    // Execute the future and signal completion
    let out = fut.await;
    tx.send(()).unwrap();
    out
}

/// Creates a stream wrapper that reports on lag when data flow is slow
///
/// This utility wrapper monitors a stream and prints warnings when more than 2 seconds
/// pass without receiving new data. Useful for debugging which streams are stuck or
/// experiencing performance issues in the distributed system.
pub fn lag_reporting_stream<S, T>(name: &str, in_stream: S) -> impl Stream<Item = T> + Send
where
    S: Stream<Item = T> + Send,
    T: Send,
{
    let mut stream = Box::pin(in_stream);
    let name = name.to_owned();

    // Create a new stream that monitors the input stream for lag
    let out_stream = async_stream::stream! {
        while let Some(item) = report_on_lag(&name, stream.next()).await {
            yield item;
        };
    };

    Box::pin(out_stream)
}

/// Creates a RecordBatch stream wrapper that logs data flow for debugging
///
/// This function wraps a RecordBatch stream with logging functionality to trace
/// data flow through the system. It logs when batches are received, their row counts,
/// and any errors that occur, providing visibility into stream processing.
pub fn reporting_stream(
    name: &str,
    in_stream: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    let schema = in_stream.schema();
    let mut stream = Box::pin(in_stream);
    let name = name.to_owned();

    // Create a new stream with logging functionality
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

/// Arrow Flight client for communicating with distributed DataFusion worker nodes
///
/// This struct provides a high-level interface for sending queries and retrieving results
/// from worker nodes in the distributed system. It wraps an Arrow Flight client with
/// additional functionality for connection management and error handling.
///
/// # Connection Management
/// The client maintains a reference to the shared channel cache to enable automatic
/// connection cleanup when errors occur. If a connection fails, it removes the cached
/// channel to force reconnection on the next request.
///
/// # Protocol Operations
/// - **do_get**: Retrieves query results from workers as streaming data
/// - **do_action**: Sends commands to workers (e.g., store execution plans)
pub struct WorkerClient {
    /// Host information for the worker this client connects to
    pub(crate) host: Host,
    /// The underlying Arrow Flight client for gRPC communication
    inner: FlightClient,
    /// Shared reference to the channel cache for connection cleanup
    channels: Arc<RwLock<HashMap<String, Channel>>>,
}

impl WorkerClient {
    /// Creates a new WorkerClient with the specified configuration
    ///
    /// This constructor is typically called by WorkerClientFactory rather than directly.
    /// It wraps an Arrow Flight client with additional distributed-specific functionality.
    ///
    /// # Arguments
    /// * `host` - Host information for the target worker
    /// * `inner` - Configured Arrow Flight client
    /// * `channels` - Shared channel cache for connection management
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

    /// Retrieves streaming query results from the worker node
    ///
    /// This method sends a do_get request to the worker with the provided ticket
    /// and returns a stream of RecordBatches. The ticket typically contains query
    /// metadata like query_id, stage_id, and partition information.
    ///
    /// # Error Handling
    /// If the request fails, the method automatically removes the cached connection
    /// to force reconnection on subsequent requests. This helps recover from
    /// network issues or worker failures.
    ///
    /// # Arguments
    /// * `ticket` - Flight ticket containing query execution metadata
    ///
    /// # Returns
    /// * `FlightRecordBatchStream` - Stream of query results from the worker
    pub async fn do_get(
        &mut self,
        ticket: Ticket,
    ) -> arrow_flight::error::Result<FlightRecordBatchStream> {
        let stream = self.inner.do_get(ticket).await.inspect_err(|e| {
            error!(
                "Error in do_get for worker {}: {e:?}. 
                Considering this channel poisoned and removing it from WorkerClientFactory cache",
                self.host
            );
            // Remove failed connection from cache to force reconnection
            self.channels.write().remove(&self.host.addr);
        })?;

        Ok(stream)
    }

    /// Sends action commands to the worker node
    ///
    /// This method sends do_action requests to workers for various control operations
    /// like storing execution plans, reporting host information, or other administrative
    /// tasks. Actions typically don't return data, just confirmation of completion.
    ///
    /// # Error Handling
    /// Similar to do_get, connection failures automatically trigger cache cleanup
    /// to ensure connection recovery on subsequent requests.
    ///
    /// # Arguments
    /// * `action` - Arrow Flight action containing command and payload
    ///
    /// # Returns
    /// * `BoxStream<Bytes>` - Stream of response messages from the worker
    pub async fn do_action(
        &mut self,
        action: arrow_flight::Action,
    ) -> arrow_flight::error::Result<BoxStream<'static, arrow_flight::error::Result<Bytes>>> {
        let result = self.inner.do_action(action).await.inspect_err(|e| {
            error!(
                "Error in do_action for worker {}: {e:?}. 
                    Considering this channel poisoned and removing it from WorkerClientFactory \
                 cache",
                self.host
            );
            // Remove failed connection from cache to force reconnection
            self.channels.write().remove(&self.host.addr);
        })?;

        Ok(result)
    }
}

/// Factory for creating and managing WorkerClient connections with connection pooling
///
/// This factory provides centralized connection management for distributed DataFusion
/// worker communications. It maintains a cache of gRPC channels to worker nodes,
/// enabling connection reuse and improved performance across the distributed system.
///
/// # Connection Pooling
/// The factory caches gRPC channels by worker address, avoiding the overhead of
/// establishing new connections for each request. Cached connections are automatically
/// cleaned up when they fail, ensuring automatic recovery from network issues.
///
/// # Thread Safety
/// The factory is designed to be used concurrently across multiple threads, with
/// internal synchronization provided by RwLock for the connection cache.
///
/// # Usage
/// Typically accessed through the global `get_client()` function rather than directly,
/// providing a singleton pattern for connection management across the application.
struct WorkerClientFactory {
    /// Cache of gRPC channels indexed by worker address
    /// Enables connection reuse and reduces connection establishment overhead
    channels: Arc<RwLock<HashMap<String, Channel>>>,
}

impl WorkerClientFactory {
    /// Creates a new WorkerClientFactory with an empty connection cache
    ///
    /// This constructor initializes the factory with a fresh connection cache.
    /// Typically called once during application startup to create the global factory.
    fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates or retrieves a cached WorkerClient for the specified host
    ///
    /// This method implements the core connection management logic. It first checks
    /// for an existing cached connection to the worker, and if none exists, establishes
    /// a new gRPC connection and caches it for future use.
    ///
    /// # Connection Process
    /// 1. Check cache for existing connection to the worker
    /// 2. If cached connection exists, reuse it
    /// 3. If no cached connection, establish new gRPC connection
    /// 4. Cache the new connection for future requests
    /// 5. Wrap connection in WorkerClient with error handling
    ///
    /// # Arguments
    /// * `host` - Host information for the target worker node
    ///
    /// # Returns
    /// * `WorkerClient` - Ready-to-use client for communicating with the worker
    ///
    /// # Errors
    /// Returns DataFusionError if connection establishment fails or times out
    pub fn get_client(&self, host: &Host) -> Result<WorkerClient, DataFusionError> {
        let url = format!("http://{}", host.addr);

        // Check if we have a cached connection for this worker
        let maybe_chan = self.channels.read().get(&host.addr).cloned();
        let chan = match maybe_chan {
            Some(chan) => {
                debug!("WorkerFactory using cached channel for {host}");
                chan
            }
            None => {
                // No cached connection - establish a new one
                let host_str = host.to_string();
                let fut = async move {
                    trace!("WorkerFactory connecting to {host_str}");
                    Channel::from_shared(url.clone())
                        .map_err(|e| internal_datafusion_err!("WorkerFactory invalid url {e:#?}"))?
                        // Configure connection timeout to prevent hanging on unreachable workers
                        // TODO: make this configurable
                        .connect_timeout(Duration::from_secs(2))
                        .connect()
                        .await
                        .map_err(|e| {
                            internal_datafusion_err!("WorkerFactory cannot connect {e:#?}")
                        })
                };

                // Use wait_for to execute the async connection in sync context
                let chan = wait_for(fut, "WorkerFactory::get_client").map_err(|e| {
                    internal_datafusion_err!(
                        "WorkerFactory Cannot wait for channel connect future {e:#?}"
                    )
                })??;
                trace!("WorkerFactory connected to {host}");

                // Cache the new connection for future use
                self.channels
                    .write()
                    .insert(host.addr.to_string(), chan.clone());

                chan
            }
        };
        debug!("WorkerFactory have channel now for {host}");

        // Create Flight client from the gRPC channel
        let flight_client = FlightClient::new(chan);
        debug!("WorkerFactory made flight client for {host}");

        // Wrap in WorkerClient with connection management capabilities
        Ok(WorkerClient::new(
            host.clone(),
            flight_client,
            self.channels.clone(),
        ))
    }
}

static FACTORY: OnceLock<WorkerClientFactory> = OnceLock::new();

/// Creates Arrow Flight clients for connecting to worker nodes
///
/// This function provides a centralized way to create and manage Flight clients for
/// communicating with distributed workers. It uses a factory pattern to reuse
/// connections and ensure consistent client configuration across the system.
pub fn get_client(host: &Host) -> Result<WorkerClient, DataFusionError> {
    // Get or initialize the global client factory
    let factory = FACTORY.get_or_init(WorkerClientFactory::new);
    factory.get_client(host)
}

/// A stream that combines multiple RecordBatch streams into a single output stream
///
/// This struct merges results from multiple worker streams into one unified stream,
/// enabling distributed query results to be consumed as if they came from a single source.
/// It uses round-robin polling to ensure fair access across all input streams.
///
/// Copied from datafusion_physical_plan::union as it's useful but not public
pub struct CombinedRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
    /// Stream entries
    entries: Vec<SendableRecordBatchStream>,
}

impl CombinedRecordBatchStream {
    /// Creates a new combined stream from multiple input streams
    ///
    /// All input streams must have the same schema. The combined stream will yield
    /// batches from all inputs in a round-robin fashion until all streams are exhausted.
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

/// Formats an execution plan with partition count information for debugging
///
/// This function creates a string representation of an execution plan tree that includes
/// the output partition count for each node. This is particularly useful for understanding
/// how data is distributed in the execution pipeline and debugging partition-related issues.
pub fn display_plan_with_partition_counts(plan: &Arc<dyn ExecutionPlan>) -> impl Display {
    let mut output = String::with_capacity(1000);

    print_node(plan, 0, &mut output);
    output
}

/// Recursively prints execution plan nodes with partition information
///
/// This helper function traverses the execution plan tree and formats each node with
/// its partition count and execution details. Used internally by display_plan_with_partition_counts
/// to build the complete plan representation.
fn print_node(plan: &Arc<dyn ExecutionPlan>, indent: usize, output: &mut String) {
    // Format node with partition count and plan details
    output.push_str(&format!(
        "[ output_partitions: {}]{:>indent$}{}",
        plan.output_partitioning().partition_count(),
        "",
        displayable(plan.as_ref()).set_show_schema(true).one_line(),
        indent = indent
    ));

    // Recursively process child nodes with increased indentation
    for child in plan.children() {
        print_node(child, indent + 2, output);
    }
}

/// Automatically registers object stores for data sources referenced in an execution plan
///
/// This function traverses an execution plan tree to find DataSourceExec nodes and
/// automatically registers appropriate object stores for their file paths. This ensures
/// that workers can access external data sources like S3, GCS, or local files when
/// executing their assigned plan portions.
pub fn register_object_store_for_paths_in_plan(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<(), DataFusionError> {
    // Function to check each plan node for data sources requiring object store registration
    let check_plan = |plan: Arc<dyn ExecutionPlan>| -> Result<_, DataFusionError> {
        for input in plan.children().into_iter() {
            if let Some(node) = input.as_any().downcast_ref::<DataSourceExec>() {
                if let Some(config) = node.data_source().as_any().downcast_ref::<FileScanConfig>() {
                    let url = &config.object_store_url;
                    // Register the appropriate object store for this data source
                    maybe_register_object_store(ctx, url.as_ref())?
                }
            }
        }
        Ok(Transformed::no(plan))
    };

    // Walk the entire plan tree to find and register all data sources
    plan.transform_down(check_plan)?;

    Ok(())
}

/// Registers an appropriate object store with the session context based on the URL scheme
///
/// This function examines a URL and registers the corresponding object store implementation
/// (S3, Google Cloud Storage, HTTP, or local filesystem) with the DataFusion session context.
/// This enables the query engine to read data from various storage backends.
///
/// # Supported URL Schemes
/// - `s3://` - Amazon S3 (using credentials from environment)
/// - `gs://` or `gcs://` - Google Cloud Storage
/// - `http://` or `https://` - HTTP-based storage
/// - Other - Local filesystem
pub fn maybe_register_object_store(ctx: &SessionContext, url: &Url) -> Result<(), DataFusionError> {
    // Determine object store type and configuration based on URL scheme
    let (ob_url, object_store) = if url.as_str().starts_with("s3://") {
        // Amazon S3 configuration
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
        // Google Cloud Storage configuration
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
        // HTTP/HTTPS configuration
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
        // Local filesystem configuration (default)
        let local = object_store::local::LocalFileSystem::new();
        (
            ObjectStoreUrl::parse("file://")?,
            Arc::new(local) as Arc<dyn ObjectStore>,
        )
    };

    debug!("Registering object store for {}", ob_url);

    // Register the object store with the session context
    ctx.register_object_store(ob_url.as_ref(), object_store);
    Ok(())
}

/// Starts a TCP listener on the specified port for distributed service communication
///
/// This function creates and binds a TCP listener that distributed services (proxy or worker)
/// use to accept incoming connections. The listener is configured to bind to all interfaces
/// (0.0.0.0) to allow connections from other nodes in the distributed cluster.
pub async fn start_up(port: usize) -> Result<TcpListener> {
    let my_host_str = format!("0.0.0.0:{}", port);

    // Bind TCP listener to the specified port on all interfaces
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
