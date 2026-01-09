use crate::config_extension_ext::ContextGrpcMetadata;
use crate::flight_service::do_get::DoGet;
use crate::networking::get_distributed_channel_resolver;
use crate::protobuf::{
    FlightAppMetadata, StageKey, datafusion_error_to_tonic_status, map_flight_to_datafusion_error,
};
use crate::{DistributedConfig, Stage};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightData, Ticket};
use bytes::Bytes;
use dashmap::DashMap;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::{Stream, TryStreamExt};
use http::{Extensions, HeaderMap};
use prost::Message;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

/// Holds a list of lazily initialized [WorkerConnection]s. Each position in the underlying
/// `connections` vector corresponds to the connection to one worker. It assumes a 1:1 mapping
/// between worker and tasks, and upon calling [WorkerConnectionPool::get_or_init_worker_connection]
/// it will initialize the corresponding position in the vector matching the provided `target_task`
/// index.
pub(crate) struct WorkerConnectionPool {
    connections: Vec<OnceLock<Result<WorkerConnection, Arc<DataFusionError>>>>,
}

impl WorkerConnectionPool {
    /// Builds a new [WorkerConnectionPool] with as many empty slots for [WorkerConnection]s as
    /// the provided `input_tasks`.
    pub(crate) fn new(input_tasks: usize) -> Self {
        let mut connections = Vec::with_capacity(input_tasks);
        for _ in 0..input_tasks {
            connections.push(OnceLock::new());
        }
        Self { connections }
    }

    /// Lazily initializes the [WorkerConnection] corresponding to the provided `target_task`
    /// (therefore maintaining one independent [WorkerConnection] per `target_task`), and
    /// returns it.
    pub(crate) fn get_or_init_worker_connection(
        &self,
        input_stage: &Stage,
        target_partitions: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
    ) -> Result<&WorkerConnection> {
        let Some(worker_connection) = self.connections.get(target_task) else {
            return internal_err!(
                "WorkerConnections: Task index {target_task} not found, only have {} tasks",
                self.connections.len()
            );
        };

        let conn = worker_connection.get_or_init(|| {
            WorkerConnection::init(input_stage, target_partitions, target_task, ctx)
                .map_err(Arc::new)
        });

        match conn {
            Ok(v) => Ok(v),
            Err(err) => Err(DataFusionError::Shared(Arc::clone(err))),
        }
    }
}

type WorkerMsg = Result<(FlightData, FlightAppMetadata, MemoryReservation), Status>;

/// Represents a connection to one [Worker]. Network boundaries will use this for streaming
/// data from single partitions while the actual network communication is handling all the partitions
/// under the hood.
///
/// This is done so that, rather than issuing one gRPC stream per partition, we issue one gRPC stream
/// per group of partitions, and we multiplex streamed record batches locally to in-memory channels.
///
/// Even if Tonic can perfectly multiplex and interleave messages from different gRPC streams through
/// the same underlying TCP connection, there do is some overhead in having one gRPC stream per
/// partition VS a single gRPC stream interleaving multiple partitions. The whole serialized plan
/// needs to be sent over the wire on every gRPC call, so the less gRPC calls we do the better.
pub(crate) struct WorkerConnection {
    task: Arc<SpawnedTask<()>>,
    per_partition_rx: DashMap<usize, UnboundedReceiver<WorkerMsg>>,
}

impl WorkerConnection {
    fn init(
        input_stage: &Stage,
        target_partition_range: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
    ) -> Result<Self> {
        let channel_resolver = get_distributed_channel_resolver(ctx.as_ref());
        let d_cfg = DistributedConfig::from_config_options(ctx.session_config().options())?;

        let context_headers = ContextGrpcMetadata::headers_from_ctx(ctx);
        // TODO: this propagation should be automatic https://github.com/datafusion-contrib/datafusion-distributed/issues/247
        let context_headers = manually_propagate_distributed_config(context_headers, d_cfg);
        let ticket = Request::from_parts(
            MetadataMap::from_headers(context_headers),
            Extensions::default(),
            Ticket {
                ticket: DoGet {
                    plan_proto: Bytes::clone(input_stage.plan.encoded()?),
                    target_partitions: target_partition_range.clone().map(|v| v as u64).collect(),
                    stage_key: Some(StageKey::new(
                        Bytes::from(input_stage.query_id.as_bytes().to_vec()),
                        input_stage.num as u64,
                        target_task as u64,
                    )),
                    target_task_index: target_task as u64,
                    target_task_count: input_stage.tasks.len() as u64,
                }
                .encode_to_vec()
                .into(),
            },
        );

        let Some(task) = input_stage.tasks.get(target_task) else {
            return internal_err!("ProgrammingError: Task {target_task} not found");
        };
        let Some(url) = task.url.clone() else {
            return internal_err!("ProgrammingError: task is unassigned, cannot proceed");
        };

        // The senders and receivers are unbounded queues used for multiplexing the record
        // batches sent through the single gRPC stream into one stream per partition.
        // The received record batches contain information of the partition to which they belong,
        // so we use that for determining where to put them.
        let mut per_partition_tx = Vec::with_capacity(target_partition_range.len());
        let per_partition_rx = DashMap::with_capacity(target_partition_range.len());
        for partition in target_partition_range.clone() {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<WorkerMsg>();
            per_partition_tx.push(tx);
            per_partition_rx.insert(partition, rx);
        }

        // We are retaining record batches in memory until they are consumed, so we need to account
        // for them in the memory pool.
        let memory_pool = Arc::clone(ctx.memory_pool());

        // This task will pull data from all the partitions in `target_partition_range`, and will
        // fan them out to the appropriate `per_partition_rx` based on the "partition" declared
        // in each individual record batch flight metadata.
        let task = SpawnedTask::spawn(async move {
            let mut client = match channel_resolver.get_flight_client_for_url(&url).await {
                Ok(v) => v,
                Err(err) => {
                    return fanout(&per_partition_tx, datafusion_error_to_tonic_status(&err));
                }
            };
            let mut interleaved_stream = match client.do_get(ticket).await {
                Ok(v) => v.into_inner(),
                Err(err) => return fanout(&per_partition_tx, err),
            };

            let consumer = MemoryConsumer::new("WorkerConnection");

            while let Some(msg) = interleaved_stream.next().await {
                let msg = match msg {
                    Ok(v) => v,
                    Err(err) => return fanout(&per_partition_tx, err),
                };
                let flight_metadata = match FlightAppMetadata::decode(msg.app_metadata.as_ref()) {
                    Ok(v) => v,
                    Err(err) => {
                        return fanout(&per_partition_tx, Status::internal(err.to_string()));
                    }
                };

                let partition = flight_metadata.partition as usize;
                // the `per_partition_tx` variable is using a normal `Vec` for storing the
                // channel transmitters, so we need to subtract the `target_partition_range.start`
                // to the `partition` in order to offset it to the appropriate index.
                let sender_i = partition - target_partition_range.start;

                let Some(o_tx) = per_partition_tx.get(sender_i) else {
                    let msg = format!(
                        "Received partition {partition} in Flight metadata, but available partitions are {target_partition_range:?}"
                    );
                    return fanout(&per_partition_tx, Status::internal(msg));
                };

                // We need to send the memory reservation in the same tuple as the actual message
                // so that it gets dropped as soon as the message leaves the queue. Dropping the
                // memory reservation means releasing the memory from the pool for that specific
                // message
                let reservation = consumer.clone_with_new_id().register(&memory_pool);
                if o_tx.send(Ok((msg, flight_metadata, reservation))).is_err() {
                    return; // channel closed
                };
            }
        });

        Ok(Self {
            task: Arc::new(task),
            per_partition_rx,
        })
    }

    /// Streams the provided `partition` from the remote worker.
    ///
    /// Note that this does not issue a network request, the actual network request happened before
    /// in the init step, and is in charge of handling not only this `partition`, but also all the
    /// partitions passed in `target_partition_range`. This method just streams all the record
    /// batches belonging to the provided `partition` from an in-memory queue, but what populates
    /// this queue is [WorkerConnection::init].
    pub(crate) fn stream_partition(
        &self,
        partition: usize,
        on_metadata: impl Fn(FlightAppMetadata) + Send + Sync + 'static,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + 'static> {
        let Some((_, partition_receiver)) = self.per_partition_rx.remove(&partition) else {
            return internal_err!(
                "WorkerConnection has no stream for target partition {partition}. Was it already consumed?"
            );
        };
        let task = Arc::clone(&self.task);
        let stream = UnboundedReceiverStream::new(partition_receiver);
        let stream = stream.map_err(|err| FlightError::Tonic(Box::new(err)));
        let stream = stream.map_ok(move |(data, meta, reservation)| {
            drop(reservation); // <- drop the reservation, freeing memory on the memory pool.
            let _ = &task; // <- keep the task that pools data from the network alive.
            on_metadata(meta);
            data
        });
        let stream = FlightRecordBatchStream::new_from_flight_data(stream);
        let stream = stream.map_err(map_flight_to_datafusion_error);
        Ok(stream)
    }
}

fn fanout(o_txs: &[UnboundedSender<WorkerMsg>], err: Status) {
    for o_tx in o_txs {
        let _ = o_tx.send(Err(err.clone()));
    }
}

/// Manual propagation of the [DistributedConfig] fields relevant for execution. Can be removed
/// after https://github.com/datafusion-contrib/datafusion-distributed/issues/247 is fixed, as this will become automatic.
pub(super) fn manually_propagate_distributed_config(
    mut headers: HeaderMap,
    d_cfg: &DistributedConfig,
) -> HeaderMap {
    headers.insert(
        "distributed.collect_metrics",
        d_cfg.collect_metrics.to_string().parse().unwrap(),
    );
    headers
}

impl Debug for WorkerConnectionPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConnections")
            .field("connections", &self.connections.len())
            .finish()
    }
}

impl Clone for WorkerConnectionPool {
    fn clone(&self) -> Self {
        Self::new(self.connections.len())
    }
}

impl Debug for WorkerConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streamer").finish()
    }
}
