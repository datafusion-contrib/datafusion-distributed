use std::borrow::Cow;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::{Buf, BufMut};
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{Result, exec_datafusion_err, exec_err, internal_err};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion_distributed::{DistributedWorkUnitFeedContext, WorkUnitFeedProvider};
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use prost::encoding::{DecodeContext, WireType};
use prost::{DecodeError, Message};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::common::df_err;

#[derive(Clone, PartialEq, prost::Message)]
struct SerializedFileScanTask {
    #[prost(bytes = "vec", tag = "1")]
    task: Vec<u8>,
}

#[derive(Clone, Debug)]
enum FileScanTaskPayload {
    Native(Box<FileScanTask>),
    Serialized(SerializedFileScanTask),
}

/// An Iceberg file scan task that remains native until a transport serializes it.
#[derive(Clone, Debug)]
pub struct FileScanTaskWorkUnit {
    payload: FileScanTaskPayload,
}

impl Default for FileScanTaskWorkUnit {
    fn default() -> Self {
        Self {
            payload: FileScanTaskPayload::Serialized(SerializedFileScanTask::default()),
        }
    }
}

impl FileScanTaskWorkUnit {
    fn new(task: FileScanTask) -> Self {
        Self {
            payload: FileScanTaskPayload::Native(Box::new(task)),
        }
    }

    fn serialized(&self) -> Cow<'_, SerializedFileScanTask> {
        match &self.payload {
            FileScanTaskPayload::Native(task) => Cow::Owned(SerializedFileScanTask {
                task: serde_json::to_vec(task)
                    .expect("Iceberg table scans produce serializable file scan tasks"),
            }),
            FileScanTaskPayload::Serialized(task) => Cow::Borrowed(task),
        }
    }

    pub(crate) fn into_task(self) -> Result<FileScanTask> {
        match self.payload {
            FileScanTaskPayload::Native(task) => Ok(*task),
            FileScanTaskPayload::Serialized(task) => {
                serde_json::from_slice(&task.task).map_err(|error| {
                    exec_datafusion_err!("failed to deserialize Iceberg file scan task: {error}")
                })
            }
        }
    }
}

impl Message for FileScanTaskWorkUnit {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        self.serialized().encode_raw(buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> std::result::Result<(), DecodeError> {
        if let FileScanTaskPayload::Native(_) = &self.payload {
            self.payload = FileScanTaskPayload::Serialized(self.serialized().into_owned());
        }
        let FileScanTaskPayload::Serialized(task) = &mut self.payload else {
            unreachable!()
        };
        task.merge_field(tag, wire_type, buf, ctx)
    }

    fn encoded_len(&self) -> usize {
        self.serialized().encoded_len()
    }

    fn clear(&mut self) {
        self.payload = FileScanTaskPayload::Serialized(SerializedFileScanTask::default());
    }
}

/// Work unit feed implementation that yields [FileScanTask] messages at execution time.
///
/// It lazily spawns a task that scans an Iceberg table, and places each resolved [FileScanTask]
/// in P * T output channels, where:
///  - P is the number of partition in each distributed task.
///  - T is the number of distributed tasks
///
/// ```text
///                    ┌───────────────────────────┐
///                    │    Lazily spawned task    │
///                    │ ┌───────────────────────┐ │
///                    │ │  Iceberg Table Scan   │ │
///                    │ └───.─────────────.─────┘ │
///                    │    ( FileScanTask  )      │
///                    │     .─────────────.       │
///                    │    ( FileScanTask  )      │
///                    │     `─────────────'       │
///                    │           ...             │
///                    │     .─────────────.       │
///                    │    ( FileScanTask  )      │
///                    │     `─────┬┬┬┬────'       │
///         ┌──────────┼───────────┘││└────────────┼───────────┐
///         │          └────────────┼┼─────────────┘           │
///         │                ┌──────┘└────────┐                │
///         │                │                │                │
///         ▼                ▼                ▼                ▼
/// ┌───────────────┐┌───────────────┐┌───────────────┐┌───────────────┐
/// │ Output mpsc 0 ││ Output mpsc 1 ││ Output mpsc 2 ││ Output mpsc 3 │
/// │.─────────────.││.─────────────.││.─────────────.││.─────────────.│
/// ( FileScanTask  )( FileScanTask  )( FileScanTask  )( FileScanTask  )
/// │`─────────────'││.─────────────.││`─────────────'││.─────────────.│
/// │               │( FileScanTask  )│               │( FileScanTask  )
/// │               ││`─────────────'││               ││.─────────────.│
/// │               ││               ││               │( FileScanTask  )
/// │               ││               ││               ││`─────────────'│
/// │               ││               ││               ││               │
/// └───────────────┘└───────────────┘└───────────────┘└───────────────┘
/// ```
///
/// Each individual output channel ends up being a [datafusion_distributed::WorkUnit] stream that
/// goes to one partition of one distributed task:
///
/// ```text
/// ┌───────────────┐┌───────────────┐┌───────────────┐┌───────────────┐
/// │ Output mpsc 0 ││ Output mpsc 1 ││ Output mpsc 2 ││ Output mpsc 3 │
/// └───────────────┘└───────────────┘└───────────────┘└───────────────┘
///         │                │                │                │
///         │                │                │                │
/// ┌───────┼────────────────┼───────┐┌───────┼────────────────┼───────┐
/// │       ▼     Task 0     ▼       ││       ▼     Task 1     ▼       │
/// │┌──────────────┐┌──────────────┐││┌──────────────┐┌──────────────┐│
/// ││ Partition 0  ││ Partition 1  ││││ Partition 2  ││ Partition 3  ││
/// │└──────────────┘└──────────────┘││└──────────────┘└──────────────┘│
/// └────────────────────────────────┘└────────────────────────────────┘
/// ```
///
/// This works seamlessly in single-node and distributed mode using
/// [datafusion_distributed::WorkUnitFeed] machinery:
/// - If the query was not distributed, the [FileScanTask]s will be streamed in-memory.
/// - If the query was distributed, the [FileScanTask]s will be streamed over the network from
///   coordinator to workers.
#[derive(Debug)]
pub struct IcebergWorkUnitFeed {
    /// A table in the catalog.
    pub(crate) iceberg_table: iceberg::table::Table,
    /// Snapshot of the table to scan.
    pub(crate) snapshot_id: Option<i64>,
    /// Projection column names, None means all columns.
    pub(crate) projection: Option<Vec<String>>,
    /// Filters to apply to the table scan.
    pub(crate) predicates: Option<Predicate>,
    /// Partitioning scheme to which the feeds should adhere.
    /// TODO: Today, only Partitioning::UnknownPartitioning partitioning is supported.
    ///  Ideally, both Range partitioning and hash partitioning should be supported.
    pub(crate) partitioning: Partitioning,
    /// Container for the lazily initialized task that scans the Iceberg table.
    /// It will start as soon as the first [IcebergWorkUnitFeed::feed] is called.
    pub(crate) sync_manager: OnceLock<Result<SyncManager, Arc<DataFusionError>>>,
}

impl Clone for IcebergWorkUnitFeed {
    fn clone(&self) -> Self {
        Self {
            iceberg_table: self.iceberg_table.clone(),
            snapshot_id: self.snapshot_id,
            projection: self.projection.clone(),
            predicates: self.predicates.clone(),
            partitioning: self.partitioning.clone(),
            sync_manager: Default::default(),
        }
    }
}

type TakeableVec<T> = Vec<Mutex<Option<T>>>;

#[derive(Debug)]
pub(crate) struct SyncManager {
    task: Arc<SpawnedTask<()>>,
    feeds: TakeableVec<UnboundedReceiver<Result<FileScanTaskWorkUnit>>>,
}

impl WorkUnitFeedProvider for IcebergWorkUnitFeed {
    type WorkUnit = FileScanTaskWorkUnit;

    fn feed(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<Self::WorkUnit>>> {
        let wuf_ctx = DistributedWorkUnitFeedContext::from_ctx(&ctx);

        // This lazily spawns the tokio task that scans the Iceberg table.
        // Only the first IcebergWorkUnitFeed::feed call will get to execute it, and the
        // rest will just observe the already initialized result.
        let sync_manager_or_err = self.sync_manager.get_or_init(|| {
            // Start the table scan only once for all the .feed() calls.
            let scan_builder = match self.snapshot_id {
                Some(snapshot_id) => self.iceberg_table.scan().snapshot_id(snapshot_id),
                None => self.iceberg_table.scan(),
            };

            let mut scan_builder = match &self.projection {
                Some(column_names) => scan_builder.select(column_names),
                None => scan_builder.select_all(),
            };
            if let Some(pred) = &self.predicates {
                scan_builder = scan_builder.with_filter(pred.clone());
            }
            let table_scan = scan_builder.build().map_err(df_err)?;

            // Fanout the FileScanTask stream across P * T output channels where:
            // - P is the number of output partitions per distributed task (`partition_count`)
            // - T is the number of distributed tasks (`fan_out_tasks`)
            let out_partitions = wuf_ctx.fan_out_tasks * self.partitioning.partition_count();
            let mut rxs = Vec::with_capacity(out_partitions);
            let mut txs = Vec::with_capacity(out_partitions);
            for _ in 0..out_partitions {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                rxs.push(Mutex::new(Some(rx)));
                txs.push(tx);
            }

            // A reference to this spawned task needs to be held, otherwise it will automatically
            // be canceled. The lifetime of this `task` variable needs to leave as long as any of
            // the return streams of the `feed()` method.
            let task = SpawnedTask::spawn(async move {
                let mut stream = match table_scan.plan_files().await {
                    Ok(stream) => stream,
                    Err(err) => {
                        let _ = txs[0].send(Err(df_err(err)));
                        return;
                    }
                };

                // Round robing across output partitions.
                // TODO: this is fine for Partitioning::UnknownPartitioning, but any other
                //  partitioning will require smarter routing across output channels.
                let mut i = 0;
                while let Some(scan_task_or_err) = stream.next().await {
                    let partition = i % txs.len();
                    let work_unit = scan_task_or_err
                        .map(FileScanTaskWorkUnit::new)
                        .map_err(df_err);
                    let _ = txs[partition].send(work_unit);
                    i += 1;
                }
            });

            Ok(SyncManager {
                task: Arc::new(task),
                feeds: rxs,
            })
        });

        let sync_manager = match sync_manager_or_err {
            Ok(sync_manager) => sync_manager,
            Err(err) => return Err(DataFusionError::Shared(Arc::clone(err))),
        };

        let Some(feed) = sync_manager.feeds.get(partition) else {
            return internal_err!("Invalid feed index {partition}");
        };

        let Some(feed) = feed.lock().unwrap().take() else {
            return exec_err!("Feed with index {partition} already taken");
        };

        let task_ref = Arc::clone(&sync_manager.task);

        Ok(UnboundedReceiverStream::new(feed)
            .inspect(move |_| {
                let _ = &task_ref; // Keep the task alive as long as one feed is alive.
            })
            .boxed())
    }
}
