use std::borrow::Cow;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::{Buf, BufMut};
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{
    Result, exec_datafusion_err, exec_err, internal_datafusion_err, internal_err,
};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion_distributed::{DistributedWorkUnitFeedContext, WorkUnitFeedProvider};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use iceberg::expr::Predicate;
use iceberg::scan::{FileScanTask, TableScan};
use prost::encoding::{DecodeContext, WireType};
use prost::{DecodeError, Message};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::IcebergConfig;
use crate::common::df_err;
use crate::file_partitions::FilePartitions;
use crate::planned_files::PlannedFiles;

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
                // The upstream serde implementation does not round-trip through
                // rmp-serde's compact struct representation.
                task: rmp_serde::to_vec_named(task.as_ref())
                    .expect("Iceberg table scans produce serializable file scan tasks"),
            }),
            FileScanTaskPayload::Serialized(task) => Cow::Borrowed(task),
        }
    }

    pub(crate) fn into_task(self) -> Result<FileScanTask> {
        match self.payload {
            FileScanTaskPayload::Native(task) => Ok(*task),
            FileScanTaskPayload::Serialized(task) => {
                rmp_serde::from_slice(&task.task).map_err(|error| {
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
/// At execution time, one dispatcher reads either retained planned files or a live
/// Iceberg scan, and places each [FileScanTask] in P * T output channels, where:
///  - P is the number of partition in each distributed task.
///  - T is the number of distributed tasks
///
/// ```text
///                    ┌───────────────────────────┐
///                    │    Lazily spawned task    │
///                    │ ┌───────────────────────┐ │
///                    │ │   File task source    │ │
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
    /// Query-local tasks discovered before execution, when enabled.
    pub(crate) planned: Option<Arc<PlannedFiles>>,
    /// Container for the lazily initialized dispatcher shared by both discovery modes.
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
            planned: self.planned.clone(),
            sync_manager: Default::default(),
        }
    }
}

impl IcebergWorkUnitFeed {
    fn table_scan(&self) -> Result<TableScan> {
        let builder = match self.snapshot_id {
            Some(id) => self.iceberg_table.scan().snapshot_id(id),
            None => self.iceberg_table.scan(),
        };
        let mut builder = match &self.projection {
            Some(columns) => builder.select(columns),
            None => builder.select_all(),
        };
        if let Some(predicate) = &self.predicates {
            builder = builder.with_filter(predicate.clone());
        }
        builder.build().map_err(df_err)
    }

    pub(crate) async fn prepare_files(
        &mut self,
        runtime: &iceberg::Runtime,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        let scan = self.table_scan()?;
        let may_filter_rows = self.predicates.is_some();
        // Keep manifest I/O off an embedder's IO-disabled query runtime. Cancel the
        // spawned planner if the query abandons physical planning.
        let (_cancel_on_drop, canceled) = oneshot::channel::<()>();
        let task = runtime.io().spawn(async move {
            tokio::select! {
                _ = canceled => exec_err!("Iceberg file planning canceled"),
                result = PlannedFiles::collect(scan, &context, may_filter_rows) => result,
            }
        });
        self.planned = Some(Arc::new(task.await.map_err(df_err)??));
        Ok(())
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
        let task_count = wuf_ctx.fan_out_tasks;
        let partitions_per_task = self.partitioning.partition_count();
        let out_partitions = task_count
            .checked_mul(partitions_per_task)
            .filter(|total| *total > 0)
            .ok_or_else(|| exec_datafusion_err!("Invalid Iceberg feed partition count"))?;
        if partition >= out_partitions {
            return internal_err!("Invalid feed index {partition}");
        }

        // Both discovery modes share one dispatcher, initialized by the first feed call.
        let sync_manager_or_err = self.sync_manager.get_or_init(|| {
            let planned = self.planned.clone();
            let greedy = IcebergConfig::from_task_context(&ctx).greedy_file_assignment;
            let table_scan = self.table_scan()?;

            // Fanout the FileScanTask stream across P * T output channels where:
            // - P is the number of output partitions per distributed task (`partition_count`)
            // - T is the number of distributed tasks (`fan_out_tasks`)
            let mut rxs = Vec::with_capacity(out_partitions);
            let mut txs = Vec::with_capacity(out_partitions);
            for _ in 0..out_partitions {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                rxs.push(Mutex::new(Some(rx)));
                txs.push(tx);
            }

            // A reference to this spawned task needs to be held, otherwise it will automatically
            // be canceled. The lifetime of this `task` variable needs to last as long as any of
            // the return streams of the `feed()` method.
            let task = SpawnedTask::spawn(async move {
                let result: Result<()> = async {
                    let mut destinations = if greedy {
                        let files = planned.as_ref().ok_or_else(|| {
                            exec_datafusion_err!(
                                "Greedy Iceberg assignment requires eager file planning"
                            )
                        })?;
                        FilePartitions::greedy(files.tasks(), task_count, partitions_per_task).await
                    } else {
                        FilePartitions::RoundRobin {
                            next_file: 0,
                            task_count,
                            partitions_per_task,
                        }
                    };
                    let mut files: BoxStream<'_, Result<FileScanTask>> =
                        if let Some(planned) = &planned {
                            // The plan retains reusable descriptors; execution owns its copies.
                            stream::iter(planned.tasks().iter().cloned())
                                .map(Ok)
                                .boxed()
                        } else {
                            table_scan
                                .plan_files()
                                .await
                                .map_err(df_err)?
                                .map(|task| task.map_err(df_err))
                                .boxed()
                        };

                    let mut dispatched = 0;
                    while let Some(task) = files.next().await {
                        let partition = destinations.next().ok_or_else(|| {
                            internal_datafusion_err!("Missing Iceberg file destination")
                        })?;
                        let _ = txs[partition].send(task.map(FileScanTaskWorkUnit::new));
                        dispatched += 1;
                        if dispatched == 256 {
                            dispatched = 0;
                            tokio::task::yield_now().await;
                        }
                    }
                    Ok(())
                }
                .await;
                if let Err(error) = result {
                    let _ = txs[0].send(Err(error));
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

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use datafusion::common::Result as DataFusionResult;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::prelude::SessionConfig;
    use datafusion_distributed::{DistributedWorkUnitFeedContext, WorkUnitFeedProvider};
    use futures::TryStreamExt;
    use iceberg::io::{MemoryStorage, Storage};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestListWriter,
        ManifestWriterBuilder, Struct,
    };

    use crate::test_utils::{FIXTURE_URI, IcebergTestHarness, taxi_metadata};
    use crate::{IcebergConfig, IcebergDataSource, IcebergWorkUnitFeed};

    #[tokio::test]
    async fn greedy_routing_balances_bytes_across_tasks_and_local_partitions()
    -> Result<(), Box<dyn Error>> {
        let expected = vec![vec![100], vec![5], vec![4, 90], vec![3, 10]];
        for reverse in [false, true] {
            let provider = file_feed(&[100, 90, 10, 5, 4, 3], reverse).await?;
            let context = assignment_context();
            let mut groups = Vec::new();
            for partition in 0..4 {
                let files: Vec<_> = provider
                    .feed(partition, Arc::clone(&context))?
                    .try_collect()
                    .await?;
                let mut lengths = files
                    .into_iter()
                    .map(|unit| unit.into_task().map(|file| file.length()))
                    .collect::<DataFusionResult<Vec<_>>>()?;
                lengths.sort_unstable();
                groups.push(lengths);
            }
            assert_eq!(groups, expected);
            assert!(provider.feed(0, context).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn equal_size_files_have_stable_destinations_and_empty_scans_stay_empty()
    -> Result<(), Box<dyn Error>> {
        for sizes in [vec![10; 4], vec![]] {
            for reverse in [false, true] {
                let provider = file_feed(&sizes, reverse).await?;
                let context = assignment_context();
                for (partition, file) in [0, 2, 1, 3].into_iter().enumerate() {
                    let units: Vec<_> = provider
                        .feed(partition, Arc::clone(&context))?
                        .try_collect()
                        .await?;
                    let paths = units
                        .into_iter()
                        .map(|unit| {
                            unit.into_task()
                                .map(|file| file.data_file_path().to_owned())
                        })
                        .collect::<DataFusionResult<Vec<_>>>()?;
                    let expected = if sizes.is_empty() {
                        vec![]
                    } else {
                        vec![format!("{FIXTURE_URI}/data/file-{file}.parquet")]
                    };
                    assert_eq!(paths, expected);
                }
            }
        }
        Ok(())
    }

    fn assignment_context() -> Arc<TaskContext> {
        let mut config = SessionConfig::new();
        let iceberg = IcebergConfig {
            greedy_file_assignment: true,
            ..Default::default()
        };
        config.options_mut().extensions.insert(iceberg);
        config.set_extension(Arc::new(DistributedWorkUnitFeedContext {
            fan_out_tasks: 2,
        }));
        Arc::new(TaskContext::default().with_session_config(config))
    }

    async fn file_feed(
        sizes: &[u64],
        reverse: bool,
    ) -> Result<IcebergWorkUnitFeed, Box<dyn Error>> {
        let metadata = taxi_metadata();
        let snapshot = metadata.current_snapshot().unwrap();
        let storage = MemoryStorage::new();
        let uri = format!("{FIXTURE_URI}/metadata/skewed-files.avro");
        let mut writer = ManifestWriterBuilder::new(
            storage.new_output(&uri)?,
            Some(snapshot.snapshot_id()),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        let mut files: Vec<_> = sizes.iter().copied().enumerate().collect();
        if reverse {
            files.reverse();
        }
        for (index, bytes) in files {
            writer.add_file(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_format(DataFileFormat::Parquet)
                    .record_count(1)
                    .file_size_in_bytes(bytes)
                    .partition(Struct::from_iter([Some(Literal::date_from_str(
                        "2024-01-10",
                    )?)]))
                    .file_path(format!("{FIXTURE_URI}/data/file-{index}.parquet"))
                    .build()?,
                snapshot.sequence_number(),
            )?;
        }
        let manifest = writer.write_manifest_file().await?;
        let mut list = ManifestListWriter::v2(
            storage
                .new_output(snapshot.manifest_list())?
                .writer()
                .await?,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        );
        list.add_manifests([manifest].into_iter())?;
        list.close().await?;
        let harness = IcebergTestHarness::builder()
            .with_file(&uri, storage.read(&uri).await?.to_vec())
            .with_file(
                snapshot.manifest_list(),
                storage.read(snapshot.manifest_list()).await?.to_vec(),
            )
            .with_table_metadata(metadata)
            .configure_session(|mut state| {
                let config = IcebergConfig {
                    eager_file_planning: true,
                    greedy_file_assignment: true,
                    ..Default::default()
                };
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                session.options_mut().extensions.insert(config);
                Ok(state)
            })?
            .build()
            .await?;
        let scan = harness.scan().await?;
        Ok(scan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<IcebergDataSource>()
            .unwrap()
            .feed()
            .inner()
            .unwrap()
            .clone())
    }
}
