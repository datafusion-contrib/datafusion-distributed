use crate::common::task_ctx_with_extension;
use crate::stage::RemoteStage;
use crate::worker::WorkerConnectionPool;
use crate::worker::generated::worker::worker_service_client::WorkerServiceClient;
use crate::worker::test_utils::worker_handles::MemoryWorkerHandle;
use crate::{
    BoxCloneSyncChannel, ChannelResolver, DistributedExt, DistributedTaskContext,
    NetworkShuffleExec, Stage, create_worker_client,
};
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result, exec_err};
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{Partitioning, PhysicalExprRef};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::TryStreamExt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use url::Url;
use uuid::Uuid;

const STREAM_TIMEOUT: Duration = Duration::from_secs(10);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

const PRODUCER_TASKS: usize = 3;
const CONSUMER_TASKS: usize = 2;
const PARTITIONS_PER_CONSUMER: usize = 4;
const INPUT_PARTITIONS: usize = 3;
const ROWS_PER_PRODUCER: usize = 384;
const BATCH_SIZE: usize = 32;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn injected_error_before_first_poll_orphans_producer_task() -> Result<()> {
    let fixture = ShuffleCleanupFixture::new().await?;
    let err = fixture
        .create_streams_then_error_before_polling(0)
        .await
        .expect_err("injected error should fail the first consumer task");
    assert!(err.to_string().contains("injected error before poll"));

    fixture.collect_consumers_from(1).await?;
    fixture.assert_producer_leaked().await?;
    fixture.invalidate_producer_tasks().await;

    Ok(())
}

struct ShuffleCleanupFixture {
    query_id: Uuid,
    schema: Arc<Schema>,
    base_task_ctx: Arc<TaskContext>,
    producer_workers: Vec<MemoryWorkerHandle>,
    input_stage_workers: Vec<Url>,
    drop_counter: Arc<AtomicUsize>,
}

impl ShuffleCleanupFixture {
    async fn new() -> Result<Self> {
        let query_id = Uuid::new_v4();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let drop_counter = Arc::new(AtomicUsize::new(0));
        let mut producer_workers = Vec::with_capacity(PRODUCER_TASKS);
        let mut channels = Vec::with_capacity(PRODUCER_TASKS);
        let mut input_stage_workers = Vec::with_capacity(PRODUCER_TASKS);

        for task_index in 0..PRODUCER_TASKS {
            let partitions_batches = make_input_partitions(
                Arc::clone(&schema),
                ROWS_PER_PRODUCER,
                BATCH_SIZE,
                INPUT_PARTITIONS,
                task_index * ROWS_PER_PRODUCER,
            )?;
            let worker = MemoryWorkerHandle::spawn(task_index, partitions_batches, None).await;
            channels.push(worker.channel());
            input_stage_workers
                .push(Url::parse(&format!("http://localhost:{task_index}")).unwrap());
            producer_workers.push(worker);
        }

        let base_task_ctx = build_task_context(channels);
        for worker in &producer_workers {
            let drop_counter = Arc::clone(&drop_counter);
            worker
                .register_plan_with(query_id, |input| {
                    build_producer_plan(input, Arc::clone(&drop_counter))
                })
                .await?;
        }

        Ok(Self {
            query_id,
            schema,
            base_task_ctx,
            producer_workers,
            input_stage_workers,
            drop_counter,
        })
    }

    async fn create_streams_then_error_before_polling(&self, task_index: usize) -> Result<()> {
        let (shuffle, task_ctx) = self.consumer(task_index);
        let mut streams = Vec::new();
        for partition in 0..PARTITIONS_PER_CONSUMER {
            streams.push(shuffle.execute(partition, Arc::clone(&task_ctx))?);
        }
        let _created_stream_count = streams.len();
        exec_err!("injected error before poll")
    }

    async fn collect_consumers_from(&self, first_task_index: usize) -> Result<()> {
        let mut join_set = JoinSet::new();
        for task_index in first_task_index..CONSUMER_TASKS {
            self.spawn_consumer(&mut join_set, task_index)?;
        }
        collect_join_set(join_set, "collect consumers from").await
    }

    fn spawn_consumer(
        &self,
        join_set: &mut JoinSet<Result<usize>>,
        task_index: usize,
    ) -> Result<()> {
        let (shuffle, task_ctx) = self.consumer(task_index);
        for partition in 0..PARTITIONS_PER_CONSUMER {
            let stream = shuffle.execute(partition, Arc::clone(&task_ctx))?;
            join_set.spawn(drain_stream(stream));
        }
        drop(shuffle);
        Ok(())
    }

    fn consumer(&self, task_index: usize) -> (NetworkShuffleExec, Arc<TaskContext>) {
        let task_ctx = Arc::new(task_ctx_with_extension(
            &self.base_task_ctx,
            DistributedTaskContext {
                task_index,
                task_count: CONSUMER_TASKS,
            },
        ));
        (self.shuffle_for_task(), task_ctx)
    }

    fn shuffle_for_task(&self) -> NetworkShuffleExec {
        let input_stage = Stage::Remote(RemoteStage {
            query_id: self.query_id,
            num: 0,
            workers: self.input_stage_workers.clone(),
        });
        NetworkShuffleExec {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&self.schema)),
                Partitioning::Hash(
                    vec![Arc::new(Column::new("id", 0))],
                    PARTITIONS_PER_CONSUMER,
                ),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            input_stage,
            worker_connections: WorkerConnectionPool::new(PRODUCER_TASKS),
        }
    }

    async fn assert_producer_leaked(&self) -> Result<()> {
        eventually("producer leak after unpolled upper streams", || async {
            let tasks_running = self.producer_tasks_running().await;
            let drops = self.drop_count();
            if tasks_running > 0 && drops < PRODUCER_TASKS {
                Ok(())
            } else {
                exec_err!(
                    "producer unexpectedly cleaned up: tasks_running={tasks_running}, tracked drops={drops}/{PRODUCER_TASKS}"
                )
            }
        })
        .await
    }

    async fn invalidate_producer_tasks(&self) {
        for worker in &self.producer_workers {
            worker.invalidate_task(self.query_id).await;
        }
    }

    async fn producer_tasks_running(&self) -> usize {
        let mut tasks = 0;
        for worker in &self.producer_workers {
            tasks += worker.tasks_running().await;
        }
        tasks
    }

    fn drop_count(&self) -> usize {
        self.drop_counter.load(Ordering::SeqCst)
    }
}

fn build_task_context(channels: Vec<BoxCloneSyncChannel>) -> Arc<TaskContext> {
    let resolver = TestChannelsResolver { channels };
    SessionStateBuilder::new()
        .with_distributed_channel_resolver(resolver)
        .build()
        .task_ctx()
}

fn build_producer_plan(
    input: Arc<dyn ExecutionPlan>,
    drop_counter: Arc<AtomicUsize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let left = Arc::new(CoalescePartitionsExec::new(Arc::clone(&input)));
    let right = input;
    let left_on: PhysicalExprRef = Arc::new(Column::new("id", 0));
    let right_on: PhysicalExprRef = Arc::new(Column::new("id", 0));
    let hash_join = Arc::new(HashJoinExec::try_new(
        left,
        right,
        vec![(left_on, right_on)],
        None,
        &JoinType::Inner,
        Some(vec![0]),
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let tracked = Arc::new(TrackedDropExec::new(hash_join, drop_counter));
    Ok(Arc::new(RepartitionExec::try_new(
        tracked,
        Partitioning::Hash(
            vec![Arc::new(Column::new("id", 0))],
            PARTITIONS_PER_CONSUMER * CONSUMER_TASKS,
        ),
    )?))
}

fn make_input_partitions(
    schema: Arc<Schema>,
    total_rows: usize,
    batch_size: usize,
    partition_count: usize,
    first_id: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    if batch_size == 0 {
        return exec_err!("batch_size must be greater than zero");
    }

    let mut partitions = vec![Vec::new(); partition_count.max(1)];
    let mut next_id = first_id as i64;
    let mut remaining = total_rows;
    let mut partition = 0;
    while remaining > 0 {
        let rows = remaining.min(batch_size);
        let ids: Vec<_> = (0..rows).map(|offset| next_id + offset as i64).collect();
        next_id += rows as i64;
        remaining -= rows;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(ids)) as ArrayRef],
        )?;
        partitions[partition].push(batch);
        partition = (partition + 1) % partitions.len();
    }

    if total_rows == 0 {
        partitions[0].push(RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef],
        )?);
    }

    Ok(partitions)
}

async fn drain_stream(stream: SendableRecordBatchStream) -> Result<usize> {
    let batches = timeout(STREAM_TIMEOUT, stream.try_collect::<Vec<_>>())
        .await
        .map_err(|_| DataFusionError::External("timed out collecting stream".into()))??;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

async fn collect_join_set(mut join_set: JoinSet<Result<usize>>, label: &str) -> Result<()> {
    let result = timeout(STREAM_TIMEOUT, async {
        let mut rows = 0;
        while let Some(task) = join_set.join_next().await {
            rows += task.map_err(|err| DataFusionError::External(Box::new(err)))??;
        }
        Ok::<usize, DataFusionError>(rows)
    })
    .await
    .map_err(|_| DataFusionError::External(format!("{label} timed out").into()))?;
    result.map(|_| ())
}

async fn eventually<F, Fut>(label: &str, mut check: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let last_err = match check().await {
            Ok(()) => return Ok(()),
            Err(err) => err.to_string(),
        };
        if Instant::now() >= deadline {
            return exec_err!("{label} timed out: {last_err}");
        }
        sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Clone)]
struct TestChannelsResolver {
    channels: Vec<BoxCloneSyncChannel>,
}

#[async_trait]
impl ChannelResolver for TestChannelsResolver {
    async fn get_worker_client_for_url(
        &self,
        url: &Url,
    ) -> Result<WorkerServiceClient<BoxCloneSyncChannel>> {
        let Some(port) = url.port() else {
            return exec_err!("missing port in test worker url {url}");
        };
        let Some(channel) = self.channels.get(port as usize) else {
            return exec_err!("missing in-memory worker channel for port {port}");
        };
        Ok(create_worker_client(channel.clone()))
    }
}

#[derive(Clone)]
struct TrackedDropExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    drop_counter: Arc<AtomicUsize>,
}

impl TrackedDropExec {
    fn new(input: Arc<dyn ExecutionPlan>, drop_counter: Arc<AtomicUsize>) -> Self {
        Self {
            properties: Arc::clone(input.properties()),
            input,
            drop_counter,
        }
    }
}

impl Drop for TrackedDropExec {
    fn drop(&mut self) {
        self.drop_counter.fetch_add(1, Ordering::SeqCst);
    }
}

impl Debug for TrackedDropExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackedDropExec").finish_non_exhaustive()
    }
}

impl DisplayAs for TrackedDropExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TrackedDropExec")
    }
}

impl ExecutionPlan for TrackedDropExec {
    fn name(&self) -> &str {
        "TrackedDropExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.input.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.input.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return exec_err!("TrackedDropExec expected one child, got {}", children.len());
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.drop_counter),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
