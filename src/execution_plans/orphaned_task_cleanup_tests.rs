use crate::common::task_ctx_with_extension;
use crate::stage::RemoteStage;
use crate::worker::WorkerConnectionPool;
use crate::worker::generated::worker::worker_service_client::WorkerServiceClient;
use crate::worker::generated::worker::{ExecuteTaskRequest, NoneHead, execute_task_request};
use crate::worker::test_utils::worker_handles::{
    MemoryWorkerHandle, execute_local_task_for_test, invalidate_task_for_test,
    register_plan_on_worker, test_task_key_with_query,
};
use crate::{
    BoxCloneSyncChannel, ChannelResolver, DistributedExt, DistributedTaskContext,
    NetworkShuffleExec, Stage, Worker, create_worker_client,
};
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_ipc::CompressionType;
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
use futures::{StreamExt, TryStreamExt};
use std::collections::HashSet;
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

#[derive(Clone, Copy)]
struct Scenario {
    rank: usize,
    name: &'static str,
    action: ScenarioAction,
    producer_shape: ProducerShape,
    producer_tasks: usize,
    consumer_tasks: usize,
    partitions_per_consumer: usize,
    input_partitions: usize,
    rows_per_producer: usize,
    batch_size: usize,
    compression: Compression,
    worker_buffer_budget_bytes: Option<usize>,
    shuffle_batch_size: Option<usize>,
}

impl Scenario {
    const fn new(rank: usize, name: &'static str, action: ScenarioAction) -> Self {
        Self {
            rank,
            name,
            action,
            producer_shape: ProducerShape::Source,
            producer_tasks: 2,
            consumer_tasks: 2,
            partitions_per_consumer: 4,
            input_partitions: 2,
            rows_per_producer: 256,
            batch_size: 32,
            compression: Compression::None,
            worker_buffer_budget_bytes: None,
            shuffle_batch_size: None,
        }
    }

    const fn with_shape(mut self, producer_shape: ProducerShape) -> Self {
        self.producer_shape = producer_shape;
        self
    }

    const fn with_topology(
        mut self,
        producer_tasks: usize,
        consumer_tasks: usize,
        partitions_per_consumer: usize,
        input_partitions: usize,
    ) -> Self {
        self.producer_tasks = producer_tasks;
        self.consumer_tasks = consumer_tasks;
        self.partitions_per_consumer = partitions_per_consumer;
        self.input_partitions = input_partitions;
        self
    }

    const fn with_rows(mut self, rows_per_producer: usize, batch_size: usize) -> Self {
        self.rows_per_producer = rows_per_producer;
        self.batch_size = batch_size;
        self
    }

    const fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    const fn with_worker_buffer_budget(mut self, bytes: usize) -> Self {
        self.worker_buffer_budget_bytes = Some(bytes);
        self
    }

    const fn with_shuffle_batch_size(mut self, rows: usize) -> Self {
        self.shuffle_batch_size = Some(rows);
        self
    }
}

#[derive(Clone, Copy)]
enum ScenarioAction {
    CompleteAllConcurrent,
    CompleteAllSequential,
    CompleteConsumersOneAtATime,
    DropConsumerBeforePoll,
    DropConsumerAfterFirstBatch,
    DropOnePartitionAfterFirstBatch,
    DropEvenPartitionsAfterFirstBatch,
    PollAllOnceThenDropAll,
    HoldPartitionWhileSiblingsFinish,
    RequestSubsetThenDropShuffle,
    RequestSubsetKeepShuffleBriefly,
    DelayedSecondConsumer,
    DropConsumersExceptLast,
    DropStreamsReverseOrder,
    CompleteOnlyOnePartitionPerConsumer,
}

#[derive(Clone, Copy)]
enum ProducerShape {
    Source,
    HashJoin,
}

#[derive(Clone, Copy)]
enum Compression {
    None,
    Lz4,
    Zstd,
}

impl Compression {
    fn as_arrow(self) -> Option<CompressionType> {
        match self {
            Self::None => None,
            Self::Lz4 => Some(CompressionType::LZ4_FRAME),
            Self::Zstd => Some(CompressionType::ZSTD),
        }
    }
}

const SCENARIOS: &[Scenario] = &[
    Scenario::new(
        1,
        "dropped_consumer_after_first_batch_sibling_completes_hash_join",
        ScenarioAction::DropConsumerAfterFirstBatch,
    )
    .with_shape(ProducerShape::HashJoin)
    .with_topology(3, 2, 4, 3)
    .with_rows(384, 32),
    Scenario::new(
        2,
        "single_partition_dropped_after_first_batch_siblings_complete_hash_join",
        ScenarioAction::DropOnePartitionAfterFirstBatch,
    )
    .with_shape(ProducerShape::HashJoin)
    .with_topology(2, 2, 6, 3),
    Scenario::new(
        3,
        "held_partition_blocks_then_releases_siblings_hash_join",
        ScenarioAction::HoldPartitionWhileSiblingsFinish,
    )
    .with_shape(ProducerShape::HashJoin)
    .with_topology(2, 2, 4, 4)
    .with_rows(512, 16),
    Scenario::new(
        4,
        "subset_request_kept_alive_briefly_then_shuffle_drop_cleans",
        ScenarioAction::RequestSubsetKeepShuffleBriefly,
    )
    .with_shape(ProducerShape::HashJoin)
    .with_topology(2, 1, 8, 4),
    Scenario::new(
        5,
        "drop_consumer_before_poll_sibling_completes_hash_join",
        ScenarioAction::DropConsumerBeforePoll,
    )
    .with_shape(ProducerShape::HashJoin)
    .with_topology(3, 2, 4, 3)
    .with_rows(384, 32),
    Scenario::new(
        6,
        "drop_even_partitions_after_first_batch",
        ScenarioAction::DropEvenPartitionsAfterFirstBatch,
    )
    .with_topology(2, 3, 4, 2),
    Scenario::new(
        7,
        "poll_all_partitions_once_then_drop_all",
        ScenarioAction::PollAllOnceThenDropAll,
    )
    .with_topology(3, 2, 4, 3),
    Scenario::new(
        8,
        "all_partitions_complete_concurrently",
        ScenarioAction::CompleteAllConcurrent,
    ),
    Scenario::new(
        9,
        "all_partitions_complete_sequentially",
        ScenarioAction::CompleteAllSequential,
    )
    .with_topology(2, 2, 6, 2),
    Scenario::new(
        10,
        "consumers_complete_one_at_a_time",
        ScenarioAction::CompleteConsumersOneAtATime,
    )
    .with_topology(3, 3, 3, 2),
    Scenario::new(
        11,
        "delayed_second_consumer",
        ScenarioAction::DelayedSecondConsumer,
    )
    .with_topology(2, 2, 5, 2),
    Scenario::new(
        12,
        "drop_all_but_last_consumer",
        ScenarioAction::DropConsumersExceptLast,
    )
    .with_topology(4, 3, 4, 2),
    Scenario::new(
        13,
        "drop_streams_reverse_order",
        ScenarioAction::DropStreamsReverseOrder,
    )
    .with_topology(2, 2, 8, 2),
    Scenario::new(
        14,
        "complete_only_one_partition_per_consumer_then_drop_shuffle",
        ScenarioAction::CompleteOnlyOnePartitionPerConsumer,
    )
    .with_topology(3, 3, 5, 3),
    Scenario::new(
        15,
        "subset_request_then_shuffle_drop",
        ScenarioAction::RequestSubsetThenDropShuffle,
    )
    .with_topology(2, 1, 7, 2),
    Scenario::new(
        16,
        "tiny_worker_connection_budget",
        ScenarioAction::DropOnePartitionAfterFirstBatch,
    )
    .with_worker_buffer_budget(16 * 1024)
    .with_topology(2, 2, 6, 3),
    Scenario::new(
        17,
        "small_shuffle_batches",
        ScenarioAction::DropConsumerAfterFirstBatch,
    )
    .with_shuffle_batch_size(8)
    .with_rows(256, 8),
    Scenario::new(
        18,
        "lz4_drop_consumer_after_first_batch",
        ScenarioAction::DropConsumerAfterFirstBatch,
    )
    .with_compression(Compression::Lz4),
    Scenario::new(
        19,
        "zstd_single_partition_drop",
        ScenarioAction::DropOnePartitionAfterFirstBatch,
    )
    .with_compression(Compression::Zstd),
    Scenario::new(
        20,
        "many_empty_output_partitions",
        ScenarioAction::CompleteAllConcurrent,
    )
    .with_topology(2, 2, 16, 2)
    .with_rows(32, 8),
    Scenario::new(
        21,
        "one_producer_many_consumers",
        ScenarioAction::DropConsumersExceptLast,
    )
    .with_topology(1, 4, 3, 2),
    Scenario::new(
        22,
        "many_producers_one_consumer",
        ScenarioAction::DropEvenPartitionsAfterFirstBatch,
    )
    .with_topology(4, 1, 6, 2),
    Scenario::new(
        23,
        "many_to_many_wide_partitions",
        ScenarioAction::DropConsumerAfterFirstBatch,
    )
    .with_topology(4, 4, 4, 3),
    Scenario::new(
        24,
        "single_batch_large_rows",
        ScenarioAction::PollAllOnceThenDropAll,
    )
    .with_topology(2, 2, 4, 1)
    .with_rows(1024, 1024),
    Scenario::new(
        25,
        "hash_join_all_partitions_complete",
        ScenarioAction::CompleteAllConcurrent,
    )
    .with_shape(ProducerShape::HashJoin)
    .with_topology(2, 2, 4, 2),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ranked_orphan_cleanup_scenarios_release_producers() -> Result<()> {
    let mut failures = Vec::new();
    for scenario in SCENARIOS {
        let fixture = ShuffleCleanupFixture::new(*scenario).await?;
        let result = async {
            fixture.run_scenario().await.map_err(|err| {
                DataFusionError::External(
                    format!(
                        "rank {} {} scenario failed: {err}",
                        scenario.rank, scenario.name
                    )
                    .into(),
                )
            })?;
            fixture.assert_producer_cleanup().await.map_err(|err| {
                DataFusionError::External(
                    format!(
                        "rank {} {} cleanup failed: {err}",
                        scenario.rank, scenario.name
                    )
                    .into(),
                )
            })
        }
        .await;
        if let Err(err) = result {
            failures.push(err.to_string());
        }
        fixture.invalidate_producer_tasks().await;
    }
    if !failures.is_empty() {
        return exec_err!(
            "{} orphan cleanup scenarios failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subset_upper_task_request_retains_producer_until_task_entry_invalidated() -> Result<()> {
    let scenario = Scenario::new(
        1,
        "upper_worker_subset_request_leaves_unrequested_partitions_alive",
        ScenarioAction::RequestSubsetKeepShuffleBriefly,
    )
    .with_shape(ProducerShape::HashJoin)
    .with_topology(2, 1, 8, 3);

    let fixture = ShuffleCleanupFixture::new(scenario).await?;
    let upper_worker = Worker::default();
    let upper_task_key = test_task_key_with_query(fixture.query_id, 99);
    let upper_task_ctx = Arc::new(task_ctx_with_extension(
        &fixture.base_task_ctx,
        DistributedTaskContext {
            task_index: 0,
            task_count: 1,
        },
    ));
    let upper_plan = Arc::new(fixture.shuffle_for_task(0));
    register_plan_on_worker(
        &upper_worker,
        upper_task_ctx,
        upper_plan,
        upper_task_key.clone(),
        scenario.partitions_per_consumer,
    )
    .await;

    let (mut streams, _) = execute_local_task_for_test(
        &upper_worker,
        ExecuteTaskRequest {
            task_key: Some(upper_task_key.clone()),
            target_partition_start: 0,
            target_partition_end: 1,
            producer_head: Some(execute_task_request::ProducerHead::None(NoneHead {})),
        },
    )
    .await?;
    let mut stream = streams.pop().expect("one requested upper partition");
    poll_one_batch(&mut stream).await?;
    drop(stream);
    drop(streams);

    eventually(
        "upper task entry retained after subset completion",
        || async {
            if upper_worker.tasks_running().await == 1 {
                Ok(())
            } else {
                exec_err!("upper task entry was unexpectedly removed")
            }
        },
    )
    .await?;

    fixture.assert_producer_cleanup().await?;

    invalidate_task_for_test(&upper_worker, &upper_task_key).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn injected_error_before_first_poll_orphans_producer_task() -> Result<()> {
    let scenario = Scenario::new(
        1,
        "injected_error_before_first_poll_orphans_producer_task",
        ScenarioAction::DropConsumerBeforePoll,
    )
    .with_shape(ProducerShape::HashJoin)
    .with_topology(3, 2, 4, 3)
    .with_rows(384, 32);

    let fixture = ShuffleCleanupFixture::new(scenario).await?;
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
    scenario: Scenario,
    query_id: Uuid,
    schema: Arc<Schema>,
    base_task_ctx: Arc<TaskContext>,
    producer_workers: Vec<MemoryWorkerHandle>,
    input_stage_workers: Vec<Url>,
    drop_counter: Arc<AtomicUsize>,
}

impl ShuffleCleanupFixture {
    async fn new(scenario: Scenario) -> Result<Self> {
        let query_id = Uuid::new_v4();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let drop_counter = Arc::new(AtomicUsize::new(0));
        let mut producer_workers = Vec::with_capacity(scenario.producer_tasks);
        let mut channels = Vec::with_capacity(scenario.producer_tasks);
        let mut input_stage_workers = Vec::with_capacity(scenario.producer_tasks);

        for task_index in 0..scenario.producer_tasks {
            let partitions_batches = make_input_partitions(
                Arc::clone(&schema),
                scenario.rows_per_producer,
                scenario.batch_size,
                scenario.input_partitions,
                task_index * scenario.rows_per_producer,
            )?;
            let worker = MemoryWorkerHandle::spawn(
                task_index,
                partitions_batches,
                scenario.compression.as_arrow(),
            )
            .await;
            channels.push(worker.channel());
            input_stage_workers
                .push(Url::parse(&format!("http://localhost:{task_index}")).unwrap());
            producer_workers.push(worker);
        }

        let base_task_ctx = build_task_context(&scenario, channels)?;
        for worker in &producer_workers {
            let drop_counter = Arc::clone(&drop_counter);
            worker
                .register_plan_with(query_id, |input| {
                    build_producer_plan(input, scenario, Arc::clone(&drop_counter))
                })
                .await?;
        }

        Ok(Self {
            scenario,
            query_id,
            schema,
            base_task_ctx,
            producer_workers,
            input_stage_workers,
            drop_counter,
        })
    }

    async fn run_scenario(&self) -> Result<()> {
        let scenario = self.scenario;
        let label = format!("rank {} {}", scenario.rank, scenario.name);
        match scenario.action {
            ScenarioAction::CompleteAllConcurrent => self.collect_all_concurrent(&label).await,
            ScenarioAction::CompleteAllSequential => self.collect_all_sequential().await,
            ScenarioAction::CompleteConsumersOneAtATime => {
                for task_index in 0..scenario.consumer_tasks {
                    self.collect_consumer(task_index).await?;
                }
                Ok(())
            }
            ScenarioAction::DropConsumerBeforePoll => {
                let (shuffle, task_ctx) = self.consumer(0);
                let mut streams = Vec::new();
                for partition in 0..scenario.partitions_per_consumer {
                    streams.push(shuffle.execute(partition, Arc::clone(&task_ctx))?);
                }
                drop(streams);
                drop(shuffle);
                self.collect_tasks_except(&[(0, None)]).await
            }
            ScenarioAction::DropConsumerAfterFirstBatch => {
                let (shuffle, task_ctx) = self.consumer(0);
                let mut streams = Vec::new();
                for partition in 0..scenario.partitions_per_consumer {
                    streams.push(shuffle.execute(partition, Arc::clone(&task_ctx))?);
                }
                if let Some(stream) = streams.first_mut() {
                    poll_one_batch(stream).await?;
                }
                drop(streams);
                drop(shuffle);
                self.collect_tasks_except(&[(0, None)]).await
            }
            ScenarioAction::DropOnePartitionAfterFirstBatch => {
                let (shuffle, task_ctx) = self.consumer(0);
                let mut stream = shuffle.execute(0, Arc::clone(&task_ctx))?;
                poll_one_batch(&mut stream).await?;
                drop(stream);
                self.collect_existing_consumer_except(&shuffle, &task_ctx, &[0])
                    .await?;
                drop(shuffle);
                self.collect_consumers_from(1).await
            }
            ScenarioAction::DropEvenPartitionsAfterFirstBatch => {
                for task_index in 0..scenario.consumer_tasks {
                    let (shuffle, task_ctx) = self.consumer(task_index);
                    let mut dropped = Vec::new();
                    for partition in (0..scenario.partitions_per_consumer).step_by(2) {
                        let mut stream = shuffle.execute(partition, Arc::clone(&task_ctx))?;
                        poll_one_batch(&mut stream).await?;
                        drop(stream);
                        dropped.push(partition);
                    }
                    self.collect_existing_consumer_except(&shuffle, &task_ctx, &dropped)
                        .await?;
                    drop(shuffle);
                }
                Ok(())
            }
            ScenarioAction::PollAllOnceThenDropAll => {
                let mut streams = Vec::new();
                for task_index in 0..scenario.consumer_tasks {
                    let (shuffle, task_ctx) = self.consumer(task_index);
                    for partition in 0..scenario.partitions_per_consumer {
                        let mut stream = shuffle.execute(partition, Arc::clone(&task_ctx))?;
                        poll_one_batch(&mut stream).await?;
                        streams.push(stream);
                    }
                    drop(shuffle);
                }
                drop(streams);
                Ok(())
            }
            ScenarioAction::HoldPartitionWhileSiblingsFinish => {
                let (shuffle, task_ctx) = self.consumer(0);
                let mut held = shuffle.execute(0, Arc::clone(&task_ctx))?;
                poll_one_batch(&mut held).await?;
                self.collect_existing_consumer_except(&shuffle, &task_ctx, &[0])
                    .await?;
                self.collect_consumers_from(1).await?;
                drop(held);
                drop(shuffle);
                Ok(())
            }
            ScenarioAction::RequestSubsetThenDropShuffle => {
                let (shuffle, task_ctx) = self.consumer(0);
                let stream = shuffle.execute(0, task_ctx)?;
                drain_stream(stream).await?;
                drop(shuffle);
                Ok(())
            }
            ScenarioAction::RequestSubsetKeepShuffleBriefly => {
                let (shuffle, task_ctx) = self.consumer(0);
                let stream = shuffle.execute(0, task_ctx)?;
                drain_stream(stream).await?;
                sleep(Duration::from_millis(100)).await;
                drop(shuffle);
                Ok(())
            }
            ScenarioAction::DelayedSecondConsumer => {
                let mut join_set = JoinSet::new();
                self.spawn_consumer(&mut join_set, 0)?;
                sleep(Duration::from_millis(25)).await;
                for task_index in 1..scenario.consumer_tasks {
                    self.spawn_consumer(&mut join_set, task_index)?;
                }
                collect_join_set(join_set, "delayed second consumer").await
            }
            ScenarioAction::DropConsumersExceptLast => {
                for task_index in 0..scenario.consumer_tasks.saturating_sub(1) {
                    let (shuffle, task_ctx) = self.consumer(task_index);
                    let mut stream = shuffle.execute(0, task_ctx)?;
                    poll_one_batch(&mut stream).await?;
                    drop(stream);
                    drop(shuffle);
                }
                self.collect_consumer(scenario.consumer_tasks - 1).await
            }
            ScenarioAction::DropStreamsReverseOrder => {
                let mut streams = Vec::new();
                for task_index in 0..scenario.consumer_tasks {
                    let (shuffle, task_ctx) = self.consumer(task_index);
                    for partition in 0..scenario.partitions_per_consumer {
                        let mut stream = shuffle.execute(partition, Arc::clone(&task_ctx))?;
                        poll_one_batch(&mut stream).await?;
                        streams.push(stream);
                    }
                    drop(shuffle);
                }
                while let Some(stream) = streams.pop() {
                    drop(stream);
                }
                Ok(())
            }
            ScenarioAction::CompleteOnlyOnePartitionPerConsumer => {
                for task_index in 0..scenario.consumer_tasks {
                    let (shuffle, task_ctx) = self.consumer(task_index);
                    let stream = shuffle.execute(0, task_ctx)?;
                    drain_stream(stream).await?;
                    drop(shuffle);
                }
                Ok(())
            }
        }
        .map_err(|err| DataFusionError::External(format!("{label}: {err}").into()))
    }

    async fn collect_all_concurrent(&self, label: &str) -> Result<()> {
        let mut join_set = JoinSet::new();
        for task_index in 0..self.scenario.consumer_tasks {
            self.spawn_consumer(&mut join_set, task_index)?;
        }
        collect_join_set(join_set, label).await
    }

    async fn collect_all_sequential(&self) -> Result<()> {
        for task_index in 0..self.scenario.consumer_tasks {
            let (shuffle, task_ctx) = self.consumer(task_index);
            for partition in 0..self.scenario.partitions_per_consumer {
                let stream = shuffle.execute(partition, Arc::clone(&task_ctx))?;
                drain_stream(stream).await?;
            }
        }
        Ok(())
    }

    async fn collect_consumer(&self, task_index: usize) -> Result<()> {
        let (shuffle, task_ctx) = self.consumer(task_index);
        let mut join_set = JoinSet::new();
        for partition in 0..self.scenario.partitions_per_consumer {
            let stream = shuffle.execute(partition, Arc::clone(&task_ctx))?;
            join_set.spawn(drain_stream(stream));
        }
        drop(shuffle);
        collect_join_set(join_set, "collect consumer").await
    }

    async fn collect_consumers_from(&self, first_task_index: usize) -> Result<()> {
        let mut join_set = JoinSet::new();
        for task_index in first_task_index..self.scenario.consumer_tasks {
            self.spawn_consumer(&mut join_set, task_index)?;
        }
        collect_join_set(join_set, "collect consumers from").await
    }

    async fn collect_existing_consumer_except(
        &self,
        shuffle: &NetworkShuffleExec,
        task_ctx: &Arc<TaskContext>,
        dropped: &[usize],
    ) -> Result<()> {
        let dropped: HashSet<_> = dropped.iter().copied().collect();
        let mut join_set = JoinSet::new();
        for partition in 0..self.scenario.partitions_per_consumer {
            if dropped.contains(&partition) {
                continue;
            }
            let stream = shuffle.execute(partition, Arc::clone(task_ctx))?;
            join_set.spawn(drain_stream(stream));
        }
        collect_join_set(join_set, "collect existing consumer except").await
    }

    async fn collect_tasks_except(&self, dropped_tasks: &[(usize, Option<usize>)]) -> Result<()> {
        let mut dropped = HashSet::new();
        for (task, partition) in dropped_tasks {
            if partition.is_none() {
                dropped.insert(*task);
            }
        }
        let mut join_set = JoinSet::new();
        for task_index in 0..self.scenario.consumer_tasks {
            if !dropped.contains(&task_index) {
                self.spawn_consumer(&mut join_set, task_index)?;
            }
        }
        collect_join_set(join_set, "collect tasks except").await
    }

    async fn create_streams_then_error_before_polling(&self, task_index: usize) -> Result<()> {
        let (shuffle, task_ctx) = self.consumer(task_index);
        let mut streams = Vec::new();
        for partition in 0..self.scenario.partitions_per_consumer {
            streams.push(shuffle.execute(partition, Arc::clone(&task_ctx))?);
        }
        let _created_stream_count = streams.len();
        exec_err!("injected error before poll")
    }

    fn spawn_consumer(
        &self,
        join_set: &mut JoinSet<Result<usize>>,
        task_index: usize,
    ) -> Result<()> {
        let (shuffle, task_ctx) = self.consumer(task_index);
        for partition in 0..self.scenario.partitions_per_consumer {
            let stream = shuffle.execute(partition, Arc::clone(&task_ctx))?;
            join_set.spawn(drain_stream(stream));
        }
        drop(shuffle);
        Ok(())
    }

    fn consumer(&self, task_index: usize) -> (NetworkShuffleExec, Arc<TaskContext>) {
        let (shuffle, task_ctx) = self.consumer_context(task_index);
        (shuffle, task_ctx)
    }

    fn consumer_context(&self, task_index: usize) -> (NetworkShuffleExec, Arc<TaskContext>) {
        let task_ctx = Arc::new(task_ctx_with_extension(
            &self.base_task_ctx,
            DistributedTaskContext {
                task_index,
                task_count: self.scenario.consumer_tasks,
            },
        ));
        (self.shuffle_for_task(task_index), task_ctx)
    }

    fn shuffle_for_task(&self, _task_index: usize) -> NetworkShuffleExec {
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
                    self.scenario.partitions_per_consumer,
                ),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            input_stage,
            worker_connections: WorkerConnectionPool::new(self.scenario.producer_tasks),
        }
    }

    async fn assert_producer_cleanup(&self) -> Result<()> {
        let expected_drops = self.scenario.producer_tasks;
        eventually(
            &format!(
                "rank {} {} producer cleanup",
                self.scenario.rank, self.scenario.name
            ),
            || async {
                let tasks_running = self.producer_tasks_running().await;
                let drops = self.drop_count();
                if tasks_running == 0 && drops == expected_drops {
                    Ok(())
                } else {
                    exec_err!(
                        "producer tasks_running={tasks_running}, tracked drops={drops}/{expected_drops}"
                    )
                }
            },
        )
        .await
    }

    async fn assert_producer_leaked(&self) -> Result<()> {
        eventually(
            &format!(
                "rank {} {} producer leak",
                self.scenario.rank, self.scenario.name
            ),
            || async {
                let tasks_running = self.producer_tasks_running().await;
                let drops = self.drop_count();
                if tasks_running > 0 && drops < self.scenario.producer_tasks {
                    Ok(())
                } else {
                    exec_err!(
                        "producer unexpectedly cleaned up: tasks_running={tasks_running}, tracked drops={drops}/{}",
                        self.scenario.producer_tasks
                    )
                }
            },
        )
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

fn build_task_context(
    scenario: &Scenario,
    channels: Vec<BoxCloneSyncChannel>,
) -> Result<Arc<TaskContext>> {
    let resolver = TestChannelsResolver { channels };
    let mut builder = SessionStateBuilder::new()
        .with_distributed_channel_resolver(resolver)
        .with_distributed_compression(scenario.compression.as_arrow())?;
    if let Some(budget) = scenario.worker_buffer_budget_bytes {
        builder = builder.with_distributed_worker_connection_buffer_budget_bytes(budget)?;
    }
    if let Some(batch_size) = scenario.shuffle_batch_size {
        builder = builder.with_distributed_shuffle_batch_size(batch_size)?;
    }
    Ok(builder.build().task_ctx())
}

fn build_producer_plan(
    input: Arc<dyn ExecutionPlan>,
    scenario: Scenario,
    drop_counter: Arc<AtomicUsize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let child: Arc<dyn ExecutionPlan> = match scenario.producer_shape {
        ProducerShape::Source => input,
        ProducerShape::HashJoin => {
            let left = Arc::new(CoalescePartitionsExec::new(Arc::clone(&input)));
            let right = input;
            let left_on: PhysicalExprRef = Arc::new(Column::new("id", 0));
            let right_on: PhysicalExprRef = Arc::new(Column::new("id", 0));
            Arc::new(HashJoinExec::try_new(
                left,
                right,
                vec![(left_on, right_on)],
                None,
                &JoinType::Inner,
                Some(vec![0]),
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )?)
        }
    };
    let tracked = Arc::new(TrackedDropExec::new(child, drop_counter));
    Ok(Arc::new(RepartitionExec::try_new(
        tracked,
        Partitioning::Hash(
            vec![Arc::new(Column::new("id", 0))],
            scenario.partitions_per_consumer * scenario.consumer_tasks,
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

async fn poll_one_batch(stream: &mut SendableRecordBatchStream) -> Result<Option<usize>> {
    match timeout(STREAM_TIMEOUT, stream.next())
        .await
        .map_err(|_| DataFusionError::External("timed out polling stream".into()))?
    {
        Some(Ok(batch)) => Ok(Some(batch.num_rows())),
        Some(Err(err)) => Err(err),
        None => Ok(None),
    }
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
