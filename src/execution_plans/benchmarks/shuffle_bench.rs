use super::fixture::{
    InMemoryChannelsResolver, benchmark_schema, make_input_partitions, rows_for_producer,
};
use crate::common::task_ctx_with_extension;
use crate::stage::RemoteStage;
use crate::worker::WorkerConnectionPool;
use crate::worker::test_utils::worker_handles::MemoryWorkerHandle;
use crate::{DistributedExt, DistributedTaskContext, NetworkShuffleExec, Stage};
use arrow::datatypes::Schema;
use arrow_ipc::CompressionType;
use datafusion::common::{Result, ScalarValue};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, PhysicalSortExpr, RangePartitioning,
    SplitPoint,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use futures::TryStreamExt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::task::JoinSet;
use url::Url;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShufflePartitioningMode {
    Hash,
    Range,
}

impl Display for ShufflePartitioningMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hash => write!(f, "hash"),
            Self::Range => write!(f, "range"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShuffleBench {
    pub scenario_name: &'static str,
    pub mode: ShufflePartitioningMode,
    pub producer_tasks: usize,
    pub consumer_tasks: usize,
    pub partitions: usize,
    pub total_rows: usize,
    pub batch_size: usize,
    pub compression: Option<CompressionType>,
}

impl ShuffleBench {
    pub fn one_to_one_baseline() -> Self {
        Self {
            scenario_name: "one_to_one_baseline",
            mode: ShufflePartitioningMode::Hash,
            producer_tasks: 1,
            consumer_tasks: 1,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        }
    }

    pub fn many_to_one_baseline(producer_tasks: usize) -> Self {
        Self {
            scenario_name: "many_to_one_baseline",
            mode: ShufflePartitioningMode::Hash,
            producer_tasks,
            consumer_tasks: 1,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        }
    }

    pub fn one_to_many_baseline(consumer_tasks: usize) -> Self {
        Self {
            scenario_name: "one_to_many_baseline",
            mode: ShufflePartitioningMode::Hash,
            producer_tasks: 1,
            consumer_tasks,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        }
    }

    pub fn many_to_many_baseline(tasks: usize) -> Self {
        Self {
            scenario_name: "many_to_many_baseline",
            mode: ShufflePartitioningMode::Hash,
            producer_tasks: tasks,
            consumer_tasks: tasks,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        }
    }

    pub fn with_mode(mut self, mode: ShufflePartitioningMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_consumer_tasks(mut self, consumer_tasks: usize) -> Self {
        self.consumer_tasks = consumer_tasks;
        self
    }

    pub fn with_total_rows(mut self, total_rows: usize) -> Self {
        self.total_rows = total_rows;
        self
    }

    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_compression(mut self, compression: Option<CompressionType>) -> Self {
        self.compression = compression;
        self
    }

    pub fn compression_name(&self) -> &'static str {
        match self.compression {
            None => "none",
            Some(CompressionType::LZ4_FRAME) => "lz4",
            Some(CompressionType::ZSTD) => "zstd",
            _ => "unknown",
        }
    }

    pub fn required_total_rows(&self) -> usize {
        self.producer_tasks
            .max(1)
            .saturating_mul(self.batch_size.max(1))
    }

    pub fn normalized(&self) -> Self {
        let mut normalized = self.clone();
        normalized.total_rows = normalized.total_rows.max(normalized.required_total_rows());
        normalized
    }

    pub fn label(&self) -> String {
        format!(
            "scenario={},mode={},producer_tasks={},consumer_tasks={},partitions={},total_rows={},batch_size={},compression={}",
            self.scenario_name,
            self.mode,
            self.producer_tasks,
            self.consumer_tasks,
            self.partitions,
            self.total_rows,
            self.batch_size,
            self.compression_name(),
        )
    }

    pub async fn run(&self) -> Result<()> {
        self.prepare().await?.run().await
    }

    pub async fn prepare(&self) -> Result<ShuffleFixture> {
        let bench = self.normalized();
        let schema = benchmark_schema();

        let mut workers = Vec::with_capacity(bench.producer_tasks);
        let mut channels = Vec::with_capacity(bench.producer_tasks);
        for task_index in 0..bench.producer_tasks {
            let partitions_batches = make_input_partitions(
                Arc::clone(&schema),
                rows_for_producer(bench.total_rows, bench.producer_tasks.max(1), task_index),
                bench.batch_size,
                1,
            )?;
            let worker =
                MemoryWorkerHandle::spawn(task_index, partitions_batches, bench.compression).await;
            channels.push(worker.channel());
            workers.push(worker);
        }

        let channel_resolver = InMemoryChannelsResolver { channels };
        let task_ctx = SessionStateBuilder::new()
            .with_distributed_channel_resolver(channel_resolver)
            .with_distributed_compression(bench.compression)?
            .build()
            .task_ctx();
        let input_stage_tasks = (0..bench.producer_tasks)
            .map(|i| Url::parse(&format!("http://localhost:{i}")).unwrap())
            .collect();

        Ok(ShuffleFixture {
            bench,
            schema,
            task_ctx,
            input_stage_workers: input_stage_tasks,
            workers,
        })
    }
}

impl Display for ShuffleBench {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

pub struct ShuffleFixture {
    bench: ShuffleBench,
    schema: Arc<Schema>,
    task_ctx: Arc<datafusion::execution::TaskContext>,
    input_stage_workers: Vec<Url>,
    workers: Vec<MemoryWorkerHandle>,
}

fn make_range_partitioning(splits_count: usize) -> RangePartitioning {
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(Column::new("id", 0)),
        Default::default(),
    )])
    .unwrap();
    if splits_count == 0 {
        return RangePartitioning::try_new(ordering, vec![]).unwrap();
    }
    let partitions = (splits_count + 1) as u128;
    let step = (u64::MAX as u128) / partitions;
    let splits = (1..=splits_count)
        .map(|k| {
            let val = (i64::MIN as i128 + (k as u128 * step) as i128) as i64;
            SplitPoint::new(vec![ScalarValue::Int64(Some(val))])
        })
        .collect::<Vec<_>>();
    RangePartitioning::try_new(ordering, splits).unwrap()
}

impl ShuffleFixture {
    pub async fn run(&self) -> Result<()> {
        let query_id = Uuid::new_v4();
        let total_partitions = self
            .bench
            .partitions
            .saturating_mul(self.bench.consumer_tasks.max(1));

        let producer_partitioning = match self.bench.mode {
            ShufflePartitioningMode::Hash => {
                Partitioning::Hash(vec![Arc::new(Column::new("id", 0))], total_partitions)
            }
            ShufflePartitioningMode::Range => {
                let range = make_range_partitioning(total_partitions.saturating_sub(1));
                Partitioning::Range(range)
            }
        };

        for worker in &self.workers {
            let prod_part = producer_partitioning.clone();
            worker
                .register_plan_with(query_id, move |input| {
                    Ok(Arc::new(RepartitionExec::try_new(input, prod_part)?))
                })
                .await?;
        }

        let input_stage = Stage::Remote(RemoteStage {
            query_id,
            num: 0,
            workers: self.input_stage_workers.clone(),
            runtime_stats: None,
        });

        let mut join_set = JoinSet::default();
        for task_index in 0..self.bench.consumer_tasks {
            let (shuffle_partitioning, properties_partitioning) = match self.bench.mode {
                ShufflePartitioningMode::Hash => {
                    let p = Partitioning::Hash(
                        vec![Arc::new(Column::new("id", 0))],
                        self.bench.partitions,
                    );
                    (p.clone(), p)
                }
                ShufflePartitioningMode::Range => {
                    let range = make_range_partitioning(total_partitions.saturating_sub(1));
                    (
                        Partitioning::Range(range),
                        Partitioning::UnknownPartitioning(1),
                    )
                }
            };
            let shuffle = NetworkShuffleExec {
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(Arc::clone(&self.schema)),
                    properties_partitioning,
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
                input_stage: input_stage.clone(),
                worker_connections: WorkerConnectionPool::new(self.bench.producer_tasks),
                partitioning: shuffle_partitioning,
            };
            let task_ctx = Arc::new(task_ctx_with_extension(
                &self.task_ctx,
                DistributedTaskContext {
                    task_index,
                    task_count: self.bench.consumer_tasks,
                },
            ));

            for partition in 0..shuffle.properties.partitioning.partition_count() {
                let stream = shuffle.execute(partition, Arc::clone(&task_ctx))?;
                join_set.spawn(async move {
                    let batches = stream.try_collect::<Vec<_>>().await?;
                    Ok::<usize, datafusion::common::DataFusionError>(
                        batches.iter().map(|batch| batch.num_rows()).sum(),
                    )
                });
            }
        }
        let mut actual_rows = 0;
        for task in join_set.join_all().await {
            actual_rows += task?;
        }
        let _row_count = actual_rows;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn smoke() -> Result<()> {
        let fixture = ShuffleBench::one_to_one_baseline()
            .with_partitions(1)
            .with_total_rows(128)
            .with_batch_size(64)
            .prepare()
            .await?;
        fixture.run().await
    }

    #[tokio::test]
    async fn smoke_range() -> Result<()> {
        let fixture = ShuffleBench::many_to_many_baseline(2)
            .with_mode(ShufflePartitioningMode::Range)
            .with_partitions(1)
            .with_total_rows(128)
            .with_batch_size(64)
            .prepare()
            .await?;
        fixture.run().await
    }

    #[tokio::test]
    async fn smoke_range_scaling() -> Result<()> {
        let fixture = ShuffleBench::many_to_many_baseline(4)
            .with_consumer_tasks(2)
            .with_mode(ShufflePartitioningMode::Range)
            .with_partitions(1)
            .with_total_rows(128)
            .with_batch_size(64)
            .prepare()
            .await?;
        fixture.run().await
    }
}
