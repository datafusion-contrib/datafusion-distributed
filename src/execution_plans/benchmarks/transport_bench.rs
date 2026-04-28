use super::fixture::{
    InMemoryChannelsResolver, benchmark_schema, make_input_partitions, rows_for_producer,
};
use crate::common::task_ctx_with_extension;
use crate::worker::test_utils::worker_handles::{MemoryWorkerHandle, TcpWorkerHandle};
use crate::{
    DefaultChannelResolver, DistributedExt, DistributedTaskContext, ExecutionTask,
    NetworkShuffleExec, Stage,
};
use arrow::datatypes::Schema;
use arrow_ipc::CompressionType;
use datafusion::common::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use futures::TryStreamExt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::task::JoinSet;
use url::Url;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub enum TransportBenchMode {
    InMemory,
    Tcp,
}

impl Display for TransportBenchMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InMemory => write!(f, "in_memory"),
            Self::Tcp => write!(f, "tcp"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TransportBench {
    pub mode: TransportBenchMode,
    pub scenario_name: &'static str,
    pub producer_tasks: usize,
    pub consumer_tasks: usize,
    pub partitions: usize,
    pub total_rows: usize,
    pub batch_size: usize,
    pub compression: Option<CompressionType>,
}

impl TransportBench {
    pub fn one_to_one_baseline(mode: TransportBenchMode) -> Self {
        Self {
            mode,
            scenario_name: "one_to_one_baseline",
            producer_tasks: 1,
            consumer_tasks: 1,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        }
    }

    pub fn one_to_many_baseline(mode: TransportBenchMode, consumer_tasks: usize) -> Self {
        Self {
            mode,
            scenario_name: "one_to_many_baseline",
            producer_tasks: 1,
            consumer_tasks,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        }
    }

    pub fn many_to_one_baseline(mode: TransportBenchMode, producer_tasks: usize) -> Self {
        Self {
            mode,
            scenario_name: "many_to_one_baseline",
            producer_tasks,
            consumer_tasks: 1,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        }
    }

    pub fn many_to_many_baseline(mode: TransportBenchMode, tasks: usize) -> Self {
        Self {
            mode,
            scenario_name: "many_to_many_baseline",
            producer_tasks: tasks,
            consumer_tasks: tasks,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        }
    }

    pub fn with_total_rows(mut self, total_rows: usize) -> Self {
        self.total_rows = total_rows;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
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
            .saturating_mul(self.producer_local_partition_count().max(1))
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

    pub async fn prepare(&self) -> Result<TransportFixture> {
        let bench = self.normalized();
        match bench.mode {
            TransportBenchMode::InMemory => bench.prepare_in_memory().await,
            TransportBenchMode::Tcp => bench.prepare_tcp().await,
        }
    }

    fn producer_local_partition_count(&self) -> usize {
        // Transport intentionally keeps the current producer-local partition layout.
        self.partitions * self.consumer_tasks.max(1)
    }

    async fn prepare_in_memory(&self) -> Result<TransportFixture> {
        let schema = benchmark_schema();
        let producer_local_partitions = self.producer_local_partition_count();

        let mut workers = Vec::with_capacity(self.producer_tasks);
        let mut channels = Vec::with_capacity(self.producer_tasks);
        for task_index in 0..self.producer_tasks {
            let partitions_batches = make_input_partitions(
                Arc::clone(&schema),
                rows_for_producer(self.total_rows, self.producer_tasks.max(1), task_index),
                self.batch_size,
                producer_local_partitions,
            )?;
            let worker =
                MemoryWorkerHandle::spawn(task_index, partitions_batches, self.compression).await;
            channels.push(worker.channel());
            workers.push(worker);
        }

        Ok(TransportFixture {
            bench: self.clone(),
            schema,
            task_ctx: SessionStateBuilder::new()
                .with_distributed_channel_resolver(InMemoryChannelsResolver { channels })
                .with_distributed_compression(self.compression)?
                .build()
                .task_ctx(),
            input_stage_tasks: (0..self.producer_tasks)
                .map(|i| ExecutionTask {
                    url: Some(Url::parse(&format!("http://localhost:{i}")).unwrap()),
                })
                .collect(),
            workers: PreparedTransportWorkers::InMemory(workers),
        })
    }

    async fn prepare_tcp(&self) -> Result<TransportFixture> {
        let schema = benchmark_schema();
        let mut workers = Vec::with_capacity(self.producer_tasks);
        for task_index in 0..self.producer_tasks {
            let partitions = make_input_partitions(
                Arc::clone(&schema),
                rows_for_producer(self.total_rows, self.producer_tasks.max(1), task_index),
                self.batch_size,
                self.producer_local_partition_count(),
            )?;
            workers.push(
                TcpWorkerHandle::spawn(
                    task_index,
                    Arc::clone(&schema),
                    partitions,
                    self.compression,
                )
                .await?,
            );
        }

        Ok(TransportFixture {
            bench: self.clone(),
            schema,
            task_ctx: SessionStateBuilder::new()
                .with_distributed_channel_resolver(DefaultChannelResolver::default())
                .with_distributed_compression(self.compression)?
                .build()
                .task_ctx(),
            input_stage_tasks: workers
                .iter()
                .map(|worker| ExecutionTask {
                    url: Some(worker.url.clone()),
                })
                .collect(),
            workers: PreparedTransportWorkers::Tcp(workers),
        })
    }
}

impl Display for TransportBench {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

pub struct TransportFixture {
    bench: TransportBench,
    schema: Arc<Schema>,
    task_ctx: Arc<datafusion::execution::TaskContext>,
    input_stage_tasks: Vec<ExecutionTask>,
    workers: PreparedTransportWorkers,
}

impl TransportFixture {
    pub async fn run(&self) -> Result<()> {
        let query_id = Uuid::new_v4();
        self.workers.register_plans(query_id).await?;

        let input_stage = Stage {
            query_id,
            num: 0,
            plan: None,
            tasks: self.input_stage_tasks.clone(),
        };

        let mut join_set = JoinSet::default();
        for task_index in 0..self.bench.consumer_tasks {
            let shuffle = NetworkShuffleExec {
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(Arc::clone(&self.schema)),
                    Partitioning::UnknownPartitioning(self.bench.partitions),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
                input_stage: input_stage.clone(),
                worker_connections: crate::worker::WorkerConnectionPool::new(
                    self.bench.producer_tasks,
                ),
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

enum PreparedTransportWorkers {
    InMemory(Vec<MemoryWorkerHandle>),
    Tcp(Vec<TcpWorkerHandle>),
}

impl PreparedTransportWorkers {
    async fn register_plans(&self, query_id: Uuid) -> Result<()> {
        match self {
            Self::InMemory(workers) => {
                for worker in workers {
                    worker.register_plan(query_id).await;
                }
                Ok(())
            }
            Self::Tcp(workers) => {
                for worker in workers {
                    worker.register_plan(query_id).await?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn smoke() -> Result<()> {
        let fixture = TransportBench::one_to_one_baseline(TransportBenchMode::InMemory)
            .with_partitions(1)
            .with_total_rows(128)
            .with_batch_size(64)
            .prepare()
            .await?;
        fixture.run().await
    }
}
