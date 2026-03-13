use crate::common::task_ctx_with_extension;
use crate::flight_service::WorkerConnectionPool;
use crate::flight_service::test_utils::memory_worker::MemoryWorker;
use crate::{
    BoxCloneSyncChannel, ChannelResolver, DistributedExt, DistributedTaskContext, ExecutionTask,
    NetworkShuffleExec, Stage, create_flight_client,
};
use arrow::datatypes::DataType::{
    Boolean, Dictionary, Float64, Int32, Int64, List, Timestamp, UInt8, Utf8,
};
use arrow::datatypes::{Field, Schema, TimeUnit};
use arrow::util::data_gen::create_random_batch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_ipc::CompressionType;
use datafusion::common::{Result, exec_err};
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

/// [ChannelResolver] implementation that returns gRPC clients backed by an in-memory
/// tokio duplex rather than a TCP connection.
#[derive(Clone)]
pub struct InMemoryChannelsResolver {
    channels: Vec<BoxCloneSyncChannel>,
}

#[async_trait::async_trait]
impl ChannelResolver for InMemoryChannelsResolver {
    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>> {
        let Some(port) = url.port() else {
            return exec_err!("Missing port in url {url}");
        };
        Ok(create_flight_client(self.channels[port as usize].clone()))
    }
}

/// Configuration for the worker connection pool benchmark.
pub struct ShuffleBench {
    pub producer_tasks: usize,
    pub consumer_tasks: usize,
    pub partitions: usize,
    pub total_rows: usize,
    pub batch_size: usize,
    pub compression: Option<CompressionType>,
}

impl Display for ShuffleBench {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}producer_{}consumer_{}partitions_{}rows_{}batch_{:?}compression",
            self.producer_tasks,
            self.consumer_tasks,
            self.partitions,
            self.total_rows,
            self.batch_size,
            self.compression
        )
    }
}

impl ShuffleBench {
    pub async fn run(&self) -> Result<()> {
        #[rustfmt::skip]
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", Int64, false),
            Field::new("metric", Float64, false),
            Field::new("flag", Boolean, true),
            Field::new("label", Utf8, true),
            Field::new("category", Dictionary(Box::new(Int32), Box::new(Utf8)), true),
            Field::new("raw", UInt8, false),
            Field::new("ts", Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("count", Int32, false),
            Field::new("tags", List(Arc::new(Field::new_list_field(Utf8, true))), true),
        ]));
        let base_batch = create_random_batch(Arc::clone(&schema), self.batch_size, 0.1, 0.5)?;

        let mut batches = vec![];
        let mut total_rows = self.total_rows;
        while total_rows > 0 {
            batches.push(base_batch.clone());
            total_rows = total_rows.saturating_sub(self.batch_size);
        }
        let mut workers = vec![];
        for input_task_i in 0..self.producer_tasks {
            workers.push(MemoryWorker::new(input_task_i).with_compression(self.compression));
        }

        let partitions_per_producer_task = self.partitions * self.consumer_tasks;

        // Round-robin the batches across workers and partitions.
        let mut worker_i = 0;
        let mut partition_i = 0;
        while let Some(batch) = batches.pop() {
            workers[worker_i].add_batch(partition_i, batch);
            partition_i += 1;
            if partition_i >= partitions_per_producer_task {
                partition_i = 0;
                worker_i += 1;
            }
            if worker_i >= workers.len() {
                worker_i = 0
            }
        }

        let mut channels = vec![];
        for worker in workers {
            channels.push(worker.into_channel().await);
        }

        let channel_resolver = InMemoryChannelsResolver { channels };

        let task_ctx = SessionStateBuilder::new()
            .with_distributed_channel_resolver(channel_resolver)
            .with_distributed_compression(self.compression)?
            .build()
            .task_ctx();

        let input_stage = Stage {
            query_id: Uuid::from_u128(0),
            num: 0,
            plan: None,
            tasks: (0..self.producer_tasks)
                .map(|i| ExecutionTask {
                    url: Some(Url::parse(&format!("http://localhost:{i}")).unwrap()),
                })
                .collect(),
        };

        let mut join_set = JoinSet::default();
        for i in 0..self.consumer_tasks {
            let shuffle = NetworkShuffleExec {
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(schema.clone()),
                    Partitioning::UnknownPartitioning(self.partitions),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
                input_stage: input_stage.clone(),
                worker_connections: WorkerConnectionPool::new(self.producer_tasks),
                metrics_collection: Arc::new(Default::default()),
            };
            let task_ctx = Arc::new(task_ctx_with_extension(
                &task_ctx,
                DistributedTaskContext {
                    task_index: i,
                    task_count: self.consumer_tasks,
                },
            ));

            for p in 0..shuffle.properties.partitioning.partition_count() {
                let stream = shuffle.execute(p, Arc::clone(&task_ctx))?;
                join_set.spawn(async move { stream.try_collect::<Vec<_>>().await });
            }
        }
        for task in join_set.join_all().await {
            task?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_benchmark_works() -> Result<()> {
        ShuffleBench {
            producer_tasks: 4,
            consumer_tasks: 4,
            partitions: 4,
            total_rows: 100_000,
            batch_size: 1024,
            compression: None,
        }
        .run()
        .await
    }
}
