use crate::config_extension_ext::set_distributed_option_extension;
use crate::{BoxCloneSyncChannel, DistributedConfig, StageKey, TaskData, Worker};
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SessionStateBuilder;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tonic::transport::{Endpoint, Server};
use uuid::Uuid;

pub fn test_stage_key(task_number: u64) -> StageKey {
    StageKey {
        query_id: Bytes::from(Uuid::from_u128(0).as_bytes().to_vec()),
        stage_id: 0,
        task_number,
    }
}

pub struct MemoryWorker {
    task_index: usize,
    schema: Option<SchemaRef>,
    partitions_batches: Vec</* partition */ Vec<RecordBatch>>,
}

impl MemoryWorker {
    pub fn new(task_index: usize) -> Self {
        Self {
            task_index,
            schema: None,
            partitions_batches: vec![],
        }
    }

    pub fn add_batch(&mut self, partition_i: usize, batch: RecordBatch) {
        while partition_i >= self.partitions_batches.len() {
            self.partitions_batches.push(vec![]);
        }
        let batches = self.partitions_batches.get_mut(partition_i).unwrap();
        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }
        batches.push(batch);
    }

    pub async fn into_channel(self) -> BoxCloneSyncChannel {
        let schema = self.schema.expect("Schema was not set");
        let worker = Worker::default();
        let task_ctx = {
            let mut cfg = datafusion::prelude::SessionConfig::default();
            set_distributed_option_extension(&mut cfg, DistributedConfig::default());
            SessionStateBuilder::new()
                .with_config(cfg)
                .with_default_features()
                .build()
                .task_ctx()
        };
        let plan = MemorySourceConfig::try_new_exec(&self.partitions_batches, schema.clone(), None)
            .expect("Failing to build MemorySourceConfig");
        let swmr_task_data = worker
            .task_data_entries
            .get_with(test_stage_key(self.task_index as _), async {
                Default::default()
            })
            .await;
        swmr_task_data
            .write(Ok(TaskData {
                task_ctx: task_ctx.clone(),
                plan: plan.clone(),
                num_partitions_remaining: Arc::new(AtomicUsize::new(self.partitions_batches.len())),
            }))
            .expect("failed to write to task data");

        let (client, server) = tokio::io::duplex(1024 * 1024);

        let mut client = Some(client);
        let channel = Endpoint::try_from(format!("http://localhost:{}", self.task_index))
            .expect("Invalid dummy URL for building an endpoint. This should never happen")
            .connect_with_connector_lazy(tower::service_fn(move |_| {
                let client = client
                    .take()
                    .expect("Client taken twice. This should never happen");
                async move { Ok::<_, std::io::Error>(TokioIo::new(client)) }
            }));

        tokio::spawn(async move {
            Server::builder()
                .add_service(worker.into_flight_server())
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
                .await
        });
        BoxCloneSyncChannel::new(channel)
    }
}
