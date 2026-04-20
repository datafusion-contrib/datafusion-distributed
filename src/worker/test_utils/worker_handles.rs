use crate::config_extension_ext::set_distributed_option_extension;
use crate::worker::generated::worker::TaskKey;
use crate::{BoxCloneSyncChannel, DistributedConfig, DistributedExt, TaskData, Worker};
use arrow_ipc::CompressionType;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::net::TcpListener;
use tonic::transport::{Endpoint, Server};
use url::Url;
use uuid::Uuid;

pub fn test_task_key_with_query(query_id: Uuid, task_number: u64) -> TaskKey {
    TaskKey {
        query_id: query_id.as_bytes().to_vec(),
        stage_id: 0,
        task_number,
    }
}

#[derive(Clone)]
pub struct MemoryWorkerHandle {
    task_index: usize,
    worker: Worker,
    schema: SchemaRef,
    partitions_batches: Vec<Vec<RecordBatch>>,
    compression: Option<CompressionType>,
    channel: BoxCloneSyncChannel,
}

impl MemoryWorkerHandle {
    pub async fn spawn(
        task_index: usize,
        partitions_batches: Vec<Vec<RecordBatch>>,
        compression: Option<CompressionType>,
    ) -> Self {
        let schema = partitions_batches
            .iter()
            .flat_map(|batches| batches.iter())
            .next()
            .expect("memory worker requires at least one batch")
            .schema();

        let worker = Worker::default();
        let (client, server) = tokio::io::duplex(1024 * 1024);

        let mut client = Some(client);
        let channel = Endpoint::try_from(format!("http://localhost:{task_index}"))
            .expect("Invalid dummy URL for building an endpoint. This should never happen")
            .connect_with_connector_lazy(tower::service_fn(move |_| {
                let client = client
                    .take()
                    .expect("Client taken twice. This should never happen");
                async move { Ok::<_, std::io::Error>(TokioIo::new(client)) }
            }));

        let server_worker = worker.clone();
        tokio::spawn(async move {
            Server::builder()
                .add_service(server_worker.into_worker_server())
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
                .await
        });

        Self {
            task_index,
            worker,
            schema,
            partitions_batches,
            compression,
            channel: BoxCloneSyncChannel::new(channel),
        }
    }

    pub fn channel(&self) -> BoxCloneSyncChannel {
        self.channel.clone()
    }

    pub async fn register_plan(&self, query_id: Uuid) {
        self.register_plan_with(query_id, Ok)
            .await
            .expect("failed to register memory worker plan");
    }

    pub async fn register_plan_with<F>(&self, query_id: Uuid, build_plan: F) -> Result<()>
    where
        F: FnOnce(Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>,
    {
        let task_ctx = benchmark_task_ctx(self.compression);
        let input = MemorySourceConfig::try_new_exec(
            &self.partitions_batches,
            Arc::clone(&self.schema),
            None,
        )?;
        let plan = build_plan(input)?;
        let partition_count = plan.properties().partitioning.partition_count();
        register_plan_on_worker(
            &self.worker,
            task_ctx,
            plan,
            test_task_key_with_query(query_id, self.task_index as _),
            partition_count,
        )
        .await;
        Ok(())
    }
}

pub struct TcpWorkerHandle {
    task_index: usize,
    worker: Worker,
    schema: SchemaRef,
    partitions_batches: Vec<Vec<RecordBatch>>,
    compression: Option<CompressionType>,
    pub url: Url,
    task: tokio::task::JoinHandle<()>,
}

impl TcpWorkerHandle {
    pub async fn spawn(
        task_index: usize,
        schema: SchemaRef,
        partitions_batches: Vec<Vec<RecordBatch>>,
        compression: Option<CompressionType>,
    ) -> Result<Self> {
        let worker = Worker::default();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|err| datafusion::common::DataFusionError::External(Box::new(err)))?;
        let port = listener
            .local_addr()
            .map_err(|err| datafusion::common::DataFusionError::External(Box::new(err)))?
            .port();
        let server_worker = worker.clone();
        let task = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let _ = Server::builder()
                .add_service(server_worker.into_worker_server())
                .serve_with_incoming(incoming)
                .await;
        });

        Ok(Self {
            task_index,
            worker,
            schema,
            partitions_batches,
            compression,
            url: Url::parse(&format!("http://127.0.0.1:{port}")).expect("valid tcp worker url"),
            task,
        })
    }

    pub async fn register_plan(&self, query_id: Uuid) -> Result<()> {
        let task_ctx = benchmark_task_ctx(self.compression);
        let plan = MemorySourceConfig::try_new_exec(
            &self.partitions_batches,
            Arc::clone(&self.schema),
            None,
        )?;

        register_plan_on_worker(
            &self.worker,
            task_ctx,
            plan,
            test_task_key_with_query(query_id, self.task_index as _),
            self.partitions_batches.len(),
        )
        .await;
        Ok(())
    }
}

impl Drop for TcpWorkerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn benchmark_task_ctx(
    compression: Option<CompressionType>,
) -> Arc<datafusion::execution::TaskContext> {
    let mut cfg = datafusion::prelude::SessionConfig::default();
    set_distributed_option_extension(&mut cfg, DistributedConfig::default());
    SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features()
        .with_distributed_compression(compression)
        .unwrap()
        .build()
        .task_ctx()
}

pub async fn register_plan_on_worker(
    worker: &Worker,
    task_ctx: Arc<datafusion::execution::TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    task_key: TaskKey,
    partition_count: usize,
) {
    let swmr_task_data = worker
        .task_data_entries
        .get_with(task_key, async { Default::default() })
        .await;
    swmr_task_data
        .write(Ok(TaskData {
            task_ctx,
            plan,
            num_partitions_remaining: Arc::new(AtomicUsize::new(partition_count)),
        }))
        .expect("failed to write to task data");
}
