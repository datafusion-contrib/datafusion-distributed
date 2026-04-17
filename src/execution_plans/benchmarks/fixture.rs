use crate::worker::generated::worker::worker_service_client::WorkerServiceClient;
use crate::{BoxCloneSyncChannel, ChannelResolver, create_worker_client};
use arrow::datatypes::DataType::{
    Boolean, Dictionary, Float64, Int32, Int64, List, Timestamp, UInt8, Utf8,
};
use arrow::datatypes::{Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::util::data_gen::create_random_batch;
use datafusion::common::{Result, exec_err};
use std::sync::Arc;
use url::Url;

pub(super) fn benchmark_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", Int64, false),
        Field::new("metric", Float64, false),
        Field::new("flag", Boolean, true),
        Field::new("label", Utf8, true),
        Field::new(
            "category",
            Dictionary(Box::new(Int32), Box::new(Utf8)),
            true,
        ),
        Field::new("raw", UInt8, false),
        Field::new("ts", Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("count", Int32, false),
        Field::new(
            "tags",
            List(Arc::new(Field::new_list_field(Utf8, true))),
            true,
        ),
    ]))
}

pub(super) fn make_input_partitions(
    schema: Arc<Schema>,
    total_rows: usize,
    batch_size: usize,
    partition_count: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    if batch_size == 0 {
        return exec_err!("benchmark batch_size must be greater than zero");
    }

    let base_batch = create_random_batch(schema, batch_size, 0.1, 0.5)?;
    let mut batches = vec![];
    let mut remaining = total_rows;
    while remaining > 0 {
        batches.push(base_batch.clone());
        remaining = remaining.saturating_sub(batch_size);
    }

    let mut partitions = vec![vec![]; partition_count.max(1)];
    let partition_len = partitions.len();
    for (batch_i, batch) in batches.into_iter().enumerate() {
        partitions[batch_i % partition_len].push(batch);
    }
    Ok(partitions)
}

pub(super) fn rows_for_producer(
    total_rows: usize,
    producer_tasks: usize,
    producer_task_idx: usize,
) -> usize {
    let base = total_rows / producer_tasks.max(1);
    let remainder = total_rows % producer_tasks.max(1);
    base + usize::from(producer_task_idx < remainder)
}

/// [ChannelResolver] implementation that returns gRPC clients backed by an in-memory
/// tokio duplex rather than a TCP connection.
#[derive(Clone)]
pub(super) struct InMemoryChannelsResolver {
    pub channels: Vec<BoxCloneSyncChannel>,
}

#[async_trait::async_trait]
impl ChannelResolver for InMemoryChannelsResolver {
    async fn get_worker_client_for_url(
        &self,
        url: &Url,
    ) -> Result<WorkerServiceClient<BoxCloneSyncChannel>> {
        let Some(port) = url.port() else {
            return exec_err!("Missing port in url {url}");
        };
        Ok(create_worker_client(self.channels[port as usize].clone()))
    }
}
