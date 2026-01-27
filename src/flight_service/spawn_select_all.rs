use arrow_flight::FlightData;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use futures::{Stream, StreamExt};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

/// Consumes all the provided streams in parallel sending their produced messages to a single
/// queue in random order. The resulting queue is returned as a stream.
///
/// Uses a bounded channel with send timeout to detect when the client has stopped consuming
/// (e.g., due to disconnect), allowing for prompt cleanup of resources.
pub(crate) fn spawn_select_all<T, El, Err>(
    inner: Vec<T>,
    pool: Arc<dyn MemoryPool>,
    queue_size: usize,
) -> impl Stream<Item = Result<El, Err>>
where
    T: Stream<Item = Result<El, Err>> + Send + Unpin + 'static,
    El: MemoryFootPrint + Send + 'static,
    Err: Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(queue_size);

    let mut tasks = vec![];
    for mut t in inner {
        let tx = tx.clone();
        let pool = Arc::clone(&pool);
        let consumer = MemoryConsumer::new("NetworkBoundary");

        tasks.push(SpawnedTask::spawn(async move {
            loop {
                // Capture the closed() event as soon as possible. We don't want to do
                // extra work if we know nobody is going to listen to it.
                let msg = tokio::select! {
                    biased;
                    _ = tx.closed() => return,
                    msg = t.next() => msg
                };
                let Some(msg) = msg else { return };

                let mut reservation = consumer.clone_with_new_id().register(&pool);
                if let Ok(msg) = &msg {
                    reservation.grow(msg.get_memory_size());
                }

                if tx.send((msg, reservation)).await.is_err() {
                    return;
                };
            }
        }))
    }

    ReceiverStream::new(rx).map(move |(msg, _reservation)| {
        // keep the tasks alive as long as the stream lives
        let _ = &tasks;
        msg
    })
}

pub(crate) trait MemoryFootPrint {
    fn get_memory_size(&self) -> usize;
}

impl MemoryFootPrint for RecordBatch {
    fn get_memory_size(&self) -> usize {
        self.get_array_memory_size()
    }
}

impl MemoryFootPrint for FlightData {
    fn get_memory_size(&self) -> usize {
        self.data_header.len() + self.data_body.len() + self.app_metadata.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryFootPrint, spawn_select_all};
    use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
    use std::error::Error;
    use std::sync::Arc;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn memory_reservation() -> Result<(), Box<dyn Error>> {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());

        let mut stream = spawn_select_all(
            vec![
                futures::stream::iter(vec![Ok::<_, String>(1), Ok(2), Ok(3)]),
                futures::stream::iter(vec![Ok(4), Ok(5)]),
            ],
            Arc::clone(&pool),
            5,
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        let reserved = pool.reserved();
        assert_eq!(reserved, 15);

        for i in [1, 2, 3] {
            let n = stream.next().await.unwrap()?;
            assert_eq!(i, n)
        }

        let reserved = pool.reserved();
        assert_eq!(reserved, 9);

        drop(stream);

        let reserved = pool.reserved();
        assert_eq!(reserved, 0);

        Ok(())
    }

    impl MemoryFootPrint for usize {
        fn get_memory_size(&self) -> usize {
            *self
        }
    }
}
