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
    let reservation = Arc::new(MemoryConsumer::new("NetworkBoundary").register(&pool));

    let mut tasks = Vec::with_capacity(inner.len());
    let mut in_rxs = Vec::with_capacity(inner.len());
    for mut t in inner {
        let (in_tx, in_rx) = tokio::sync::mpsc::channel(queue_size);
        in_rxs.push(ReceiverStream::new(in_rx));
        let reservation = Arc::clone(&reservation);

        tasks.push(SpawnedTask::spawn(async move {
            loop {
                // Capture the closed() event as soon as possible. We don't want to do
                // extra work if we know nobody is going to listen to it.
                let msg = tokio::select! {
                    biased;
                    _ = in_tx.closed() => return,
                    msg = t.next() => msg
                };
                let Some(msg) = msg else { return };

                if let Ok(msg) = &msg {
                    reservation.grow(msg.get_memory_size());
                }

                if in_tx.send(msg).await.is_err() {
                    return;
                };
            }
        }))
    }

    futures::stream::select_all(in_rxs).map(move |msg| {
        if let Ok(msg) = &msg {
            reservation.shrink(msg.get_memory_size());
        }
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

        let mut consumed = 0;
        for _ in 0..3 {
            consumed += stream.next().await.unwrap()?;
        }

        let reserved = pool.reserved();
        assert_eq!(reserved, 15 - consumed);

        drop(stream);

        let reserved = pool.reserved();
        assert_eq!(reserved, 0);

        Ok(())
    }

    #[tokio::test]
    async fn memory_reservation_backpressure() -> Result<(), Box<dyn Error>> {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());

        let mut stream = spawn_select_all(
            vec![futures::stream::iter(vec![
                Ok::<_, String>(1),
                Ok(2),
                Ok(3),
            ])],
            Arc::clone(&pool),
            1,
        );
        // First two messages are buffered (1+2)
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        let reserved = pool.reserved();
        assert_eq!(reserved, 3);

        // First message is pulled
        let n = stream.next().await.unwrap()?;
        assert_eq!(n, 1);

        // The third message is buffered, but the first one came out (2+3)
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        let reserved = pool.reserved();
        assert_eq!(reserved, 5);

        // Second message is pulled
        let n = stream.next().await.unwrap()?;
        assert_eq!(n, 2);

        // Only the third message is buffered
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        let reserved = pool.reserved();
        assert_eq!(reserved, 3);

        // The third message is pulled
        let n = stream.next().await.unwrap()?;
        assert_eq!(n, 3);

        // Nothing remains in the pool
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
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
