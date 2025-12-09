use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, plan_err};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use std::borrow::Borrow;
use std::sync::Arc;
use tokio_stream::wrappers::UnboundedReceiverStream;

pub(super) fn require_one_child<L, T>(
    children: L,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>
where
    L: AsRef<[T]>,
    T: Borrow<Arc<dyn ExecutionPlan>>,
{
    let children = children.as_ref();
    if children.len() != 1 {
        return plan_err!("Expected exactly 1 children, got {}", children.len());
    }
    Ok(children[0].borrow().clone())
}

pub(super) fn scale_partitioning_props(
    props: &PlanProperties,
    f: impl FnOnce(usize) -> usize,
) -> PlanProperties {
    PlanProperties::new(
        props.eq_properties.clone(),
        scale_partitioning(&props.partitioning, f),
        props.emission_type,
        props.boundedness,
    )
}

pub(super) fn scale_partitioning(
    partitioning: &Partitioning,
    f: impl FnOnce(usize) -> usize,
) -> Partitioning {
    match &partitioning {
        Partitioning::RoundRobinBatch(p) => Partitioning::RoundRobinBatch(f(*p)),
        Partitioning::Hash(hash, p) => Partitioning::Hash(hash.clone(), f(*p)),
        Partitioning::UnknownPartitioning(p) => Partitioning::UnknownPartitioning(f(*p)),
    }
}

/// Consumes all the provided streams in parallel sending their produced messages to a single
/// queue in random order. The resulting queue is returned as a stream.
// FIXME: It should not be necessary to do this, it should be fine to just consume
//  all the messages with a normal tokio::stream::select_all, however, that has the chance
//  of deadlocking the stream on the server side (https://github.com/datafusion-contrib/datafusion-distributed/issues/228).
//  Even having these channels bounded would result in deadlocks (learned it the hard way).
//  Until we figure out what's wrong there, this is a good enough solution.
pub(super) fn spawn_select_all<T, El, Err>(
    inner: Vec<T>,
    pool: Arc<dyn MemoryPool>,
) -> impl Stream<Item = Result<El, Err>>
where
    T: Stream<Item = Result<El, Err>> + Send + Unpin + 'static,
    El: MemoryFootPrint + Send + 'static,
    Err: Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let mut tasks = vec![];
    for mut t in inner {
        let tx = tx.clone();
        let pool = Arc::clone(&pool);
        let consumer = MemoryConsumer::new("NetworkBoundary");

        tasks.push(SpawnedTask::spawn(async move {
            while let Some(msg) = t.next().await {
                let mut reservation = consumer.clone_with_new_id().register(&pool);
                if let Ok(msg) = &msg {
                    reservation.grow(msg.get_memory_size());
                }

                if tx.send((msg, reservation)).is_err() {
                    return;
                };
            }
        }))
    }

    UnboundedReceiverStream::new(rx).map(move |(msg, _reservation)| {
        // keep the tasks alive as long as the stream lives
        let _ = &tasks;
        msg
    })
}

pub(super) trait MemoryFootPrint {
    fn get_memory_size(&self) -> usize;
}

impl MemoryFootPrint for RecordBatch {
    fn get_memory_size(&self) -> usize {
        self.get_array_memory_size()
    }
}

#[cfg(test)]
mod tests {
    use crate::execution_plans::common::{MemoryFootPrint, spawn_select_all};
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
