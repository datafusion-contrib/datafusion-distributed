use arrow::array::RecordBatch;
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
pub(super) fn spawn_select_all<T>(
    inner: Vec<T>,
    pool: Arc<dyn MemoryPool>,
) -> impl Stream<Item = Result<RecordBatch, DataFusionError>>
where
    T: Stream<Item = Result<RecordBatch, DataFusionError>> + Send + Unpin + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let mut tasks = vec![];
    for mut t in inner {
        let tx = tx.clone();
        let pool = Arc::clone(&pool);
        let consumer = MemoryConsumer::new("NetworkShuffleExec");

        tasks.push(SpawnedTask::spawn(async move {
            while let Some(msg) = t.next().await {
                let mut reservation = consumer.clone_with_new_id().register(&pool);
                if let Ok(msg) = &msg {
                    reservation.grow(msg.get_array_memory_size());
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
