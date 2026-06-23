use crate::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
use crate::{DefaultSessionBuilder, DistributedExt, DistributedTaskContext, NetworkShuffleExec};
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err};
use datafusion::datasource::MemTable;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const ROWS_PER_PRODUCER: usize = 32;

// Failure shape this test protects:
//
// coordinator task 0
//   NetworkShuffleExec::execute(p0)
//     creates remote streams, then errors before polling them
//                 |
//                 v
// worker stage 1 task
//   RepartitionExec
//     +-- output partition for failed task 0     (never polled)
//     +-- output partition for sibling task 1   (may keep polling)
//
// If the failed upper task only drops its local streams and no query-wide
// cleanup reaches the worker, the repartition producer can stay alive and
// retain its child plan/state after the coordinator query has already failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn normal_planning_injected_error_before_first_poll_orphans_repartition_task() -> Result<()> {
    let worker_ref = Arc::new(Mutex::new(None));
    let failed_once = Arc::new(AtomicBool::new(false));
    let tracked_repartitions = Arc::new(AtomicUsize::new(0));
    let tracked_drops = Arc::new(AtomicUsize::new(0));

    let mut ctx = start_configured_in_memory_context(3, DefaultSessionBuilder, {
        let worker_ref = Arc::clone(&worker_ref);
        let failed_once = Arc::clone(&failed_once);
        let tracked_repartitions = Arc::clone(&tracked_repartitions);
        let tracked_drops = Arc::clone(&tracked_drops);
        move |mut worker| {
            *worker_ref.lock().unwrap() = Some(worker.clone());
            worker.add_on_plan_hook({
                let failed_once = Arc::clone(&failed_once);
                let tracked_repartitions = Arc::clone(&tracked_repartitions);
                let tracked_drops = Arc::clone(&tracked_drops);
                move |plan, _session_config| {
                    install_normal_planning_repro_hooks(
                        plan,
                        Arc::clone(&failed_once),
                        Arc::clone(&tracked_repartitions),
                        Arc::clone(&tracked_drops),
                    )
                }
            });
            worker
        }
    })
    .await;

    ctx.set_distributed_task_estimator(3);
    register_input_table(&ctx)?;
    let plan = ctx
        .sql("SELECT id, COUNT(*) AS count FROM input GROUP BY id ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;
    assert!(plan.is::<crate::DistributedExec>());

    let err = execute_stream(plan, ctx.task_ctx())?
        .try_collect::<Vec<_>>()
        .await
        .expect_err("injected worker error should fail the query");
    assert!(err.to_string().contains("injected error before poll"));

    let worker = worker_ref
        .lock()
        .unwrap()
        .clone()
        .expect("test worker should have been captured");
    eventually(
        "normal distributed planning producer leak after unpolled upper streams",
        || async {
            let tasks_running = worker.tasks_running().await;
            let tracked_repartitions = tracked_repartitions.load(Ordering::SeqCst);
            let drops = tracked_drops.load(Ordering::SeqCst);
            if tasks_running > 0 && tracked_repartitions > 0 && drops < tracked_repartitions {
                Ok(())
            } else {
                exec_err!(
                    "producer unexpectedly cleaned up: tasks_running={tasks_running}, tracked drops={drops}/{tracked_repartitions}"
                )
            }
        },
    )
    .await
}

fn install_normal_planning_repro_hooks(
    plan: Arc<dyn ExecutionPlan>,
    failed_once: Arc<AtomicBool>,
    tracked_repartitions: Arc<AtomicUsize>,
    tracked_drops: Arc<AtomicUsize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_up(|plan| {
            if plan.is::<NetworkShuffleExec>() {
                return Ok(Transformed::yes(Arc::new(
                    FailBeforePollingNetworkShuffleExec::new(plan, Arc::clone(&failed_once)),
                )));
            }

            if plan.is::<RepartitionExec>() {
                let children = plan.children();
                if children.len() != 1 {
                    return exec_err!("RepartitionExec expected one child");
                }
                tracked_repartitions.fetch_add(1, Ordering::SeqCst);
                let tracked = Arc::new(TrackedDropExec::new(
                    Arc::clone(children[0]),
                    Arc::clone(&tracked_drops),
                ));
                return plan.with_new_children(vec![tracked]).map(Transformed::yes);
            }

            Ok(Transformed::no(plan))
        })?
        .data)
}

fn register_input_table(ctx: &SessionContext) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batches = (0..3)
        .map(|task_index| {
            make_input_partitions(Arc::clone(&schema), task_index).map(|mut partitions| {
                partitions
                    .pop()
                    .expect("one partition")
                    .pop()
                    .expect("one batch")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ctx.register_table(
        "input",
        Arc::new(MemTable::try_new(
            Arc::clone(&schema),
            batches.into_iter().map(|batch| vec![batch]).collect(),
        )?),
    )?;
    Ok(())
}

fn make_input_partitions(schema: Arc<Schema>, task_index: usize) -> Result<Vec<Vec<RecordBatch>>> {
    let first_id = task_index * ROWS_PER_PRODUCER;
    let ids = (0..ROWS_PER_PRODUCER).map(|offset| (first_id + offset) as i64);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(ids)) as ArrayRef],
    )?;
    Ok(vec![vec![batch]])
}

async fn eventually<F, Fut>(label: &str, mut check: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let last_err = match check().await {
            Ok(()) => return Ok(()),
            Err(err) => err.to_string(),
        };
        if Instant::now() >= deadline {
            return exec_err!("{label} timed out: {last_err}");
        }
        sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Clone)]
struct TrackedDropExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    drop_counter: Arc<AtomicUsize>,
}

impl TrackedDropExec {
    fn new(input: Arc<dyn ExecutionPlan>, drop_counter: Arc<AtomicUsize>) -> Self {
        Self {
            properties: Arc::clone(input.properties()),
            input,
            drop_counter,
        }
    }
}

impl Drop for TrackedDropExec {
    fn drop(&mut self) {
        self.drop_counter.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct FailBeforePollingNetworkShuffleExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    failed_once: Arc<AtomicBool>,
}

impl FailBeforePollingNetworkShuffleExec {
    fn new(input: Arc<dyn ExecutionPlan>, failed_once: Arc<AtomicBool>) -> Self {
        Self {
            properties: Arc::clone(input.properties()),
            input,
            failed_once,
        }
    }
}

impl Debug for FailBeforePollingNetworkShuffleExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailBeforePollingNetworkShuffleExec")
            .finish_non_exhaustive()
    }
}

impl DisplayAs for FailBeforePollingNetworkShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailBeforePollingNetworkShuffleExec")
    }
}

impl ExecutionPlan for FailBeforePollingNetworkShuffleExec {
    fn name(&self) -> &str {
        "FailBeforePollingNetworkShuffleExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.input.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.input.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return exec_err!(
                "FailBeforePollingNetworkShuffleExec expected one child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.failed_once),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let d_ctx = DistributedTaskContext::from_ctx(&context);
        if d_ctx.task_index == 0 && !self.failed_once.swap(true, Ordering::SeqCst) {
            let mut streams = Vec::new();
            for partition in 0..self.properties.partitioning.partition_count() {
                streams.push(self.input.execute(partition, Arc::clone(&context))?);
            }
            let _created_stream_count = streams.len();
            return exec_err!("injected error before poll");
        }

        self.input.execute(partition, context)
    }
}

impl Debug for TrackedDropExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackedDropExec").finish_non_exhaustive()
    }
}

impl DisplayAs for TrackedDropExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TrackedDropExec")
    }
}

impl ExecutionPlan for TrackedDropExec {
    fn name(&self) -> &str {
        "TrackedDropExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.input.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.input.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return exec_err!("TrackedDropExec expected one child, got {}", children.len());
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.drop_counter),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
