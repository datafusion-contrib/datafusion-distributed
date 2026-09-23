use crate::common::require_one_child;
use crate::coordinator::prepare_dynamic_plan::prepare_dynamic_plan;
use crate::coordinator::prepare_static_plan::prepare_static_plan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::coordinator::store::{Store, StoreSnapshot, task_keys_for_plan};
use crate::distributed_planner::DEFAULT_METRICS_FINALIZATION_TIMEOUT_MS;
use crate::dynamic_filtering::{
    is_dynamic_filtering_enabled, sever_dynamic_filter_relationships_in_plan_for_display,
};
use crate::{DistributedConfig, TaskCompletedDynamicFilters, TaskKey, TaskMetrics};
use datafusion::common::internal_datafusion_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{HashMap, Result, exec_datafusion_err, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::fmt::Formatter;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

/// Non-blocking, point-in-time view of the reports expected from a prepared distributed plan.
#[derive(Debug)]
pub struct TaskMetricsSnapshot {
    /// Metrics received so far, including actual sampling metrics from tasks that never ran.
    pub reported: HashMap<TaskKey, TaskMetrics>,
    /// Tasks whose coordinator channels are still open and have not reported metrics.
    pub pending: Vec<TaskKey>,
    /// Tasks whose channels closed without a report. Their work cannot be accounted for.
    pub missing_reports: Vec<TaskKey>,
}

impl TaskMetricsSnapshot {
    fn from_store(snapshot: StoreSnapshot<TaskMetrics>) -> Self {
        Self {
            reported: snapshot.reported,
            pending: snapshot.pending,
            missing_reports: snapshot.terminal_without_report,
        }
    }

    /// Whether every expected task has either reported or had its stream end without a report.
    pub fn all_terminal(&self) -> bool {
        self.pending.is_empty()
    }

    /// Whether every expected task has actually reported metrics. This distinguishes a
    /// finished-but-incomplete query from one whose measurements are complete.
    pub fn all_reported(&self) -> bool {
        self.all_terminal() && self.missing_reports.is_empty()
    }

    /// Tasks that reported real sampling metrics but never received ExecuteTask.
    pub fn unexecuted_tasks(&self) -> Vec<TaskKey> {
        self.reported
            .iter()
            .filter_map(|(key, report)| report.was_not_executed().then_some(*key))
            .collect()
    }
}

/// [ExecutionPlan] that executes the inner plan in distributed mode.
/// Before executing it, two modifications are lazily performed on the plan:
/// 1. Assigns worker URLs to all the stages. Unless explicitly set in
///    [crate::RouteTasksHandler], a random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    /// [ExecutionPlan] exposed through [`ExecutionPlan::children`] and used as the input to
    /// execution.
    ///
    /// Initially, this is the plan present before execution:
    /// - If the plan was distributed statically, this will be the final distributed plan with all
    ///   the appropriate network boundaries in it.
    /// - If the plan is going to be distributed dynamically during execution, this is the initial
    ///   non-distributed plan.
    ///
    /// Post-execution rewrites replace this plan in the returned clone while leaving the original
    /// [`DistributedExec`] unchanged.
    base_plan: Arc<dyn ExecutionPlan>,
    /// Complete plans produced during static or dynamic preparation.
    prepared_plan: Arc<OnceLock<PreparedPlan>>,
    /// DataFusion metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Storage where metrics collected from workers at runtime will place their results as they
    /// finish their respective remote tasks.
    pub(crate) metrics_store: Option<Arc<Store<TaskMetrics>>>,
    /// Storage for the completed dynamic filters reported by each worker task.
    pub(crate) completed_dynamic_filter_store: Option<Arc<Store<TaskCompletedDynamicFilters>>>,
    /// Set when the result stream ends; all post-query waits use the same deadline.
    finalization_deadline: watch::Sender<Option<Instant>>,
    finalization_timeout: Duration,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedPlan {
    /// The coordinator-side plan prepared for execution.
    pub(super) head_stage: Arc<dyn ExecutionPlan>,
    /// The complete distributed plan reconstructed for visualization, including all stages.
    pub(super) plan_for_viz: Arc<dyn ExecutionPlan>,
}

impl DistributedExec {
    pub fn new(base_plan: Arc<dyn ExecutionPlan>) -> Self {
        let (finalization_deadline, _) = watch::channel(None);
        Self {
            base_plan,
            prepared_plan: Arc::new(OnceLock::new()),
            metrics: ExecutionPlanMetricsSet::new(),
            metrics_store: None,
            completed_dynamic_filter_store: None,
            finalization_deadline,
            finalization_timeout: Duration::from_millis(DEFAULT_METRICS_FINALIZATION_TIMEOUT_MS),
        }
    }

    /// Enables task metrics collection from remote workers.
    pub fn with_metrics_collection(mut self, enabled: bool) -> Self {
        self.metrics_store = match enabled {
            true => Some(Arc::new(Store::new())),
            false => None,
        };
        self
    }

    /// Enables collection of completed dynamic filters from remote workers for display.
    pub fn with_dynamic_filter_collection(mut self, enabled: bool) -> Self {
        self.completed_dynamic_filter_store = match enabled {
            true => Some(Arc::new(Store::new())),
            false => None,
        };
        self
    }

    pub(crate) fn with_finalization_timeout(mut self, timeout: Duration) -> Self {
        self.finalization_timeout = timeout;
        self
    }

    async fn finalization_deadline(&self) -> Instant {
        wait_for_finalization_deadline(self.finalization_deadline.subscribe()).await
    }

    /// Waits for complete metrics, if collection is enabled and execution has been prepared.
    /// Returns `None` if a task failed to report or the finalization timeout elapsed; use
    /// [`Self::metrics_snapshot`] to inspect reports and their completeness in that case.
    pub async fn wait_for_metrics(&self) -> Option<HashMap<TaskKey, TaskMetrics>> {
        self.complete_metrics().await.ok()
    }

    /// Returns all available metrics immediately, including gaps in task numbers, along with
    /// pending and terminal-without-report task keys. Returns `None` if metrics are disabled or
    /// the distributed plan has not yet been prepared.
    pub fn metrics_snapshot(&self) -> Option<TaskMetricsSnapshot> {
        let store = self.metrics_store.as_ref()?;
        let plan = &self.prepared_plan.get()?.plan_for_viz;
        Some(TaskMetricsSnapshot::from_store(
            store.snapshot(&task_keys_for_plan(plan)),
        ))
    }

    pub(crate) async fn complete_metrics(&self) -> Result<HashMap<TaskKey, TaskMetrics>> {
        let store = self
            .metrics_store
            .as_ref()
            .ok_or_else(|| exec_datafusion_err!("metrics collection is disabled"))?;
        let plan = &self.prepared_plan()?.plan_for_viz;
        let keys = task_keys_for_plan(plan);
        wait_for_complete_metrics(store, &keys, self.finalization_deadline().await).await
    }

    /// Waits until all worker tasks have reported their completed dynamic filters back via
    /// the coordinator channel if dynamic filter collection is enabled.
    pub(crate) async fn wait_for_dynamic_filters(
        &self,
    ) -> Option<HashMap<TaskKey, TaskCompletedDynamicFilters>> {
        let store = self.completed_dynamic_filter_store.as_ref()?;
        let plan = &self.prepared_plan.get()?.plan_for_viz;
        tokio::time::timeout_at(
            self.finalization_deadline().await,
            store.wait_for(&task_keys_for_plan(plan)),
        )
        .await
        .ok()
    }

    fn prepared_plan(&self) -> Result<PreparedPlan> {
        self.prepared_plan.get().cloned().ok_or_else(|| {
            internal_datafusion_err!("No prepared plan found. Was execute() called?")
        })
    }

    /// Returns the plan reconstructed during preparation for visualization and rewriting.
    pub(crate) fn plan_for_viz(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.prepared_plan()?.plan_for_viz)
    }

    /// Returns the prepared visualization plan when available, or the original optimized plan
    /// before execution has prepared one.
    pub(crate) fn plan_for_viz_or_base_plan(&self) -> Arc<dyn ExecutionPlan> {
        self.prepared_plan
            .get()
            .map(|prepared| Arc::clone(&prepared.plan_for_viz))
            .unwrap_or_else(|| Arc::clone(&self.base_plan))
    }

    /// Returns the coordinator-side plan executed by [`DistributedExec`].
    ///
    /// Unlike [`Self::plan_for_viz`], this contains [`Stage::Remote`] boundaries instead of the
    /// remote execution-plan nodes. It also retains the original plan-node instances whose
    /// metrics were populated during execution.
    ///
    /// [`Stage::Remote`]: crate::stage::Stage::Remote
    pub(crate) fn head_stage(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.prepared_plan()?.head_stage)
    }

    /// Returns a new [`DistributedExec`] with an updated visualization plan while leaving its
    /// public child unchanged.
    pub(crate) fn with_plan_for_viz(
        &self,
        plan_for_viz: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut prepared_plan = self.prepared_plan()?;
        prepared_plan.plan_for_viz = plan_for_viz;
        Ok(Arc::new(Self {
            base_plan: Arc::clone(&self.base_plan),
            prepared_plan: Arc::new(OnceLock::from(prepared_plan)),
            metrics: self.metrics.clone(),
            metrics_store: self.metrics_store.clone(),
            completed_dynamic_filter_store: self.completed_dynamic_filter_store.clone(),
            finalization_deadline: self.finalization_deadline.clone(),
            finalization_timeout: self.finalization_timeout,
        }))
    }
}

async fn wait_for_finalization_deadline(mut rx: watch::Receiver<Option<Instant>>) -> Instant {
    (*rx.wait_for(Option::is_some)
        .await
        .expect("DistributedExec owns the finalization deadline sender"))
    .expect("deadline was set before the receiver woke")
}

async fn wait_for_complete_metrics(
    store: &Store<TaskMetrics>,
    keys: &[TaskKey],
    deadline: Instant,
) -> Result<HashMap<TaskKey, TaskMetrics>> {
    let snapshot = tokio::time::timeout_at(deadline, store.wait_for_terminal(keys))
        .await
        .map_err(|_| {
            exec_datafusion_err!(
                "timed out waiting for worker task metrics; pending: {:?}",
                store.snapshot(keys).pending
            )
        })?;
    if !snapshot.terminal_without_report.is_empty() {
        return exec_err!(
            "worker task metrics missing after coordinator channel closed: {:?}",
            snapshot.terminal_without_report
        );
    }
    Ok(snapshot.reported)
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedExec")
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        "DistributedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.base_plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.base_plan]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = require_one_child(&children)?;
        // Replacing the public child is independent from replacing the visualization plan. A
        // post-execution rewrite updates the latter explicitly via `Self::with_plan_for_viz`.
        let prepared_plan = self
            .prepared_plan
            .get()
            .cloned()
            .map_or_else(OnceLock::new, OnceLock::from);
        Ok(Arc::new(DistributedExec {
            base_plan: child,
            prepared_plan: Arc::new(prepared_plan),
            metrics: self.metrics.clone(),
            metrics_store: self.metrics_store.clone(),
            completed_dynamic_filter_store: self.completed_dynamic_filter_store.clone(),
            finalization_deadline: self.finalization_deadline.clone(),
            finalization_timeout: self.finalization_timeout,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition > 0 {
            // The DistributedExec node calls try_assign_urls() lazily upon calling .execute(). This means
            // that .execute() must only be called once, as we cannot afford to perform several
            // random URL assignation while calling multiple partitions, as they will differ,
            // producing an invalid plan
            return exec_err!(
                "DistributedExec must only have 1 partition, but it was called with partition index {partition}"
            );
        }

        let base_plan = Arc::clone(&self.base_plan);
        let prepared_plan = Arc::clone(&self.prepared_plan);
        let collect_dynamic_filters = self.completed_dynamic_filter_store.is_some();
        let finalization_deadline = self.finalization_deadline.clone();
        let finalization_timeout = self.finalization_timeout;

        let query_coordinator = Arc::new(QueryCoordinator::new(
            Arc::clone(&context),
            &self.metrics,
            self.metrics_store.clone(),
            self.completed_dynamic_filter_store.clone(),
        ));

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), 1);
        let tx = builder.tx();

        builder.spawn(async move {
            // Dropping this `guard` is what signals the coordinator->worker channel to be dropped,
            // which triggers a chain reaction that ends up also gracefully closing the
            // worker->coordinator channel. The flow looks like this:
            // 1. The query ends normally, as all Arrow RecordBatches are already streamed.
            // 2. The `guard` here is dropped.
            // 3. In StageCoordinator::send_plan_task(), `end_stream_notifier` fires and the
            //    coordinator->worker channel is gracefully ended.
            // 4. The coordinator->worker channel EOS is received in `impl_coordinator_channel.rs`.
            // 5. The metrics are send back in the worker->coordinator channel, and then that
            //    channel is closed.
            let guard = query_coordinator
                .end_query_guard(finalization_deadline.clone(), finalization_timeout);

            let d_cfg = DistributedConfig::from_config_options(context.session_config().options())?;
            let mut prepared = match d_cfg.dynamic_task_count {
                true => prepare_dynamic_plan(&query_coordinator, &base_plan).await?,
                false => prepare_static_plan(&query_coordinator, &base_plan).await?,
            };

            let dynamic_filtering_enabled = is_dynamic_filtering_enabled(context.session_config());
            prepared.plan_for_viz = match dynamic_filtering_enabled && collect_dynamic_filters {
                true => sever_dynamic_filter_relationships_in_plan_for_display(
                    prepared.plan_for_viz,
                    &context,
                )?,
                false => prepared.plan_for_viz,
            };
            let head_stage = Arc::clone(&prepared.head_stage);
            prepared_plan.set(prepared).map_err(|_| {
                internal_datafusion_err!("DistributedExec was already prepared for execution")
            })?;
            let mut stream = head_stage.execute(partition, context)?;
            while let Some(msg) = stream.next().await {
                if tx.send(msg).await.is_err() {
                    break; // channel closed
                }
            }
            drop(guard);
            drop(tx);
            // DataFusion does not close the result stream until this spawned task returns.
            // A half-open coordinator channel can otherwise hang result collection itself,
            // even if the later metrics rewrite has its own timeout. Cancel stalled
            // background tasks on timeout; their metric receivers then become terminal
            // without a report, so accounting remains explicitly incomplete.
            query_coordinator
                .drain_pending_tasks(
                    wait_for_finalization_deadline(finalization_deadline.subscribe()).await,
                )
                .await?;
            Ok(())
        });

        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::metrics::MetricsSet;
    use uuid::Uuid;

    #[tokio::test]
    async fn a_never_closing_channel_cannot_block_complete_metrics_indefinitely() {
        let store = Store::<TaskMetrics>::new();
        let key = TaskKey {
            query_id: Uuid::new_v4(),
            stage_id: 1,
            task_number: 0,
        };
        let deadline = Instant::now() + Duration::from_millis(25);
        let result = wait_for_complete_metrics(&store, &[key], deadline).await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("timed out waiting")
        );
        assert_eq!(store.snapshot(&[key]).pending, vec![key]);
        // Rewriting after the drain used up the budget must not start another wait.
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            wait_for_complete_metrics(&store, &[key], deadline),
        )
        .await
        .expect("expired deadline should return without another timeout window");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn closed_channel_without_metrics_is_an_error_not_zero_metrics() {
        let store = Store::<TaskMetrics>::new();
        let query_id = Uuid::new_v4();
        let reported = TaskKey {
            query_id,
            stage_id: 1,
            task_number: 0,
        };
        let lost = TaskKey {
            query_id,
            stage_id: 1,
            task_number: 2,
        };
        store.insert(
            reported,
            TaskMetrics {
                pre_order_plan_metrics: vec![],
                task_metrics: MetricsSet::new(),
            },
        );
        store.mark_terminal(lost);
        let result = wait_for_complete_metrics(
            &store,
            &[reported, lost],
            Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert!(result.unwrap_err().to_string().contains("metrics missing"));
        let snapshot = TaskMetricsSnapshot::from_store(store.snapshot(&[reported, lost]));
        assert!(snapshot.all_terminal());
        assert!(!snapshot.all_reported());
        assert_eq!(snapshot.missing_reports, vec![lost]);
        assert_eq!(snapshot.reported.len(), 1);
    }
}
