use crate::common::require_one_child;
use crate::coordinator::prepare_dynamic_plan::prepare_dynamic_plan;
use crate::coordinator::prepare_static_plan::prepare_static_plan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::coordinator::store::{Store, task_keys_for_plan};
use crate::dynamic_filtering::sever_dynamic_filter_relationships_in_plan_for_display;
use crate::{DistributedConfig, TaskCompletedDynamicFilters, TaskKey, TaskMetrics};
use datafusion::common::internal_datafusion_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{HashMap, Result, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::fmt::Formatter;
use std::sync::{Arc, OnceLock};

/// [ExecutionPlan] that executes the inner plan in distributed mode.
/// Before executing it, two modifications are lazily performed on the plan:
/// 1. Assigns worker URLs to all the stages. Unless explicitly set in
///    [crate::RouteTasksHandler], a random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    /// Initial [ExecutionPlan] present before execution.
    /// - If the plan was distributed statically, this will be the final distributed plan with all
    ///   the appropriate network boundaries in it.
    /// - If the plan is going to be distributed dynamically during execution, this is the initial
    ///   non-distributed plan.
    base_plan: Arc<dyn ExecutionPlan>,
    prepared_execution: Arc<OnceLock<PreparedExecution>>,
    /// DataFusion metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Storage where metrics collected from workers at runtime will place their results as they
    /// finish their respective remote tasks.
    pub(crate) metrics_store: Option<Arc<Store<TaskMetrics>>>,
    /// Storage for the completed dynamic filters reported by each worker task.
    pub(crate) completed_dynamic_filter_store: Option<Arc<Store<TaskCompletedDynamicFilters>>>,
}

/// Execution state produced by distributed planning (static or dynamic) retained
/// for post-execution work such as plan rewrites to display metrics and dynamic filters.
#[derive(Debug, Clone)]
struct PreparedExecution {
    /// Resulting plan reconstructed after static or dynamic planning.
    plan_for_viz: Arc<dyn ExecutionPlan>,
    /// The head stage actually executed locally by the coordinator.
    head_stage: Arc<dyn ExecutionPlan>,
    /// The task context of the [`DistributedExec`]. Useful for decoding protobufs.
    task_ctx: Arc<TaskContext>,
}

pub(super) struct PreparedPlan {
    /// The head stage meant to be executed locally by the coordinator.
    pub(super) head_stage: Arc<dyn ExecutionPlan>,
    /// A final representation of the plan for visualization purposes.
    pub(super) plan_for_viz: Arc<dyn ExecutionPlan>,
}

impl DistributedExec {
    pub fn new(base_plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            base_plan,
            prepared_execution: Arc::new(OnceLock::new()),
            metrics: ExecutionPlanMetricsSet::new(),
            metrics_store: None,
            completed_dynamic_filter_store: None,
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

    /// Waits until all worker tasks have reported their metrics back via the coordinator channel.
    ///
    /// Metrics are delivered asynchronously after query execution completes, so callers that need
    /// complete metrics (e.g. for observability or display) should await this before inspecting
    /// [`Self::task_metrics`] or calling [`rewrite_distributed_plan_with_metrics`].
    ///
    /// [`rewrite_distributed_plan_with_metrics`]: crate::rewrite_distributed_plan_with_metrics
    pub async fn wait_for_metrics(&self) {
        let Some(task_metrics) = &self.metrics_store else {
            return;
        };
        let Ok(plan) = self.plan_for_viz() else {
            return;
        };
        task_metrics.wait_for(&task_keys_for_plan(&plan)).await;
    }

    pub(crate) async fn wait_for_dynamic_filters(
        &self,
    ) -> Result<Option<HashMap<TaskKey, TaskCompletedDynamicFilters>>> {
        let Some(store) = &self.completed_dynamic_filter_store else {
            return Ok(None);
        };
        let plan = self.plan_for_viz()?;
        Ok(Some(store.wait_for(&task_keys_for_plan(&plan)).await))
    }

    fn prepared_execution(&self) -> Result<PreparedExecution> {
        self.prepared_execution.get().cloned().ok_or_else(|| {
            internal_datafusion_err!("No prepared execution found. Was execute() called?")
        })
    }

    /// Returns the plan reconstructed from the execution for visualization and rewriting.
    pub(crate) fn plan_for_viz(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.prepared_execution()?.plan_for_viz)
    }

    /// Returns the prepared visualization plan when available, or the original optimized plan
    /// before execution has prepared one.
    pub(crate) fn plan_for_viz_or_base_plan(&self) -> Arc<dyn ExecutionPlan> {
        self.prepared_execution
            .get()
            .map(|prepared| Arc::clone(&prepared.plan_for_viz))
            .unwrap_or_else(|| Arc::clone(&self.base_plan))
    }

    /// Returns the head stage that was actually executed. Unlike [`Self::plan_for_viz`] (which is
    /// reconstructed for visualization, with `Stage::Local` boundaries and rebuilt ancestor
    /// `Arc`s), this returns the original `Arc` instances whose metrics were populated during
    /// execution.
    pub(crate) fn head_stage(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.prepared_execution()?.head_stage)
    }

    pub(crate) fn task_ctx(&self) -> Result<Arc<TaskContext>> {
        Ok(self.prepared_execution()?.task_ctx)
    }

    /// Builds a non-executable visualization result while preserving the state needed by a
    /// subsequent rewrite. Dynamic filters must be rewritten before metrics.
    pub(crate) fn with_plan_for_viz(
        &self,
        plan_for_viz: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut prepared_execution = self.prepared_execution()?;
        prepared_execution.plan_for_viz = Arc::clone(&plan_for_viz);
        Ok(Arc::new(Self {
            base_plan: Arc::clone(&self.base_plan),
            prepared_execution: Arc::new(OnceLock::from(prepared_execution)),
            metrics: self.metrics.clone(),
            metrics_store: self.metrics_store.clone(),
            completed_dynamic_filter_store: self.completed_dynamic_filter_store.clone(),
        }))
    }
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
        vec![
            self.prepared_execution
                .get()
                .map(|prepared| &prepared.plan_for_viz)
                .unwrap_or(&self.base_plan),
        ]
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
        if self.prepared_execution.get().is_some() {
            return self.with_plan_for_viz(child);
        }
        Ok(Arc::new(DistributedExec {
            base_plan: child,
            prepared_execution: Arc::new(OnceLock::new()),
            metrics: self.metrics.clone(),
            metrics_store: self.metrics_store.clone(),
            completed_dynamic_filter_store: self.completed_dynamic_filter_store.clone(),
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
        let prepared_execution = Arc::clone(&self.prepared_execution);
        let collect_dynamic_filters = self.completed_dynamic_filter_store.is_some();

        let query_coordinator = QueryCoordinator::new(
            Arc::clone(&context),
            &self.metrics,
            self.metrics_store.clone(),
            self.completed_dynamic_filter_store.clone(),
        );

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
            let guard = query_coordinator.end_query_guard();

            let d_cfg = DistributedConfig::from_config_options(context.session_config().options())?;
            let result = match d_cfg.dynamic_task_count {
                true => prepare_dynamic_plan(&query_coordinator, &base_plan).await?,
                false => prepare_static_plan(&query_coordinator, &base_plan)?,
            };

            let plan_for_viz = match collect_dynamic_filters {
                true => sever_dynamic_filter_relationships_in_plan_for_display(
                    result.plan_for_viz,
                    &context,
                )?,
                false => result.plan_for_viz,
            };
            prepared_execution
                .set(PreparedExecution {
                    plan_for_viz,
                    head_stage: Arc::clone(&result.head_stage),
                    task_ctx: Arc::clone(&context),
                })
                .map_err(|_| {
                    internal_datafusion_err!("DistributedExec was already prepared for execution")
                })?;
            let mut stream = result.head_stage.execute(partition, context)?;
            while let Some(msg) = stream.next().await {
                if tx.send(msg).await.is_err() {
                    break; // channel closed
                }
            }
            drop(guard);
            drop(tx);
            query_coordinator.drain_pending_tasks().await?;
            Ok(())
        });

        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
