use crate::MaxLatencyMetric;
use crate::common::OnceLockResult;
use crate::common::now_ns;
use crate::distributed_planner::ProducerHead;
use crate::worker::generated::worker as pb;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::CustomMetricValue;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
/// TaskData stores state for a single task being executed by this Endpoint. It may be shared
/// by concurrent requests for the same task which execute separate partitions.
pub struct TaskData {
    /// Task context suitable for execute different partitions from the same task.
    pub(super) task_ctx: Arc<TaskContext>,
    pub(crate) base_plan: Arc<dyn ExecutionPlan>,
    pub(crate) final_plan: Arc<OnceLockResult<Arc<dyn ExecutionPlan>>>,
    /// Sender half of the metrics channel. `impl_execute_task` takes this (via `Option::take`)
    /// once all partitions have finished or been dropped, sending the collected metrics back to
    /// the coordinator through the `CoordinatorChannel` side channel.
    pub(super) metrics_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<pb::TaskMetrics>>>>,
    /// Metrics related to the execution of a task within a stage. This metrics, instead of being
    /// associated to a specific node, they are global to the task, like the time at which the plan
    /// was fed by the coordinator to the worker.
    pub(super) task_data_metrics: Arc<TaskDataMetrics>,
}

pub(crate) const PLAN_ADDED_AT_METRIC: &str = "plan_added_at";
pub(crate) const PLAN_EXECUTED_AT_METRIC: &str = "plan_executed_at";
pub(crate) const PLAN_FINISHED_AT_METRIC: &str = "plan_finished_at";

#[derive(Debug)]
pub(super) struct TaskDataMetrics {
    pub(super) query_start_time_ns: u64,
    /// When the plan was set by the coordinator.
    pub(super) plan_added_at: MaxLatencyMetric,
    /// When the plan execution was triggered by the parent worker.
    pub(super) plan_executed_at: MaxLatencyMetric,
    /// When the execution stream finished.
    pub(super) plan_finished_at: MaxLatencyMetric,
}

impl TaskDataMetrics {
    pub(super) fn new(query_start_time_ns: u64) -> Self {
        let plan_added_at = MaxLatencyMetric::default();
        plan_added_at.add_duration(Duration::from_nanos(
            now_ns().saturating_sub(query_start_time_ns),
        ));
        Self {
            query_start_time_ns,
            plan_added_at,
            plan_finished_at: MaxLatencyMetric::default(),
            plan_executed_at: MaxLatencyMetric::default(),
        }
    }

    pub(super) fn mark_execution_started_once(&self) {
        if self.plan_executed_at.value() == 0 {
            self.plan_executed_at.add_duration(Duration::from_nanos(
                now_ns().saturating_sub(self.query_start_time_ns),
            ))
        }
    }

    pub(super) fn mark_execution_finished(&self) {
        self.plan_finished_at.add_duration(Duration::from_nanos(
            now_ns().saturating_sub(self.query_start_time_ns),
        ))
    }

    pub(super) fn to_proto_metrics_set(&self) -> pb::MetricsSet {
        let mut task_metrics_set = pb::MetricsSet { metrics: vec![] };

        fn new_metric(name: &str, value: usize) -> pb::Metric {
            pb::Metric {
                partition: None,
                labels: vec![],
                value: Some(pb::metric::Value::CustomMaxLatency(pb::MaxLatency {
                    name: name.to_string(),
                    value: value as u64,
                })),
            }
        }
        task_metrics_set.metrics.push(new_metric(
            PLAN_ADDED_AT_METRIC,
            self.plan_added_at.as_usize(),
        ));
        task_metrics_set.metrics.push(new_metric(
            PLAN_EXECUTED_AT_METRIC,
            self.plan_executed_at.as_usize(),
        ));
        task_metrics_set.metrics.push(new_metric(
            PLAN_FINISHED_AT_METRIC,
            self.plan_finished_at.as_usize(),
        ));

        task_metrics_set
    }
}

impl TaskData {
    /// Returns the total number of partitions in this task.
    pub(crate) fn total_partitions(&self) -> usize {
        match self.final_plan.get() {
            Some(Ok(plan)) => plan.output_partitioning().partition_count(),
            _ => self
                .base_plan
                .properties()
                .output_partitioning()
                .partition_count(),
        }
    }

    pub(crate) fn plan(
        &self,
        producer_head: pb::execute_task_request::ProducerHead,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = self.final_plan.get_or_init(|| {
            let producer_head =
                ProducerHead::from_proto(producer_head, &self.base_plan.schema(), &self.task_ctx)?;

            Ok(producer_head.insert(Arc::clone(&self.base_plan))?)
        });
        match result {
            Ok(plan) => Ok(Arc::clone(plan)),
            Err(err) => Err(DataFusionError::Shared(Arc::clone(err))),
        }
    }
}
