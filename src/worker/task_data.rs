use crate::common::OnceLockResult;
use crate::common::now_ns;
use crate::distributed_planner::ProducerHead;
use crate::protocol::ProducerHeadSpec;
use crate::{MaxLatencyMetric, TaskMetrics};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::{Metric, MetricValue, MetricsSet};
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
/// TaskData stores state for a single task being executed by this Endpoint. It may be shared
/// by concurrent requests for the same task which execute separate partitions.
pub struct TaskData {
    /// Task context suitable for execute different partitions from the same task.
    pub(crate) task_ctx: Arc<TaskContext>,
    pub(crate) base_plan: Arc<dyn ExecutionPlan>,
    pub(crate) final_plan: Arc<OnceLockResult<Arc<dyn ExecutionPlan>>>,
    /// Sender half of the metrics channel. `impl_coordinator_channel` takes this (via
    /// `Option::take`) when the coordinator channel reaches EOS, sending the collected metrics
    /// back to the coordinator through the `CoordinatorChannel` side channel.
    pub(super) metrics_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<TaskMetrics>>>>,
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
    pub(super) query_start_time_ns: usize,
    /// When the plan was set by the coordinator.
    pub(super) plan_added_at: MaxLatencyMetric,
    /// When the plan execution was triggered by the parent worker.
    pub(super) plan_executed_at: MaxLatencyMetric,
    /// When the execution stream finished.
    pub(super) plan_finished_at: MaxLatencyMetric,
}

impl TaskDataMetrics {
    pub(super) fn new(query_start_time_ns: usize) -> Self {
        let plan_added_at = MaxLatencyMetric::default();
        plan_added_at.add_duration(Duration::from_nanos(
            now_ns::<u64>().saturating_sub(query_start_time_ns as u64),
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
                now_ns::<u64>().saturating_sub(self.query_start_time_ns as u64),
            ))
        }
    }

    pub(super) fn mark_execution_finished(&self) {
        self.plan_finished_at.add_duration(Duration::from_nanos(
            now_ns::<u64>().saturating_sub(self.query_start_time_ns as u64),
        ))
    }

    pub(super) fn to_metrics_set(&self) -> MetricsSet {
        let mut metrics_set = MetricsSet::new();
        metrics_set.push(max_latency_metric(
            PLAN_ADDED_AT_METRIC,
            &self.plan_added_at,
        ));
        metrics_set.push(max_latency_metric(
            PLAN_EXECUTED_AT_METRIC,
            &self.plan_executed_at,
        ));
        metrics_set.push(max_latency_metric(
            PLAN_FINISHED_AT_METRIC,
            &self.plan_finished_at,
        ));

        metrics_set
    }
}

fn max_latency_metric(name: &'static str, value: &MaxLatencyMetric) -> Arc<Metric> {
    Arc::new(Metric::new(
        MetricValue::Custom {
            name: Cow::Borrowed(name),
            value: Arc::new(MaxLatencyMetric::from_nanos(value.value())),
        },
        None,
    ))
}

impl TaskData {
    pub(crate) fn plan(
        &self,
        producer_head_spec: &ProducerHeadSpec,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = self.final_plan.get_or_init(|| {
            let producer_head = ProducerHead::from_spec(
                producer_head_spec,
                self.base_plan.schema(),
                &self.task_ctx,
            )?;

            Ok(producer_head.insert(Arc::clone(&self.base_plan))?)
        });
        match result {
            Ok(plan) => Ok(Arc::clone(plan)),
            Err(err) => Err(DataFusionError::Shared(Arc::clone(err))),
        }
    }
}
