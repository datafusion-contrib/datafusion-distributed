use crate::TaskData;
use crate::execution_plans::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec};
use crate::protobuf::StageKey;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricValue;
use moka::future::Cache;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::{Request, Response, Status};

use super::generated::observability::{GetTaskMetricsResponse, TaskMetricsSummary};
use super::{
    ObservabilityService,
    generated::observability::{GetTaskMetricsRequest, PingRequest, PingResponse},
};

pub struct ObservabilityServiceImpl {
    task_data_entries: Arc<Cache<StageKey, Arc<OnceCell<TaskData>>>>,
}

impl ObservabilityServiceImpl {
    pub fn new(task_data_entries: Arc<Cache<StageKey, Arc<OnceCell<TaskData>>>>) -> Self {
        Self { task_data_entries }
    }
}

#[tonic::async_trait]
impl ObservabilityService for ObservabilityServiceImpl {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse { value: 1 }))
    }

    async fn get_task_metrics(
        &self,
        _request: Request<GetTaskMetricsRequest>,
    ) -> Result<Response<GetTaskMetricsResponse>, Status> {
        let mut task_summaries = Vec::new();

        for entry in self.task_data_entries.iter() {
            let (stage_key, task_data_cell) = entry;

            if let Some(task_data) = task_data_cell.get() {
                task_summaries.push(aggregate_task_metrics(&task_data.plan, stage_key.clone()));
            }
        }

        Ok(Response::new(GetTaskMetricsResponse { task_summaries }))
    }
}

/// Converts internal StageKey to observability proto StageKey
fn convert_stage_key(key: &StageKey) -> super::StageKey {
    super::StageKey {
        query_id: key.query_id.to_vec(),
        stage_id: key.stage_id,
        task_number: key.task_number,
    }
}

/// Aggregates operator metrics across a single task and returns a TaskMetricsSummary.
/// Walks the plan tree recursively, summing metrics across all operators.
/// Output rows are taken from the root node only. Stops at network boundaries
/// since those represent child stages running on other workers.
fn aggregate_task_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    stage_key: Arc<StageKey>,
) -> TaskMetricsSummary {
    let mut output_rows = 0u64;
    let mut elapsed_compute = 0u64;
    let mut current_memory_usage = 0u64;
    let mut spill_count = 0u64;

    // Extract output_rows from root node only
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            if let MetricValue::OutputRows(c) = metric.value() {
                output_rows += c.value() as u64;
            }
        }
    }

    // Sum remaining metrics across all nodes
    accumulate_metrics(
        plan,
        &mut elapsed_compute,
        &mut current_memory_usage,
        &mut spill_count,
    );

    TaskMetricsSummary {
        stage_key: Some(convert_stage_key(&stage_key)),
        output_rows,
        elapsed_compute,
        current_memory_usage,
        spill_count,
    }
}

/// Recursively walks the plan tree, accumulating metrics from each node.
/// Stops at network boundary nodes since they are child stages on other workers.
fn accumulate_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    elapsed_compute: &mut u64,
    current_memory_usage: &mut u64,
    spill_count: &mut u64,
) {
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            match metric.value() {
                MetricValue::ElapsedCompute(t) => *elapsed_compute += t.value() as u64,
                // FIXME: Guage drops the current memory usage snapshot before it's read, giving a
                // 0 value each time.
                MetricValue::CurrentMemoryUsage(g) => *current_memory_usage += g.value() as u64,
                MetricValue::SpillCount(c) => *spill_count += c.value() as u64,
                _ => {}
            }
        }
    }

    for child in plan.children() {
        if child
            .as_any()
            .downcast_ref::<NetworkShuffleExec>()
            .is_some()
            || child
                .as_any()
                .downcast_ref::<NetworkCoalesceExec>()
                .is_some()
            || child
                .as_any()
                .downcast_ref::<NetworkBroadcastExec>()
                .is_some()
        {
            continue;
        }
        accumulate_metrics(child, elapsed_compute, current_memory_usage, spill_count);
    }
}
