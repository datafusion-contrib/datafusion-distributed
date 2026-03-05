use crate::flight_service::TaskData;
use crate::protobuf::StageKey;
use datafusion::physical_plan::ExecutionPlan;
use moka::future::Cache;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::{Request, Response, Status};

use super::{
    GetTaskProgressResponse, ObservabilityService, TaskProgress, TaskStatus, WorkerMetrics,
    generated::observability::{GetTaskProgressRequest, PingRequest, PingResponse},
};

pub struct ObservabilityServiceImpl {
    task_data_entries: Arc<Cache<StageKey, Arc<OnceCell<TaskData>>>>,
    #[cfg(feature = "system-metrics")]
    system: std::sync::Mutex<sysinfo::System>,
}

impl ObservabilityServiceImpl {
    pub fn new(task_data_entries: Arc<Cache<StageKey, Arc<OnceCell<TaskData>>>>) -> Self {
        Self {
            task_data_entries,
            #[cfg(feature = "system-metrics")]
            system: std::sync::Mutex::new(sysinfo::System::new()),
        }
    }
}

#[tonic::async_trait]
impl ObservabilityService for ObservabilityServiceImpl {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse { value: 1 }))
    }

    async fn get_task_progress(
        &self,
        _request: Request<GetTaskProgressRequest>,
    ) -> Result<Response<GetTaskProgressResponse>, Status> {
        let mut tasks = Vec::new();

        for entry in self.task_data_entries.iter() {
            let (internal_key, task_data_cell) = entry;

            // Only include initialized tasks
            if let Some(task_data) = task_data_cell.get() {
                let total_partitions = task_data.total_partitions() as u64;
                let remaining = task_data.num_partitions_remaining() as u64;
                let completed_partitions = total_partitions.saturating_sub(remaining);
                let output_rows = output_rows_from_plan(&task_data.plan);

                tasks.push(TaskProgress {
                    stage_key: Some(convert_stage_key(&internal_key)),
                    total_partitions,
                    completed_partitions,
                    status: TaskStatus::Running as i32,
                    output_rows,
                });
            }
        }

        let worker_metrics = Some(self.collect_worker_metrics());

        Ok(Response::new(GetTaskProgressResponse {
            tasks,
            worker_metrics,
        }))
    }
}

impl ObservabilityServiceImpl {
    fn collect_worker_metrics(&self) -> WorkerMetrics {
        #[cfg(feature = "system-metrics")]
        {
            use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate};

            let pid = Pid::from_u32(std::process::id());
            let mut sys = self.system.lock().unwrap_or_else(|e| e.into_inner());
            sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_cpu().with_memory(),
            );

            if let Some(process) = sys.process(pid) {
                let num_cpus = std::thread::available_parallelism()
                    .map(|n| n.get() as f64)
                    .unwrap_or(1.0);
                WorkerMetrics {
                    rss_bytes: process.memory(),
                    cpu_usage_percent: process.cpu_usage() as f64 / num_cpus,
                }
            } else {
                WorkerMetrics::default()
            }
        }

        // When the `system-metrics` feature is not enabled, gracefully degrade
        // by returning default (zeroed) metrics instead of querying the OS.
        #[cfg(not(feature = "system-metrics"))]
        {
            WorkerMetrics::default()
        }
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

/// Extracts output rows from the root plan node's metrics.
fn output_rows_from_plan(plan: &Arc<dyn ExecutionPlan>) -> u64 {
    plan.metrics().and_then(|m| m.output_rows()).unwrap_or(0) as u64
}
