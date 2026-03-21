use super::{
    GetTaskProgressResponse, ObservabilityService, TaskProgress, TaskStatus, WorkerMetrics,
    generated::observability::{GetTaskProgressRequest, PingRequest, PingResponse},
};
use crate::worker::generated::worker::TaskKey;
use crate::worker::{SingleWriteMultiRead, TaskData};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use moka::future::Cache;
use std::sync::Arc;
#[cfg(feature = "system-metrics")]
use std::time::Duration;
#[cfg(feature = "system-metrics")]
use sysinfo::{Pid, ProcessRefreshKind};
#[cfg(feature = "system-metrics")]
use tokio::sync::watch;
use tonic::{Request, Response, Status};

type ResultTaskData = Result<TaskData, Arc<DataFusionError>>;

pub struct ObservabilityServiceImpl {
    task_data_entries: Arc<Cache<TaskKey, Arc<SingleWriteMultiRead<ResultTaskData>>>>,
    #[cfg(feature = "system-metrics")]
    system: watch::Receiver<WorkerMetrics>,
}

impl ObservabilityServiceImpl {
    pub fn new(
        task_data_entries: Arc<Cache<TaskKey, Arc<SingleWriteMultiRead<ResultTaskData>>>>,
    ) -> Self {
        #[cfg(feature = "system-metrics")]
        let (tx, rx) = tokio::sync::watch::channel(WorkerMetrics::default());

        #[cfg(feature = "system-metrics")]
        {
            let pid = Pid::from_u32(std::process::id());
            let mut sys = sysinfo::System::new_all();

            // Spawn background task to periodically collect and send system metrics.
            tokio::task::spawn(async move {
                loop {
                    sys.refresh_process_specifics(
                        pid,
                        ProcessRefreshKind::new().with_cpu().with_memory(),
                    );

                    if let Some(process) = sys.process(pid) {
                        let num_cpus = std::thread::available_parallelism()
                            .map(|n| n.get() as f64)
                            .unwrap_or(1.0);
                        let metrics = WorkerMetrics {
                            rss_bytes: process.memory(),
                            cpu_usage_percent: process.cpu_usage() as f64 / num_cpus,
                        };
                        if tx.send(metrics).is_err() {
                            break;
                        }
                    } else if tx.send(WorkerMetrics::default()).is_err() {
                        break;
                    };

                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
        }

        Self {
            task_data_entries,
            #[cfg(feature = "system-metrics")]
            system: rx,
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
            if let Some(Ok(task_data)) = task_data_cell.read_now() {
                let total_partitions = task_data.total_partitions() as u64;
                let remaining = task_data.num_partitions_remaining() as u64;
                let completed_partitions = total_partitions.saturating_sub(remaining);
                let output_rows = output_rows_from_plan(&task_data.plan);

                tasks.push(TaskProgress {
                    task_key: Some((*internal_key).clone()),
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
        #[cfg(not(feature = "system-metrics"))]
        {
            WorkerMetrics::default()
        }

        #[cfg(feature = "system-metrics")]
        return *self.system.borrow();
    }
}

/// Extracts output rows from the root plan node's metrics.
fn output_rows_from_plan(plan: &Arc<dyn ExecutionPlan>) -> u64 {
    plan.metrics().and_then(|m| m.output_rows()).unwrap_or(0) as u64
}
