use crate::flight_service::{SingleWriteMultiRead, TaskData};
use crate::protobuf::StageKey;
use moka::future::Cache;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use super::{
    GetTaskProgressResponse, ObservabilityService, TaskProgress, TaskStatus,
    generated::observability::{GetTaskProgressRequest, PingRequest, PingResponse},
};

type ResultTaskData = Result<TaskData, Status>;

#[allow(dead_code)] // TEMP: will be used in future implementations.
pub struct ObservabilityServiceImpl {
    task_data_entries: Arc<Cache<StageKey, Arc<SingleWriteMultiRead<ResultTaskData>>>>,
}

impl ObservabilityServiceImpl {
    pub fn new(
        task_data_entries: Arc<Cache<StageKey, Arc<SingleWriteMultiRead<ResultTaskData>>>>,
    ) -> Self {
        Self { task_data_entries }
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

                tasks.push(TaskProgress {
                    stage_key: Some(convert_stage_key(&internal_key)),
                    total_partitions,
                    completed_partitions,
                    status: TaskStatus::Running as i32,
                });
            }
        }

        Ok(Response::new(GetTaskProgressResponse { tasks }))
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
