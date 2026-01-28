use crate::common::ttl_map::TTLMap;
use crate::flight_service::TaskData;
use crate::protobuf::StageKey;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::{Request, Response, Status};

use super::{
    GetTaskProgressResponse, ObservabilityService, TaskProgress, TaskStatus,
    generated::observability::{GetTaskProgressRequest, PingRequest, PingResponse},
};

pub struct ObservabilityServiceImpl {
    task_data_entries: Arc<TTLMap<StageKey, Arc<OnceCell<TaskData>>>>,
}

impl ObservabilityServiceImpl {
    pub fn new(task_data_entries: Arc<TTLMap<StageKey, Arc<OnceCell<TaskData>>>>) -> Self {
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
            let internal_key = entry.key();
            let task_data_cell = entry.value();

            // Only include initialized tasks
            if let Some(task_data) = task_data_cell.get() {
                let total_partitions = task_data.total_partitions() as u64;
                let remaining = task_data.num_partitions_remaining() as u64;
                let completed_partitions = total_partitions.saturating_sub(remaining);

                tasks.push(TaskProgress {
                    stage_key: Some(convert_stage_key(internal_key)),
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
