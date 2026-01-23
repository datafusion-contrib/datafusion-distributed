use crate::common::ttl_map::TTLMap;
use crate::flight_service::TaskData;
use crate::protobuf::StageKey;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::{Request, Response, Status};

use super::{ObservabilityService, PingRequest, PingResponse};

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
        Ok(tonic::Response::new(PingResponse { value: 1 }))
    }
}
