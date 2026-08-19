use crate::{
    CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest, GetWorkerInfoResponse,
    SetPlanRequest, Worker, WorkerChannel, WorkerToCoordinatorMsg,
};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use futures::StreamExt;
use futures::stream::BoxStream;
use http::HeaderMap;
use std::sync::Arc;

/// [WorkerChannel] that proxies method invocations to a local [Worker] referenced as a field.
pub(crate) struct InProcessWorkerClient {
    local_worker: Worker,
}

impl InProcessWorkerClient {
    /// Builds a new [InProcessWorkerClient] that will proxy request to the provided [Worker],
    /// avoiding any serde on the inputs or outputs.
    pub(crate) fn new(local_worker: Worker) -> Self {
        Self { local_worker }
    }
}

#[async_trait]
impl WorkerChannel for InProcessWorkerClient {
    async fn coordinator_channel(
        &mut self,
        headers: HeaderMap,
        set_plan_request: SetPlanRequest,
        c2w_stream: BoxStream<'static, CoordinatorToWorkerMsg>,
        _metrics: ExecutionPlanMetricsSet,
        _task_ctx: &Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<WorkerToCoordinatorMsg>>> {
        self.local_worker
            .coordinator_channel(headers, set_plan_request, c2w_stream.map(Ok).boxed())
            .await
    }

    async fn execute_task(
        &mut self,
        _headers: HeaderMap,
        request: ExecuteTaskRequest,
        _metrics: ExecutionPlanMetricsSet,
        _task_ctx: &Arc<TaskContext>,
    ) -> Result<Vec<BoxStream<'static, Result<RecordBatch>>>> {
        let (result, _) = self.local_worker.execute_task(request).await?;
        Ok(result.into_iter().map(|v| v.boxed()).collect())
    }

    async fn get_worker_info(
        &mut self,
        _request: GetWorkerInfoRequest,
    ) -> Result<GetWorkerInfoResponse> {
        Ok(GetWorkerInfoResponse {
            version: self.local_worker.version().to_string(),
        })
    }
}
