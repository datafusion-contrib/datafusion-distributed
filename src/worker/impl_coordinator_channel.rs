use crate::Worker;
use crate::common::deserialize_uuid;
use crate::work_unit_feed::set_work_unit_received_time;
use crate::worker::LocalWorkerContext;
use crate::worker::generated::worker::coordinator_to_worker_msg::Inner;
use crate::worker::generated::worker::worker_service_server::WorkerService;
use crate::worker::generated::worker::{
    CoordinatorToWorkerMsg, WorkerToCoordinatorMsg, worker_to_coordinator_msg,
};
use datafusion::common::DataFusionError;
use futures::{FutureExt, StreamExt, TryStreamExt};
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use url::Url;

impl Worker {
    pub(super) async fn impl_coordinator_channel(
        &self,
        request: Request<Streaming<CoordinatorToWorkerMsg>>,
    ) -> Result<Response<<Worker as WorkerService>::CoordinatorChannelStream>, Status> {
        let (grpc_headers, _ext, mut body) = request.into_parts();

        // The first message must be a SetPlanRequest.
        let Some(msg) = body.next().await else {
            return Err(Status::internal("Empty Coordinator stream"));
        };
        let Some(Inner::SetPlanRequest(request)) = msg?.inner else {
            return Err(Status::internal(
                "First Coordinator message must be SetPlanRequest",
            ));
        };
        let key = request.task_key.clone().ok_or_else(missing("task_key"))?;
        let target_worker_url = request.target_worker_url.clone();
        let headers = grpc_headers.into_headers();

        // Flight's local-bypass read needs the worker's own registry + URL on the session, so a
        // co-located target is served from memory instead of dialing back over gRPC.
        let task_data_entries = Arc::clone(&self.task_data_entries);
        let outcome = self
            .set_task_plan(request, headers, move |cfg| {
                Ok(cfg.with_extension(Arc::new(LocalWorkerContext {
                    task_data_entries,
                    self_url: Url::parse(&target_worker_url)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                })))
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let metrics_rx = outcome.metrics_rx;

        // Continue reading remaining messages (work unit feed data) in the background.
        let mut work_unit_senders = Some(outcome.work_unit_senders);
        let task_data_entries = Arc::clone(&self.task_data_entries);
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            let mut body = body.map_ok(set_work_unit_received_time);
            while let Some(Ok(msg)) = body.next().await {
                let Some(msg) = msg.inner else {
                    continue;
                };
                match msg {
                    Inner::SetPlanRequest(_) => {
                        // SetPlanRequest should be the first already polled message in the stream,
                        // if some reached here it means that something is wrong.
                        continue;
                    }
                    Inner::WorkUnitBatch(msg) => {
                        let Some(work_unit_senders) = work_unit_senders.as_mut() else {
                            continue;
                        };
                        for wu in msg.batch {
                            let Ok(id) = deserialize_uuid(&wu.id) else {
                                continue;
                            };
                            let partition = wu.partition as usize;
                            let Some(tx) = work_unit_senders.get(&(id, partition)) else {
                                continue;
                            };
                            if tx.send(Ok(wu)).is_err() {
                                // Channel closed, this sender needs to be dropped, as none will ever
                                // be listening on the other side.
                                work_unit_senders.remove(&(id, partition));
                                continue;
                            }
                        }
                    }
                    Inner::WorkUnitEos(_) => {
                        // No further work unit message will be received here, so drop all the
                        // sender sides so that receiver sides see an EOS upon draining the
                        // remaining messages.
                        //
                        // The [WorkUnitEos] message just applies work units, and it's not a global
                        // EOS signal for the coordinator->worker stream, as there might be more
                        // messages of different nature in that stream.
                        let _ = work_unit_senders.take();
                    }
                }
            }
            #[allow(clippy::disallowed_methods)]
            tokio::spawn(async move { task_data_entries.invalidate(&key).await });
        });

        // Stream back the metrics once the task finishes executing.
        // The oneshot receiver resolves when impl_execute_task sends the collected
        // metrics after all partitions have finished or been dropped.
        let metrics_stream = metrics_rx.into_stream();
        let metrics_stream = metrics_stream.filter_map(|task_metrics| async move {
            match task_metrics {
                Ok(task_metrics) => Some(WorkerToCoordinatorMsg {
                    inner: Some(worker_to_coordinator_msg::Inner::TaskMetrics(task_metrics)),
                }),
                Err(_) => None, // channel dropped without sending any message
            }
        });
        Ok(Response::new(metrics_stream.map(Ok).boxed()))
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}
