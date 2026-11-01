use crate::TaskKey;
use async_trait::async_trait;
use http::HeaderMap;
use tonic::Status;

/// A gRPC call to a [Worker](crate::Worker), as a [WorkerCallAuthorizer] sees it: the call's
/// headers and the [TaskKey] it names, decoded but not yet acted on.
///
/// Non-exhaustive, so an authorizer decides what to do with a call a later release adds.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum WorkerCall<'a> {
    /// A `CoordinatorChannel` call, which sets the plan of a task.
    SetPlan {
        headers: &'a HeaderMap,
        task_key: &'a TaskKey,
    },
    /// An `ExecuteTask` call, which streams partitions of a task whose plan was set.
    ExecuteTask {
        headers: &'a HeaderMap,
        task_key: &'a TaskKey,
    },
}

impl<'a> WorkerCall<'a> {
    /// The headers the call arrived with.
    pub fn headers(&self) -> &'a HeaderMap {
        match self {
            Self::SetPlan { headers, .. } | Self::ExecuteTask { headers, .. } => headers,
        }
    }

    /// The task the call names.
    pub fn task_key(&self) -> &'a TaskKey {
        match self {
            Self::SetPlan { task_key, .. } | Self::ExecuteTask { task_key, .. } => task_key,
        }
    }
}

/// Decides whether a [Worker](crate::Worker) serves a gRPC call, before the call touches the
/// worker's task entries.
///
/// Set with [Worker::with_call_authorizer](crate::Worker::with_call_authorizer). An `Err` is
/// returned to the caller as the call's status. In-process calls through a
/// [LocalWorkerContext](crate::LocalWorkerContext) do not cross gRPC and are not authorized.
#[async_trait]
pub trait WorkerCallAuthorizer: Send + Sync {
    async fn authorize(&self, call: WorkerCall<'_>) -> Result<(), Status>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::{BoxCloneSyncChannel, create_worker_client};
    use crate::{
        DistributedConfig, ExecuteTaskRequest, MaybeEncoded, ProducerHead, SetPlanRequest, Worker,
        WorkerToCoordinatorMsg,
    };
    use datafusion::arrow::datatypes::Schema;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::stream::BoxStream;
    use futures::{StreamExt, stream};
    use http::HeaderValue;
    use std::sync::{Arc, Mutex};
    use tonic::body::Body;
    use tower::ServiceExt;
    use url::Url;
    use uuid::Uuid;

    const ADMITTED: &str = "x-admitted";

    /// Admits a call that carries [ADMITTED], and records the calls it admitted.
    #[derive(Default)]
    struct Recorded(Mutex<Vec<(&'static str, TaskKey)>>);

    #[async_trait]
    impl WorkerCallAuthorizer for Arc<Recorded> {
        async fn authorize(&self, call: WorkerCall<'_>) -> Result<(), Status> {
            if !call.headers().contains_key(ADMITTED) {
                return Err(Status::permission_denied("refused by the authorizer"));
            }
            let kind = match call {
                WorkerCall::SetPlan { .. } => "set plan",
                WorkerCall::ExecuteTask { .. } => "execute task",
            };
            self.0.lock().unwrap().push((kind, *call.task_key()));
            Ok(())
        }
    }

    fn task_key() -> TaskKey {
        TaskKey {
            query_id: Uuid::new_v4(),
            stage_id: 1,
            task_number: 0,
        }
    }

    fn headers(admitted: bool) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if admitted {
            headers.insert(ADMITTED, HeaderValue::from_static("yes"));
        }
        headers
    }

    struct Served {
        worker: Worker,
        recorded: Arc<Recorded>,
        ctx: Arc<TaskContext>,
    }

    impl Served {
        fn new() -> Self {
            let recorded = Arc::new(Recorded::default());
            let ctx = SessionContext::new_with_config(
                SessionConfig::new().with_option_extension(DistributedConfig::default()),
            )
            .task_ctx();
            Self {
                worker: Worker::default().with_call_authorizer(Arc::clone(&recorded)),
                recorded,
                ctx,
            }
        }

        fn client(&self) -> Box<dyn crate::WorkerChannel> {
            let server = self.worker.clone().into_worker_server();
            let channel =
                ServiceExt::<http::Request<Body>>::map_err(server, |never| match never {});
            create_worker_client(BoxCloneSyncChannel::new(channel))
        }

        async fn set_plan(
            &self,
            key: TaskKey,
            admitted: bool,
        ) -> datafusion::common::Result<
            BoxStream<'static, datafusion::common::Result<WorkerToCoordinatorMsg>>,
        > {
            let request = SetPlanRequest {
                task_key: key,
                task_count: 1,
                plan: MaybeEncoded::Decoded(Arc::new(EmptyExec::new(Arc::new(Schema::empty())))),
                work_unit_feed_declarations: vec![],
                dynamic_filter_remote_producer_ids: vec![],
                target_worker_url: Url::parse("http://localhost:1").unwrap(),
                query_start_time_ns: 0,
            };
            self.client()
                .coordinator_channel(
                    headers(admitted),
                    request,
                    stream::pending().boxed(),
                    ExecutionPlanMetricsSet::new(),
                    &self.ctx,
                )
                .await
        }

        async fn execute_task(
            &self,
            key: TaskKey,
            admitted: bool,
        ) -> datafusion::common::Result<()> {
            let request = ExecuteTaskRequest {
                task_key: key,
                target_partition_start: 0,
                target_partition_end: 1,
                producer_head: ProducerHead::None,
            };
            for mut partition in self
                .client()
                .execute_task(
                    headers(admitted),
                    request,
                    ExecutionPlanMetricsSet::new(),
                    &self.ctx,
                )
                .await?
            {
                while partition.next().await.transpose()?.is_some() {}
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_refused_call_never_reaches_the_task_entries() {
        let served = Served::new();
        let key = task_key();
        for refused in [
            served.set_plan(key, false).await.map(drop),
            served.execute_task(key, false).await,
        ] {
            let refused = refused.unwrap_err().to_string();
            assert!(refused.contains("refused by the authorizer"), "{refused}");
        }
        assert_eq!(served.worker.tasks_running().await, 0);
        assert!(served.recorded.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_admitted_call_is_served_after_the_authorizer_saw_its_task_key() {
        let served = Served::new();
        let key = task_key();
        let installed = served.set_plan(key, true).await.unwrap();
        served.execute_task(key, true).await.unwrap();
        drop(installed);
        assert_eq!(
            *served.recorded.0.lock().unwrap(),
            [("set plan", key), ("execute task", key)]
        );
    }
}
