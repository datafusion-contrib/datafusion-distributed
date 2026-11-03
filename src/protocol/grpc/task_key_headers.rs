use crate::TaskKey;
use tonic::Status;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};

/// Set by the gRPC client on every `CoordinatorChannel` and `ExecuteTask` call to the query id of
/// the [TaskKey] the call names, as a hyphenated UUID, overwriting a passthrough header of the same
/// name. A tower layer in front of a worker can authorize a call by its query from this header
/// without decoding the body.
///
/// The worker refuses a call that carries it more than once, or with a value other than the query
/// id of the [TaskKey] in the body. A call without it is served.
pub const TASK_KEY_QUERY_ID_HEADER: &str = "x-datafusion-distributed-task-key-query-id";

pub(super) fn set_task_key_headers(metadata: &mut MetadataMap, task_key: &TaskKey) {
    let query_id = AsciiMetadataValue::try_from(task_key.query_id.to_string())
        .expect("a hyphenated UUID is a valid header value");
    metadata.insert(TASK_KEY_QUERY_ID_HEADER, query_id);
}

/// Refuses a call whose task key headers do not name the [TaskKey] decoded from its body, so a
/// layer that authorized the headers authorized the task the worker acts on.
pub(super) fn check_task_key_headers(
    metadata: &MetadataMap,
    task_key: &TaskKey,
) -> Result<(), Status> {
    let mut query_ids = metadata.get_all(TASK_KEY_QUERY_ID_HEADER).iter();
    let Some(query_id) = query_ids.next() else {
        return Ok(());
    };
    if query_ids.next().is_some()
        || query_id.as_encoded_bytes() != task_key.query_id.to_string().as_bytes()
    {
        return Err(Status::invalid_argument(format!(
            "{TASK_KEY_QUERY_ID_HEADER} must appear at most once and match the request's task key"
        )));
    }
    Ok(())
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
    use http::{HeaderMap, HeaderValue};
    use std::sync::{Arc, Mutex};
    use tonic::body::Body;
    use tower::ServiceExt;
    use url::Url;
    use uuid::Uuid;

    #[tokio::test]
    async fn a_call_whose_query_id_header_names_another_query_is_refused() {
        let served = Served::new(|headers| {
            headers.insert(TASK_KEY_QUERY_ID_HEADER, header_value(Uuid::new_v4()));
        });
        let key = task_key();
        assert_refused(served.set_plan(key, HeaderMap::new()).await.map(drop));
        assert_refused(served.execute_task(key, HeaderMap::new()).await);
        assert_eq!(served.worker.tasks_running().await, 0);
    }

    #[tokio::test]
    async fn a_call_carrying_the_query_id_header_twice_is_refused() {
        let served = Served::new(|headers| {
            let sent = headers[TASK_KEY_QUERY_ID_HEADER].clone();
            headers.append(TASK_KEY_QUERY_ID_HEADER, sent);
        });
        let key = task_key();
        assert_refused(served.set_plan(key, HeaderMap::new()).await.map(drop));
        assert_refused(served.execute_task(key, HeaderMap::new()).await);
        assert_eq!(served.worker.tasks_running().await, 0);
    }

    #[tokio::test]
    async fn the_client_overwrites_a_passthrough_header_of_the_same_name() {
        let seen = Arc::new(Mutex::new(vec![]));
        let served = Served::new({
            let seen = Arc::clone(&seen);
            move |headers: &mut HeaderMap| {
                let sent = headers.get_all(TASK_KEY_QUERY_ID_HEADER).iter().cloned();
                seen.lock().unwrap().push(sent.collect::<Vec<_>>());
            }
        });
        let key = task_key();
        let mut passthrough = HeaderMap::new();
        passthrough.insert(TASK_KEY_QUERY_ID_HEADER, header_value(Uuid::new_v4()));
        let installed = served.set_plan(key, passthrough.clone()).await.unwrap();
        served.execute_task(key, passthrough).await.unwrap();
        drop(installed);
        let sent = header_value(key.query_id);
        assert_eq!(*seen.lock().unwrap(), [[sent.clone()], [sent]]);
    }

    #[tokio::test]
    async fn a_call_without_the_query_id_header_is_served() {
        let served = Served::new(|headers| {
            headers.remove(TASK_KEY_QUERY_ID_HEADER);
        });
        let key = task_key();
        let installed = served.set_plan(key, HeaderMap::new()).await.unwrap();
        served.execute_task(key, HeaderMap::new()).await.unwrap();
        drop(installed);
    }

    fn assert_refused<T>(result: datafusion::common::Result<T>) {
        let Err(refused) = result else {
            panic!("the call was served");
        };
        let refused = refused.to_string();
        assert!(refused.contains(TASK_KEY_QUERY_ID_HEADER), "{refused}");
    }

    fn task_key() -> TaskKey {
        TaskKey {
            query_id: Uuid::new_v4(),
            stage_id: 1,
            task_number: 0,
        }
    }

    fn header_value(query_id: Uuid) -> HeaderValue {
        HeaderValue::from_str(&query_id.to_string()).unwrap()
    }

    /// A worker behind a channel that applies `rewrite` to the headers of each call it carries.
    struct Served {
        worker: Worker,
        rewrite: Arc<dyn Fn(&mut HeaderMap) + Send + Sync>,
        ctx: Arc<TaskContext>,
    }

    impl Served {
        fn new(rewrite: impl Fn(&mut HeaderMap) + Send + Sync + 'static) -> Self {
            let ctx = SessionContext::new_with_config(
                SessionConfig::new().with_option_extension(DistributedConfig::default()),
            )
            .task_ctx();
            Self {
                worker: Worker::default(),
                rewrite: Arc::new(rewrite),
                ctx,
            }
        }

        fn client(&self) -> Box<dyn crate::WorkerChannel> {
            let rewrite = Arc::clone(&self.rewrite);
            let server = self.worker.clone().into_worker_server();
            let channel = ServiceExt::<http::Request<Body>>::map_request(
                server,
                move |mut request: http::Request<Body>| {
                    rewrite(request.headers_mut());
                    request
                },
            );
            let channel =
                ServiceExt::<http::Request<Body>>::map_err(channel, |never| match never {});
            create_worker_client(BoxCloneSyncChannel::new(channel))
        }

        async fn set_plan(
            &self,
            key: TaskKey,
            headers: HeaderMap,
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
                    headers,
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
            headers: HeaderMap,
        ) -> datafusion::common::Result<()> {
            let request = ExecuteTaskRequest {
                task_key: key,
                target_partition_start: 0,
                target_partition_end: 1,
                producer_head: ProducerHead::None,
            };
            for mut partition in self
                .client()
                .execute_task(headers, request, ExecutionPlanMetricsSet::new(), &self.ctx)
                .await?
            {
                while partition.next().await.transpose()?.is_some() {}
            }
            Ok(())
        }
    }
}
