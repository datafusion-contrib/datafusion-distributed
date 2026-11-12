#[cfg(all(feature = "tpch", feature = "chaos-tests", test))]
mod tests {
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::runtime::JoinSet;
    use datafusion::common::{HashMap, Result};
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::grpc::{
        TASK_KEY_QUERY_ID_HEADER, TASK_KEY_STAGE_ID_HEADER, TASK_KEY_TASK_NUMBER_HEADER,
    };
    use datafusion_distributed::test_utils::localhost::LocalHostWorkerResolver;
    use datafusion_distributed::{
        DistributedConfig, DistributedExt, RandomRouteTaskHandler, SessionStateBuilderExt, Worker,
        WorkerQueryContext,
    };
    use datafusion_distributed_benchmarks::datasets::{
        output::TESTDATA_ROOT, parquet_layout::ParquetLayout, register_tables, tpch,
    };
    use moka::future::FutureExt;
    use rand::SeedableRng;
    use rand::prelude::{Rng, StdRng};
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::env;
    use std::future::Future;
    use std::ops::Range;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};
    use tokio::net::TcpListener;
    use tokio::sync::{OnceCell, Semaphore};
    use tonic::Status;
    use tonic::body::Body;
    use tonic::transport::Server;
    use tower::{Layer, Service};
    use url::Url;

    const NUM_WORKERS: usize = 8;
    const MAX_IN_FLIGHT: usize = 1000;
    const TOTAL_QUERIES: usize = 100;
    const CONCURRENT_QUERIES_PER_CLIENT_RANDOM_RANGE: Range<usize> = 1..4;
    const CONCURRENT_CLIENTS: usize = 10;
    const TPCH_SCALE_FACTOR: f64 = 0.1;
    const TPCH_MAX_FILE_SIZE: usize = 2_000_000;

    fn seed() -> u64 {
        env::var("CHAOS_SEED").map_or_else(
            |_| rand::random(),
            |seed| seed.parse().expect("CHAOS_SEED must be a u64"),
        )
    }

    fn adaptive() -> bool {
        env::var("ADAPTIVE").is_ok_and(|v| v == "true")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn chaos() -> Result<()> {
        let seed = seed();
        println!("seed: {seed}");
        let mut rng = StdRng::seed_from_u64(seed);
        let cfg = ChaosClusterConfig {
            num_workers: NUM_WORKERS,
            max_in_flight: MAX_IN_FLIGHT,
            seed,
        };
        let (ctx, _guard, workers) = chaos_localhost_cluster(cfg).await?;
        let data_dir = ensure_tpch_data().await;
        register_tables(&ctx, &data_dir).await?;

        let mut queries = VecDeque::new();

        for _ in 0..TOTAL_QUERIES {
            let mut batch = vec![];
            for _ in 0..rng.random_range(CONCURRENT_QUERIES_PER_CLIENT_RANDOM_RANGE) {
                let query = tpch::get_query("q20")?;
                batch.push(query.clone());
            }
            queries.push_back(batch);
        }
        let queries = Arc::new(Mutex::new(queries));

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let ctx = Arc::new(ctx);
        let mut concurrent_clients = JoinSet::new();
        for _ in 0..CONCURRENT_CLIENTS {
            let ctx = Arc::clone(&ctx);
            let queries = Arc::clone(&queries);
            let tx = tx.clone();
            concurrent_clients.spawn(async move {
                while let Some(query_batch) = { queries.lock().unwrap().pop_front() } {
                    let mut futures = vec![];
                    for query in query_batch {
                        let ctx = Arc::clone(&ctx);
                        futures.push(async move { ctx.sql(&query).await?.collect().await }.boxed());
                    }

                    for res in futures::future::join_all(futures).await {
                        tx.send(res?).unwrap();
                    }
                }
                Ok::<_, DataFusionError>(())
            });
        }
        drop(tx);

        for result in concurrent_clients.join_all().await {
            result?;
        }

        let first = rx.recv().await.expect("No result returned");
        let first = pretty_format_batches(&first)?;
        while let Some(next_result) = rx.recv().await {
            let next_result = pretty_format_batches(&next_result)?;
            pretty_assertions::assert_eq!(first.to_string(), next_result.to_string());
        }
        assert_no_tasks_running_eventually(&workers).await;

        Ok(())
    }

    #[derive(Clone)]
    struct ChaosLayer {
        in_flight: Arc<Semaphore>,
        state: ChaosState,
    }

    impl ChaosLayer {
        fn new(max_in_flight: usize, state: ChaosState) -> Self {
            Self {
                in_flight: Arc::new(Semaphore::new(max_in_flight)),
                state,
            }
        }
    }

    impl<S> Layer<S> for ChaosLayer {
        type Service = ChaosService<S>;

        fn layer(&self, inner: S) -> Self::Service {
            ChaosService {
                inner,
                in_flight: self.in_flight.clone(),
                state: self.state.clone(),
            }
        }
    }

    #[derive(Clone)]
    struct ChaosService<S> {
        inner: S,
        in_flight: Arc<Semaphore>,
        state: ChaosState,
    }

    impl<S> Service<http::Request<Body>> for ChaosService<S>
    where
        S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        type Response = http::Response<Body>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, request: http::Request<Body>) -> Self::Future {
            let path = request.uri().path();

            // Only CoordinatorChannel errors are recoverable, if this is not a CoordinatorChannel
            // call, we must not pollute it with failures.
            if !path.ends_with("worker.WorkerService/CoordinatorChannel") {
                return self.inner.call(request).boxed();
            }

            let permit = match self.in_flight.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    return Box::pin(async {
                        Ok(Status::resource_exhausted("chaos: worker overloaded").into_http())
                    });
                }
            };
            let task_key = ChaosTaskKey::from_headers(request.headers());
            let action = self.state.action(task_key);
            let response = self.inner.call(request);

            Box::pin(async move {
                let _permit = permit;
                match action {
                    ChaosAction::Pass => response.await,
                    ChaosAction::Delay(duration) => {
                        tokio::time::sleep(duration).await;
                        response.await
                    }
                    ChaosAction::Timeout(duration) => {
                        // Wait until the worker has accepted the coordinator channel and created
                        // its response stream. Dropping that response before returning the error
                        // simulates a failure after request processing but before the client has
                        // received response headers.
                        drop(response.await?);
                        tokio::time::sleep(duration).await;
                        Ok(Status::deadline_exceeded("chaos: worker timed out").into_http())
                    }
                    ChaosAction::Unavailable(duration) => {
                        // See the Timeout branch: this deliberately exercises cleanup of an
                        // accepted coordinator channel, rather than rejecting it at admission.
                        drop(response.await?);
                        tokio::time::sleep(duration).await;
                        Ok(Status::unavailable("chaos: worker restarting").into_http())
                    }
                }
            })
        }
    }

    #[derive(Debug, Eq, PartialEq)]
    enum ChaosAction {
        Pass,
        Delay(Duration),
        Timeout(Duration),
        Unavailable(Duration),
    }

    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct ChaosTaskKey {
        query_id: String,
        stage_id: u64,
        task_number: u64,
    }

    impl ChaosTaskKey {
        fn from_headers(headers: &http::HeaderMap) -> Self {
            let header = |name| {
                headers
                    .get(name)
                    .unwrap_or_else(|| panic!("missing {name}"))
                    .to_str()
                    .unwrap_or_else(|_| panic!("invalid {name}"))
            };
            Self {
                query_id: header(TASK_KEY_QUERY_ID_HEADER).to_string(),
                stage_id: header(TASK_KEY_STAGE_ID_HEADER)
                    .parse()
                    .expect("stage id must be a u64"),
                task_number: header(TASK_KEY_TASK_NUMBER_HEADER)
                    .parse()
                    .expect("task number must be a u64"),
            }
        }
    }

    #[derive(Clone)]
    struct ChaosState {
        seed: u64,
        attempts: Arc<Mutex<HashMap<ChaosTaskKey, usize>>>,
    }

    impl ChaosState {
        fn new(seed: u64) -> Self {
            Self {
                seed,
                attempts: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn action(&self, task_key: ChaosTaskKey) -> ChaosAction {
            let attempt = {
                let mut attempts = self.attempts.lock().unwrap();
                let attempt = attempts.entry(task_key.clone()).or_default();
                let current = *attempt;
                *attempt += 1;
                current
            };
            ChaosAction::for_task(self.seed, &task_key, attempt)
        }
    }

    impl ChaosAction {
        fn for_task(seed: u64, task_key: &ChaosTaskKey, attempt: usize) -> Self {
            let mut task_seed = [0; 32];
            task_seed[..8].copy_from_slice(&seed.to_le_bytes());
            task_seed[8..16].copy_from_slice(&task_key.stage_id.to_le_bytes());
            task_seed[16..24].copy_from_slice(&task_key.task_number.to_le_bytes());

            let mut task_rng = StdRng::from_seed(task_seed);
            let num_errors = match task_rng.random::<u8>() {
                0..=4 => 2,
                5..=14 => 1,
                _ => 0,
            };

            task_seed[24..].copy_from_slice(&(attempt as u64).to_le_bytes());
            let mut attempt_rng = StdRng::from_seed(task_seed);
            let duration = Duration::from_millis(10 + u64::from(attempt_rng.random::<u8>() % 90));
            if attempt < num_errors {
                if attempt_rng.random() {
                    Self::Timeout(duration)
                } else {
                    Self::Unavailable(duration)
                }
            } else if attempt_rng.random::<u8>() <= 4 {
                Self::Delay(duration)
            } else {
                Self::Pass
            }
        }
    }

    #[test]
    fn chaos_actions_repeat_per_task_independent_of_request_order() {
        let task_a = ChaosTaskKey {
            query_id: "query-a".to_string(),
            stage_id: 7,
            task_number: 3,
        };
        let task_b = ChaosTaskKey {
            query_id: "query-b".to_string(),
            stage_id: 7,
            task_number: 4,
        };

        let first = ChaosState::new(42);
        let first_a = first.action(task_a.clone());
        let first_b = first.action(task_b.clone());
        let second_a = first.action(task_a.clone());

        let second = ChaosState::new(42);
        let second_b = second.action(task_b);
        let repeated_first_a = second.action(task_a.clone());
        let repeated_second_a = second.action(task_a);

        assert_eq!(first_a, repeated_first_a);
        assert_eq!(second_a, repeated_second_a);
        assert_eq!(first_b, second_b);
    }

    async fn chaos_worker_session_builder(ctx: WorkerQueryContext) -> Result<SessionState> {
        Ok(ctx.builder.build())
    }

    struct ChaosClusterConfig {
        num_workers: usize,
        max_in_flight: usize,
        seed: u64,
    }

    async fn chaos_localhost_cluster(
        cfg: ChaosClusterConfig,
    ) -> Result<(SessionContext, JoinSet<()>, Vec<Worker>)> {
        let listeners = futures::future::try_join_all(
            (0..cfg.num_workers)
                .map(|_| TcpListener::bind("127.0.0.1:0"))
                .collect::<Vec<_>>(),
        )
        .await
        .expect("Failed to bind to address");

        let ports: Vec<u16> = listeners
            .iter()
            .map(|listener| {
                listener
                    .local_addr()
                    .expect("Failed to get local address")
                    .port()
            })
            .collect();

        let mut join_set = JoinSet::new();
        let mut workers = vec![];
        let chaos_state = ChaosState::new(cfg.seed);
        for listener in listeners {
            let worker = Worker::from_session_builder(chaos_worker_session_builder);
            workers.push(worker.clone());

            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let chaos_state = chaos_state.clone();
            let max_in_flight = cfg.max_in_flight;

            join_set.spawn(async move {
                Server::builder()
                    .layer(ChaosLayer::new(max_in_flight, chaos_state))
                    .add_service(worker.into_worker_server())
                    .serve_with_incoming(incoming)
                    .await
                    .unwrap();
            });
        }
        let first_worker_url = Url::parse(&format!("http://localhost:{}", ports[0])).unwrap();

        let worker_resolver = LocalHostWorkerResolver::new(ports.clone());
        let mut distributed_config = DistributedConfig::default();
        distributed_config.max_coordinator_channel_retries = 5;
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_distributed_option_extension(distributed_config)
            .with_distributed_planner()
            .with_distributed_route_task_handler(RandomRouteTaskHandler::new(Some(cfg.seed)))
            .with_distributed_dynamic_task_count(adaptive())?
            .with_distributed_local_worker_context(
                workers[0].to_local_worker_context(first_worker_url),
            )
            .with_distributed_worker_resolver(worker_resolver)
            .with_distributed_file_scan_config_bytes_per_partition(1)?
            .build();

        Ok((SessionContext::from(state), join_set, workers))
    }

    async fn assert_no_tasks_running_eventually(workers: &[Worker]) {
        let start = Instant::now();
        loop {
            let tasks_running =
                futures::future::join_all(workers.iter().map(Worker::tasks_running))
                    .await
                    .into_iter()
                    .sum::<usize>();
            if tasks_running == 0 {
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "Expected no retained task entries, but {tasks_running} are still running"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // OnceCell to ensure TPCH tables are generated only once for tests
    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    pub async fn ensure_tpch_data() -> std::path::PathBuf {
        let spec = tpch::TpchSpec {
            location: TESTDATA_ROOT.to_string(),
            scale_factor: TPCH_SCALE_FACTOR,
            layout: ParquetLayout {
                max_file_size: TPCH_MAX_FILE_SIZE,
                ..ParquetLayout::default()
            },
        };
        let data_dir = std::path::PathBuf::from(spec.dataset());
        INIT_TEST_TPCH_TABLES
            .get_or_init(|| async {
                tpch::generate_data(&spec)
                    .await
                    .expect("Failed to generate TPC-H data");
            })
            .await;
        data_dir
    }
}
