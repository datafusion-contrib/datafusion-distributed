#[cfg(all(feature = "tpch", feature = "chaos-tests", test))]
mod tests {
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::common::runtime::JoinSet;
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::LocalHostWorkerResolver;
    use datafusion_distributed::{
        DistributedExt, SessionStateBuilderExt, Worker, WorkerQueryContext,
    };
    use datafusion_distributed_benchmarks::datasets::{register_tables, tpch};
    use moka::future::FutureExt;
    use rand::SeedableRng;
    use rand::prelude::{IndexedRandom, Rng, StdRng};
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::env;
    use std::fs;
    use std::future::Future;
    use std::ops::Range;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;
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
    const QUERIES: &[&str] = &[
        "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11", "q12", "q13", "q14",
        "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22",
    ];

    fn seed() -> u64 {
        env::var("CHAOS_SEED").map_or_else(
            |_| rand::random(),
            |seed| seed.parse().expect("CHAOS_SEED must be a u64"),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Still no good retrying mechanism that make this test pass"]
    async fn chaos() -> Result<()> {
        let seed = seed();
        println!("seed: {seed}");
        let mut rng = StdRng::seed_from_u64(seed);
        let cfg = ChaosClusterConfig {
            num_workers: NUM_WORKERS,
            max_in_flight: MAX_IN_FLIGHT,
            seed,
        };
        let (ctx, _guard) = chaos_localhost_cluster(cfg).await;
        let data_dir = ensure_tpch_data().await;
        register_tables(&ctx, &data_dir).await?;

        let mut queries = VecDeque::new();

        for _ in 0..TOTAL_QUERIES {
            let mut batch = vec![];
            for _ in 0..rng.random_range(CONCURRENT_QUERIES_PER_CLIENT_RANDOM_RANGE) {
                let query = tpch::get_query(QUERIES.choose(&mut rng).unwrap())?;
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

        for result in concurrent_clients.join_all().await {
            result?;
        }

        let first = rx.recv().await.expect("No result returned");
        let first = pretty_format_batches(&first)?;
        while let Some(next_result) = rx.recv().await {
            let next_result = pretty_format_batches(&next_result)?;
            pretty_assertions::assert_eq!(first.to_string(), next_result.to_string());
        }

        Ok(())
    }

    #[derive(Clone)]
    struct ChaosLayer {
        in_flight: Arc<Semaphore>,
        rng: Arc<Mutex<StdRng>>,
    }

    impl ChaosLayer {
        fn new(max_in_flight: usize, seed: u64) -> Self {
            Self {
                in_flight: Arc::new(Semaphore::new(max_in_flight)),
                rng: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
            }
        }
    }

    impl<S> Layer<S> for ChaosLayer {
        type Service = ChaosService<S>;

        fn layer(&self, inner: S) -> Self::Service {
            ChaosService {
                inner,
                in_flight: self.in_flight.clone(),
                rng: self.rng.clone(),
            }
        }
    }

    #[derive(Clone)]
    struct ChaosService<S> {
        inner: S,
        in_flight: Arc<Semaphore>,
        rng: Arc<Mutex<StdRng>>,
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
            let permit = match self.in_flight.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    return Box::pin(async {
                        Ok(Status::resource_exhausted("chaos: worker overloaded").into_http())
                    });
                }
            };
            let action = ChaosAction::random(&mut *self.rng.lock().unwrap());
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
                        tokio::time::sleep(duration).await;
                        Ok(Status::deadline_exceeded("chaos: worker timed out").into_http())
                    }
                    ChaosAction::Unavailable(duration) => {
                        tokio::time::sleep(duration).await;
                        Ok(Status::unavailable("chaos: worker restarting").into_http())
                    }
                }
            })
        }
    }

    enum ChaosAction {
        Pass,
        Delay(Duration),
        Timeout(Duration),
        Unavailable(Duration),
    }

    impl ChaosAction {
        fn random(rng: &mut impl Rng) -> Self {
            let duration = Duration::from_millis(10 + u64::from(rng.random::<u8>() % 90));
            match rng.random::<u8>() {
                0..=4 => Self::Delay(duration),
                5..=7 => Self::Timeout(duration),
                8..=9 => Self::Unavailable(duration),
                _ => Self::Pass,
            }
        }
    }

    async fn chaos_worker_session_builder(ctx: WorkerQueryContext) -> Result<SessionState> {
        Ok(ctx.builder.build())
    }

    struct ChaosClusterConfig {
        num_workers: usize,
        max_in_flight: usize,
        seed: u64,
    }

    async fn chaos_localhost_cluster(cfg: ChaosClusterConfig) -> (SessionContext, JoinSet<()>) {
        let mut layer_rng = StdRng::seed_from_u64(cfg.seed);
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
        for listener in listeners {
            let layer_seed = layer_rng.random();
            let worker = Worker::from_session_builder(chaos_worker_session_builder);
            workers.push(worker.clone());

            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

            join_set.spawn(async move {
                Server::builder()
                    .layer(ChaosLayer::new(cfg.max_in_flight, layer_seed))
                    .add_service(worker.into_worker_server())
                    .serve_with_incoming(incoming)
                    .await
                    .unwrap();
            });
        }
        let first_worker_url = Url::parse(&format!("http://localhost:{}", ports[0])).unwrap();

        let worker_resolver = LocalHostWorkerResolver::new(ports.clone());
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_distributed_planner()
            .with_distributed_local_worker_context(
                workers[0].to_local_worker_context(first_worker_url),
            )
            .with_distributed_worker_resolver(worker_resolver)
            .with_distributed_file_scan_config_bytes_per_partition(1)
            .unwrap()
            .build();

        (SessionContext::from(state), join_set)
    }

    // OnceCell to ensure TPCH tables are generated only once for tests
    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    const TPCH_SCALE_FACTOR: f64 = 1.0;
    const TPCH_DATA_PARTS: usize = 16;

    pub async fn ensure_tpch_data() -> std::path::PathBuf {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("testdata/tpch/chaos_sf{TPCH_SCALE_FACTOR}"));
        INIT_TEST_TPCH_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap() {
                    tpch::generate_tpch_data(&data_dir, TPCH_SCALE_FACTOR, TPCH_DATA_PARTS)
                        .expect("Failed to generate TPC-H data");
                }
            })
            .await;
        data_dir
    }
}
