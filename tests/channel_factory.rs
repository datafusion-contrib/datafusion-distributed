#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::runtime::SpawnedTask;
    use datafusion::common::{
        DataFusionError, assert_contains, config_datafusion_err, exec_datafusion_err, exec_err,
    };
    use datafusion_distributed::grpc::{BoxCloneSyncChannel, DefaultChannelResolver};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        ChannelResolver, DistributedExt, GetWorkerInfoRequest, Worker, WorkerQueryContext,
        assert_snapshot,
    };
    use std::error::Error;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tonic::transport::{Channel, Server};
    use url::Url;

    #[tokio::test]
    async fn custom_channels_are_reused_across_distributed_queries() -> Result<(), Box<dyn Error>> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&attempts);
        let resolver = DefaultChannelResolver::with_channel_factory(move |url| {
            count.fetch_add(1, Ordering::SeqCst);
            connect(url)
        });
        let worker_resolver = resolver.clone();
        let (mut ctx, _guard, _) =
            start_localhost_context(3, move |worker_ctx: WorkerQueryContext| {
                let resolver = worker_resolver.clone();
                async move {
                    Ok(worker_ctx
                        .builder
                        .with_distributed_channel_resolver(resolver)
                        .build())
                }
            })
            .await;
        ctx.set_distributed_channel_resolver(resolver);
        register_parquet_tables(&ctx).await?;

        let query = r#"SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 20.0 ORDER BY "MinTemp" DESC"#;
        let mut results = vec![];
        for _ in 0..2 {
            let batches =
                tokio::time::timeout(Duration::from_secs(5), ctx.sql(query).await?.collect())
                    .await??;
            results.push(pretty_format_batches(&batches)?.to_string());
            // The helper executes one worker locally; the other two share cached channels.
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
        }
        assert_eq!(results[0], results[1]);
        assert_snapshot!(results[0], @r"
        +---------+-----------+
        | MinTemp | RainToday |
        +---------+-----------+
        | 20.9    | No        |
        +---------+-----------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn failed_factory_is_retried_then_cached() -> Result<(), Box<dyn Error>> {
        let (endpoint, _server) = worker().await?;
        let attempts = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&attempts);
        let resolver = DefaultChannelResolver::with_channel_factory(move |_| {
            let attempt = count.fetch_add(1, Ordering::SeqCst);
            let endpoint = endpoint.clone();
            async move {
                if attempt == 0 {
                    return exec_err!("injected channel construction failure");
                }
                connect(endpoint).await
            }
        });
        let route = Url::parse("http://logical-worker.invalid:1234")?;
        let error = info(&resolver, &route).await.unwrap_err();
        assert_contains!(error.to_string(), "injected channel construction failure");
        assert_eq!(info(&resolver, &route).await?, "custom-channel-test");
        assert_eq!(info(&resolver, &route).await?, "custom-channel-test");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_waiter_does_not_discard_shared_construction() -> Result<(), Box<dyn Error>> {
        let (endpoint, _server) = worker().await?;
        let attempts = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let count = Arc::clone(&attempts);
        let factory_started = Arc::clone(&started);
        let factory_release = Arc::clone(&release);
        let resolver = DefaultChannelResolver::with_channel_factory(move |_| {
            let endpoint = endpoint.clone();
            let started = Arc::clone(&factory_started);
            let release = Arc::clone(&factory_release);
            count.fetch_add(1, Ordering::SeqCst);
            async move {
                started.notify_one();
                release.notified().await;
                connect(endpoint).await
            }
        });
        let route = Url::parse("http://logical-worker.invalid:1234")?;
        let first_resolver = resolver.clone();
        let first_route = route.clone();
        let first = SpawnedTask::spawn(async move { info(&first_resolver, &first_route).await });
        tokio::time::timeout(Duration::from_secs(5), started.notified()).await?;
        drop(first);
        release.notify_one();
        assert_eq!(info(&resolver, &route).await?, "custom-channel-test");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn info(resolver: &DefaultChannelResolver, url: &Url) -> Result<String, DataFusionError> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut client = resolver.get_worker_client_for_url(url).await?;
            Ok(client
                .get_worker_info(GetWorkerInfoRequest {})
                .await?
                .version)
        })
        .await
        .map_err(|error| exec_datafusion_err!("Worker request timed out: {error}"))?
    }

    async fn connect(url: Url) -> Result<BoxCloneSyncChannel, DataFusionError> {
        let endpoint = Channel::from_shared(url.to_string())
            .map_err(|error| config_datafusion_err!("Invalid test endpoint: {error}"))?
            .buffer_size(1);
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| exec_datafusion_err!("Test connection failed: {error}"))?;
        Ok(BoxCloneSyncChannel::new(channel))
    }

    async fn worker() -> Result<(Url, SpawnedTask<()>), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = Url::parse(&format!("http://{}", listener.local_addr()?))?;
        let task = SpawnedTask::spawn(async move {
            Server::builder()
                .add_service(
                    Worker::default()
                        .with_version("custom-channel-test")
                        .into_worker_server(),
                )
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("test worker server failed");
        });
        Ok((url, task))
    }
}
