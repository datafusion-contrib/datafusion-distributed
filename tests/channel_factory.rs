#![cfg(feature = "grpc")]

mod tests {
    use datafusion::common::runtime::SpawnedTask;
    use datafusion::common::{
        DataFusionError, assert_contains, config_datafusion_err, exec_datafusion_err, exec_err,
    };
    use datafusion_distributed::grpc::{BoxCloneSyncChannel, DefaultChannelResolver};
    use datafusion_distributed::{ChannelResolver, GetWorkerInfoRequest, Worker};
    use futures::future::join_all;
    use std::error::Error;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tonic::transport::{Channel, Server};
    use url::Url;

    #[tokio::test]
    async fn custom_channels_are_shared_across_requests_and_clones() -> Result<(), Box<dyn Error>> {
        let (endpoint, _server) = worker().await?;
        let attempts = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&attempts);
        let resolver = DefaultChannelResolver::with_channel_factory(move |url: Url| {
            assert_eq!(url.host_str(), Some("logical-worker.invalid"));
            count.fetch_add(1, Ordering::SeqCst);
            connect(endpoint.clone())
        });
        let route = Url::parse("http://logical-worker.invalid:1234")?;
        for result in join_all((0..8).map(|_| info(&resolver, &route))).await {
            assert_eq!(result?, "custom-channel-test");
        }
        assert_eq!(
            info(&resolver.clone(), &route).await?,
            "custom-channel-test"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        let mut another_route = route.clone();
        another_route.set_port(Some(5678)).unwrap();
        assert_eq!(
            info(&resolver, &another_route).await?,
            "custom-channel-test"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
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
