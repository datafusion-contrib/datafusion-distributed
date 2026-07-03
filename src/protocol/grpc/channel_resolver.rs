use crate::protocol::grpc::worker_client::create_worker_client;
use crate::{ChannelResolver, WorkerChannel};
use async_trait::async_trait;
use datafusion::common::{DataFusionError, config_datafusion_err, exec_datafusion_err};
use futures::FutureExt;
use futures::future::Shared;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tonic::body::Body;
use tonic::codegen::BoxFuture;
use tonic::transport::Channel;
use tower::ServiceExt;
use url::Url;

// Unlike TaskContext, a DataFusion RuntimeEnv does not allow to introduce user-defined extensions.
// For the default implementation of the ChannelResolvers, we cannot inject one DefaultChannelResolver
// per TaskContext, as this holds reference to Tonic channels that must outlive a single TaskContext.
//
// The Tonic channels need to be established and reused under a whole RuntimeEnv scope, not a single
// TaskContext, which forces us to put the default implementation in a static global variable that
// stores and reuses tonic channels per RuntimeEnv's pointer address.
pub(crate) static DEFAULT_CHANNEL_RESOLVER_PER_RUNTIME: LazyLock<
    moka::sync::Cache<
        /* Arc<RuntimeEnv> pointer address */ usize,
        /* ChannelResolver that reuses built channels */ Arc<DefaultChannelResolver>,
    >,
> = LazyLock::new(|| moka::sync::Cache::builder().max_capacity(256).build());

pub type BoxCloneSyncChannel = tower::util::BoxCloneSyncService<
    http::Request<Body>,
    http::Response<Body>,
    tonic::transport::Error,
>;

type ChannelCacheValue = Shared<BoxFuture<BoxCloneSyncChannel, Arc<DataFusionError>>>;

/// Default implementation of a [ChannelResolver] that connects to the workers given the URL once
/// and stores the connection instance in a TTI cache.
///
/// Sane default over which other [ChannelResolver] can be built for better customization of the
/// [WorkerServiceClient]s.
#[derive(Clone)]
pub struct DefaultChannelResolver {
    cache: Arc<moka::sync::Cache<Url, ChannelCacheValue>>,
}

impl Default for DefaultChannelResolver {
    fn default() -> Self {
        Self {
            cache: Arc::new(
                moka::sync::Cache::builder()
                    // Use an unrealistic max capacity, just in case there is a logic error on the
                    // user part that produces an unreasonable amount of URLs.
                    .max_capacity(64556)
                    // If a channel has not been used in 5 mins, delete it.
                    .time_to_idle(Duration::from_secs(5 * 60))
                    .build(),
            ),
        }
    }
}

impl DefaultChannelResolver {
    /// Gets the cached [BoxCloneSyncChannel] for the given URL, or builds a new one.
    pub async fn get_channel(&self, url: &Url) -> Result<BoxCloneSyncChannel, DataFusionError> {
        let channel = self.cache.get_with_by_ref(url, move || {
            let url = url.to_string();
            async move {
                let endpoint = Channel::from_shared(url.clone()).map_err(|err| {
                    config_datafusion_err!(
                        "Invalid URL '{url}' returned by WorkerResolver implementation: {err}"
                    )
                })?;
                let mut channel = endpoint.connect().await.map_err(|err| {
                    DataFusionError::Context(
                        format!("{err:?}"),
                        Box::new(exec_datafusion_err!(
                            "Error connecting to Distributed DataFusion worker on '{url}': {err}"
                        )),
                    )
                })?;
                channel.ready().await.map_err(|err| {
                    DataFusionError::Context(
                        format!("{err:?}"),
                        Box::new(exec_datafusion_err!(
                            "Error waiting for Distributed DataFusion channel to be ready on '{url}': {err}"
                        )),
                    )
                })?;
                Ok(BoxCloneSyncChannel::new(channel))
            }
                .boxed()
                .shared()
        });

        channel.await.map_err(|err| {
            self.cache.invalidate(url);
            DataFusionError::Shared(err)
        })
    }
}

#[async_trait]
impl ChannelResolver for DefaultChannelResolver {
    async fn get_worker_client_for_url(
        &self,
        url: &Url,
    ) -> Result<Box<dyn WorkerChannel>, DataFusionError> {
        self.get_channel(url).await.map(create_worker_client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Worker;
    use datafusion::common::assert_contains;
    use datafusion::common::runtime::SpawnedTask;
    use std::error::Error;
    use std::time::Instant;
    use tokio::net::TcpListener;
    use tonic::transport::Server;

    #[tokio::test]
    async fn fails_establishing_connection() -> Result<(), Box<dyn Error>> {
        let (url, _guard) = spawn_http_localhost_worker().await?;
        drop(_guard);
        let channel_resolver = DefaultChannelResolver::default();
        let err = channel_resolver.get_channel(&url).await.unwrap_err();
        assert_contains!(err.to_string(), "tcp connect error");
        Ok(())
    }

    #[tokio::test]
    async fn can_establish_connection() -> Result<(), Box<dyn Error>> {
        let (url, _guard) = spawn_http_localhost_worker().await?;
        let channel_resolver = DefaultChannelResolver::default();
        channel_resolver.get_channel(&url).await?;
        Ok(())
    }

    #[tokio::test]
    async fn channel_resolve_is_cached() -> Result<(), Box<dyn Error>> {
        let (url, _guard) = spawn_http_localhost_worker().await?;
        let channel_resolver = DefaultChannelResolver::default();

        let start = Instant::now();
        channel_resolver.get_channel(&url).await?;
        let first_call = start.elapsed();

        let start = Instant::now();
        channel_resolver.get_channel(&url).await?;
        let second_call = start.elapsed();

        assert!(first_call > second_call);
        Ok(())
    }

    async fn spawn_http_localhost_worker() -> Result<(Url, SpawnedTask<()>), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;

        let port = listener
            .local_addr()
            .expect("Failed to get local address")
            .port();

        let task = SpawnedTask::spawn(async {
            let worker = Worker::default();
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            if let Err(err) = Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(incoming)
                .await
            {
                panic!("{err}")
            }
        });

        Ok((Url::parse(&format!("http://127.0.0.1:{port}"))?, task))
    }
}
