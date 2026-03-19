use url::Url;

use crate::worker::generated::worker::GetWorkerInfoRequest;

use super::ChannelResolver;

/// Checks whether a worker at the given URL reports the expected version by calling
/// the [`WorkerService::GetWorkerInfo`] RPC.
///
/// ```ignore
/// // In a background task, periodically filter worker URLs by version:
/// let channel_resolver = DefaultChannelResolver::default();
/// let compatible_urls: Arc<RwLock<Vec<Url>>> = /* shared with WorkerResolver */;
///
/// tokio::spawn(async move {
///     loop {
///         let mut filtered = vec![];
///         for url in &all_known_urls {
///             if worker_has_version(&channel_resolver, url, "1.0").await {
///                 filtered.push(url.clone());
///             }
///         }
///         *compatible_urls.write().unwrap() = filtered;
///         tokio::time::sleep(Duration::from_secs(5)).await;
///     }
/// });
/// ```
/// Returns `false` if the worker is unreachable, returns an error, or reports a
/// different version. This is intended to be used inside [`WorkerResolver`] implementations
/// for filtering out workers with incompatible versions during rolling deployments.
pub async fn worker_has_version(
    channel_resolver: &dyn ChannelResolver,
    url: &Url,
    expected_version: &str,
) -> bool {
    let Ok(mut client) = channel_resolver.get_worker_client_for_url(url).await else {
        return false;
    };

    let Ok(response) = client.get_worker_info(GetWorkerInfoRequest {}).await else {
        return false;
    };

    response.into_inner().version_number == expected_version
}
