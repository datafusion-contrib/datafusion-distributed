use url::Url;

use crate::worker::generated::worker::GetWorkerInfoRequest;

use super::ChannelResolver;

/// Checks whether a worker at the given URL reports the expected version by calling
/// the [`WorkerService::GetWorkerInfo`] RPC.
///
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
