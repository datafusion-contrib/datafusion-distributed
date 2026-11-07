use crate::common::RetryOutcome;
use crate::{
    CoordinatorToWorkerDialer, DistributedConfig, LocalWorkerContext, NetworkBoundaryExt,
    RouteTaskEvent, RouteTaskEventResponse, RouteTaskHandler, Stage, TaskKey, ok_or_some_err,
};
use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashSet, Result, internal_err};
use datafusion::error::DataFusionError;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricBuilder;
use rand::prelude::{SliceRandom, StdRng};
use rand::{Rng, SeedableRng};
use std::time::Duration;
use tokio::time::sleep;
use url::Url;

/// Assigns a task to a random URL from the registered [WorkerResolver].
pub(crate) struct RandomRouteTaskHandler {
    seed: Option<u64>,
}

impl RandomRouteTaskHandler {
    pub(crate) fn new(seed: Option<u64>) -> Self {
        Self { seed }
    }

    fn retry_seed(&self, key: &TaskKey) -> Option<[u8; 32]> {
        self.seed.map(|_| routing_seed(key, self.seed, true))
    }
}

#[async_trait]
impl RouteTaskHandler for RandomRouteTaskHandler {
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        let urls = ok_or_some_err!(ev.worker_resolver.get_urls());
        let url = stage_contiguous_rand_offset(&ev.task_key, self.seed, &urls)?.clone();
        let d_cfg = ok_or_some_err!(DistributedConfig::from_task_context(ev.task_ctx));
        Some(
            dial_with_failover(
                ev.dialer,
                url,
                urls,
                self.retry_seed(&ev.task_key),
                ev.metrics,
                d_cfg,
            )
            .await,
        )
    }
}

async fn dial_with_failover(
    dialer: &dyn CoordinatorToWorkerDialer,
    url: Url,
    mut retry_urls: Vec<Url>,
    retry_seed: Option<[u8; 32]>,
    metrics: &ExecutionPlanMetricsSet,
    d_cfg: &DistributedConfig,
) -> Result<RouteTaskEventResponse> {
    let max_retries = d_cfg.max_coordinator_channel_retries;
    let initial_backoff = Duration::from_millis(d_cfg.coordinator_channel_retry_initial_backoff_ms);
    let max_backoff = Duration::from_millis(d_cfg.coordinator_channel_retry_max_backoff_ms);

    let mut url = url;
    let mut attempted_urls = HashSet::from([url.clone()]);
    retry_urls.retain(|candidate| attempted_urls.insert(candidate.clone()));
    shuffle_retry_urls(&mut retry_urls, retry_seed);

    let mut coordinator_to_worker_same_url_retries = None;
    let mut coordinator_to_worker_other_url_retries = None;

    let mut errs = vec![];
    let mut same_url_retries = 0;
    for attempt in 0..max_retries.saturating_add(1) {
        let err = match dialer.dial(url.clone()).await {
            Ok(result) => return Ok(result),
            Err(err) => err,
        };

        match RetryOutcome::try_from_err(&err) {
            Some(RetryOutcome::SameUrl) => {
                coordinator_to_worker_same_url_retries
                    .get_or_insert_with(|| {
                        MetricBuilder::new(metrics)
                            .global_counter("coordinator_to_worker_same_url_retries")
                    })
                    .add(1);
                errs.push(err);
                if attempt < max_retries {
                    sleep(same_url_retry_backoff(
                        same_url_retries,
                        initial_backoff,
                        max_backoff,
                    ))
                    .await;
                    same_url_retries += 1;
                }
            }
            Some(RetryOutcome::OtherUrl) => {
                coordinator_to_worker_other_url_retries
                    .get_or_insert_with(|| {
                        MetricBuilder::new(metrics)
                            .global_counter("coordinator_to_worker_other_url_retries")
                    })
                    .add(1);
                same_url_retries = 0;
                errs.push(err);
                let Some(next_url) = retry_urls.pop() else {
                    break;
                };
                url = next_url;
            }
            None => {
                return Err(err);
            }
        }
    }
    match errs.len() {
        0 => internal_err!("No URLs available for coordinator-to-worker connection"),
        1 => Err(errs.swap_remove(0)),
        _ => Err(DataFusionError::Collection(errs)),
    }
}

fn shuffle_retry_urls<T>(urls: &mut [T], seed: Option<[u8; 32]>) {
    match seed {
        Some(seed) => urls.shuffle(&mut StdRng::from_seed(seed)),
        None => urls.shuffle(&mut rand::rng()),
    }
}

fn same_url_retry_backoff(
    retry: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
) -> Duration {
    let mut backoff = initial_backoff.min(max_backoff);
    for _ in 0..retry {
        backoff = backoff.saturating_mul(2).min(max_backoff);
        if backoff == max_backoff {
            break;
        }
    }
    backoff
}

/// Chooses an item using a random starting offset shared by every task in this stage.
/// Successive task numbers rotate through the list from that offset.
fn stage_contiguous_rand_offset<T>(
    key: &TaskKey,
    explicit_seed: Option<u64>,
    list: impl IntoIterator<Item = T>,
) -> Option<T> {
    let list = list.into_iter().collect::<Vec<_>>();
    if list.is_empty() {
        return None;
    }

    let mut rng = StdRng::from_seed(routing_seed(key, explicit_seed, false));
    let start = rng.random_range(0..list.len());
    let index = (start + key.task_number % list.len()) % list.len();
    list.into_iter().nth(index)
}

fn routing_seed(key: &TaskKey, explicit_seed: Option<u64>, include_task: bool) -> [u8; 32] {
    let mut seed = [0; 32];
    match explicit_seed {
        Some(explicit_seed) => seed[..8].copy_from_slice(&explicit_seed.to_le_bytes()),
        None => seed[..16].copy_from_slice(key.query_id.as_bytes()),
    }
    seed[16..24].copy_from_slice(&(key.stage_id as u64).to_le_bytes());
    if include_task {
        seed[24..].copy_from_slice(&(key.task_number as u64).to_le_bytes());
    }
    seed
}

/// If there's a single task, it co-locates it in the coordinator if it can also act as a worker.
pub(crate) struct SingleTaskCoordinatorRouteTaskHandler;

#[async_trait]
impl RouteTaskHandler for SingleTaskCoordinatorRouteTaskHandler {
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        if ev.task_count != 1 {
            return None;
        }

        let local_worker_context = ev
            .task_ctx
            .session_config()
            .get_extension::<LocalWorkerContext>()?;

        // This is co-locating the task in the coordinator, so there's no retries to be handled
        // here, as no remote connection will be established at any point.
        Some(ev.dialer.dial(local_worker_context.self_url.clone()).await)
    }
}

/// If there's a single task, it co-locates it one of the remote workers that is already handling
/// a child task, avoiding network transfers.
pub(crate) struct SingleTaskChildUrlRouteTaskHandler;

#[async_trait]
impl RouteTaskHandler for SingleTaskChildUrlRouteTaskHandler {
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        if ev.task_count != 1 {
            return None;
        }
        let mut single_stage_url = None;
        ev.task_specialized_plan
            .apply(|plan| {
                let Some(nb) = plan.as_network_boundary() else {
                    return Ok(TreeNodeRecursion::Continue);
                };

                if let Stage::Remote(remote) = nb.input_stage()
                    && remote.workers.len() == 1
                {
                    single_stage_url = Some(remote.workers[0].clone());
                    return Ok(TreeNodeRecursion::Stop);
                }

                Ok(TreeNodeRecursion::Jump)
            })
            .expect("Cannot fail");
        let single_stage_url = single_stage_url?;

        // This is just a best-effort optimization for co-locating a single-tasked stage within
        // one of the workers that is driving the stage below. If dialing fails, it returns None
        // and the following task-routing handlers will get to decide.
        ev.dialer.dial(single_stage_url).await.ok().map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn seeded_routing_repeats_worker_choices_across_queries() {
        let handler = RandomRouteTaskHandler::new(Some(42));
        let assignments = |query_id| {
            (0..4)
                .map(|task_number| {
                    let key = TaskKey {
                        query_id,
                        stage_id: 7,
                        task_number,
                    };
                    stage_contiguous_rand_offset(&key, handler.seed, 0..4).unwrap()
                })
                .collect::<Vec<_>>()
        };

        let first = assignments(Uuid::from_u128(1));
        let second = assignments(Uuid::from_u128(2));
        assert_eq!(first, second);

        let mut sorted = first;
        sorted.sort_unstable();
        assert_eq!(sorted, vec![0, 1, 2, 3]);
    }

    #[test]
    fn seeded_routing_repeats_failover_order_across_queries() {
        let handler = RandomRouteTaskHandler::new(Some(42));
        let key = |query_id| TaskKey {
            query_id,
            stage_id: 7,
            task_number: 3,
        };
        let mut first = vec![0, 1, 2, 3];
        let mut second = first.clone();

        shuffle_retry_urls(&mut first, handler.retry_seed(&key(Uuid::from_u128(1))));
        shuffle_retry_urls(&mut second, handler.retry_seed(&key(Uuid::from_u128(2))));

        assert_eq!(first, second);
    }
}
