use crate::{
    LocalWorkerContext, NetworkBoundaryExt, RouteTaskEvent, RouteTaskEventResponse,
    RouteTaskHandler, Stage, TaskKey, ok_or_some_err,
};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

/// Assigns a task to a random URL from the registered [WorkerResolver].
pub(crate) struct RandomRouteTaskHandler;

#[async_trait]
impl RouteTaskHandler for RandomRouteTaskHandler {
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        let urls = ok_or_some_err!(ev.worker_resolver.get_urls());
        let url = stage_contiguous_rand_choice(&ev.task_key, &urls)?;

        Some(ev.dialer.dial(url.clone()).await)
    }
}

/// Chooses an item using a random starting offset shared by every task in this stage.
/// Successive task numbers rotate through the list from that offset.
fn stage_contiguous_rand_choice<T>(key: &TaskKey, list: impl IntoIterator<Item = T>) -> Option<T> {
    let list = list.into_iter().collect::<Vec<_>>();
    if list.is_empty() {
        return None;
    }

    let mut seed = [0; 32];
    seed[..16].copy_from_slice(key.query_id.as_bytes());
    seed[16..24].copy_from_slice(&(key.stage_id as u64).to_le_bytes());
    let mut rng = StdRng::from_seed(seed);
    let start = rng.random_range(0..list.len());
    let index = (start + key.task_number % list.len()) % list.len();
    list.into_iter().nth(index)
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

        Some(ev.dialer.dial(single_stage_url).await)
    }
}
