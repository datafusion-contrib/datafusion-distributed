use crate::{
    DistributedGetterExt, LocalWorkerContext, NetworkBoundaryExt, RouteTasksEvent,
    RouteTasksEventResponse, Stage, WorkerResolver,
};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use rand::Rng;

/// Randomly chooses `ev.task_count` urls from the registered URLs.
pub(crate) fn random_routing(ev: RouteTasksEvent) -> Option<Result<RouteTasksEventResponse>> {
    let worker_resolver = match ev
        .task_ctx
        .session_config()
        .get_distributed_worker_resolver()
    {
        Ok(r) => r,
        Err(err) => return Some(Err(err)),
    };

    let available_urls = match worker_resolver.get_urls() {
        Ok(urls) if !urls.is_empty() => urls,
        Ok(_) => return Some(exec_err!("0 URLs available during routing")),
        Err(err) => return Some(Err(err)),
    };

    let start_idx = rand::rng().random_range(0..available_urls.len());
    let urls = (0..ev.task_count)
        .map(|i| available_urls[(start_idx + i) % available_urls.len()].clone())
        .collect::<Vec<_>>();

    Some(Ok(RouteTasksEventResponse::new(urls)))
}

/// If there's a single task, it co-locates it in the coordinator if it can also act as a worker.
pub(crate) fn single_task_coordinator_routing(
    ev: RouteTasksEvent,
) -> Option<Result<RouteTasksEventResponse>> {
    if ev.task_count != 1 {
        return None;
    }
    ev.task_ctx
        .session_config()
        .get_extension::<LocalWorkerContext>()
        .map(|v| Ok(RouteTasksEventResponse::new(vec![v.self_url.clone()])))
}

/// If there's a single task, it co-locates it one of the remote workers that is already handling
/// a child task, avoiding network transfers.
pub(crate) fn single_task_child_url_routing(
    ev: RouteTasksEvent,
) -> Option<Result<RouteTasksEventResponse>> {
    if ev.task_count != 1 {
        return None;
    }
    let mut single_stage_url = None;
    ev.plan
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

    single_stage_url.map(|url| Ok(RouteTasksEventResponse::new(vec![url])))
}
