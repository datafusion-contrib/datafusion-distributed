use crate::protocol::in_process::InProcessWorkerClient;
use crate::{Worker, WorkerChannel};
use datafusion::execution::TaskContext;
use std::sync::Arc;
use url::Url;

/// Context injected to the [TaskContext] extensions that provides information about the presence
/// of a [Worker] locally.
///
/// This information can be used for executing tasks locally bypassing remote comms if the tasks
/// that needs to be remotely executed happens to be owned by this same worker.
pub struct LocalWorkerContext {
    /// The registry of in-flight tasks the [Worker] in the current scope owns.
    pub(crate) local_worker: Worker,
    /// The URL of the [Worker] in scope. When trying to reach to a target URL that happens
    /// to be the same as this one, local comms are preferred instead.
    pub(crate) self_url: Url,
}

impl LocalWorkerContext {
    pub(crate) fn from_ctx(ctx: &Arc<TaskContext>) -> Option<Arc<Self>> {
        ctx.session_config().get_extension::<Self>()
    }

    pub(crate) fn to_worker_channel(&self) -> Box<dyn WorkerChannel> {
        Box::new(InProcessWorkerClient::new(self.local_worker.clone()))
    }
}
