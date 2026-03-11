use datafusion::execution::TaskContext;
use std::sync::Arc;

/// Builds a new [TaskContext] by cloning the provided one and adding the config extension to it.
pub(crate) fn task_ctx_with_extension<T: Send + Sync + 'static>(
    ctx: &TaskContext,
    ext: T,
) -> TaskContext {
    TaskContext::new(
        ctx.task_id(),
        ctx.session_id(),
        ctx.session_config().clone().with_extension(Arc::new(ext)),
        ctx.scalar_functions().clone(),
        ctx.aggregate_functions().clone(),
        ctx.window_functions().clone(),
        ctx.runtime_env(),
    )
}
