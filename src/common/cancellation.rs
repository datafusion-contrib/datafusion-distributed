use datafusion::execution::TaskContext;
use tokio_util::sync::CancellationToken;

/// Per-execution cancellation token, attached to the [TaskContext] by `DistributedExec` when a
/// plan starts. One token per execution: not on the transport (a shared, process-wide instance
/// cannot own per-execution state) and not per connection (too granular).
#[derive(Clone)]
pub(crate) struct DistributedCancellationToken(pub(crate) CancellationToken);

/// Returns the per-execution [CancellationToken] attached to `ctx`, or a fresh never-cancelled one
/// if none is set (a context that did not come through `DistributedExec`). A transport's producers
/// and consumers watch this instead of the transport carrying a `cancellation()` method.
///
/// The token does not replace transport-level stream close. Close propagates hop by hop and
/// surfaces only when the next blocking operation fails; the token reaches every in-process
/// participant the moment the head stream drops, including ones whose channels were never opened
/// or never read.
///
/// Two caveats for watchers:
/// - The token fires on any drop of the head stream, including after normal exhaustion. Treat it
///   as teardown, not failure.
/// - It lives in a session-config extension, so it is process-local: it does not cross plan
///   serialization to remote workers. Out-of-process producers need their own teardown signal.
pub fn get_distributed_cancellation_token(ctx: &TaskContext) -> CancellationToken {
    ctx.session_config()
        .get_extension::<DistributedCancellationToken>()
        .map(|t| t.0.clone())
        .unwrap_or_default()
}
