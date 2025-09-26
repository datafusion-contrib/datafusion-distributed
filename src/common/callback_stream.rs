use futures::Stream;
use pin_project::{pin_project, pinned_drop};
use std::fmt::Display;
use std::pin::Pin;
use std::task::{Context, Poll};

/// The reason why the stream ended:
/// - [CallbackStreamEndReason::Finished] if it finished gracefully
/// - [CallbackStreamEndReason::Aborted] if it was abandoned.
#[derive(Debug)]
pub enum CallbackStreamEndReason {
    /// The stream finished gracefully.
    Finished,
    /// The stream was abandoned.
    Aborted,
}

impl Display for CallbackStreamEndReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Stream that executes a callback when it is fully consumed or gets cancelled.
#[pin_project(PinnedDrop)]
pub struct CallbackStream<S, F>
where
    S: Stream,
    F: FnOnce(CallbackStreamEndReason),
{
    #[pin]
    stream: S,
    callback: Option<F>,
}

impl<S, F> Stream for CallbackStream<S, F>
where
    S: Stream,
    F: FnOnce(CallbackStreamEndReason),
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.stream.poll_next(cx) {
            Poll::Ready(None) => {
                // Stream is fully consumed, execute the callback
                if let Some(callback) = this.callback.take() {
                    callback(CallbackStreamEndReason::Finished);
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

#[pinned_drop]
impl<S, F> PinnedDrop for CallbackStream<S, F>
where
    S: Stream,
    F: FnOnce(CallbackStreamEndReason),
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        if let Some(callback) = this.callback.take() {
            callback(CallbackStreamEndReason::Aborted);
        }
    }
}

/// Wrap a stream with a callback that will be executed when the stream is fully
/// consumed or gets canceled.
pub fn with_callback<S, F>(stream: S, callback: F) -> CallbackStream<S, F>
where
    S: Stream,
    F: FnOnce(CallbackStreamEndReason) + Send + 'static,
{
    CallbackStream {
        stream,
        callback: Some(callback),
    }
}
