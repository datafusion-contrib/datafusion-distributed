use futures::Stream;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Wraps a stream and fires a callback when the stream is dropped.
///
/// The wrapped stream is dropped before the callback runs. This lets callbacks
/// observe state, such as metrics, that inner streams finalize in `Drop`.
///
/// This is useful for cleanup operations like releasing memory reservations,
/// cancelling background tasks, or logging when a stream consumer stops early.
///
/// # Example
/// ```ignore
/// let stream = on_drop_stream(inner_stream, || {
///     println!("Stream was dropped!");
/// });
/// ```
pub(crate) fn on_drop_stream<S, F>(inner: S, on_drop: F) -> OnDropStream<S, F>
where
    S: Stream,
    F: FnOnce(),
{
    OnDropStream {
        inner: OnDropInner::Live(inner),
        on_drop: Some(on_drop),
    }
}

/// A stream wrapper that fires a callback when dropped.
#[pin_project(PinnedDrop)]
pub(crate) struct OnDropStream<S, F: FnOnce()> {
    #[pin]
    inner: OnDropInner<S>,
    on_drop: Option<F>,
}

#[pin_project(project = OnDropInnerProj, project_replace = OnDropInnerProjOwn)]
enum OnDropInner<S> {
    Live(#[pin] S),
    Dropped,
}

impl<S, F> Stream for OnDropStream<S, F>
where
    S: Stream,
    F: FnOnce(),
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project().inner.project() {
            OnDropInnerProj::Live(inner) => inner.poll_next(cx),
            OnDropInnerProj::Dropped => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            OnDropInner::Live(inner) => inner.size_hint(),
            OnDropInner::Dropped => (0, Some(0)),
        }
    }
}

#[pin_project::pinned_drop]
impl<S, F: FnOnce()> PinnedDrop for OnDropStream<S, F> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        this.inner.project_replace(OnDropInner::Dropped);
        if let Some(on_drop) = this.on_drop.take() {
            on_drop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DropFlagStream {
        dropped: Arc<AtomicBool>,
    }

    impl Stream for DropFlagStream {
        type Item = ();

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(None)
        }
    }

    impl Drop for DropFlagStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn fires_on_drop_when_fully_consumed() {
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_clone = Arc::clone(&dropped);

        let stream = futures::stream::iter(vec![1, 2, 3]);
        let stream = on_drop_stream(stream, move || {
            dropped_clone.store(true, Ordering::SeqCst);
        });

        // Fully consume the stream
        let items: Vec<_> = stream.collect().await;
        assert_eq!(items, vec![1, 2, 3]);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn fires_on_drop_when_partially_consumed() {
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_clone = Arc::clone(&dropped);

        let stream = futures::stream::iter(vec![1, 2, 3, 4, 5]);
        let mut stream = on_drop_stream(stream, move || {
            dropped_clone.store(true, Ordering::SeqCst);
        });

        // Only consume part of the stream
        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, Some(2));
        assert!(!dropped.load(Ordering::SeqCst));

        // Drop the stream
        drop(stream);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn fires_on_drop_when_never_consumed() {
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_clone = Arc::clone(&dropped);

        let stream = futures::stream::iter(vec![1, 2, 3]);
        let stream = on_drop_stream(stream, move || {
            dropped_clone.store(true, Ordering::SeqCst);
        });

        assert!(!dropped.load(Ordering::SeqCst));
        drop(stream);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn fires_on_drop_after_inner_stream_is_dropped() {
        let inner_dropped = Arc::new(AtomicBool::new(false));
        let observed_inner_drop = Arc::new(AtomicBool::new(false));
        let observed_inner_drop_clone = Arc::clone(&observed_inner_drop);
        let inner_dropped_clone = Arc::clone(&inner_dropped);

        let stream = DropFlagStream {
            dropped: inner_dropped,
        };
        let stream = on_drop_stream(stream, move || {
            observed_inner_drop_clone
                .store(inner_dropped_clone.load(Ordering::SeqCst), Ordering::SeqCst);
        });

        drop(stream);

        assert!(observed_inner_drop.load(Ordering::SeqCst));
    }
}
