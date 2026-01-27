use futures::Stream;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Wraps a stream and fires a callback when the stream is dropped.
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
        inner,
        on_drop: Some(on_drop),
    }
}

/// A stream wrapper that fires a callback when dropped.
#[pin_project(PinnedDrop)]
pub(crate) struct OnDropStream<S, F: FnOnce()> {
    #[pin]
    inner: S,
    on_drop: Option<F>,
}

impl<S, F> Stream for OnDropStream<S, F>
where
    S: Stream,
    F: FnOnce(),
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[pin_project::pinned_drop]
impl<S, F: FnOnce()> PinnedDrop for OnDropStream<S, F> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
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
}
