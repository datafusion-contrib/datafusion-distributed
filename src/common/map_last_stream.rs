use futures::{Stream, StreamExt, stream};
use std::task::Poll;

/// Maps the last element of the provided stream.
pub(crate) fn map_last_stream<T>(
    mut input: impl Stream<Item = T> + Unpin,
    map_f: impl FnOnce(T) -> T,
) -> impl Stream<Item = T> + Unpin {
    let mut final_closure = Some(map_f);

    // this is used to peek the new value so that we can map upon emitting the last message
    let mut current_value = None;

    stream::poll_fn(move |cx| match futures::ready!(input.poll_next_unpin(cx)) {
        Some(new_val) => {
            match current_value.take() {
                // This is the first value, so we store it and repoll to get the next value
                None => {
                    current_value = Some(new_val);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }

                Some(existing) => {
                    current_value = Some(new_val);

                    Poll::Ready(Some(existing))
                }
            }
        }
        // this is our last value, so we map it using the user provided closure
        None => match current_value.take() {
            Some(existing) => {
                // make sure we wake ourselves to finish the stream
                cx.waker().wake_by_ref();

                if let Some(closure) = final_closure.take() {
                    Poll::Ready(Some(closure(existing)))
                } else {
                    unreachable!("the closure is only executed once")
                }
            }
            None => Poll::Ready(None),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    #[tokio::test]
    async fn test_map_last_stream_empty_stream() {
        let input = stream::empty::<i32>();
        let mapped = map_last_stream(input, |x| x + 10);
        let result: Vec<i32> = mapped.collect().await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[tokio::test]
    async fn test_map_last_stream_single_element() {
        let input = stream::iter(vec![5]);
        let mapped = map_last_stream(input, |x| x * 2);
        let result: Vec<i32> = mapped.collect().await;
        assert_eq!(result, vec![10]);
    }

    #[tokio::test]
    async fn test_map_last_stream_multiple_elements() {
        let input = stream::iter(vec![1, 2, 3, 4]);
        let mapped = map_last_stream(input, |x| x + 100);
        let result: Vec<i32> = mapped.collect().await;
        assert_eq!(result, vec![1, 2, 3, 104]); // Only the last element is transformed
    }

    #[tokio::test]
    async fn test_map_last_stream_preserves_order() {
        let input = stream::iter(vec![10, 20, 30, 40, 50]);
        let mapped = map_last_stream(input, |x| x - 50);
        let result: Vec<i32> = mapped.collect().await;
        assert_eq!(result, vec![10, 20, 30, 40, 0]); // Last element: 50 - 50 = 0
    }
}
