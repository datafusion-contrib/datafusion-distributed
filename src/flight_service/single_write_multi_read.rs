use std::fmt;
use std::time::Duration;
use tokio::sync::watch;

/// Synchronization primitive that allows multiple readers to wait for one writer to write
/// a clonable piece of data.
///
/// - If the writer writes before anyone is reading, all subsequent readers will immediately
///   resolve to the written piece of data.
/// - If one or more readers try to read before something is written, then they will asynchronously
///   wait until a writer writes something.
pub struct SingleWriteMultiRead<T: Clone> {
    tx: watch::Sender<Option<T>>,
    rx: watch::Receiver<Option<T>>,
}

impl<T: Clone> Default for SingleWriteMultiRead<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) enum SingleWriteMultiReadError {
    AlreadyWritten,
    Timeout,
    NoValue,
}

impl fmt::Display for SingleWriteMultiReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyWritten => write!(f, "already written"),
            Self::Timeout => write!(f, "timed out waiting for value"),
            Self::NoValue => write!(f, "sender dropped without writing a value"),
        }
    }
}

impl std::error::Error for SingleWriteMultiReadError {}

impl<T: Clone> SingleWriteMultiRead<T> {
    pub(crate) fn new() -> Self {
        let (tx, rx) = watch::channel(None);
        Self { tx, rx }
    }

    /// Write the value. Only the first call is meaningful;
    /// subsequent calls overwrite silently.
    pub(crate) fn write(&self, item: T) -> Result<(), SingleWriteMultiReadError> {
        let mut already_written = false;
        self.tx.send_modify(|v| {
            if v.is_none() {
                *v = Some(item);
            } else {
                already_written = true;
            }
        });
        if already_written {
            return Err(SingleWriteMultiReadError::AlreadyWritten);
        }
        Ok(())
    }

    /// Reads the current value, if any, not waiting for it to be set by a writer.
    pub(crate) fn read_now(&self) -> Option<T> {
        self.rx.borrow().clone()
    }

    /// Await until the writer has placed a value.
    pub(crate) async fn read(
        &self,
        timeout_duration: Duration,
    ) -> Result<T, SingleWriteMultiReadError> {
        let mut rx = self.rx.clone();
        let result = tokio::time::timeout(timeout_duration, rx.wait_for(|v| v.is_some()))
            .await
            .map(|r| r.map(|guard| guard.clone()));
        match result {
            Ok(Ok(val)) => val.ok_or(SingleWriteMultiReadError::NoValue),
            Ok(Err(_)) => {
                // Sender dropped; return the last value if one was written
                rx.borrow()
                    .clone()
                    .ok_or(SingleWriteMultiReadError::NoValue)
            }
            Err(_) => Err(SingleWriteMultiReadError::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const TIMEOUT: Duration = Duration::from_secs(1);
    const SHORT_TIMEOUT: Duration = Duration::from_millis(50);

    #[tokio::test]
    async fn write_before_read() {
        let swmr = SingleWriteMultiRead::new();
        swmr.write(42).unwrap();
        assert_eq!(swmr.read(TIMEOUT).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn write_after_read() {
        let swmr = Arc::new(SingleWriteMultiRead::new());
        let handle = {
            let swmr = Arc::clone(&swmr);
            tokio::spawn(async move { swmr.read(TIMEOUT).await.unwrap() })
        };
        swmr.write(99).unwrap();
        assert_eq!(handle.await.unwrap(), 99);
    }

    #[tokio::test]
    async fn double_write() {
        let swmr = Arc::new(SingleWriteMultiRead::new());
        let handle = {
            let swmr = Arc::clone(&swmr);
            tokio::spawn(async move { swmr.read(TIMEOUT).await.unwrap() })
        };
        swmr.write(99).unwrap();
        swmr.write(99).unwrap_err();
        assert_eq!(handle.await.unwrap(), 99);
    }

    #[tokio::test]
    async fn read_times_out_when_no_write() {
        let swmr = SingleWriteMultiRead::<i32>::new();
        let err = swmr.read(SHORT_TIMEOUT).await.unwrap_err();
        assert!(matches!(err, SingleWriteMultiReadError::Timeout));
    }

    #[tokio::test]
    async fn read_after_write_with_many_concurrent_readers() {
        let swmr = Arc::new(SingleWriteMultiRead::new());
        swmr.write(55).unwrap();
        let mut handles = Vec::new();
        for _ in 0..5 {
            let swmr = Arc::clone(&swmr);
            handles.push(tokio::spawn(
                async move { swmr.read(TIMEOUT).await.unwrap() },
            ));
        }
        for handle in handles {
            assert_eq!(handle.await.unwrap(), 55);
        }
    }

    #[tokio::test]
    async fn write_after_read_multiple_readers() {
        let swmr = Arc::new(SingleWriteMultiRead::new());
        let mut handles = Vec::new();
        for _ in 0..10 {
            let swmr = Arc::clone(&swmr);
            handles.push(tokio::spawn(
                async move { swmr.read(TIMEOUT).await.unwrap() },
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        swmr.write(7).unwrap();
        for handle in handles {
            assert_eq!(handle.await.unwrap(), 7);
        }
    }
}
