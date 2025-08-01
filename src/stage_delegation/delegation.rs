use super::StageContext;
use dashmap::{DashMap, Entry};
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::DataFusionError;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio::time;
use tokio::time::Instant;

/// In each stage of the distributed plan, there will be N workers. All these workers
/// need to coordinate to pull data from the next stage, which will contain M workers.
///
/// The way this is done is that for each stage, 1 worker is elected as "delegate", and
/// the rest of the workers are mere actors that wait for the delegate to tell them
/// where to go.
///
/// Each actor in a stage knows the url of the rest of the actors, so the delegate actor can
/// go one by one telling them what does the next stage look like. That way, all the actors
/// will agree on where to go to pull data from even if they are hosted in different physical
/// machines.
///
/// While starting a stage, several things can happen:
/// 1. The delegate can be very quick and choose the next stage context even before the other
///    actors have started waiting.
/// 2. The delegate can be very slow, and other actors might be waiting for the next context
///    info before the delegate even starting the choice of the next stage context.
///
/// On 1, the `add_delegate_info` call will create an entry in the [DashMap] with a
/// [oneshot::Receiver] already populated with the [StageContext], that other actors
/// are free to pick up at their own pace.
///
/// On 2, the `wait_for_delegate_info` call will create an entry in the [DashMap] with a
/// [oneshot::Sender], and listen on the other end of the channel [oneshot::Receiver] for
/// the delegate to put something there.
///
/// It's possible for [StageContext] to "get lost" if `add_delegate_info` is called without
/// a corresponding call to `wait_for_delegate_info` or vice versa. In this case, a task will
/// reap any contexts that live for longer than the `gc_ttl`.
pub struct StageDelegation {
    stage_targets: Arc<DashMap<(String, usize), Value>>,
    wait_timeout: Duration,

    /// notify is used to shut down the garbage collection task when the StageDelegation is dropped.
    notify: Arc<Notify>,
}

impl Default for StageDelegation {
    fn default() -> Self {
        let stage_targets = Arc::new(DashMap::default());
        let notify = Arc::new(Notify::new());

        let result = Self {
            stage_targets: stage_targets.clone(),
            wait_timeout: Duration::from_secs(5),
            notify: notify.clone(),
        };

        // Run the GC task.
        tokio::spawn(run_gc(
            stage_targets.clone(),
            notify.clone(),
            Duration::from_secs(30), /* gc period */
        ));

        result
    }
}

const GC_PERIOD_SECONDS: usize = 30;

// run_gc will continuously clear expired entries from the map, checking every `period`. The
// function terminates if `shutdown` is signalled.
async fn run_gc(
    stage_targets: Arc<DashMap<(String, usize), Value>>,
    shutdown: Arc<Notify>,
    period: Duration,
) {
    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                break;
            }
            _ = tokio::time::sleep(period) => {
               // Performance: This iterator is sharded, so it won't lock the whole map.
                stage_targets.retain(|_key, value| {
                  value.expiry.gt(&Instant::now())
                  });
            }
        }
    }
}

impl Drop for StageDelegation {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}

impl StageDelegation {
    /// Puts the [StageContext] info so that an actor can pick it up with `wait_for_delegate_info`.
    ///
    /// - If the actor was already waiting for this info, it just puts it on the
    ///   existing transmitter end.
    /// - If no actor was waiting for this info, build a new channel and store the receiving end
    ///   so that actor can pick it up when it is ready.
    pub fn add_delegate_info(
        &self,
        stage_id: String,
        actor_idx: usize,
        next_stage_context: StageContext,
    ) -> Result<(), DataFusionError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let tx = match self.stage_targets.entry((stage_id, actor_idx)) {
            Entry::Occupied(entry) => match entry.get().value {
                Oneof::Sender(_) => match entry.remove().value {
                    Oneof::Sender(tx) => tx,
                    Oneof::Receiver(_) => unreachable!(),
                },
                // This call is idempotent. If there's already a Receiver end here, it means that
                // add_delegate_info() for the same stage_id was already called once.
                Oneof::Receiver(_) => return Ok(()),
            },
            Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                entry.insert(Value {
                    expiry: Instant::now().add(self.gc_ttl()),
                    value: Oneof::Receiver(rx),
                });
                tx
            }
        };

        tx.send(next_stage_context)
            .map_err(|_| exec_datafusion_err!("Could not send stage context info"))
    }

    /// Waits for the [StageContext] info to be provided by the delegate and returns it.
    ///
    /// - If the delegate already put this info, consume it immediately and return it.
    /// - If the delegate did not put this info yet, create a new channel for the delegate to
    ///   store the info, and wait for that to happen, returning the info when it's ready.
    pub async fn wait_for_delegate_info(
        &self,
        stage_id: String,
        actor_idx: usize,
    ) -> Result<StageContext, DataFusionError> {
        let rx = match self.stage_targets.entry((stage_id.clone(), actor_idx)) {
            Entry::Occupied(entry) => match entry.get().value {
                Oneof::Sender(_) => {
                    return exec_err!(
                        "Programming error: while waiting for delegate info the entry in the \
                    StageDelegation target map cannot be a Sender"
                    )
                }
                Oneof::Receiver(_) => match entry.remove().value {
                    Oneof::Sender(_) => unreachable!(),
                    Oneof::Receiver(rx) => rx,
                },
            },
            Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                entry.insert(Value {
                    expiry: Instant::now().add(self.gc_ttl()),
                    value: Oneof::Sender(tx),
                });
                rx
            }
        };

        tokio::time::timeout(self.wait_timeout, rx)
            .await
            .map_err(|_| exec_datafusion_err!("Timeout waiting for delegate to post stage info for stage {stage_id} in actor {actor_idx}"))?
            .map_err(|err| {
                exec_datafusion_err!(
                    "Error waiting for delegate to tell us in which stage we are in: {err}"
                )
            })
    }

    // gc_ttl is used to set the expiry of elements in the map. Use 2 * the waiter wait duration
    // to avoid running gc too early.
    fn gc_ttl(&self) -> Duration {
        self.wait_timeout * 2
    }
}

struct Value {
    expiry: Instant,
    value: Oneof,
}

enum Oneof {
    Sender(oneshot::Sender<StageContext>),
    Receiver(oneshot::Receiver<StageContext>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage_delegation::StageContext;
    use futures::TryFutureExt;
    use std::sync::Arc;
    use uuid::Uuid;

    fn create_test_stage_context() -> StageContext {
        StageContext {
            id: Uuid::new_v4().to_string(),
            delegate: 0,
            prev_actors: 0,
            actors: vec![
                "http://localhost:8080".to_string(),
                "http://localhost:8081".to_string(),
            ],
            partitioning: Default::default(),
        }
    }

    #[tokio::test]
    async fn test_delegate_first_then_actor_waits() {
        let delegation = StageDelegation::default();
        let stage_id = Uuid::new_v4().to_string();
        let stage_context = create_test_stage_context();

        // Delegate adds info first
        delegation
            .add_delegate_info(stage_id.clone(), 0, stage_context.clone())
            .unwrap();

        // Actor waits for info (should get it immediately)
        let received_context = delegation
            .wait_for_delegate_info(stage_id, 0)
            .await
            .unwrap();
        assert_eq!(stage_context, received_context);

        // The stage target was cleaned up.
        assert_eq!(delegation.stage_targets.len(), 0);
    }

    #[tokio::test]
    async fn test_actor_waits_first_then_delegate_adds() {
        let delegation = Arc::new(StageDelegation::default());
        let stage_id = Uuid::new_v4().to_string();
        let stage_context = create_test_stage_context();

        // Spawn a task that waits for delegate info
        let delegation_clone = Arc::clone(&delegation);
        let id = stage_id.clone();
        let wait_task =
            tokio::spawn(async move { delegation_clone.wait_for_delegate_info(id, 0).await });

        // Give the wait task a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Delegate adds info
        delegation
            .add_delegate_info(stage_id, 0, stage_context.clone())
            .unwrap();

        // Wait task should complete with the stage context
        let received_context = wait_task.await.unwrap().unwrap();
        assert_eq!(stage_context, received_context);

        // The stage target was cleaned up.
        assert_eq!(delegation.stage_targets.len(), 0);
    }

    #[tokio::test]
    async fn test_multiple_actors_waiting_for_same_stage() {
        let delegation = Arc::new(StageDelegation::default());
        let stage_id = Uuid::new_v4().to_string();
        let stage_context = create_test_stage_context();

        // First actor waits
        let delegation_clone1 = Arc::clone(&delegation);
        let id = stage_id.clone();
        let wait_task1 =
            tokio::spawn(async move { delegation_clone1.wait_for_delegate_info(id, 0).await });

        // Give the first wait task a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Second actor tries to wait for the same stage - this should fail gracefully
        // since there can only be one waiting receiver per stage
        let result = delegation.wait_for_delegate_info(stage_id.clone(), 0).await;
        assert!(result.is_err());

        // Delegate adds info - the first actor should receive it
        delegation
            .add_delegate_info(stage_id, 0, stage_context.clone())
            .unwrap();

        let received_context = wait_task1.await.unwrap().unwrap();
        assert_eq!(received_context.id, stage_context.id);
        assert_eq!(0, delegation.stage_targets.len())
    }

    #[tokio::test]
    async fn test_different_stages_concurrent() {
        let delegation = Arc::new(StageDelegation::default());
        let stage_id1 = Uuid::new_v4().to_string();
        let stage_id2 = Uuid::new_v4().to_string();
        let stage_context1 = create_test_stage_context();
        let stage_context2 = create_test_stage_context();

        // Both actors wait for different stages
        let delegation_clone1 = Arc::clone(&delegation);
        let delegation_clone2 = Arc::clone(&delegation);
        let id1 = stage_id1.clone();
        let id2 = stage_id2.clone();
        let wait_task1 =
            tokio::spawn(async move { delegation_clone1.wait_for_delegate_info(id1, 0).await });
        let wait_task2 =
            tokio::spawn(async move { delegation_clone2.wait_for_delegate_info(id2, 0).await });

        // Give wait tasks a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Delegates add info for both stages
        delegation
            .add_delegate_info(stage_id1, 0, stage_context1.clone())
            .unwrap();
        delegation
            .add_delegate_info(stage_id2, 0, stage_context2.clone())
            .unwrap();

        // Both should receive their respective contexts
        let received_context1 = wait_task1.await.unwrap().unwrap();
        let received_context2 = wait_task2.await.unwrap().unwrap();

        assert_eq!(received_context1.id, stage_context1.id.to_string());
        assert_eq!(received_context2.id, stage_context2.id.to_string());

        // The stage target was cleaned up.
        assert_eq!(delegation.stage_targets.len(), 0);
    }

    #[tokio::test]
    async fn test_add_delegate_info_twice_same_stage() {
        let delegation = StageDelegation::default();
        let stage_id = Uuid::new_v4().to_string();
        let stage_context = create_test_stage_context();

        // First add should succeed
        delegation
            .add_delegate_info(stage_id.clone(), 0, stage_context.clone())
            .unwrap();

        // Second add for same stage should succeed (idempotent)
        delegation
            .add_delegate_info(stage_id.clone(), 0, stage_context.clone())
            .unwrap();

        // Receiving should still work even if `add_delegate_info` was called two times
        let received_context = delegation
            .wait_for_delegate_info(stage_id, 0)
            .await
            .unwrap();
        assert_eq!(received_context, stage_context);
    }

    #[tokio::test]
    async fn test_waiter_timeout_and_gc_cleanup() {
        let stage_targets = Arc::new(DashMap::default());
        let shutdown = Arc::new(Notify::new());
        let delegation = StageDelegation {
            stage_targets: stage_targets.clone(),
            wait_timeout: Duration::from_millis(1),
            notify: shutdown.clone(),
        };
        let stage_id = Uuid::new_v4().to_string();

        // Actor waits but times out
        let result = delegation.wait_for_delegate_info(stage_id, 0).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Timeout"));

        // Wait for expiry time to pass.
        tokio::time::sleep(delegation.gc_ttl()).await;

        // Run GC to clean up expired entries
        let gc_task = tokio::spawn(run_gc(
            stage_targets.clone(),
            shutdown.clone(),
            Duration::from_millis(5),
        ));

        // Wait for GC to clear the map
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if stage_targets.len() == 0 {
                break;
            }
        }

        // Stop GC by dropping the delegation. Assert that it has shutdown.
        drop(delegation);
        gc_task.await.unwrap();

        // After GC, map should be cleared.
        assert_eq!(stage_targets.len(), 0);
    }

    #[tokio::test]
    async fn test_writer_only_and_gc_cleanup() {
        let stage_targets = Arc::new(DashMap::default());
        let shutdown = Arc::new(Notify::new());
        let delegation = StageDelegation {
            stage_targets: stage_targets.clone(),
            wait_timeout: Duration::from_millis(1),
            notify: shutdown.clone(),
        };
        let stage_id = Uuid::new_v4().to_string();
        let stage_context = create_test_stage_context();

        // Writer adds info without anyone waiting
        let result = delegation.add_delegate_info(stage_id, 0, stage_context);

        assert!(result.is_ok());

        // Entry should be in map
        assert_eq!(stage_targets.len(), 1);

        // Wait for expiry time to pass (gc_ttl is 2 * wait_timeout)
        tokio::time::sleep(delegation.gc_ttl()).await;

        // Run GC to cleanup expired entries
        let gc_task = tokio::spawn(run_gc(
            stage_targets.clone(),
            shutdown.clone(),
            Duration::from_millis(10),
        ));

        // Wait for GC to clear the map
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if stage_targets.len() == 0 {
                break;
            }
        }

        // Stop GC.
        drop(delegation);
        gc_task.await.unwrap();

        // After GC, map should be cleared
        assert_eq!(stage_targets.len(), 0);
    }
}
