use crate::TaskKey;
use crate::distributed_planner::NetworkBoundaryExt;
use datafusion::common::HashMap;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use tokio::sync::watch;

/// Terminal state of a task-scoped entry in a [Store]. Tasks without an entry are still pending.
#[derive(Debug, Clone)]
enum TaskEntry<T> {
    /// The task reported a value.
    Reported(T),
    /// The task finished without reporting a value, for example because it was planned but never
    /// executed, or because its worker->coordinator stream ended abruptly.
    TerminalWithoutReport,
}

type StoreMap<T> = HashMap<TaskKey, TaskEntry<T>>;

/// Point-in-time task state for the expected tasks in a plan.
#[derive(Debug)]
pub(crate) struct StoreSnapshot<T> {
    pub(crate) reported: HashMap<TaskKey, T>,
    pub(crate) pending: Vec<TaskKey>,
    pub(crate) terminal_without_report: Vec<TaskKey>,
}

/// Stores task-scoped values and notifies waiters when entries change.
#[derive(Debug, Clone)]
pub(crate) struct Store<T> {
    tx: watch::Sender<StoreMap<T>>,
    rx: watch::Receiver<StoreMap<T>>,
}

impl<T> Store<T> {
    pub(crate) fn new() -> Self {
        let (tx, rx) = watch::channel(HashMap::new());
        Self { tx, rx }
    }

    /// Records the value reported by a task, making it terminal.
    pub(crate) fn insert(&self, key: TaskKey, value: T) {
        self.tx.send_modify(|map| {
            map.insert(key, TaskEntry::Reported(value));
        });
    }

    /// Marks a task as terminal without a reported value. Does nothing if the task already
    /// reported a value.
    pub(crate) fn mark_terminal(&self, key: TaskKey) {
        self.tx.send_if_modified(|map| {
            if map.contains_key(&key) {
                return false;
            }
            map.insert(key, TaskEntry::TerminalWithoutReport);
            true
        });
    }

    /// Waits until every expected task is terminal, and returns the values of the tasks that
    /// reported one. Tasks that became terminal without reporting are absent from the result.
    pub(crate) async fn wait_for(&self, expected_keys: &[TaskKey]) -> HashMap<TaskKey, T>
    where
        T: Clone,
    {
        self.wait_for_terminal(expected_keys).await.reported
    }

    pub(crate) async fn wait_for_terminal(&self, expected_keys: &[TaskKey]) -> StoreSnapshot<T>
    where
        T: Clone,
    {
        let mut rx = self.rx.clone();
        if !expected_keys.is_empty() {
            let _ = rx
                .wait_for(|map| expected_keys.iter().all(|key| map.contains_key(key)))
                .await;
        }
        snapshot(&rx.borrow(), expected_keys)
    }

    pub(crate) fn snapshot(&self, expected_keys: &[TaskKey]) -> StoreSnapshot<T>
    where
        T: Clone,
    {
        snapshot(&self.rx.borrow(), expected_keys)
    }
}

fn snapshot<T: Clone>(map: &StoreMap<T>, expected_keys: &[TaskKey]) -> StoreSnapshot<T> {
    let mut result = StoreSnapshot {
        // Keep every available report, even if it is not in the prepared plan's expected keys.
        // The expected keys determine completeness, not which reports are observable.
        reported: map
            .iter()
            .filter_map(|(key, entry)| match entry {
                TaskEntry::Reported(value) => Some((*key, value.clone())),
                TaskEntry::TerminalWithoutReport => None,
            })
            .collect(),
        pending: vec![],
        terminal_without_report: vec![],
    };
    for key in expected_keys {
        match map.get(key) {
            Some(TaskEntry::Reported(_)) => {}
            Some(TaskEntry::TerminalWithoutReport) => result.terminal_without_report.push(*key),
            None => result.pending.push(*key),
        }
    }
    result
}

pub(crate) fn task_keys_for_plan(plan: &Arc<dyn ExecutionPlan>) -> Vec<TaskKey> {
    let mut task_keys = Vec::new();
    let _ = plan.apply(|plan| {
        if let Some(boundary) = plan.as_network_boundary() {
            let stage = boundary.input_stage();
            for task_number in 0..stage.task_count() {
                task_keys.push(TaskKey {
                    query_id: stage.query_id(),
                    stage_id: stage.num(),
                    task_number,
                });
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    task_keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn task_key(query_id: Uuid, task_number: usize) -> TaskKey {
        TaskKey {
            query_id,
            stage_id: 1,
            task_number,
        }
    }

    #[test]
    fn mark_terminal_does_not_override_reported_value() {
        let query_id = Uuid::new_v4();
        let key = task_key(query_id, 0);
        let store = Store::new();
        store.insert(key, 10);
        store.mark_terminal(key);

        let snapshot = store.snapshot(&[key]);
        assert_eq!(snapshot.reported.get(&key), Some(&10));
        assert!(snapshot.pending.is_empty());
        assert!(snapshot.terminal_without_report.is_empty());
    }

    #[test]
    fn snapshot_is_non_blocking_and_distinguishes_pending_from_lost() {
        let query_id = Uuid::new_v4();
        let keys = [
            task_key(query_id, 0),
            task_key(query_id, 1),
            task_key(query_id, 2),
        ];
        let store = Store::new();
        store.insert(keys[2], 12);
        store.mark_terminal(keys[0]);

        let partial = store.snapshot(&keys);
        assert_eq!(partial.reported, HashMap::from([(keys[2], 12)]));
        assert_eq!(partial.pending, vec![keys[1]]);
        assert_eq!(partial.terminal_without_report, vec![keys[0]]);

        store.insert(keys[1], 11);
        let unplanned = task_key(query_id, 10);
        store.insert(unplanned, 110);
        let terminal = store.snapshot(&keys);
        assert!(terminal.pending.is_empty());
        assert_eq!(terminal.reported.len(), 3);
        assert_eq!(terminal.reported.get(&unplanned), Some(&110));
        assert_eq!(terminal.terminal_without_report, vec![keys[0]]);
    }
}
