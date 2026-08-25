use crate::TaskKey;
use crate::distributed_planner::NetworkBoundaryExt;
use datafusion::common::HashMap;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use tokio::sync::watch;

type StoreMap<T> = HashMap<TaskKey, T>;

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

    pub(crate) fn insert(&self, key: TaskKey, value: T) {
        self.tx.send_modify(|map| {
            map.insert(key, value);
        });
    }

    pub(crate) fn get(&self, key: &TaskKey) -> Option<T>
    where
        T: Clone,
    {
        self.rx.borrow().get(key).cloned()
    }

    pub(crate) async fn wait_for(&self, expected_keys: &[TaskKey]) -> StoreMap<T>
    where
        T: Clone,
    {
        let mut rx = self.rx.clone();
        if !expected_keys.is_empty() {
            let _ = rx
                .wait_for(|map| expected_keys.iter().all(|key| map.contains_key(key)))
                .await;
        }
        rx.borrow().clone()
    }

    #[cfg(test)]
    pub(crate) fn from_entries(entries: impl IntoIterator<Item = (TaskKey, T)>) -> Self {
        let map: HashMap<_, _> = entries.into_iter().collect();
        let (tx, rx) = watch::channel(map);
        Self { tx, rx }
    }
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
