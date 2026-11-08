// SPDX-License-Identifier: Apache-2.0

use super::SingleWriteMultiRead;
use super::worker_service::ResultTaskData;
use crate::TaskKey;
use datafusion::common::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) type TaskEntry = Arc<SingleWriteMultiRead<ResultTaskData>>;

#[derive(Clone, Default)]
pub(crate) struct ActiveTasks {
    entries: Arc<Mutex<HashMap<TaskKey, ActiveTask>>>,
}

struct ActiveTask {
    entry: TaskEntry,
    channels: usize,
}

impl ActiveTasks {
    #[cfg(any(test, feature = "grpc", feature = "integration"))]
    pub(crate) fn snapshot(&self) -> Vec<(TaskKey, TaskEntry)> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, task)| (*key, Arc::clone(&task.entry)))
            .collect()
    }

    pub(super) fn get(&self, key: &TaskKey) -> Option<TaskEntry> {
        self.entries
            .lock()
            .unwrap()
            .get(key)
            .map(|task| Arc::clone(&task.entry))
    }

    pub(super) fn register(&self, key: TaskKey, entry: TaskEntry) -> (TaskEntry, Registration) {
        let mut entries = self.entries.lock().unwrap();
        let task = entries.entry(key).or_insert_with(|| ActiveTask {
            entry: Arc::clone(&entry),
            channels: 0,
        });
        task.channels += 1;
        (
            Arc::clone(&task.entry),
            Registration {
                tasks: self.clone(),
                key,
            },
        )
    }
}

pub(super) struct Registration {
    tasks: ActiveTasks,
    key: TaskKey,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let removed = {
            let mut entries = self.tasks.entries.lock().unwrap();
            let Some(task) = entries.get_mut(&self.key) else {
                return;
            };
            task.channels -= 1;
            if task.channels == 0 {
                entries.remove(&self.key)
            } else {
                None
            }
        };
        drop(removed);
    }
}
