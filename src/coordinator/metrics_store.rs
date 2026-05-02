use crate::TaskKey;
use datafusion::common::HashMap;
use tokio::sync::watch;

type MetricsMap = HashMap<TaskKey, Vec<crate::worker::generated::worker::MetricsSet>>;

/// Stores the metrics collected from all worker tasks, and notifies waiters when new entries arrive.
#[derive(Debug, Clone)]
pub struct MetricsStore {
    tx: watch::Sender<MetricsMap>,
    pub(crate) rx: watch::Receiver<MetricsMap>,
}

impl MetricsStore {
    pub(crate) fn new() -> Self {
        let (tx, rx) = watch::channel(HashMap::new());
        Self { tx, rx }
    }

    pub(crate) fn insert(
        &self,
        key: TaskKey,
        metrics: Vec<crate::worker::generated::worker::MetricsSet>,
    ) {
        self.tx.send_modify(|map| {
            map.insert(key, metrics);
        });
    }

    pub(crate) fn get(
        &self,
        key: &TaskKey,
    ) -> Option<Vec<crate::worker::generated::worker::MetricsSet>> {
        self.rx.borrow().get(key).cloned()
    }

    #[cfg(test)]
    pub(crate) fn from_entries(
        entries: impl IntoIterator<Item = (TaskKey, Vec<crate::worker::generated::worker::MetricsSet>)>,
    ) -> Self {
        let map: HashMap<_, _> = entries.into_iter().collect();
        let (tx, rx) = watch::channel(map);
        Self { tx, rx }
    }
}
