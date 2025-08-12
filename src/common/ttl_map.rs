/*
TTLMap is a DashMap that automatically removes entries after a specified time-to-live (TTL).

How the Time Wheel Works

Time Buckets:  [0] [1] [2] [3] [4] [5] [6] [7] ...
Current Time:           ^
                        |
                   time % buckets.len()

When inserting key "A" at time=2:
- Key "A" goes into bucket[(2-1) % 8] = bucket[1]
- Key "A" will be expired when time advances to bucket[1] again

Generally, keys in a bucket expire when the wheel makes a full rotation, making
the total TTL equal to the tick duration * buckets.len().

Usage
```rust
let params = TTLMapParams { tick: Duration::from_secs(30), ttl: Duration::from_mins(5) };
let ttl_map = TTLMap::new(params).await.unwrap();
let value = ttl_map.get_or_init(key, || initial_value).await;
```

TODO: If an existing entry is accessed, we don't extend its TTL. It's unclear if this is
necessary for any use cases. This functionality could be added if needed.
 */
use dashmap::{DashMap, Entry};
use datafusion::error::DataFusionError;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// TTLMap is a key-value store that automatically removes entries after a specified time-to-live.
pub struct TTLMap<K, V> {
    /// Time wheel buckets containing keys to be expired. Each bucket epresents
    /// a time slot. Keys in bucket[i] will be expired when time % buckets.len() == i
    buckets: Arc<Mutex<Vec<HashSet<K>>>>,

    /// The actual key-value storage using DashMap for concurrent access
    data: Arc<DashMap<K, V>>,

    /// Atomic counter tracking the current time slot for the time wheel.
    /// Incremented by the background GC task every `tick` duration.
    time: Arc<AtomicU64>,

    /// Background task handle for the garbage collection process.
    /// When dropped, the GC task is automatically aborted.
    _task: Option<tokio::task::JoinHandle<()>>,

    // grandularity of the time wheel. How often a bucket is cleared.
    tick: Duration,
}

pub struct TTLMapParams {
    // tick is how often the map is checks for expired entries
    // must be less than ttl
    pub tick: Duration,
    // ttl is the time-to-live for entries
    pub ttl: Duration,
}

impl Default for TTLMapParams {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(3),
            ttl: Duration::from_secs(60),
        }
    }
}

impl<K, V> TTLMap<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Default + Clone + Send + Sync + 'static,
{
    // new creates a new TTLMap.
    pub async fn new(params: TTLMapParams) -> Result<Self, DataFusionError> {
        if params.tick > params.ttl {
            return Err(DataFusionError::Configuration(
                "tick duration must be less than or equal to ttl duration".to_string(),
            ));
        }
        let mut map = Self::_new(params.tick, params.ttl).await;
        map._start_gc();
        Ok(map)
    }

    async fn _new(tick: Duration, ttl: Duration) -> Self {
        let bucket_count = (ttl.as_millis() / tick.as_millis()) as usize;
        let mut buckets = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            buckets.push(HashSet::new());
        }
        let stage_targets = Arc::new(DashMap::new());
        let time_wheel = Arc::new(Mutex::new(buckets));
        let time = Arc::new(AtomicU64::new(0));
        Self {
            buckets: time_wheel,
            data: stage_targets,
            time,
            _task: None,
            tick,
        }
    }

    // Start and set the background GC task.
    fn _start_gc(&mut self) {
        self._task = Some(tokio::spawn(Self::run_gc_loop(
            self.data.clone(),
            self.buckets.clone(),
            self.time.clone(),
            self.tick,
        )))
    }

    /// get_or_default executes the provided closure with a reference to the map entry for the given key.
    /// If the key does not exist, it inserts a new entry with the default value.
    pub async fn get_or_init<F>(&self, key: K, f: F) -> V
    where
        F: FnOnce() -> V,
    {
        let mut new_entry = false;
        let value = match self.data.entry(key.clone()) {
            Entry::Vacant(entry) => {
                let value = f();
                entry.insert(value.clone());
                new_entry = true;
                value
            }
            Entry::Occupied(entry) => entry.get().clone(),
        };

        // Insert the key into the previous bucket, meaning the key will be evicted
        // when the wheel completes a full rotation.
        if new_entry {
            let time = self.time.load(std::sync::atomic::Ordering::SeqCst);
            {
                let mut buckets = self.buckets.lock().await;
                let bucket_index = (time.wrapping_sub(1)) % buckets.len() as u64;
                buckets[bucket_index as usize].insert(key);
            }
        }

        value
    }

    /// run_gc_loop will continuously clear expired entries from the map, checking every `period`. The
    /// function terminates if `shutdown` is signalled.
    async fn run_gc_loop(
        map: Arc<DashMap<K, V>>,
        time_wheel: Arc<Mutex<Vec<HashSet<K>>>>,
        time: Arc<AtomicU64>,
        period: Duration,
    ) {
        loop {
            Self::gc(map.clone(), time_wheel.clone(), time.clone()).await;
            tokio::time::sleep(period).await;
        }
    }

    /// gc clears expired entries from the map and advances time by 1.
    async fn gc(
        map: Arc<DashMap<K, V>>,
        time_wheel: Arc<Mutex<Vec<HashSet<K>>>>,
        time: Arc<AtomicU64>,
    ) {
        let keys = {
            let mut guard = time_wheel.lock().await;
            let len = guard.len();
            let index = time.load(std::sync::atomic::Ordering::SeqCst) % len as u64;
            // Replace the HashSet at the index with an empty one and return the original
            std::mem::replace(&mut guard[index as usize], HashSet::new())
        };

        // Remove expired keys from the map.
        // TODO: it may be worth exploring if we can group keys by shard and do a batched
        // remove.
        for key in keys {
            map.remove(&key);
        }
        // May wrap.
        time.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_basic_insert_and_get() {
        let ttl_map =
            TTLMap::<String, i32>::_new(Duration::from_millis(100), Duration::from_secs(1)).await;

        ttl_map.get_or_init("key1".to_string(), || 42).await;

        let value = ttl_map.get_or_init("key1".to_string(), || 0).await;
        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn test_time_wheel_bucket_calculation() {
        let ttl_map =
            TTLMap::<String, i32>::_new(Duration::from_millis(100), Duration::from_secs(1)).await;

        // With 1s TTL and 100ms tick, we should have 10 buckets
        assert_eq!(ttl_map.buckets.lock().await.len(), 10);
    }

    #[tokio::test]
    async fn test_gc_expiration() {
        let ttl_map =
            TTLMap::<String, i32>::_new(Duration::from_millis(100), Duration::from_secs(1)).await;

        // Initial batch of entries
        ttl_map.get_or_init("key1".to_string(), || 42).await;
        ttl_map.get_or_init("key2".to_string(), || 84).await;
        assert_eq!(ttl_map.data.len(), 2);

        // Run partial GC cycles (should not expire yet)
        for _ in 0..5 {
            TTLMap::gc(
                ttl_map.data.clone(),
                ttl_map.buckets.clone(),
                ttl_map.time.clone(),
            )
            .await;
        }
        assert_eq!(ttl_map.data.len(), 2); // Still there

        // Add more entries mid-cycle
        ttl_map.get_or_init("key3".to_string(), || 168).await;
        ttl_map.get_or_init("key4".to_string(), || 0).await; // Default value (0)
        ttl_map.get_or_init("key5".to_string(), || 210).await;
        assert_eq!(ttl_map.data.len(), 5);

        // Verify default value was set
        let default_value = ttl_map.get_or_init("key4".to_string(), || 0).await;
        assert_eq!(default_value, 0);

        // Complete the first rotation to expire initial entries
        for _ in 5..10 {
            TTLMap::gc(
                ttl_map.data.clone(),
                ttl_map.buckets.clone(),
                ttl_map.time.clone(),
            )
            .await;
        }
        assert_eq!(ttl_map.data.len(), 3); // Initial entries expired, new entries still alive

        // Add entries after expiration
        ttl_map.get_or_init("new_key1".to_string(), || 999).await;
        ttl_map.get_or_init("new_key2".to_string(), || 0).await; // Default value
        assert_eq!(ttl_map.data.len(), 5); // 3 from mid-cycle + 2 new ones

        // Verify values
        let value1 = ttl_map.get_or_init("new_key1".to_string(), || 0).await;
        assert_eq!(value1, 999);
        let value2 = ttl_map.get_or_init("new_key2".to_string(), || 0).await;
        assert_eq!(value2, 0);

        // Run additional GC cycles to expire remaining entries
        // Mid-cycle entries (bucket 4) expire at time=14, late entries (bucket 9) expire at time=19
        for _ in 10..20 {
            TTLMap::gc(
                ttl_map.data.clone(),
                ttl_map.buckets.clone(),
                ttl_map.time.clone(),
            )
            .await;
        }
        assert_eq!(ttl_map.data.len(), 0); // All entries expired
    }

    #[tokio::test]
    async fn test_concurrent_gc_and_access() {
        let ttl_map = TTLMap::<String, i32>::new(TTLMapParams {
            tick: Duration::from_millis(2),
            ttl: Duration::from_millis(10),
        })
        .await
        .unwrap();

        assert!(ttl_map._task.is_some());

        let ttl_map = Arc::new(ttl_map);

        // Spawn 5 concurrent tasks
        let mut handles = Vec::new();
        for task_id in 0..5 {
            let map = Arc::clone(&ttl_map);
            let handle = tokio::spawn(async move {
                for i in 0..20 {
                    let key = format!("task{}_key{}", task_id, i % 4);
                    map.get_or_init(key, || task_id * 100 + i).await;
                    sleep(Duration::from_millis(1)).await;
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_wraparound_time() {
        let ttl_map = TTLMap::<String, i32>::_new(
            Duration::from_millis(10),
            Duration::from_millis(20), // 2 buckets
        )
        .await;

        // Manually set time near overflow
        ttl_map.time.store(u64::MAX - 2, Ordering::SeqCst);

        ttl_map.get_or_init("test_key".to_string(), || 999).await;

        // Run GC to cause time wraparound
        for _ in 0..5 {
            TTLMap::gc(
                ttl_map.data.clone(),
                ttl_map.buckets.clone(),
                ttl_map.time.clone(),
            )
            .await;
        }

        // Entry should be expired and time should have wrapped
        assert_eq!(ttl_map.data.len(), 0);
        let final_time = ttl_map.time.load(Ordering::SeqCst);
        assert!(final_time < 100);
    }
}
