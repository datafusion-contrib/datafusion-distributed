//! TTLMap is a DashMap that automatically removes entries after a specified time-to-live (TTL).
//!
//! How the Time Wheel Works
//!
//! Time Buckets:  [0] [1] [2] [3] [4] [5] [6] [7] ...
//! Current Time:           ^
//!                         |
//!                    time % buckets.len()
//!
//! When inserting key "A" at time=2:
//! - Key "A" goes into bucket[(2-1) % 8] = bucket[1]
//! - Key "A" will be expired when time advances to bucket[1] again
//!
//! Generally, keys in a bucket expire when the wheel makes a full rotation, making
//! the total TTL equal to the tick duration * buckets.len().
//!
//! Usage
//! ```rust,ignore
//! use std::time::Duration;
//! use datafusion_distributed::common::ttl_map::{TTLMapConfig, TTLMap};
//! let config = TTLMapConfig { tick: Duration::from_secs(5), ttl: Duration::from_secs(60) };
//! let ttl_map = TTLMap::try_new(config).unwrap();
//! let value = ttl_map.get_or_init("key", || "value");
//! ```
//! TODO(#101): If an existing entry is accessed, reset its TTL timer.
use dashmap::{DashMap, Entry};
use datafusion::error::DataFusionError;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// A bucket is a set of keys that are managed edited asynchronously.
#[derive(Clone)]
struct Bucket<K> {
    /// tx is used to send a BucketOp to the task
    tx: UnboundedSender<BucketOp<K>>,
    /// This task is responsible for managing keys
    _task: Arc<tokio::task::JoinHandle<()>>,
}

/// BucketOp is used to communicate with the task responsible for managing keys.
enum BucketOp<K> {
    Insert { key: K },
    Clear,
}

impl<K> Bucket<K>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
{
    /// new creates a new Bucket
    fn new<V>(data: Arc<DashMap<K, V>>) -> Self
    where
        V: Send + Sync + 'static,
    {
        // TODO: To avoid unbounded growth, consider making this bounded. Alternatively, we can
        // introduce a dynamic GC period to ensure that GC can keep up.
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            tx,
            _task: Arc::new(tokio::spawn(
                async move { Bucket::task(rx, data.clone()).await },
            )),
        }
    }

    fn register_key(&self, key: K) {
        // We can safely ignore the error here because the receiver is never closed.
        // If the receiver is dropped, it means this struct is being dropped.
        let _ = self.tx.send(BucketOp::Insert { key });
    }

    fn clear(&self) {
        // We can safely ignore the error here because the receiver is never closed.
        // If the receiver is dropped, it means this struct is being dropped.
        let _ = self.tx.send(BucketOp::Clear);
    }

    /// task is responsible for managing a subset of keys in the map.
    async fn task<V>(mut rx: UnboundedReceiver<BucketOp<K>>, data: Arc<DashMap<K, V>>)
    where
        V: Send + Sync + 'static,
    {
        let mut shard = HashSet::new();
        while let Some(op) = rx.recv().await {
            match op {
                BucketOp::Insert { key } => {
                    shard.insert(key);
                }
                BucketOp::Clear => {
                    let keys_to_delete = std::mem::take(&mut shard);
                    for key in keys_to_delete {
                        data.remove(&key);
                    }
                }
            }
        }
    }
}

/// TTLMap is a key-value store that automatically removes entries after a specified time-to-live.
pub struct TTLMap<K, V> {
    /// The buckets in the time wheel
    buckets: Vec<Bucket<K>>,

    /// gc_scheduler_task is responsible scheduling GC operations among the Buckets in the time wheel.
    gc_scheduler_task: Option<Vec<tokio::task::JoinHandle<()>>>,

    /// The actual key-value storage using DashMap for concurrent access
    data: Arc<DashMap<K, V>>,

    /// Atomic counter tracking the current time. Incremented by the background GC task every `tick` duration.
    time: Arc<AtomicU64>,

    config: TTLMapConfig,

    #[cfg(test)]
    metrics: TTLMapMetrics,
}

#[cfg(test)]
#[derive(Default)]
struct TTLMapMetrics {
    dash_map_lock_contention_time: AtomicUsize,
    ttl_accounting_time: AtomicUsize,
}

/// TTLMapConfig configures the TTLMap parameters such as the TTL and tick period.
pub struct TTLMapConfig {
    /// How often the map is checks for expired entries.
    /// This must be less than `ttl`. It's recommended to set `ttl` to a multiple
    /// of `tick`.
    pub tick: Duration,
    /// The time-to-live for entries
    pub ttl: Duration,
}

impl Default for TTLMapConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(3),
            ttl: Duration::from_secs(60),
        }
    }
}

impl TTLMapConfig {
    fn is_valid(&self, tick: Duration, ttl: Duration) -> Result<(), DataFusionError> {
        if tick > ttl && !tick.is_zero() {
            return Err(DataFusionError::Configuration(
                "`tick` must be nonzero and <= `ttl`".to_string(),
            ));
        }
        Ok(())
    }
}

impl<K, V> TTLMap<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Default + Clone + Send + Sync + 'static,
{
    // try_new creates a new TTLMap.
    pub fn try_new(config: TTLMapConfig) -> Result<Self, DataFusionError> {
        config.is_valid(config.tick, config.ttl)?;
        let mut map = Self::_new(config);
        map._start_gc();
        Ok(map)
    }

    fn _new(config: TTLMapConfig) -> Self {
        let stage_targets = Arc::new(DashMap::new());
        let bucket_count = (config.ttl.as_nanos() / config.tick.as_nanos()) as usize;
        let mut buckets = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            buckets.push(Bucket::new(stage_targets.clone()));
        }

        Self {
            buckets,
            data: stage_targets,
            time: Arc::new(AtomicU64::new(0)),
            gc_scheduler_task: None,
            config,
            #[cfg(test)]
            metrics: Default::default(),
        }
    }

    // Start and set the background GC task.
    fn _start_gc(&mut self) {
        let time = self.time.clone();
        let buckets = self.buckets.clone();
        let period = self.config.tick;

        let gc_task = tokio::spawn(async move {
            Self::run_gc_loop(time, period, &buckets).await;
        });

        self.gc_scheduler_task = Some(vec![gc_task]);
    }

    /// get_or_default executes the provided closure with a reference to the map entry for the given key.
    /// If the key does not exist, it inserts a new entry with the default value.
    pub fn get_or_init<F>(&self, key: K, init: F) -> V
    where
        F: FnOnce() -> V,
    {
        let mut new_entry = false;

        #[cfg(test)]
        let start = std::time::Instant::now();

        let value = match self.data.entry(key.clone()) {
            Entry::Vacant(entry) => {
                let value = init();
                entry.insert(value.clone());
                new_entry = true;
                value
            }
            Entry::Occupied(entry) => entry.get().clone(),
        };

        #[cfg(test)]
        self.metrics
            .dash_map_lock_contention_time
            .fetch_add(start.elapsed().as_nanos() as usize, Relaxed);

        // Insert the key into the previous bucket, meaning the key will be evicted
        // when the wheel completes a full rotation.
        if new_entry {
            #[cfg(test)]
            let start = std::time::Instant::now();

            let time = self.time.load(std::sync::atomic::Ordering::SeqCst);
            let bucket_index = (time.wrapping_sub(1)) % self.buckets.len() as u64;
            self.buckets[bucket_index as usize].register_key(key);

            #[cfg(test)]
            self.metrics
                .ttl_accounting_time
                .fetch_add(start.elapsed().as_nanos() as usize, Relaxed);
        }

        value
    }

    /// Removes the key from the map.
    /// TODO: Consider removing the key from the time bucket as well. We would need to know which
    /// bucket the key was in to do this. One idea is to store the bucket idx in the map value.
    pub fn remove(&self, key: K) {
        self.data.remove(&key);
    }

    /// Returns the number of entries currently stored in the map
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns an iterator over the keys currently stored in the map
    #[cfg(test)]
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.data.iter().map(|entry| entry.key().clone())
    }

    /// run_gc_loop will continuously clear expired entries from the map, checking every `period`. The
    /// function terminates if `shutdown` is signalled.
    async fn run_gc_loop(time: Arc<AtomicU64>, period: Duration, buckets: &[Bucket<K>]) {
        loop {
            tokio::time::sleep(period).await;
            Self::gc(time.clone(), buckets);
        }
    }

    fn gc(time: Arc<AtomicU64>, buckets: &[Bucket<K>]) {
        let index = time.load(std::sync::atomic::Ordering::SeqCst) % buckets.len() as u64;
        buckets[index as usize].clear();
        time.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_basic_insert_and_get() {
        let ttl_map = TTLMap::<String, i32>::_new(TTLMapConfig::default());

        ttl_map.get_or_init("key1".to_string(), || 42);

        let value = ttl_map.get_or_init("key1".to_string(), || 0);
        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn test_time_wheel_bucket_calculation() {
        let ttl_map = TTLMap::<String, i32>::_new(TTLMapConfig {
            ttl: Duration::from_secs(1),
            tick: Duration::from_millis(100),
        });

        // With 1s TTL and 100ms tick, we should have 10 buckets
        assert_eq!(ttl_map.buckets.len(), 10);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_gc_expiration() {
        // Create map with 10 buckets.
        let ttl_map = TTLMap::<String, i32>::_new(TTLMapConfig {
            ttl: Duration::from_secs(1),
            tick: Duration::from_millis(100),
        });

        // Initial batch of entries
        ttl_map.get_or_init("key1".to_string(), || 42);
        ttl_map.get_or_init("key2".to_string(), || 84);
        assert_eq!(ttl_map.data.len(), 2);

        // Run partial GC cycles (should not expire yet)
        for _ in 0..5 {
            TTLMap::<String, i32>::gc(ttl_map.time.clone(), &ttl_map.buckets);
        }
        assert_eventually(|| ttl_map.data.len() == 2, Duration::from_millis(100)).await; // Still there

        // Add more entries mid-cycle
        ttl_map.get_or_init("key3".to_string(), || 168);
        ttl_map.get_or_init("key4".to_string(), || 0); // Default value (0)
        ttl_map.get_or_init("key5".to_string(), || 210);
        assert_eq!(ttl_map.data.len(), 5);

        // Verify default value was set
        let default_value = ttl_map.get_or_init("key4".to_string(), || 0);
        assert_eq!(default_value, 0);

        // Complete the first rotation to expire initial entries
        for _ in 5..10 {
            TTLMap::<String, i32>::gc(ttl_map.time.clone(), &ttl_map.buckets);
        }

        assert_eventually(|| ttl_map.data.len() == 3, Duration::from_millis(100)).await; // Initial entries expired, new entries still alive

        // Add entries after expiration
        ttl_map.get_or_init("new_key1".to_string(), || 999);
        ttl_map.get_or_init("new_key2".to_string(), || 0); // Default value
        assert_eq!(ttl_map.data.len(), 5); // 3 from mid-cycle + 2 new ones

        // Verify values
        let value1 = ttl_map.get_or_init("new_key1".to_string(), || 0);
        assert_eq!(value1, 999);
        let value2 = ttl_map.get_or_init("new_key2".to_string(), || 0);
        assert_eq!(value2, 0);

        // Run additional GC cycles to expire remaining entries
        // Mid-cycle entries (bucket 4) expire at time=14, late entries (bucket 9) expire at time=19
        for _ in 10..20 {
            TTLMap::<String, i32>::gc(ttl_map.time.clone(), &ttl_map.buckets);
        }
        assert_eventually(|| ttl_map.data.is_empty(), Duration::from_millis(100)).await;
        // All entries expired
    }

    // assert_eventually checks a condition every 10ms for a maximum of timeout
    async fn assert_eventually<F>(assertion: F, timeout: Duration)
    where
        F: Fn() -> bool,
    {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if assertion() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("Assertion failed within {:?}", timeout);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore] // the test is flaky, uncomment once flakyness is solved
    async fn test_concurrent_gc_and_access() {
        let ttl_map = TTLMap::<String, i32>::try_new(TTLMapConfig {
            ttl: Duration::from_millis(10),
            tick: Duration::from_millis(2),
        })
        .unwrap();

        assert!(ttl_map.gc_scheduler_task.is_some());

        let ttl_map = Arc::new(ttl_map);

        // Spawn 5 concurrent tasks
        let mut handles = Vec::new();
        for task_id in 0..10 {
            let map = Arc::clone(&ttl_map);
            handles.push(tokio::spawn(async move {
                for i in 0..100 {
                    let key = format!("task{}_key{}", task_id, i % 10);
                    map.get_or_init(key.clone(), || task_id * 100 + i);
                }
            }));
            let map2 = Arc::clone(&ttl_map);
            handles.push(tokio::spawn(async move {
                // Remove some keys which may or may not exist.
                for i in 0..50 {
                    let key = format!("task{}_key{}", task_id, i % 15);
                    map2.remove(key)
                }
            }));
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        assert_eventually(|| ttl_map.data.is_empty(), Duration::from_millis(20)).await;
    }

    #[tokio::test]
    async fn test_wraparound_time() {
        let ttl_map = TTLMap::<String, i32>::_new(TTLMapConfig {
            ttl: Duration::from_millis(20),
            tick: Duration::from_millis(10),
        });

        // Manually set time near overflow
        ttl_map.time.store(u64::MAX - 2, Ordering::SeqCst);

        ttl_map.get_or_init("test_key".to_string(), || 999);

        // Run GC to cause time wraparound
        for _ in 0..5 {
            TTLMap::<String, i32>::gc(ttl_map.time.clone(), &ttl_map.buckets);
        }

        // Entry should be expired and time should have wrapped
        assert_eventually(|| ttl_map.data.is_empty(), Duration::from_millis(100)).await;
        let final_time = ttl_map.time.load(Ordering::SeqCst);
        assert!(final_time < 100);
    }

    // Run with `cargo test bench_lock_contention --release -- --nocapture` to see output.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn bench_lock_contention() {
        use std::time::Instant;

        let config = TTLMapConfig {
            tick: Duration::from_micros(1),
            ttl: Duration::from_micros(2),
        };

        let ttl_map = TTLMap::<i32, i32>::try_new(config).unwrap();

        let ttl_map = Arc::new(ttl_map);

        let start_time = Instant::now();
        let task_count = 100_000;

        // Spawn 10 tasks that repeatedly read the same keys
        let mut handles = Vec::new();
        for task_id in 0..task_count {
            let map = Arc::clone(&ttl_map);
            let handle = tokio::spawn(async move {
                // All tasks fight for the same keys - maximum contention
                let start = Instant::now();
                let _value = map.get_or_init(rand::random(), || task_id * 1000);
                start.elapsed().as_nanos()
            });
            handles.push(handle);
        }

        // Wait for all tasks and collect operation counts
        let mut avg_time = 0;
        for handle in handles {
            avg_time += handle.await.unwrap();
        }
        avg_time /= task_count as u128;

        let elapsed = start_time.elapsed();

        println!("\n=== TTLMap Lock Contention Benchmark ===");
        println!("Tasks: {}", task_count);
        println!("Total time: {:.2?}", elapsed);
        println!("Average latency: {:.2} ns per operation", avg_time);
        println!("Entries remaining: {}", ttl_map.data.len());
        println!(
            "DashMap Lock contention time: {}ms",
            ttl_map
                .metrics
                .dash_map_lock_contention_time
                .load(Ordering::SeqCst)
                / 1_000_000
        );
        println!(
            "Accounting time: {}ms",
            ttl_map.metrics.ttl_accounting_time.load(Ordering::SeqCst) / 1_000_000
        );
    }

    #[tokio::test]
    async fn test_remove_with_manual_gc() {
        let ttl_map = TTLMap::<String, i32>::_new(TTLMapConfig {
            ttl: Duration::from_millis(50),
            tick: Duration::from_millis(10),
        });

        ttl_map.get_or_init("key1".to_string(), || 100);
        ttl_map.get_or_init("key2".to_string(), || 200);
        ttl_map.get_or_init("key3".to_string(), || 300);
        assert_eq!(ttl_map.data.len(), 3);

        // Remove key2 and verify the others remain.
        ttl_map.remove("key2".to_string());
        assert_eq!(ttl_map.data.len(), 2);
        let val1 = ttl_map.get_or_init("key1".to_string(), || 999);
        assert_eq!(val1, 100);
        let val3 = ttl_map.get_or_init("key3".to_string(), || 999);
        assert_eq!(val3, 300);

        // key2 should be recreated with new value
        let val2 = ttl_map.get_or_init("key2".to_string(), || 999);
        assert_eq!(val2, 999); // New value since it was removed
        assert_eq!(ttl_map.data.len(), 3);
        let val3 = ttl_map.get_or_init("key2".to_string(), || 200);
        assert_eq!(val3, 999);

        // Remove key1 before GCing.
        ttl_map.remove("key1".to_string());

        // Run GC and verify the map is empty.
        for _ in 0..5 {
            TTLMap::<String, i32>::gc(ttl_map.time.clone(), &ttl_map.buckets);
        }
        assert_eventually(|| ttl_map.data.is_empty(), Duration::from_millis(100)).await;
    }
}
