/*
TTLMap is a Moka-based cache that automatically removes entries after a specified time-to-live (TTL).

Usage
```rust
let params = TTLMapParams { tick: Duration::from_secs(30), ttl: Duration::from_mins(5) };
let ttl_map = TTLMap::new(params).await.unwrap();
let value = ttl_map.get_or_init(key, || initial_value).await;
```
 */
use datafusion::error::DataFusionError;
use moka::future::Cache;
use std::hash::Hash;
use std::time::Duration;

// TTLMap is a key-value store that automatically removes entries after a specified time-to-live.
pub struct TTLMap<K, V> {
    /// The Moka cache with TTL functionality
    cache: Cache<K, V>,
}

pub struct TTLMapParams {
    // tick is ignored when using Moka (kept for API compatibility)
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
        let cache = Cache::builder()
            .time_to_live(params.ttl)
            .build();
        Ok(Self { cache })
    }

    async fn _new(_tick: Duration, ttl: Duration) -> Self {
        let cache = Cache::builder()
            .time_to_live(ttl)
            .build();
        Self { cache }
    }

    /// get_or_default executes the provided closure with a reference to the map entry for the given key.
    /// If the key does not exist, it inserts a new entry with the default value.
    pub async fn get_or_init<F>(&self, key: K, f: F) -> V
    where
        F: FnOnce() -> V,
    {
        self.cache.get_with(key, async move { f() }).await
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
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
    async fn test_moka_cache_created() {
        let ttl_map =
            TTLMap::<String, i32>::_new(Duration::from_millis(100), Duration::from_secs(1)).await;

        // Verify that the cache is properly initialized
        assert_eq!(ttl_map.cache.entry_count(), 0);
    }

    #[tokio::test]
    async fn test_gc_expiration() {
        let ttl_map =
            TTLMap::<String, i32>::_new(Duration::from_millis(50), Duration::from_millis(100)).await;

        // Initial batch of entries
        ttl_map.get_or_init("key1".to_string(), || 42).await;
        ttl_map.get_or_init("key2".to_string(), || 84).await;
        
        // Verify entries exist by checking values
        let value1 = ttl_map.get_or_init("key1".to_string(), || 999).await;
        assert_eq!(value1, 42);
        let value2 = ttl_map.get_or_init("key2".to_string(), || 999).await;
        assert_eq!(value2, 84);

        // Add more entries
        ttl_map.get_or_init("key3".to_string(), || 168).await;
        ttl_map.get_or_init("key4".to_string(), || 0).await; // Default value (0)
        ttl_map.get_or_init("key5".to_string(), || 210).await;

        // Verify default value was set
        let default_value = ttl_map.get_or_init("key4".to_string(), || 999).await;
        assert_eq!(default_value, 0);

        // Wait for TTL to expire entries
        sleep(Duration::from_millis(150)).await;

        // Run maintenance to clean up expired entries
        ttl_map.cache.run_pending_tasks().await;

        // Verify entries are expired by trying to get with different default values
        let expired_value1 = ttl_map.get_or_init("key1".to_string(), || 777).await;
        assert_eq!(expired_value1, 777); // Should get new default value, not cached 42

        // Add new entries after expiration
        ttl_map.get_or_init("new_key1".to_string(), || 999).await;
        ttl_map.get_or_init("new_key2".to_string(), || 0).await; // Default value

        // Verify values
        let value1 = ttl_map.get_or_init("new_key1".to_string(), || 0).await;
        assert_eq!(value1, 999);
        let value2 = ttl_map.get_or_init("new_key2".to_string(), || 0).await;
        assert_eq!(value2, 0);
    }

    #[tokio::test]
    async fn test_concurrent_gc_and_access() {
        let ttl_map = TTLMap::<String, i32>::new(TTLMapParams {
            tick: Duration::from_millis(2),
            ttl: Duration::from_millis(10),
        })
        .await
        .unwrap();

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
    async fn test_basic_ttl_behavior() {
        let ttl_map = TTLMap::<String, i32>::_new(
            Duration::from_millis(10),
            Duration::from_millis(50),
        )
        .await;

        ttl_map.get_or_init("test_key".to_string(), || 999).await;
        
        // Verify entry exists
        let value = ttl_map.get_or_init("test_key".to_string(), || 111).await;
        assert_eq!(value, 999);

        // Wait for expiration and run maintenance
        sleep(Duration::from_millis(60)).await;
        ttl_map.cache.run_pending_tasks().await;

        // Entry should be expired - trying to get it should return new default
        let expired_value = ttl_map.get_or_init("test_key".to_string(), || 111).await;
        assert_eq!(expired_value, 111);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn bench_lock_contention() {
        use std::time::Instant;

        let ttl_map = TTLMap::<i32, i32>::new(TTLMapParams {
            tick: Duration::from_secs(1),
            ttl: Duration::from_secs(60),
        })
        .await
        .unwrap();

        let ttl_map = Arc::new(ttl_map);

        let start_time = Instant::now();
        let task_count = 100_000;

        // Spawn tasks that repeatedly access random keys
        let mut handles = Vec::new();
        for task_id in 0..task_count {
            let map = Arc::clone(&ttl_map);
            let handle = tokio::spawn(async move {
                let start = Instant::now();
                let _value = map.get_or_init(rand::random(), || task_id * 1000).await;
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

        println!("\n=== TTLMap Moka Benchmark ===");
        println!("Tasks: {}", task_count);
        println!("Total time: {:.2?}", elapsed);
        println!("Average latency: {:.2} μs per operation", avg_time / 1_000);
        println!("Throughput: {:.2} ops/sec", task_count as f64 / elapsed.as_secs_f64());
    }
}
