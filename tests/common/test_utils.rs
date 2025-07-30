//! Test utilities for integration and unit tests
//!
//! This module provides shared functionality for tests, including port allocation
//! to avoid hardcoding and race conditions between concurrent test runs.

use std::sync::Mutex;

/// Global port management for tests to avoid hardcoding and race conditions
static CURRENT_MAX_PORT: Mutex<u16> = Mutex::new(21000); // Starting port

/// Maximum ports needed per cluster (proxy + workers + buffer)
const MAX_CLUSTER_SIZE: u16 = 10;

/// Allocate the next available port range for a test cluster
///
/// Returns the base port for the cluster (proxy will use this, workers will use base_port + 1, base_port + 2, etc.)
/// Thread-safe and prevents race conditions between concurrent tests.
///
/// # Example
///
/// ```rust
/// use crate::common::test_utils::allocate_port_range;
///
/// let base_port = allocate_port_range();
/// println!("Proxy will use port: {}", base_port);
/// println!("Workers will use ports: {}-{}", base_port + 1, base_port + 5);
/// ```
pub fn allocate_port_range() -> u16 {
    let mut current_port = CURRENT_MAX_PORT.lock().unwrap();
    let base_port = *current_port;
    *current_port += MAX_CLUSTER_SIZE;
    println!(
        "🔌 Allocated port range: {}-{}",
        base_port,
        base_port + MAX_CLUSTER_SIZE - 1
    );
    base_port
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_port_allocation() {
        // Test that port allocation increments correctly
        // Note: We can't predict the exact starting port due to potential
        // interference from concurrent tests, so we test the relative increments

        let port1 = allocate_port_range();
        let port2 = allocate_port_range();
        let port3 = allocate_port_range();

        // Verify they're properly spaced by MAX_CLUSTER_SIZE
        assert_eq!(port2, port1 + MAX_CLUSTER_SIZE);
        assert_eq!(port3, port2 + MAX_CLUSTER_SIZE);

        // Verify they're all different
        assert_ne!(port1, port2);
        assert_ne!(port2, port3);
        assert_ne!(port1, port3);
    }

    #[test]
    fn test_concurrent_port_allocation() {
        use std::collections::HashSet;
        use std::thread;

        // Spawn multiple threads to test thread safety
        // Don't reset allocation to avoid interfering with other tests
        let handles: Vec<_> = (0..5)
            .map(|_| thread::spawn(|| allocate_port_range()))
            .collect();

        let mut ports = HashSet::new();
        for handle in handles {
            let port = handle.join().unwrap();
            // Each port should be unique
            assert!(ports.insert(port), "Duplicate port allocated: {}", port);
        }

        // We should have 5 unique ports
        assert_eq!(ports.len(), 5);
    }
}
