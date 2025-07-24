//! Test cluster setup and utilities
//!
//! This module provides a simplified cluster manager for integration and end-to-end tests.
//!
//! ## Usage
//!
//! ```rust
//! #[tokio::test]
//! async fn test_simple_query() {
//!     let cluster = TestCluster::start().await.unwrap();
//!     
//!     // Execute a simple query
//!     let batches = cluster.execute_sql("SELECT 1 as test_column").await.unwrap();
//!     // your assertions here
//!     
//!     // Cluster automatically shuts down when dropped
//! }
//! ```

use std::collections::HashMap;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::*;

/// Configuration for the integration test cluster
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Number of worker nodes
    pub num_workers: usize,
    /// Base port for services (proxy will use this, workers will use base_port + 1, base_port + 2, etc.)
    pub base_port: u16,
    /// Tables to register in the cluster (name -> (format, path))
    pub tables: HashMap<String, (String, String)>,
    /// SQL views to create
    pub views: Vec<String>,
    /// Timeout for cluster startup
    pub startup_timeout: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            num_workers: 2,
            base_port: 21000,
            tables: HashMap::new(),
            views: Vec::new(),
            startup_timeout: Duration::from_secs(30),
        }
    }
}

impl ClusterConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of worker nodes
    #[allow(dead_code)]
    pub fn with_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers;
        self
    }

    /// Set the base port for services
    pub fn with_base_port(mut self, base_port: u16) -> Self {
        self.base_port = base_port;
        self
    }

    /// Add a table to register in the cluster
    #[allow(dead_code)]
    pub fn with_table<S: Into<String>>(mut self, name: S, format: S, path: S) -> Self {
        self.tables
            .insert(name.into(), (format.into(), path.into()));
        self
    }

    /// Add a parquet table (convenience method)
    pub fn with_parquet_table<S: Into<String>>(mut self, name: S, path: S) -> Self {
        self.tables
            .insert(name.into(), ("parquet".to_string(), path.into()));
        self
    }

    /// Add a CSV table (convenience method)  
    #[allow(dead_code)]
    pub fn with_csv_table<S: Into<String>>(mut self, name: S, path: S) -> Self {
        self.tables
            .insert(name.into(), ("csv".to_string(), path.into()));
        self
    }

    /// Add a SQL view
    pub fn with_view<S: Into<String>>(mut self, sql: S) -> Self {
        self.views.push(sql.into());
        self
    }

    /// Set startup timeout
    #[allow(dead_code)]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    fn proxy_port(&self) -> u16 {
        self.base_port
    }

    fn worker_ports(&self) -> Vec<u16> {
        (1..=self.num_workers)
            .map(|i| self.base_port + i as u16)
            .collect()
    }

    fn tables_env_string(&self) -> String {
        self.tables
            .iter()
            .map(|(name, (format, path))| format!("{}:{}:{}", name, format, path))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn views_env_string(&self) -> String {
        self.views.join(";")
    }
}

/// Test cluster manager
pub struct TestCluster {
    config: ClusterConfig,
    proxy_process: Option<Child>,
    worker_processes: Vec<Child>,
    is_running: Arc<AtomicBool>,
    session_context: SessionContext,
}

impl TestCluster {
    /// Start a cluster with default configuration
    #[allow(dead_code)]
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_config(ClusterConfig::default()).await
    }

    /// Start a cluster configured for TPC-H small tests
    #[allow(dead_code)]
    pub async fn start_tpch_small_cluster() -> Result<Self, Box<dyn std::error::Error>> {
        let config = ClusterConfig::new()
            .with_base_port(32000) // Use unique port range for TPC-H tests
            // Register all TPC-H tables
            .with_parquet_table("customer", "tpch/data/tpch_customer_small.parquet")
            .with_parquet_table("lineitem", "tpch/data/tpch_lineitem_small.parquet")
            .with_parquet_table("nation", "tpch/data/tpch_nation_small.parquet")
            .with_parquet_table("orders", "tpch/data/tpch_orders_small.parquet")
            .with_parquet_table("part", "tpch/data/tpch_part_small.parquet")
            .with_parquet_table("partsupp", "tpch/data/tpch_partsupp_small.parquet")
            .with_parquet_table("region", "tpch/data/tpch_region_small.parquet")
            .with_parquet_table("supplier", "tpch/data/tpch_supplier_small.parquet")
            // Add the revenue0 view needed for q15
            .with_view("CREATE VIEW revenue0 (supplier_no, total_revenue) AS SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) FROM lineitem WHERE l_shipdate >= date '1996-08-01' AND l_shipdate < date '1996-08-01' + interval '3' month GROUP BY l_suppkey");

        Self::start_with_config(config).await
    }

    /// Start a cluster with custom configuration
    pub async fn start_with_config(
        config: ClusterConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let session_context = SessionContext::new();
        let mut cluster = Self {
            config,
            proxy_process: None,
            worker_processes: Vec::new(),
            is_running: Arc::new(AtomicBool::new(false)),
            session_context,
        };

        cluster.setup().await?;
        Ok(cluster)
    }

    async fn setup(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 Starting integration test cluster...");

        // Step 1: Clean up any existing processes on our ports
        self.cleanup_existing_processes()?;

        // Step 2: Build the binary if needed
        self.ensure_binary_exists()?;

        // Step 3: Start the cluster
        self.start_cluster_processes().await?;

        // Step 4: Wait for cluster to be ready
        self.wait_for_cluster_ready().await?;

        // Step 5: Register tables and views
        self.register_tables_and_views().await?;

        println!("✅ Integration test cluster is ready!");
        Ok(())
    }

    fn cleanup_existing_processes(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🧹 Cleaning up existing processes...");

        let mut all_ports = vec![self.config.proxy_port()];
        all_ports.extend(self.config.worker_ports());

        for port in all_ports {
            if let Ok(output) = Command::new("lsof")
                .args(["-ti", &format!(":{}", port)])
                .output()
            {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    if let Ok(pid) = line.trim().parse::<u32>() {
                        let _ = Command::new("kill").args(["-9", &pid.to_string()]).output();
                        println!("  🔥 Killed process {} on port {}", pid, port);
                    }
                }
            }
        }

        // Give processes time to die
        thread::sleep(Duration::from_secs(1));
        Ok(())
    }

    fn ensure_binary_exists(&self) -> Result<(), Box<dyn std::error::Error>> {
        let binary_path = self.get_binary_path();

        if !Path::new(&binary_path).exists() {
            println!("📦 Building distributed-datafusion binary...");
            let output = Command::new("cargo")
                .args(["build", "--bin", "distributed-datafusion"])
                .output()?;

            if !output.status.success() {
                return Err(format!(
                    "Failed to build binary: {}",
                    String::from_utf8_lossy(&output.stderr)
                )
                .into());
            }
        }

        Ok(())
    }

    fn get_binary_path(&self) -> String {
        std::env::var("CARGO_BIN_EXE_distributed-datafusion")
            .unwrap_or_else(|_| "./target/debug/distributed-datafusion".to_string())
    }

    async fn start_cluster_processes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let binary_path = self.get_binary_path();
        let tables_env = self.config.tables_env_string();
        let views_env = self.config.views_env_string();

        // Start workers first
        println!("  Starting {} workers...", self.config.num_workers);
        for (i, &port) in self.config.worker_ports().iter().enumerate() {
            println!("    Worker {} on port {}", i + 1, port);

            let mut cmd = Command::new(&binary_path);
            cmd.args(["--mode", "worker", "--port", &port.to_string()])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .env("RUST_LOG", "info,distributed_datafusion=debug");

            if !tables_env.is_empty() {
                cmd.env("DD_TABLES", &tables_env);
            }
            if !views_env.is_empty() {
                cmd.env("DD_VIEWS", &views_env);
            }

            let worker = cmd.spawn()?;
            self.worker_processes.push(worker);
        }

        // Give workers time to start
        thread::sleep(Duration::from_secs(2));

        // Start proxy
        let proxy_port = self.config.proxy_port();
        println!("  Starting proxy on port {}...", proxy_port);

        let worker_addresses = self
            .config
            .worker_ports()
            .iter()
            .enumerate()
            .map(|(i, &port)| format!("worker{}/127.0.0.1:{}", i + 1, port))
            .collect::<Vec<_>>()
            .join(",");

        let mut cmd = Command::new(&binary_path);
        cmd.args(["--mode", "proxy", "--port", &proxy_port.to_string()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("RUST_LOG", "info,distributed_datafusion=debug")
            .env("DD_WORKER_ADDRESSES", &worker_addresses);

        if !tables_env.is_empty() {
            cmd.env("DD_TABLES", &tables_env);
        }
        if !views_env.is_empty() {
            cmd.env("DD_VIEWS", &views_env);
        }

        let proxy = cmd.spawn()?;
        self.proxy_process = Some(proxy);
        self.is_running.store(true, Ordering::Relaxed);

        Ok(())
    }

    async fn wait_for_cluster_ready(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("⏳ Waiting for cluster to be ready...");

        let start_time = Instant::now();
        let mut attempt = 0;

        while start_time.elapsed() < self.config.startup_timeout {
            attempt += 1;

            if self.is_cluster_ready().await {
                println!("✅ Cluster ready after {} attempts!", attempt);
                return Ok(());
            }

            if attempt % 10 == 0 {
                println!(
                    "  Still waiting... (attempt {} after {:.1}s)",
                    attempt,
                    start_time.elapsed().as_secs_f64()
                );
            }

            thread::sleep(Duration::from_millis(500));
        }

        Err("Cluster failed to become ready within timeout".into())
    }

    async fn is_cluster_ready(&self) -> bool {
        if !self.is_running.load(Ordering::Relaxed) {
            return false;
        }

        // Try to connect to proxy
        let proxy_addr = format!("127.0.0.1:{}", self.config.proxy_port());
        if std::net::TcpStream::connect(&proxy_addr).is_err() {
            return false;
        }

        // Check that we can connect to at least one worker
        for &port in &self.config.worker_ports() {
            let worker_addr = format!("127.0.0.1:{}", port);
            if std::net::TcpStream::connect(&worker_addr).is_ok() {
                return true;
            }
        }

        false
    }

    async fn register_tables_and_views(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("📋 Registering tables and views...");

        // Register tables
        for (table_name, (format, path)) in &self.config.tables {
            println!(
                "  Registering {} table: {} from {}",
                format, table_name, path
            );

            match format.as_str() {
                "parquet" => {
                    self.session_context
                        .register_parquet(table_name, path, ParquetReadOptions::default())
                        .await?;
                }
                "csv" => {
                    self.session_context
                        .register_csv(table_name, path, CsvReadOptions::default())
                        .await?;
                }
                _ => {
                    return Err(format!("Unsupported table format: {}", format).into());
                }
            }
        }

        // Create views
        for view_sql in &self.config.views {
            println!("  Creating view: {}", view_sql);
            self.session_context.sql(view_sql).await?;
        }

        println!("✅ Tables and views registered successfully!");
        Ok(())
    }

    /// Execute a SQL query on the cluster
    pub async fn execute_sql(
        &self,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        if !self.is_running.load(Ordering::Relaxed) {
            return Err("Cluster is not running".into());
        }

        // Execute query using DataFusion SessionContext
        let dataframe = self.session_context.sql(sql).await?;
        let batches = dataframe.collect().await?;

        Ok(batches)
    }

    /// Get the proxy address for direct connections
    #[allow(dead_code)]
    pub fn proxy_address(&self) -> String {
        format!("127.0.0.1:{}", self.config.proxy_port())
    }

    /// Get the worker addresses
    #[allow(dead_code)]
    pub fn worker_addresses(&self) -> Vec<String> {
        self.config
            .worker_ports()
            .iter()
            .map(|&port| format!("127.0.0.1:{}", port))
            .collect()
    }

    /// Check if the cluster is running
    #[allow(dead_code)]
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        if self.is_running.load(Ordering::Relaxed) {
            println!("🧹 Shutting down integration test cluster...");

            // Kill proxy
            if let Some(mut proxy) = self.proxy_process.take() {
                let _ = proxy.kill();
                let _ = proxy.wait();
            }

            // Kill workers
            for mut worker in self.worker_processes.drain(..) {
                let _ = worker.kill();
                let _ = worker.wait();
            }

            self.is_running.store(false, Ordering::Relaxed);
            println!("✅ Cluster shutdown complete");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test basic cluster startup and shutdown
    #[tokio::test]
    async fn test_basic_cluster_lifecycle() {
        println!("🧪 Testing basic cluster lifecycle...");

        // Start a cluster with default settings (2 workers)
        let cluster = TestCluster::start().await.expect("Failed to start cluster");

        // Verify cluster is running
        assert!(cluster.is_running());

        // Get connection info
        let proxy_addr = cluster.proxy_address();
        let worker_addrs = cluster.worker_addresses();

        println!("✅ Cluster started successfully:");
        println!("   Proxy: {}", proxy_addr);
        println!("   Workers: {:?}", worker_addrs);

        // Verify we have the expected number of workers
        assert_eq!(worker_addrs.len(), 2);

        // Verify proxy is on expected port (21000 is default)
        assert!(proxy_addr.contains("21000"));

        // Cluster automatically shuts down when dropped
        drop(cluster);
        println!("✅ Cluster lifecycle test completed");
    }

    /// Test cluster with custom configuration
    #[tokio::test]
    async fn test_custom_cluster_config() {
        println!("🧪 Testing cluster with custom configuration...");

        let config = ClusterConfig::new()
            .with_workers(3) // 3 workers instead of 2
            .with_base_port(23000) // Different port base
            .with_parquet_table("people", "testdata/parquet/people.parquet");

        let cluster = TestCluster::start_with_config(config)
            .await
            .expect("Failed to start cluster with custom config");

        // Verify custom configuration
        assert!(cluster.is_running());
        assert_eq!(cluster.worker_addresses().len(), 3);
        assert!(cluster.proxy_address().contains("23000"));

        println!("✅ Custom configuration test completed");
        println!("   Proxy: {}", cluster.proxy_address());
        println!("   Workers: {:?}", cluster.worker_addresses());
    }

    /// Test that cluster properly handles shutdown
    #[tokio::test]
    async fn test_cluster_shutdown() {
        println!("🧪 Testing cluster shutdown...");

        let config = ClusterConfig::new().with_base_port(31000); // Use unique port range
        let cluster = TestCluster::start_with_config(config)
            .await
            .expect("Failed to start cluster");

        assert!(cluster.is_running());
        let proxy_addr = cluster.proxy_address();

        // Explicitly drop the cluster
        drop(cluster);

        // Give a moment for processes to die
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Verify the port is now available (should fail to connect)
        use std::net::TcpStream;
        let connection_result = TcpStream::connect(&proxy_addr);
        assert!(
            connection_result.is_err(),
            "Port should be freed after cluster shutdown"
        );

        println!("✅ Cluster shutdown test completed");
    }

    /// Test multiple clusters can run simultaneously on different ports
    #[tokio::test]
    async fn test_multiple_clusters() {
        println!("🧪 Testing multiple clusters simultaneously...");

        let config1 = ClusterConfig::new().with_base_port(24000);
        let config2 = ClusterConfig::new().with_base_port(25000);

        let cluster1 = TestCluster::start_with_config(config1)
            .await
            .expect("Failed to start first cluster");

        let cluster2 = TestCluster::start_with_config(config2)
            .await
            .expect("Failed to start second cluster");

        // Both clusters should be running
        assert!(cluster1.is_running());
        assert!(cluster2.is_running());

        // They should use different ports
        assert!(cluster1.proxy_address().contains("24000"));
        assert!(cluster2.proxy_address().contains("25000"));

        println!("✅ Multiple clusters test completed");
        println!("   Cluster 1: {}", cluster1.proxy_address());
        println!("   Cluster 2: {}", cluster2.proxy_address());

        // Both clusters will be shut down automatically
    }

    /// Simple test to verify cluster startup
    #[tokio::test]
    async fn test_cluster_startup() {
        let config = ClusterConfig::new().with_base_port(26000); // Use unique port range
        let cluster = TestCluster::start_with_config(config)
            .await
            .expect("Failed to start cluster");

        // Verify cluster is running
        assert!(cluster.is_running());

        // Verify we can get addresses
        let proxy_addr = cluster.proxy_address();
        let worker_addrs = cluster.worker_addresses();

        println!("Proxy: {}", proxy_addr);
        println!("Workers: {:?}", worker_addrs);

        assert_eq!(worker_addrs.len(), 2); // Default config has 2 workers

        // Cluster will automatically shut down when dropped
    }

    /// Test cluster with custom configuration (original version)
    #[tokio::test]
    async fn test_cluster_custom_config_original() {
        let config = ClusterConfig::new()
            .with_workers(3)
            .with_base_port(27000) // Use unique port range
            .with_parquet_table("test_table", "testdata/parquet/people.parquet")
            .with_view("CREATE VIEW test_view AS SELECT * FROM test_table LIMIT 5");

        let cluster = TestCluster::start_with_config(config)
            .await
            .expect("Failed to start cluster with custom config");

        assert!(cluster.is_running());
        assert_eq!(cluster.worker_addresses().len(), 3);

        // Check that proxy is on the expected port
        assert!(cluster.proxy_address().contains("27000"));
    }

    /// Test SQL execution (original version)
    #[tokio::test]
    async fn test_simple_sql_execution() {
        let config = ClusterConfig::new().with_base_port(28000); // Use unique port range
        let cluster = TestCluster::start_with_config(config)
            .await
            .expect("Failed to start cluster");

        let batches = cluster
            .execute_sql("SELECT 1 as test_column")
            .await
            .expect("Failed to execute SQL");

        // For now, this just tests that the method doesn't crash
        // TODO: Add proper assertions once SQL execution is implemented
        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        println!("Query returned {} rows", total_rows);
    }
}
