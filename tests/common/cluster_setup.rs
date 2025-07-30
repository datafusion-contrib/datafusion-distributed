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
//!     let batches = cluster.execute_sql_distributed("SELECT 1 as test_column").await.unwrap();
//!     // your assertions here
//!
//!     // Cluster automatically shuts down when dropped
//! }
//! ```

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;

use super::test_utils::allocate_port_range;

use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::StreamExt;
use insta::assert_snapshot;
use tonic::transport::Channel;

/// CLUSTER CONFIG CONSTANTS
const DEFAULT_NUM_WORKERS: usize = 2;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const CHANNEL_TIMEOUT: Duration = Duration::from_secs(60);
const QUERY_TIMEOUT: Duration = Duration::from_secs(120);
const CONNECTION_CLEANUP_TIMEOUT: Duration = Duration::from_millis(100);

/// Supported table formats for the cluster
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableFormat {
    Parquet,
    Csv,
}

impl TableFormat {
    /// Convert to string representation used in environment variables
    pub fn as_str(&self) -> &'static str {
        match self {
            TableFormat::Parquet => "parquet",
            TableFormat::Csv => "csv",
        }
    }
}

impl std::fmt::Display for TableFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for TableFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "parquet" => Ok(TableFormat::Parquet),
            "csv" => Ok(TableFormat::Csv),
            _ => Err(format!(
                "Unsupported table format: '{}'. Supported formats: parquet, csv",
                s
            )),
        }
    }
}

/// Configuration for a table to be registered in the cluster
#[derive(Debug, Clone)]
pub struct TableConfig {
    pub name: String,
    pub format: TableFormat,
    pub path: String,
}

impl TableConfig {
    pub fn new<S: Into<String>>(name: S, format: TableFormat, path: S) -> Self {
        Self {
            name: name.into(),
            format,
            path: path.into(),
        }
    }

    pub fn parquet<S: Into<String>>(name: S, path: S) -> Self {
        Self {
            name: name.into(),
            format: TableFormat::Parquet,
            path: path.into(),
        }
    }

    pub fn csv<S: Into<String>>(name: S, path: S) -> Self {
        Self {
            name: name.into(),
            format: TableFormat::Csv,
            path: path.into(),
        }
    }
}

/// Configuration for the integration test cluster
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Number of worker nodes
    pub num_workers: usize,
    /// Base port for services (proxy will use this, workers will use base_port + 1, base_port + 2, etc.)
    pub base_port: u16,
    /// Tables to register in the cluster
    pub tables: Vec<TableConfig>,
    /// SQL views to create
    pub views: Vec<String>,
    /// Timeout for cluster startup
    pub startup_timeout: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            num_workers: DEFAULT_NUM_WORKERS,
            base_port: allocate_port_range(),
            tables: Vec::new(),
            views: Vec::new(),
            startup_timeout: STARTUP_TIMEOUT,
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

    /// Add a table to register in the cluster (string format - may fail if format is invalid)
    #[allow(dead_code)]
    pub fn with_table<S: Into<String>>(
        mut self,
        name: S,
        format: S,
        path: S,
    ) -> Result<Self, String> {
        let format_str = format.into();
        let table_format = format_str
            .parse::<TableFormat>()
            .map_err(|e| format!("Invalid table format '{}': {}", format_str, e))?;

        let table_config = TableConfig::new(name, table_format, path);
        self.add_table(table_config)?;
        Ok(self)
    }

    /// Add a table to register in the cluster (using TableFormat enum)
    #[allow(dead_code)]
    pub fn with_table_format<S: Into<String>>(
        mut self,
        name: S,
        format: TableFormat,
        path: S,
    ) -> Result<Self, String> {
        let table_config = TableConfig::new(name, format, path);
        self.add_table(table_config)?;
        Ok(self)
    }

    /// Add a parquet table (convenience method)
    pub fn with_parquet_table<S: Into<String>>(mut self, name: S, path: S) -> Result<Self, String> {
        let table_config = TableConfig::parquet(name, path);
        self.add_table(table_config)?;
        Ok(self)
    }

    /// Add a CSV table (convenience method)
    #[allow(dead_code)]
    pub fn with_csv_table<S: Into<String>>(mut self, name: S, path: S) -> Result<Self, String> {
        let table_config = TableConfig::csv(name, path);
        self.add_table(table_config)?;
        Ok(self)
    }

    /// Helper method to add a table and check for duplicates
    fn add_table(&mut self, table_config: TableConfig) -> Result<(), String> {
        // Check for duplicate table names
        if self.tables.iter().any(|t| t.name == table_config.name) {
            return Err(format!("Table '{}' already exists", table_config.name));
        }

        self.tables.push(table_config);
        Ok(())
    }

    /// Add a pre-configured table directly
    #[allow(dead_code)]
    pub fn with_table_config(mut self, table_config: TableConfig) -> Result<Self, String> {
        self.add_table(table_config)?;
        Ok(self)
    }

    /// Find a table configuration by name
    #[allow(dead_code)]
    pub fn find_table(&self, name: &str) -> Option<&TableConfig> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Get all table names
    #[allow(dead_code)]
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.iter().map(|t| t.name.as_str()).collect()
    }

    /// Check if a table with the given name exists
    #[allow(dead_code)]
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.iter().any(|t| t.name == name)
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

    fn tables_from_env(&self) -> String {
        self.tables
            .iter()
            .map(|table_config| {
                format!(
                    "{}:{}:{}",
                    table_config.name,
                    table_config.format.as_str(),
                    table_config.path
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn views_from_env(&self) -> String {
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
            // Use dynamically allocated port
            .with_base_port(allocate_port_range())
            // Register all TPC-H tables
            .with_parquet_table("customer", "tpch/data/tpch_customer_small.parquet")?
            .with_parquet_table("lineitem", "tpch/data/tpch_lineitem_small.parquet")?
            .with_parquet_table("nation", "tpch/data/tpch_nation_small.parquet")?
            .with_parquet_table("orders", "tpch/data/tpch_orders_small.parquet")?
            .with_parquet_table("part", "tpch/data/tpch_part_small.parquet")?
            .with_parquet_table("partsupp", "tpch/data/tpch_partsupp_small.parquet")?
            .with_parquet_table("region", "tpch/data/tpch_region_small.parquet")?
            .with_parquet_table("supplier", "tpch/data/tpch_supplier_small.parquet")?
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
        let tables_env = self.config.tables_from_env();
        let views_env = self.config.views_from_env();

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

        // Give workers time to start and be ready for connections
        println!("  Waiting for workers to be ready...");
        thread::sleep(Duration::from_secs(5));

        // Start proxy
        let proxy_port = self.config.proxy_port();
        println!("  Starting proxy on port {}...", proxy_port);

        let worker_addresses = self
            .config
            .worker_ports()
            .iter()
            .map(|&port| format!("127.0.0.1:{}", port))
            .collect::<Vec<_>>()
            .join(",");

        println!("  Setting DD_WORKER_ADDRESSES: {}", worker_addresses);

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
        for table_config in &self.config.tables {
            println!(
                "  Registering {} table: {} from {}",
                table_config.format, table_config.name, table_config.path
            );

            match table_config.format {
                TableFormat::Parquet => {
                    self.session_context
                        .register_parquet(
                            &table_config.name,
                            &table_config.path,
                            ParquetReadOptions::default(),
                        )
                        .await?;
                }
                TableFormat::Csv => {
                    self.session_context
                        .register_csv(
                            &table_config.name,
                            &table_config.path,
                            CsvReadOptions::default(),
                        )
                        .await?;
                }
            }
        }

        // Create views
        for view_sql in &self.config.views {
            println!("  Creating view: {}", view_sql);
            self.session_context.sql(view_sql).await?;
        }

        println!("✅ Tables and views registered successfully!");

        // Give additional time for worker discovery to complete
        println!("  Allowing time for worker discovery...");
        thread::sleep(Duration::from_secs(3));

        Ok(())
    }

    /// Execute a SQL query using single-node DataFusion
    /// This has nothing to do with the distributed cluster, it is just a helper function
    /// to execute a query using the single-node DataFusion SessionContext for output comparison
    /// Use the same SessionContext to access the same tables and views as the distributed cluster
    pub async fn execute_sql_single_node(
        &self,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        if !self.is_running.load(Ordering::Relaxed) {
            return Err("Cluster is not running".into());
        }

        // Execute query using LOCAL DataFusion SessionContext - NOT distributed!
        let dataframe = self.session_context.sql(sql).await?;
        let batches = dataframe.collect().await?;

        Ok(batches)
    }

    /// Execute a SQL query using the distributed cluster via Flight SQL
    /// This communicates with the running proxy to issue query, prepare, plan and execute it
    pub async fn execute_sql_distributed(
        &self,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        if !self.is_running.load(Ordering::Relaxed) {
            return Err("Cluster is not running".into());
        }

        println!("🔍 Executing distributed query: {}", sql);

        // Wrap the entire operation in a timeout
        let result = tokio::time::timeout(QUERY_TIMEOUT, async {
            // Scope the connection to ensure it gets dropped
            let batches = {
                // Connect to the RUNNING proxy via Flight SQL
                let proxy_url = format!("http://{}", self.proxy_address());

                let channel = Channel::from_shared(proxy_url)?
                    .tcp_nodelay(true)
                    .tcp_keepalive(Some(CHANNEL_TIMEOUT))
                    .http2_keep_alive_interval(CHANNEL_TIMEOUT)
                    .keep_alive_timeout(CHANNEL_TIMEOUT)
                    .connect_timeout(CHANNEL_TIMEOUT)
                    .connect()
                    .await?;

                // Use Flight SQL service client
                let mut client = FlightSqlServiceClient::new(channel);

                // get_flight_info_statement - proxy calls prepare() and distribute_plan()
                let flight_info = client
                    .execute(sql.to_string(), None)
                    .await?;

                // do_get_statement - proxy executes distributed plan and returns results
                let mut batches = Vec::new();

                // The proxy should return exactly one endpoint (itself)
                if flight_info.endpoint.len() != 1 {
                    return Err(format!("Expected exactly 1 endpoint from proxy, got {}", flight_info.endpoint.len()).into());
                }

                let endpoint = &flight_info.endpoint[0];
                if let Some(ticket) = &endpoint.ticket {
                    let mut stream = client.do_get(ticket.clone()).await?;

                    // Collect all batches from the proxy's coordinated stream
                    while let Some(batch_result) = stream.next().await {
                        match batch_result {
                            Ok(batch) => {
                                println!("📦 Received RecordBatch with {} rows, {} columns from distributed cluster",
                                       batch.num_rows(), batch.num_columns());
                                batches.push(batch);
                            }
                            Err(e) => {
                                return Err(format!("Failed to decode batch from distributed cluster: {}", e).into());
                            }
                        }
                    }
                } else {
                    return Err("No ticket found in flight endpoint".into());
                }

                // Explicitly drop the client to ensure connection cleanup
                drop(client);
                batches
            }; // Channel and client should be dropped here

            println!("✅ Successfully executed distributed query and collected {} batches", batches.len());

            // Add a small delay to allow connection cleanup
            tokio::time::sleep(CONNECTION_CLEANUP_TIMEOUT).await;

            Ok(batches)
        }).await;

        match result {
            Ok(batches_result) => batches_result,
            Err(_) => Err(format!(
                "Query execution timeout after {} seconds",
                QUERY_TIMEOUT.as_secs()
            )
            .into()),
        }
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
        // Start a cluster with default settings (2 workers)
        let cluster = TestCluster::start().await.expect("Failed to start cluster");

        // Verify cluster is running
        assert!(cluster.is_running());

        // Get connection info
        let proxy_addr = cluster.proxy_address();
        let worker_addrs = cluster.worker_addresses();

        // check proxy address is valid
        // we do not check port because it is dynamically allocated
        assert!(proxy_addr.contains("127.0.0.1:"));

        // Verify we have the expected number of workers
        assert_eq!(worker_addrs.len(), 2);

        // Cluster automatically shuts down when dropped
        drop(cluster);
    }

    /// Test cluster with custom configuration
    #[tokio::test]
    async fn test_custom_cluster_config() {
        let config = ClusterConfig::new()
            .with_workers(3) // 3 workers instead of 2
            .with_base_port(allocate_port_range()) // Dynamically allocated port base
            .with_parquet_table("people", "testdata/parquet/people.parquet")
            .expect("Failed to configure parquet table");

        let cluster = TestCluster::start_with_config(config)
            .await
            .expect("Failed to start cluster with custom config");

        // Verify custom configuration
        assert!(cluster.is_running());
        assert_eq!(cluster.worker_addresses().len(), 3);
        assert!(cluster.proxy_address().contains("127.0.0.1:"));
    }

    /// Test that cluster properly handles shutdown
    #[tokio::test]
    async fn test_cluster_shutdown() {
        println!("🧪 Testing cluster shutdown...");

        let config = ClusterConfig::new().with_base_port(allocate_port_range()); // Use dynamically allocated port range
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

        let config1 = ClusterConfig::new().with_base_port(allocate_port_range());
        let config2 = ClusterConfig::new().with_base_port(allocate_port_range());

        let cluster1 = TestCluster::start_with_config(config1)
            .await
            .expect("Failed to start first cluster");

        let cluster2 = TestCluster::start_with_config(config2)
            .await
            .expect("Failed to start second cluster");

        // Both clusters should be running
        assert!(cluster1.is_running());
        assert!(cluster2.is_running());

        // They should use different ports (dynamically allocated)
        let proxy1_addr = cluster1.proxy_address();
        let proxy2_addr = cluster2.proxy_address();
        let proxy1_port = proxy1_addr.split(':').last().unwrap();
        let proxy2_port = proxy2_addr.split(':').last().unwrap();
        assert_ne!(proxy1_port, proxy2_port);

        println!("✅ Multiple clusters test completed");
        println!("   Cluster 1: {}", cluster1.proxy_address());
        println!("   Cluster 2: {}", cluster2.proxy_address());

        // Both clusters will be shut down automatically
    }

    /// Simple test to verify cluster startup
    #[tokio::test]
    async fn test_cluster_startup() {
        let config = ClusterConfig::new().with_base_port(allocate_port_range()); // Use dynamically allocated port range
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
            .with_base_port(allocate_port_range()) // Use dynamically allocated port range
            .with_parquet_table("test_table", "testdata/parquet/people.parquet")
            .expect("Failed to configure parquet table")
            .with_view("CREATE VIEW test_view AS SELECT * FROM test_table LIMIT 5");

        let cluster = TestCluster::start_with_config(config)
            .await
            .expect("Failed to start cluster with custom config");

        assert!(cluster.is_running());
        assert_eq!(cluster.worker_addresses().len(), 3);

        // Check that proxy is on a valid port (dynamically allocated)
        assert!(cluster.proxy_address().contains("127.0.0.1:"));
    }

    /// Test distributed SQL execution
    #[tokio::test]
    async fn test_distributed_sql_execution() {
        let config = ClusterConfig::new()
            .with_base_port(allocate_port_range()) // Use dynamically allocated port range
            .with_parquet_table("test_table", "testdata/parquet/people.parquet")
            .expect("Failed to configure test table");

        let cluster = TestCluster::start_with_config(config)
            .await
            .expect("Failed to start cluster");

        let single_result = cluster
            .execute_sql_single_node("SELECT COUNT(*) as count FROM test_table")
            .await;

        let distributed_result = cluster
            .execute_sql_distributed("SELECT COUNT(*) as count FROM test_table")
            .await;

        // Compare the results
        let single_result_formatted = pretty_format_batches(&single_result.unwrap()).unwrap();
        let distributed_result_formatted =
            pretty_format_batches(&distributed_result.unwrap()).unwrap();
        assert_snapshot!(distributed_result_formatted, @r"
        +-------+
        | count |
        +-------+
        | 2     |
        +-------+
        ");
        assert_snapshot!(single_result_formatted, @r"
        +-------+
        | count |
        +-------+
        | 2     |
        +-------+
        ");
    }

    /// Test the new Vec-based table configuration API (config-only, no files accessed)
    #[test]
    fn test_table_config_api() {
        println!("🧪 Testing new table configuration API...");

        // Test building configuration with various methods
        // NOTE: This test only verifies configuration building - no files are actually accessed
        // Using dummy paths since this is purely testing the configuration API
        let config = ClusterConfig::new()
            .with_base_port(29000)
            .with_parquet_table("table1", "/dummy/path/table1.parquet") // Dummy path - config test only
            .expect("Failed to add parquet table")
            .with_table_config(TableConfig::csv("table2", "/dummy/path/table2.csv")) // Dummy path - config test only
            .expect("Failed to add csv table")
            .with_table_format("table3", TableFormat::Parquet, "/dummy/path/table3.parquet") // Dummy path - config test only
            .expect("Failed to add table with format");

        // Test query methods
        assert_eq!(config.tables.len(), 3);
        assert!(config.has_table("table1"));
        assert!(config.has_table("table2"));
        assert!(config.has_table("table3"));
        assert!(!config.has_table("nonexistent"));

        let table_names = config.table_names();
        assert_eq!(table_names.len(), 3);
        assert!(table_names.contains(&"table1"));
        assert!(table_names.contains(&"table2"));
        assert!(table_names.contains(&"table3"));

        // Test finding specific table
        let table1 = config.find_table("table1").unwrap();
        assert_eq!(table1.name, "table1");
        assert_eq!(table1.format, TableFormat::Parquet);
        assert_eq!(table1.path, "/dummy/path/table1.parquet");

        let table2 = config.find_table("table2").unwrap();
        assert_eq!(table2.name, "table2");
        assert_eq!(table2.format, TableFormat::Csv);
        assert_eq!(table2.path, "/dummy/path/table2.csv");

        let table3 = config.find_table("table3").unwrap();
        assert_eq!(table3.name, "table3");
        assert_eq!(table3.format, TableFormat::Parquet);
        assert_eq!(table3.path, "/dummy/path/table3.parquet");

        println!("✅ Table configuration API test completed");
    }

    /// Test duplicate table name detection
    #[test]
    fn test_duplicate_table_detection() {
        println!("🧪 Testing duplicate table name detection...");

        // Test duplicate table names (using dummy paths - config test only)
        let result = ClusterConfig::new()
            .with_parquet_table("table1", "/dummy/path1.parquet") // Dummy path - config test only
            .expect("First table should succeed")
            .with_parquet_table("table1", "/dummy/path2.parquet"); // Same name, dummy path - config test only

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Table 'table1' already exists"));

        println!("✅ Duplicate detection test completed");
    }

    /// Test TableFormat parsing
    #[test]
    fn test_table_format_parsing() {
        println!("🧪 Testing TableFormat parsing...");

        // Test successful parsing
        assert_eq!(
            "parquet".parse::<TableFormat>().unwrap(),
            TableFormat::Parquet
        );
        assert_eq!("csv".parse::<TableFormat>().unwrap(), TableFormat::Csv);
        assert_eq!(
            "PARQUET".parse::<TableFormat>().unwrap(),
            TableFormat::Parquet
        ); // Case insensitive
        assert_eq!("CSV".parse::<TableFormat>().unwrap(), TableFormat::Csv);

        // Test failed parsing
        let result = "invalid".parse::<TableFormat>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unsupported table format"));

        // Test Display trait
        assert_eq!(TableFormat::Parquet.to_string(), "parquet");
        assert_eq!(TableFormat::Csv.to_string(), "csv");

        println!("✅ TableFormat parsing test completed");
    }
}
