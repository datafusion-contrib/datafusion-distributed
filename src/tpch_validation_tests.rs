//! Self-Contained TPC-H Validation Tests
//! 
//! This module provides comprehensive validation tests that compare TPC-H query results
//! between regular DataFusion and distributed DataFusion systems.
//! 
//! ## Features
//! - ✅ Automatic cluster setup and teardown
//! - ✅ Automatic TPC-H data generation
//! - ✅ Automatic dependency installation
//! - ✅ Complete result comparison with tolerance
//! - ✅ CI-ready with detailed reporting
//! 
//! ## Usage
//! 
//! Just run the tests - everything is automated:
//! ```bash
//! # Run all TPC-H validation tests
//! cargo test --lib tpch_validation_tests -- --nocapture
//! 
//! # Run single query test (for debugging)
//! cargo test --lib test_tpch_validation_single_query -- --ignored --nocapture
//! ```
//! 
//! ## What the tests do automatically:
//! 1. Kill any existing processes on ports 40400-40402
//! 2. Install tpchgen-cli if not available
//! 3. Generate TPC-H data at /tmp/tpch_s1 if not present
//! 4. Start distributed cluster (1 proxy + 2 workers)
//! 5. Run validation tests comparing DataFusion vs Distributed
//! 6. Clean up cluster processes
//! 
//! ## Architecture
//! - **Proxy**: Port 40400
//! - **Worker 1**: Port 40401  
//! - **Worker 2**: Port 40402
//! - **TPC-H Data**: /tmp/tpch_s1 (scale factor 1)
//! - **Queries**: ./tpch/queries/q1.sql through q22.sql

#[cfg(test)]
mod tpch_validation_tests {
    use std::path::Path;
    use std::fs;
    use std::time::{Duration, Instant};
    use std::process::{Command, Child, Stdio};
    use std::thread;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::io::Read;

    use datafusion::prelude::*;
    use datafusion::execution::config::SessionConfig;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;

    /// Test configuration
    const PROXY_PORT: u16 = 40400;
    const WORKER_PORT_1: u16 = 40401;
    const WORKER_PORT_2: u16 = 40402;
    const TPCH_DATA_PATH: &str = "/tmp/tpch_s1";
    const QUERY_PATH: &str = "./tpch/queries";
    const FLOATING_POINT_TOLERANCE: f64 = 1e-5;
    const SETUP_TIMEOUT_SECONDS: u64 = 60; // Increased timeout for cluster startup

    /// Validation results
    #[derive(Debug)]
    struct ValidationResults {
        total_queries: usize,
        passed_queries: usize,
        failed_queries: usize,
        skipped_queries: usize,
        results: Vec<ComparisonResult>,
        total_time: Duration,
    }

    /// Individual query comparison result
    #[derive(Debug, Clone)]
    struct ComparisonResult {
        query_name: String,
        matches: bool,
        row_count_datafusion: usize,
        row_count_distributed: usize,
        error_message: Option<String>,
        execution_time_datafusion: Duration,
        execution_time_distributed: Duration,
    }

    /// Distributed cluster manager
    struct ClusterManager {
        proxy_process: Option<Child>,
        worker_processes: Vec<Child>,
        is_running: Arc<AtomicBool>,
    }

    impl ClusterManager {
        fn new() -> Self {
            Self {
                proxy_process: None,
                worker_processes: Vec::new(),
                is_running: Arc::new(AtomicBool::new(false)),
            }
        }

        /// Setup everything needed for tests
        async fn setup() -> Result<Self, Box<dyn std::error::Error>> {
            let mut manager = Self::new();
            
            println!("🚀 Setting up TPC-H validation environment...");
            
            // Step 1: Kill existing processes
            manager.kill_existing_processes()?;
            
            // Step 2: Install tpchgen-cli if needed
            manager.ensure_tpchgen_cli().await?;
            
            // Step 3: Generate TPC-H data if needed
            manager.ensure_tpch_data().await?;
            
            // Step 4: Install Python Flight SQL packages if needed
            manager.ensure_python_packages().await?;
            
            // Step 5: Start distributed cluster
            manager.start_cluster().await?;
            
            // Step 6: Wait for cluster to be ready
            manager.wait_for_cluster_ready().await?;
            
            println!("✅ TPC-H validation environment ready!");
            Ok(manager)
        }

        /// Kill any existing processes on our ports
        fn kill_existing_processes(&self) -> Result<(), Box<dyn std::error::Error>> {
            println!("🔧 Killing existing processes on ports {}, {}, {}...", 
                     PROXY_PORT, WORKER_PORT_1, WORKER_PORT_2);
            
            let ports = [PROXY_PORT, WORKER_PORT_1, WORKER_PORT_2];
            
            for port in ports {
                // Find and kill processes using lsof
                let output = Command::new("lsof")
                    .args(&["-ti", &format!(":{}", port)])
                    .output();
                
                if let Ok(output) = output {
                    let pids = String::from_utf8_lossy(&output.stdout);
                    for pid in pids.lines() {
                        let pid = pid.trim();
                        if !pid.is_empty() {
                            println!("  Killing process {} on port {}", pid, port);
                            let _ = Command::new("kill")
                                .args(&["-9", pid])
                                .output();
                        }
                    }
                }
            }
            
            // Also kill any distributed-datafusion processes
            let _ = Command::new("pkill")
                .args(&["-f", "distributed-datafusion"])
                .output();
            
            // Give processes time to die
            thread::sleep(Duration::from_secs(2));
            
            Ok(())
        }

        /// Ensure tpchgen-cli is installed
        async fn ensure_tpchgen_cli(&self) -> Result<(), Box<dyn std::error::Error>> {
            println!("🔧 Checking for tpchgen-cli...");
            
            // Check if already installed
            if Command::new("tpchgen-cli")
                .arg("--version")
                .output()
                .is_ok() {
                println!("✅ tpchgen-cli already installed");
                return Ok(());
            }
            
            println!("📦 Installing tpchgen-cli...");
            let output = Command::new("cargo")
                .args(&["install", "tpchgen-cli"])
                .output()
                .map_err(|e| format!("Failed to install tpchgen-cli: {}", e))?;
            
            if !output.status.success() {
                return Err(format!("tpchgen-cli installation failed: {}", 
                                 String::from_utf8_lossy(&output.stderr)).into());
            }
            
            println!("✅ tpchgen-cli installed successfully");
            Ok(())
        }

        /// Ensure TPC-H data exists
        async fn ensure_tpch_data(&self) -> Result<(), Box<dyn std::error::Error>> {
            println!("🔧 Checking for TPC-H data at {}...", TPCH_DATA_PATH);
            
            // Check if all required tables exist
            let required_tables = ["customer", "lineitem", "nation", "orders", 
                                 "part", "partsupp", "region", "supplier"];
            
            let mut all_exist = true;
            for table in &required_tables {
                let path = format!("{}/{}.parquet", TPCH_DATA_PATH, table);
                if !Path::new(&path).exists() {
                    all_exist = false;
                    break;
                }
            }
            
            if all_exist {
                println!("✅ TPC-H data already exists");
                return Ok(());
            }
            
            println!("📊 Generating TPC-H data (scale factor 1)...");
            
            // Create directory if it doesn't exist
            if let Some(parent) = Path::new(TPCH_DATA_PATH).parent() {
                fs::create_dir_all(parent)?;
            }
            
            let output = Command::new("tpchgen-cli")
                .args(&[
                    "--format", "parquet",
                    "--scale", "1",
                    "--output", TPCH_DATA_PATH
                ])
                .output()
                .map_err(|e| format!("Failed to generate TPC-H data: {}", e))?;
            
            if !output.status.success() {
                return Err(format!("TPC-H data generation failed: {}", 
                                 String::from_utf8_lossy(&output.stderr)).into());
            }
            
            println!("✅ TPC-H data generated successfully");
            Ok(())
        }

        /// Ensure Python Flight SQL packages are installed
        async fn ensure_python_packages(&self) -> Result<(), Box<dyn std::error::Error>> {
            println!("🔧 Checking Python Flight SQL packages...");
            
            // Check if packages are already installed
            let check_cmd = Command::new("python3")
                .args(&["-c", "import adbc_driver_flightsql; import duckdb; print('OK')"])
                .output();
            
            if let Ok(output) = check_cmd {
                if output.status.success() && String::from_utf8_lossy(&output.stdout).contains("OK") {
                    println!("✅ Python packages already installed");
                    return Ok(());
                }
            }
            
            println!("📦 Installing Python Flight SQL packages...");
            let install_cmd = Command::new("python3")
                .args(&["-m", "pip", "install", "adbc_driver_manager", "adbc_driver_flightsql", "duckdb", "pyarrow"])
                .output()
                .map_err(|e| format!("Failed to install Python packages: {}", e))?;
            
            if !install_cmd.status.success() {
                return Err(format!("Python packages installation failed: {}", 
                                 String::from_utf8_lossy(&install_cmd.stderr)).into());
            }
            
            println!("✅ Python packages installed successfully");
            Ok(())
        }

        /// Start the distributed cluster
        async fn start_cluster(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            println!("🚀 Starting distributed DataFusion cluster...");
            
            // First, build the binary to avoid concurrent compilation issues
            println!("📦 Building distributed-datafusion binary...");
            let build_result = Command::new("cargo")
                .args(&["build", "--bin", "distributed-datafusion"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .map_err(|e| format!("Failed to build binary: {}", e))?;
            
            if !build_result.status.success() {
                let stderr = String::from_utf8_lossy(&build_result.stderr);
                return Err(format!("Failed to build binary: {}", stderr).into());
            }
            
            // Get the binary path
            let binary_path = "./target/debug/distributed-datafusion";
            
            // Create TPC-H table definitions
            let tpch_tables = format!(
                "customer:parquet:{}/customer.parquet,\
                 lineitem:parquet:{}/lineitem.parquet,\
                 nation:parquet:{}/nation.parquet,\
                 orders:parquet:{}/orders.parquet,\
                 part:parquet:{}/part.parquet,\
                 partsupp:parquet:{}/partsupp.parquet,\
                 region:parquet:{}/region.parquet,\
                 supplier:parquet:{}/supplier.parquet",
                TPCH_DATA_PATH, TPCH_DATA_PATH, TPCH_DATA_PATH, TPCH_DATA_PATH,
                TPCH_DATA_PATH, TPCH_DATA_PATH, TPCH_DATA_PATH, TPCH_DATA_PATH
            );
            
            // Setup TPC-H views configuration (required for q15)
            let tpch_views = "CREATE VIEW revenue0 (supplier_no, total_revenue) AS SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) FROM lineitem WHERE l_shipdate >= date '1996-08-01' AND l_shipdate < date '1996-08-01' + interval '3' month GROUP BY l_suppkey";
            
            // Start workers first
            println!("  Starting worker 1 on port {}...", WORKER_PORT_1);
            let worker1 = Command::new(binary_path)
                .args(&[
                    "--mode", "worker",
                    "--port", &WORKER_PORT_1.to_string()
                ])
                .env("DFRAY_TABLES", &tpch_tables) // Set TPC-H tables
                .env("DFRAY_VIEWS", &tpch_views) // Set TPC-H views (required for q15)
                .stdout(Stdio::piped()) // Capture output for debugging
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| format!("Failed to start worker 1: {}", e))?;
            
            println!("  Starting worker 2 on port {}...", WORKER_PORT_2);
            let worker2 = Command::new(binary_path)
                .args(&[
                    "--mode", "worker", 
                    "--port", &WORKER_PORT_2.to_string()
                ])
                .env("DFRAY_TABLES", &tpch_tables) // Set TPC-H tables
                .env("DFRAY_VIEWS", &tpch_views) // Set TPC-H views (required for q15)
                .stdout(Stdio::piped()) // Capture output for debugging
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| format!("Failed to start worker 2: {}", e))?;
            
            self.worker_processes.push(worker1);
            self.worker_processes.push(worker2);
            
            // Give workers time to start
            thread::sleep(Duration::from_secs(3));
            
            // Start proxy
            println!("  Starting proxy on port {}...", PROXY_PORT);
            let worker_addresses = format!("worker1/127.0.0.1:{},worker2/127.0.0.1:{}", WORKER_PORT_1, WORKER_PORT_2);
            let proxy = Command::new(binary_path)
                .args(&[
                    "--mode", "proxy",
                    "--port", &PROXY_PORT.to_string()
                ])
                .env("DFRAY_WORKER_ADDRESSES", &worker_addresses) // Set worker addresses
                .env("DFRAY_TABLES", &tpch_tables) // Set TPC-H tables
                .env("DFRAY_VIEWS", &tpch_views) // Set TPC-H views (required for q15)
                .stdout(Stdio::piped()) // Capture output for debugging
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| format!("Failed to start proxy: {}", e))?;
            
            self.proxy_process = Some(proxy);
            self.is_running.store(true, Ordering::Relaxed);
            
            println!("✅ Cluster started successfully");
            Ok(())
        }

        /// Wait for cluster to be ready
        async fn wait_for_cluster_ready(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            println!("⏳ Waiting for cluster to be ready...");
            
            // First, give the proxy extra time to start after workers
            thread::sleep(Duration::from_secs(5));
            
            let start_time = Instant::now();
            let timeout = Duration::from_secs(SETUP_TIMEOUT_SECONDS);
            let mut attempt = 0;
            
            while start_time.elapsed() < timeout {
                attempt += 1;
                
                // Check if processes are still running
                if !self.are_processes_running() {
                    return Err("Some cluster processes have died".into());
                }
                
                if self.is_cluster_ready().await {
                    println!("✅ Cluster is ready after {} attempts!", attempt);
                    return Ok(());
                }
                
                if attempt % 10 == 0 {
                    println!("  Still waiting... (attempt {} after {:.1}s)", attempt, start_time.elapsed().as_secs_f64());
                }
                
                thread::sleep(Duration::from_millis(1000)); // Check every second
            }
            
            Err(format!("Cluster failed to become ready within {} seconds (tried {} times)", 
                       SETUP_TIMEOUT_SECONDS, attempt).into())
        }

        /// Check if all processes are still running
        fn are_processes_running(&mut self) -> bool {
            // Check proxy
            if let Some(proxy) = &mut self.proxy_process {
                match proxy.try_wait() {
                    Ok(Some(exit_status)) => {
                        println!("⚠️ Proxy process has exited with status: {}", exit_status);
                        Self::capture_process_output(proxy, "proxy");
                        return false;
                    },
                    Ok(None) => {}, // Still running
                    Err(e) => {
                        println!("⚠️ Error checking proxy process: {}", e);
                        return false;
                    }
                }
            }
            
            // Check workers
            for (i, worker) in self.worker_processes.iter_mut().enumerate() {
                match worker.try_wait() {
                    Ok(Some(exit_status)) => {
                        println!("⚠️ Worker {} process has exited with status: {}", i + 1, exit_status);
                        Self::capture_process_output(worker, &format!("worker {}", i + 1));
                        return false;
                    },
                    Ok(None) => {}, // Still running
                    Err(e) => {
                        println!("⚠️ Error checking worker {} process: {}", i + 1, e);
                        return false;
                    }
                }
            }
            
            true
        }

        /// Capture and display process output for debugging
        fn capture_process_output(process: &mut Child, process_name: &str) {
            if let Some(mut stdout) = process.stdout.take() {
                let mut stdout_buf = Vec::new();
                if let Ok(_) = stdout.read_to_end(&mut stdout_buf) {
                    let stdout_str = String::from_utf8_lossy(&stdout_buf);
                    if !stdout_str.trim().is_empty() {
                        println!("📄 {} stdout:\n{}", process_name, stdout_str);
                    }
                }
            }

            if let Some(mut stderr) = process.stderr.take() {
                let mut stderr_buf = Vec::new();
                if let Ok(_) = stderr.read_to_end(&mut stderr_buf) {
                    let stderr_str = String::from_utf8_lossy(&stderr_buf);
                    if !stderr_str.trim().is_empty() {
                        println!("❌ {} stderr:\n{}", process_name, stderr_str);
                    }
                }
            }
        }

        /// Check if cluster is ready to accept queries
        async fn is_cluster_ready(&self) -> bool {
            use std::net::TcpStream;
            
            // Try to connect to proxy
            let addr = format!("127.0.0.1:{}", PROXY_PORT);
            
            match TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(2)) {
                Ok(_) => {
                    // Also check that we can connect to at least one worker
                    self.can_connect_to_workers()
                },
                Err(_) => false,
            }
        }

        /// Check if we can connect to workers
        fn can_connect_to_workers(&self) -> bool {
            use std::net::TcpStream;
            
            let worker_ports = [WORKER_PORT_1, WORKER_PORT_2];
            let mut connected_workers = 0;
            
            for port in worker_ports {
                let addr = format!("127.0.0.1:{}", port);
                if TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(1)).is_ok() {
                    connected_workers += 1;
                }
            }
            
            // We need at least one worker to be ready
            connected_workers > 0
        }

        /// Get the proxy address for client connections
        fn get_proxy_address(&self) -> String {
            format!("127.0.0.1:{}", PROXY_PORT)
        }
    }

    impl Drop for ClusterManager {
        fn drop(&mut self) {
            println!("🧹 Cleaning up cluster...");
            
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
            
            // Final cleanup - kill any remaining processes
            let _ = Command::new("pkill")
                .args(&["-f", "distributed-datafusion"])
                .output();
            
            println!("✅ Cluster cleanup complete");
        }
    }

    impl ValidationResults {
        fn new() -> Self {
            Self {
                total_queries: 0,
                passed_queries: 0,
                failed_queries: 0,
                skipped_queries: 0,
                results: Vec::new(),
                total_time: Duration::default(),
            }
        }

        fn success_rate(&self) -> f64 {
            if self.total_queries == 0 {
                0.0
            } else {
                (self.passed_queries as f64 / self.total_queries as f64) * 100.0
            }
        }

        fn print_summary(&self) {
            println!("\n🏁 TPC-H Validation Summary");
            println!("==========================");
            println!("📊 Total queries: {}", self.total_queries);
            println!("✅ Passed: {}", self.passed_queries);
            println!("❌ Failed: {}", self.failed_queries);
            println!("⏭️  Skipped: {}", self.skipped_queries);
            println!("📈 Success rate: {:.1}%", self.success_rate());
            println!("⏱️  Total time: {:.2}s", self.total_time.as_secs_f64());
            
            if self.failed_queries > 0 {
                println!("\n❌ Failed queries:");
                for result in &self.results {
                    if !result.matches {
                        println!("   {} - {}", result.query_name, 
                            result.error_message.as_deref().unwrap_or("Results differ"));
                    }
                }
            }
        }
    }

    /// Create a SessionContext with all TPC-H tables registered
    async fn create_tpch_context() -> Result<SessionContext, Box<dyn std::error::Error>> {
        let session_config = SessionConfig::default();
        let ctx = SessionContext::new_with_config(session_config);
        
        // Define all TPC-H tables
        let tables = vec![
            "customer", "lineitem", "nation", "orders", 
            "part", "partsupp", "region", "supplier"
        ];

        for table in tables {
            let table_path = format!("{}/{}.parquet", TPCH_DATA_PATH, table);
            ctx.register_parquet(table, &table_path, ParquetReadOptions::default()).await
                .map_err(|e| format!("Failed to register table {}: {}", table, e))?;
        }

        // Create revenue0 view required for q15 (following validate_tpch_correctness.sh)
        let revenue0_view_sql = "CREATE VIEW revenue0 (supplier_no, total_revenue) AS
SELECT
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
FROM
    lineitem
WHERE
    l_shipdate >= date '1996-08-01'
    AND l_shipdate < date '1996-08-01' + interval '3' month
GROUP BY
    l_suppkey";
        
        ctx.sql(revenue0_view_sql).await
            .map_err(|e| format!("Failed to create revenue0 view: {}", e))?
            .collect().await
            .map_err(|e| format!("Failed to execute revenue0 view creation: {}", e))?;

        Ok(ctx)
    }

    /// Read a TPC-H query file
    fn read_query_file(query_name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let path = format!("{}/{}.sql", QUERY_PATH, query_name);
        let content = fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read {}: {}", path, e))?;
        Ok(content)
    }

    /// Execute a query using regular DataFusion
    async fn execute_query_datafusion(
        ctx: &SessionContext, 
        sql: &str, 
        query_name: &str
    ) -> Result<(Vec<RecordBatch>, Duration), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        
        let df = ctx.sql(sql).await
            .map_err(|e| format!("DataFusion SQL parsing failed for {}: {}", query_name, e))?;
        
        let batches = df.collect().await
            .map_err(|e| format!("DataFusion query execution failed for {}: {}", query_name, e))?;
        
        let execution_time = start_time.elapsed();
        Ok((batches, execution_time))
    }

    /// Execute a query using distributed DataFusion
    async fn execute_query_distributed(
        cluster: &ClusterManager,
        sql: &str, 
        query_name: &str
    ) -> Result<(String, Duration), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        
        // Write SQL to temporary file (revenue0 view now handled via DFRAY_VIEWS env var)
        let temp_sql_file = format!("/tmp/{}_query.sql", query_name);
        fs::write(&temp_sql_file, sql)
            .map_err(|e| format!("Failed to write SQL file for {}: {}", query_name, e))?;
        
        // Create a temporary Python script to execute the query
        let temp_python_script = format!("/tmp/{}_query.py", query_name);
        let python_script = format!(r#"
import adbc_driver_flightsql.dbapi as dbapi
import duckdb
import sys
import time

try:
    # Connect to the distributed cluster
    conn = dbapi.connect("grpc://localhost:{}")
    cur = conn.cursor()
    
    # Read and execute the SQL query
    with open('{}', 'r') as f:
        sql = f.read()
    
    start_time = time.time()
    cur.execute(sql)
    reader = cur.fetch_record_batch()
    
    # Convert results to string using DuckDB for consistent formatting
    results = duckdb.sql("select * from reader")
    
    # Fetch all results before closing connections
    all_rows = results.fetchall()
    
    # Close cursor and connection properly
    cur.close()
    conn.close()
    
    # Print results in a format that can be parsed
    print("--- RESULTS START ---")
    for row in all_rows:
        print("|" + "|".join([str(cell) if cell is not None else "NULL" for cell in row]) + "|")
    print("--- RESULTS END ---")
    
except Exception as e:
    print(f"Error executing distributed query: {{str(e)}}", file=sys.stderr)
    sys.exit(1)
"#, cluster.get_proxy_address().split(':').last().unwrap(), temp_sql_file);
        
        fs::write(&temp_python_script, python_script)
            .map_err(|e| format!("Failed to write Python script for {}: {}", query_name, e))?;
        
        // Execute using Python Flight SQL client
        let output = Command::new("python3")
            .args(&[&temp_python_script])
            .output()
            .map_err(|e| format!("Failed to execute distributed query for {}: {}", query_name, e))?;
        
        // Clean up temp files
        let _ = fs::remove_file(&temp_sql_file);
        let _ = fs::remove_file(&temp_python_script);
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Distributed query failed for {}: {}", query_name, stderr).into());
        }
        
        let result = String::from_utf8_lossy(&output.stdout).to_string();
        let execution_time = start_time.elapsed();
        
        Ok((result, execution_time))
    }

    /// Convert record batches to sorted strings for comparison (following validate_tpch_correctness.sh approach)
    fn batches_to_sorted_strings(batches: &[RecordBatch]) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut rows = Vec::new();
        
        if batches.is_empty() {
            return Ok(rows);
        }
        
        let formatted = pretty_format_batches(batches)
            .map_err(|e| format!("Failed to format batches: {}", e))?;
        
        let formatted_str = formatted.to_string();
        let lines: Vec<&str> = formatted_str.lines().collect();
        
        // Debug: print the raw DataFusion output (uncomment for debugging)
        // println!("🔍 DEBUG: Raw DataFusion output:");
        // for (i, line) in lines.iter().enumerate() {
        //     println!("  [{}] '{}'", i, line);
        // }
        
        let mut in_table = false;
        let mut header_found = false;
        
        for (_line_idx, line) in lines.iter().enumerate() {
            let trimmed = line.trim();
            
            // Detect table boundaries (+---+)
            if trimmed.starts_with('+') && trimmed.contains('-') && trimmed.ends_with('+') {
                if !in_table {
                    // Start of table
                    in_table = true;
                    header_found = false;
                    // println!("🔍 DEBUG: Start of table detected at line {}", line_idx);
                } else if header_found && !rows.is_empty() {
                    // End of table - we've seen header and collected some data
                    // println!("🔍 DEBUG: End of table detected at line {} (after collecting {} rows)", line_idx, rows.len());
                    break;
                } else if header_found {
                    // This is the header separator line - continue processing
                    // println!("🔍 DEBUG: Header separator detected at line {}", line_idx);
                }
                continue;
            }
            
            // Process lines within table
            if in_table && trimmed.starts_with('|') && trimmed.ends_with('|') {
                // Remove | boundaries and split into fields
                if let Some(content) = trimmed.strip_prefix('|').and_then(|s| s.strip_suffix('|')) {
                    let fields: Vec<&str> = content.split('|').map(|f| f.trim()).collect();
                    
                    // println!("🔍 DEBUG: Processing table row at line {}: fields = {:?}", line_idx, fields);
                    
                    // Check if this is a header line (column names)
                    // Header lines contain mostly alphabetic column names
                    let is_header = fields.iter().all(|field| {
                        !field.is_empty() && field.chars().all(|c| c.is_alphabetic() || c == '_' || c == '(' || c == ')' || c == '.')
                    });
                    
                    // println!("🔍 DEBUG: is_header = {}, header_found = {}", is_header, header_found);
                    
                    if is_header && !header_found {
                        header_found = true;
                        // println!("🔍 DEBUG: Header row detected and skipped");
                        continue;
                    } else if header_found {
                        // This is a data row
                        let clean_fields: Vec<String> = fields.iter().map(|f| f.trim().to_string()).collect();
                        
                        // Check if all fields are empty (NULL result)
                        if clean_fields.iter().all(|f| f.is_empty()) {
                            // println!("🔍 DEBUG: Adding NULL row");
                            rows.push("NULL".to_string());
                        } else {
                            let row_data = clean_fields.join("|");
                            // println!("🔍 DEBUG: Adding data row: '{}'", row_data);
                            rows.push(row_data);
                        }
                    }
                }
            }
        }
        
        // Handle empty table with header but no data
        if in_table && header_found && rows.is_empty() {
            // println!("🔍 DEBUG: Empty table detected, adding NULL");
            rows.push("NULL".to_string());
        }
        
        // println!("🔍 DEBUG: Final rows count: {}", rows.len());
        // for (i, row) in rows.iter().enumerate() {
        //     println!("  [{}] '{}'", i, row);
        // }
        
        rows.sort();
        Ok(rows)
    }

    /// Parse distributed query output to sorted strings
    fn distributed_output_to_sorted_strings(output: &str) -> Vec<String> {
        let mut rows = Vec::new();
        
        // Filter out warnings and empty lines, following validate_tpch_correctness.sh approach
        let lines: Vec<&str> = output.lines()
            .filter(|line| {
                let trimmed = line.trim();
                !trimmed.is_empty() && 
                !trimmed.starts_with("Warning:") && 
                !trimmed.contains("warnings.warn")
            })
            .collect();
        
        if lines.is_empty() {
            return rows;
        }
        
        // Check if first line looks like column headers (mostly words separated by spaces)
        let first_line = lines[0].trim();
        let fields: Vec<&str> = first_line.split_whitespace().collect();
        let is_header = fields.len() > 1 && fields.iter().all(|field| {
            field.chars().all(|c| c.is_alphabetic() || c == '_')
        });
        
        let start_idx = if is_header { 1 } else { 0 };
        
        // Process data rows (skip header if detected)
        for line in &lines[start_idx..] {
            let trimmed = line.trim();
            
            // Handle various output formats
            if trimmed.starts_with('|') && trimmed.ends_with('|') {
                // Table format: |col1|col2|col3|
                if let Some(data_part) = trimmed.strip_prefix('|').and_then(|s| s.strip_suffix('|')) {
                    let data = data_part.trim();
                    if !data.is_empty() {
                        rows.push(data.to_string());
                    }
                }
            } else if !trimmed.is_empty() && (trimmed.contains(char::is_numeric) || trimmed.contains('|')) {
                // DataFrame format or other data rows
                rows.push(trimmed.to_string());
            }
        }
        
        rows.sort();
        rows
    }

    /// Compare two sets of results with tolerance for floating-point differences
    fn compare_results(
        query_name: &str,
        datafusion_batches: &[RecordBatch],
        distributed_output: &str,
        datafusion_time: Duration,
        distributed_time: Duration,
    ) -> ComparisonResult {
        let df_rows = match batches_to_sorted_strings(datafusion_batches) {
            Ok(rows) => rows,
            Err(e) => {
                return ComparisonResult {
                    query_name: query_name.to_string(),
                    matches: false,
                    row_count_datafusion: 0,
                    row_count_distributed: 0,
                    error_message: Some(format!("Failed to format DataFusion output: {}", e)),
                    execution_time_datafusion: datafusion_time,
                    execution_time_distributed: distributed_time,
                };
            }
        };
        
        let dist_rows = distributed_output_to_sorted_strings(distributed_output);
        
        let row_count_datafusion = df_rows.len();
        let row_count_distributed = dist_rows.len();
        
        // Always log the actual results for debugging
        println!("\n🔍 DETAILED COMPARISON FOR {}:", query_name.to_uppercase());
        println!("📊 DataFusion Results ({} rows):", row_count_datafusion);
        for (i, row) in df_rows.iter().enumerate() {
            println!("  [{}] {}", i, row);
        }
        
        println!("🌐 Distributed Results ({} rows):", row_count_distributed);
        for (i, row) in dist_rows.iter().enumerate() {
            println!("  [{}] {}", i, row);
        }
        
        println!("📈 Raw Distributed Output:");
        println!("{}", distributed_output);
        
        // Check row count first
        if row_count_datafusion != row_count_distributed {
            let mut detailed_error = format!(
                "Row count mismatch: DataFusion={}, Distributed={}\n\n", 
                row_count_datafusion, row_count_distributed
            );
            
            // Show which rows are missing/extra
            if row_count_datafusion > row_count_distributed {
                detailed_error.push_str("Missing rows in Distributed (present in DataFusion only):\n");
                for (i, df_row) in df_rows.iter().enumerate() {
                    if i >= dist_rows.len() || !dist_rows.contains(df_row) {
                        detailed_error.push_str(&format!("  [{}] {}\n", i, df_row));
                    }
                }
            } else {
                detailed_error.push_str("Extra rows in Distributed (not in DataFusion):\n");
                for (i, dist_row) in dist_rows.iter().enumerate() {
                    if i >= df_rows.len() || !df_rows.contains(dist_row) {
                        detailed_error.push_str(&format!("  [{}] {}\n", i, dist_row));
                    }
                }
            }
            
            return ComparisonResult {
                query_name: query_name.to_string(),
                matches: false,
                row_count_datafusion,
                row_count_distributed,
                error_message: Some(detailed_error),
                execution_time_datafusion: datafusion_time,
                execution_time_distributed: distributed_time,
            };
        }
        
        // Compare rows with floating-point tolerance
        for (i, (df_row, dist_row)) in df_rows.iter().zip(dist_rows.iter()).enumerate() {
            if !compare_rows_with_tolerance(df_row, dist_row, FLOATING_POINT_TOLERANCE) {
                return ComparisonResult {
                    query_name: query_name.to_string(),
                    matches: false,
                    row_count_datafusion,
                    row_count_distributed,
                    error_message: Some(format!("Row {} differs:\n  DataFusion: '{}'\n  Distributed: '{}'", i, df_row, dist_row)),
                    execution_time_datafusion: datafusion_time,
                    execution_time_distributed: distributed_time,
                };
            }
        }
        
        println!("✅ Results match perfectly!");
        
        ComparisonResult {
            query_name: query_name.to_string(),
            matches: true,
            row_count_datafusion,
            row_count_distributed,
            error_message: None,
            execution_time_datafusion: datafusion_time,
            execution_time_distributed: distributed_time,
        }
    }

    /// Compare two row strings with tolerance for floating-point differences
    fn compare_rows_with_tolerance(row1: &str, row2: &str, tolerance: f64) -> bool {
        let fields1: Vec<&str> = row1.split('|').map(|s| s.trim()).collect();
        let fields2: Vec<&str> = row2.split('|').map(|s| s.trim()).collect();
        
        if fields1.len() != fields2.len() {
            return false;
        }
        
        for (field1, field2) in fields1.iter().zip(fields2.iter()) {
            // Handle NULL values
            if field1.to_uppercase() == "NULL" || field2.to_uppercase() == "NULL" {
                if field1.to_uppercase() != field2.to_uppercase() {
                    return false;
                }
                continue;
            }
            
            // Try to parse as numbers and compare with tolerance
            if let (Ok(num1), Ok(num2)) = (field1.parse::<f64>(), field2.parse::<f64>()) {
                let diff = (num1 - num2).abs();
                let rel_diff = if num1.abs().max(num2.abs()) > 1e-9 {
                    diff / num1.abs().max(num2.abs())
                } else {
                    diff
                };
                
                if rel_diff > tolerance {
                    return false;
                }
            } else {
                // Compare as strings
                if field1 != field2 {
                    return false;
                }
            }
        }
        
        true
    }

    /// Get list of TPC-H query files
    fn get_tpch_queries() -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut queries = Vec::new();
        
        for i in 1..=22 {
            let query_name = format!("q{}", i);
            let query_path = format!("{}/{}.sql", QUERY_PATH, query_name);
            
            if Path::new(&query_path).exists() {
                queries.push(query_name);
            }
        }
        
        if queries.is_empty() {
            return Err(format!("No TPC-H query files found in {}", QUERY_PATH).into());
        }
        
        queries.sort();
        Ok(queries)
    }

    /// Main validation test that runs all TPC-H queries
    /// 
    /// This test is completely self-contained and handles:
    /// - Cluster setup and teardown
    /// - TPC-H data generation
    /// - Dependency installation
    /// - Result comparison and reporting
    #[tokio::test]
    async fn test_tpch_validation_all_queries() {
        println!("🎯 Starting comprehensive TPC-H validation test");
        
        // Setup cluster and environment
        let cluster = match ClusterManager::setup().await {
            Ok(cluster) => cluster,
            Err(e) => {
                panic!("❌ Failed to setup test environment: {}", e);
            }
        };
        
        let start_time = Instant::now();
        let mut results = ValidationResults::new();
        
        // Create DataFusion context
        let ctx = match create_tpch_context().await {
            Ok(ctx) => ctx,
            Err(e) => {
                panic!("❌ Failed to create TPC-H context: {}", e);
            }
        };
        
        // Get query list
        let queries = match get_tpch_queries() {
            Ok(queries) => queries,
            Err(e) => {
                panic!("❌ Failed to get TPC-H queries: {}", e);
            }
        };
        
        results.total_queries = queries.len();
        println!("📋 Found {} TPC-H queries to validate", results.total_queries);
        
        // Run each query
        for query_name in &queries {
            println!("\n🔍 Validating query: {}", query_name);
            
            // Read query SQL
            let sql = match read_query_file(query_name) {
                Ok(sql) => sql,
                Err(e) => {
                    println!("  ❌ Failed to read query file: {}", e);
                    results.skipped_queries += 1;
                    continue;
                }
            };
            
            // Execute with DataFusion
            println!("  📊 Running with DataFusion...");
            let (datafusion_batches, datafusion_time) = match execute_query_datafusion(&ctx, &sql, query_name).await {
                Ok(result) => result,
                Err(e) => {
                    println!("  ❌ DataFusion execution failed: {}", e);
                    results.failed_queries += 1;
                    results.results.push(ComparisonResult {
                        query_name: query_name.clone(),
                        matches: false,
                        row_count_datafusion: 0,
                        row_count_distributed: 0,
                        error_message: Some(format!("DataFusion execution failed: {}", e)),
                        execution_time_datafusion: Duration::default(),
                        execution_time_distributed: Duration::default(),
                    });
                    continue;
                }
            };
            
            // Execute with distributed system
            println!("  🌐 Running with distributed DataFusion...");
            let (distributed_output, distributed_time) = match execute_query_distributed(&cluster, &sql, query_name).await {
                Ok(result) => result,
                Err(e) => {
                    println!("  ❌ Distributed execution failed: {}", e);
                    results.failed_queries += 1;
                    results.results.push(ComparisonResult {
                        query_name: query_name.clone(),
                        matches: false,
                        row_count_datafusion: datafusion_batches.iter().map(|b| b.num_rows()).sum(),
                        row_count_distributed: 0,
                        error_message: Some(format!("Distributed execution failed: {}", e)),
                        execution_time_datafusion: datafusion_time,
                        execution_time_distributed: Duration::default(),
                    });
                    continue;
                }
            };
            
            // Compare results
            println!("  🔄 Comparing results...");
            let comparison = compare_results(
                query_name,
                &datafusion_batches,
                &distributed_output,
                datafusion_time,
                distributed_time,
            );
            
            if comparison.matches {
                println!("  ✅ Results match! ({}/{} rows, DataFusion: {:.2}s, Distributed: {:.2}s)", 
                    comparison.row_count_datafusion,
                    comparison.row_count_distributed,
                    comparison.execution_time_datafusion.as_secs_f64(),
                    comparison.execution_time_distributed.as_secs_f64());
                results.passed_queries += 1;
            } else {
                println!("  ❌ Results differ: {}", comparison.error_message.as_deref().unwrap_or("Unknown"));
                results.failed_queries += 1;
            }
            
            results.results.push(comparison);
        }
        
        results.total_time = start_time.elapsed();
        results.print_summary();
        
        // Note: Cluster cleanup happens automatically via Drop trait
        
        // For CI: fail the test if any queries failed
        if results.failed_queries > 0 {
            panic!("TPC-H validation failed: {} out of {} queries failed", 
                  results.failed_queries, results.total_queries);
        }
        
        println!("\n🎉 All TPC-H validation tests passed successfully!");
    }

    /// Test a single TPC-H query (useful for debugging)
    /// 
    /// This test is marked with #[ignore] - use `cargo test --ignored` to run it.
    /// Modify the query_name to test different queries.
    #[tokio::test]
    #[ignore]
    async fn test_tpch_validation_single_query() {
        let query_name = "q1"; // Change this to test different queries
        
        println!("🔍 Testing single query: {}", query_name);
        
        // Setup cluster
        let cluster = ClusterManager::setup().await.expect("Failed to setup cluster");
        
        // Create contexts
        let ctx = create_tpch_context().await.expect("Failed to create context");
        let sql = read_query_file(query_name).expect("Failed to read query");
        
        // Execute both versions
        let (df_batches, df_time) = execute_query_datafusion(&ctx, &sql, query_name).await
            .expect("DataFusion execution failed");
        
        let (dist_output, dist_time) = execute_query_distributed(&cluster, &sql, query_name).await
            .expect("Distributed execution failed");
        
        // Compare
        let comparison = compare_results(query_name, &df_batches, &dist_output, df_time, dist_time);
        
        println!("Result: {}", if comparison.matches { "✅ PASSED" } else { "❌ FAILED" });
        if let Some(error) = &comparison.error_message {
            println!("Error: {}", error);
        }
        println!("DataFusion time: {:.2}s", comparison.execution_time_datafusion.as_secs_f64());
        println!("Distributed time: {:.2}s", comparison.execution_time_distributed.as_secs_f64());
        
        assert!(comparison.matches, "Query {} validation failed: {}", 
               query_name, comparison.error_message.unwrap_or_default());
    }
} 