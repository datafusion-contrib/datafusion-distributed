use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::config::SessionConfig;
use datafusion::prelude::*;

/// Test configuration constants
pub const PROXY_PORT: u16 = 40400;
pub const FIRST_WORKER_PORT: u16 = 40401;
pub const NUM_WORKERS: usize = 2;
pub const TPCH_DATA_PATH: &str = "/tmp/tpch_s1";
pub const QUERY_PATH: &str = "./tpch/queries";
pub const FLOATING_POINT_TOLERANCE: f64 = 1e-5;
pub const SETUP_TIMEOUT_SECONDS: u64 = 60;

// Output verbosity configuration
pub const MAX_ROWS_TO_PRINT: usize = 2;
pub const VERBOSE_COMPARISON: bool = false;
pub const SHOW_RAW_DISTRIBUTED_OUTPUT: bool = false;

// Timing configuration
pub const PROCESS_KILL_WAIT_SECONDS: u64 = 1;
pub const WORKER_STARTUP_WAIT_SECONDS: u64 = 1;
pub const PROXY_STARTUP_WAIT_SECONDS: u64 = 1;
pub const CLUSTER_READY_POLL_INTERVAL_MS: u64 = 500;

/// Helper function to enable verbose mode for specific queries during debugging
pub fn should_be_verbose(_query_name: &str) -> bool {
    // Enable verbose mode for specific queries you want to debug
    // Example: VERBOSE_COMPARISON || _query_name == "q16"
    VERBOSE_COMPARISON
}

/// Get worker port for a given worker index (0-based)
pub fn get_worker_port(worker_index: usize) -> u16 {
    FIRST_WORKER_PORT + worker_index as u16
}

/// Get all worker ports
pub fn get_all_worker_ports() -> Vec<u16> {
    (0..NUM_WORKERS).map(get_worker_port).collect()
}

/// Validation results
#[derive(Debug)]
pub struct ValidationResults {
    pub total_queries: usize,
    pub passed_queries: usize,
    pub failed_queries: usize,
    pub results: Vec<ComparisonResult>,
    pub total_time: Duration,
}

/// Individual query comparison result
#[derive(Debug, Clone)]
pub struct ComparisonResult {
    pub query_name: String,
    pub matches: bool,
    pub row_count_datafusion: usize,
    pub row_count_distributed: usize,
    pub error_message: Option<String>,
    pub execution_time_datafusion: Duration,
    pub execution_time_distributed: Duration,
}

/// Distributed cluster manager
pub struct ClusterManager {
    pub proxy_process: Option<Child>,
    pub worker_processes: Vec<Child>,
    pub worker_ports: Vec<u16>,
    pub is_running: Arc<AtomicBool>,
    pub reusable_python_script: Option<String>,
}

impl ClusterManager {
    pub fn new() -> Self {
        Self {
            proxy_process: None,
            worker_processes: Vec::new(),
            worker_ports: get_all_worker_ports(),
            is_running: Arc::new(AtomicBool::new(false)),
            reusable_python_script: None,
        }
    }

    /// Helper function to run a command and return output with error handling
    pub fn run_command_with_output(
        command: &str,
        args: &[&str],
        operation_desc: &str,
    ) -> Result<std::process::Output, Box<dyn std::error::Error>> {
        let output = Command::new(command)
            .args(args)
            .output()
            .map_err(|e| format!("Failed to {}: {}", operation_desc, e))?;

        if !output.status.success() {
            return Err(format!(
                "{} failed: {}",
                operation_desc,
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(output)
    }

    /// Helper function to check if a command exists (returns true if it succeeds)
    pub fn command_exists(command: &str, args: &[&str]) -> bool {
        Command::new(command).args(args).output().is_ok()
    }

    /// Helper function to run Python commands
    pub fn run_python_command(
        args: &[&str],
        operation_desc: &str,
    ) -> Result<std::process::Output, Box<dyn std::error::Error>> {
        Self::run_command_with_output("python3", args, operation_desc)
    }

    /// Helper function to run Cargo commands
    pub fn run_cargo_command(
        args: &[&str],
        operation_desc: &str,
    ) -> Result<std::process::Output, Box<dyn std::error::Error>> {
        Self::run_command_with_output("cargo", args, operation_desc)
    }

    /// Helper function to spawn a process with common configuration
    pub fn spawn_process(
        binary_path: &str,
        args: &[&str],
        env_vars: &[(&str, &str)],
        operation_desc: &str,
    ) -> Result<Child, Box<dyn std::error::Error>> {
        let mut cmd = Command::new(binary_path);
        cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());

        for (key, value) in env_vars {
            cmd.env(key, value);
        }

        cmd.spawn()
            .map_err(|e| format!("Failed to {}: {}", operation_desc, e).into())
    }

    /// Helper function to write content to a temporary file
    pub fn write_temp_file(
        filename: &str,
        content: &str,
        operation_desc: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let temp_path = format!("/tmp/{}", filename);
        fs::write(&temp_path, content)
            .map_err(|e| format!("Failed to write {} file: {}", operation_desc, e))?;
        Ok(temp_path)
    }

    /// Helper function to clean up temporary files
    pub fn cleanup_temp_files(files: &[&str]) {
        for file in files {
            let _ = fs::remove_file(file);
        }
    }

    /// Generate the reusable Python script for executing distributed queries
    pub fn generate_reusable_python_script(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let python_script = format!(
            r#"
import adbc_driver_flightsql.dbapi as dbapi
import duckdb
import sys
import time

if len(sys.argv) != 2:
    print("Usage: python script.py <sql_file_path>", file=sys.stderr)
    sys.exit(1)

sql_file_path = sys.argv[1]

try:
    # Connect to the distributed cluster
    conn = dbapi.connect("grpc://localhost:{}")
    cur = conn.cursor()
    
    # Read and execute the SQL query
    with open(sql_file_path, 'r') as f:
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
"#,
            self.get_proxy_address().split(':').last().unwrap()
        );

        let script_path = Self::write_temp_file(
            "reusable_distributed_query.py",
            &python_script,
            "reusable Python script for distributed queries",
        )?;

        self.reusable_python_script = Some(script_path);
        Ok(())
    }

    /// Setup everything needed for tests
    pub async fn setup() -> Result<Self, Box<dyn std::error::Error>> {
        let mut cluster = ClusterManager::new();

        // Step 1: Kill existing processes
        cluster.kill_existing_processes()?;

        // Step 2: Install tpchgen-cli if needed
        cluster.ensure_tpchgen_cli().await?;

        // Step 3: Generate TPC-H data if needed
        cluster.ensure_tpch_data().await?;

        // Step 4: Install Python Flight SQL packages if needed
        cluster.ensure_python_packages().await?;

        // Step 5: Start distributed cluster
        cluster.start_cluster().await?;

        // Step 6: Wait for cluster to be ready
        cluster.wait_for_cluster_ready().await?;

        // Step 7: Generate reusable Python script
        cluster.generate_reusable_python_script()?;

        Ok(cluster)
    }

    /// Kill any existing processes on our specific test ports only
    pub fn kill_existing_processes(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🧹 Cleaning up existing processes on test ports...");

        let mut ports = vec![PROXY_PORT];
        ports.extend(&self.worker_ports);

        for port in &ports {
            // Find and kill processes using lsof
            if let Ok(output) = Command::new("lsof")
                .args(&["-ti", &format!(":{}", port)])
                .output()
            {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    if let Ok(pid) = line.trim().parse::<u32>() {
                        let _ = Command::new("kill")
                            .args(&["-9", &pid.to_string()])
                            .output();
                        println!("  🔥 Killed process {} on port {}", pid, port);
                    }
                }
            }
        }

        // Give processes time to die
        thread::sleep(Duration::from_secs(PROCESS_KILL_WAIT_SECONDS));

        Ok(())
    }

    /// Ensure tpchgen-cli is installed
    pub async fn ensure_tpchgen_cli(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🔧 Checking for tpchgen-cli...");

        // Check if already installed
        if Self::command_exists("tpchgen-cli", &["--version"]) {
            println!("✅ tpchgen-cli already installed");
            return Ok(());
        }

        println!("📦 Installing tpchgen-cli...");
        let _output = Self::run_cargo_command(&["install", "tpchgen-cli"], "install tpchgen-cli")?;

        println!("✅ tpchgen-cli installed successfully");
        Ok(())
    }

    /// Ensure TPC-H data exists
    pub async fn ensure_tpch_data(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🔧 Checking for TPC-H data at {}...", TPCH_DATA_PATH);

        // Check if all required tables exist
        let required_tables = [
            "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
        ];

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

        let _output = Self::run_command_with_output(
            "tpchgen-cli",
            &[
                "--scale-factor",
                "1",
                "--format",
                "parquet",
                "--output-dir",
                TPCH_DATA_PATH,
            ],
            "generate TPC-H data",
        )?;

        println!("✅ TPC-H data generated successfully");
        Ok(())
    }

    /// Ensure Python Flight SQL packages are installed
    pub async fn ensure_python_packages(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🔧 Checking Python Flight SQL packages...");

        // Check if packages are already installed
        let check_cmd = Command::new("python3")
            .args(&["-c", "import adbc_driver_manager; import adbc_driver_flightsql; import duckdb; import pyarrow; print('OK')"])
            .output();

        if let Ok(output) = check_cmd {
            if output.status.success() && String::from_utf8_lossy(&output.stdout).contains("OK") {
                println!("✅ Python packages already installed");
                return Ok(());
            }
        }

        println!("📦 Installing Python Flight SQL packages...");
        let install_cmd = Self::run_python_command(
            &[
                "-m",
                "pip",
                "install",
                "adbc_driver_manager",
                "adbc_driver_flightsql",
                "duckdb",
                "pyarrow",
            ],
            "install Python packages",
        )?;

        if !install_cmd.status.success() {
            return Err(format!(
                "Python packages installation failed: {}",
                String::from_utf8_lossy(&install_cmd.stderr)
            )
            .into());
        }

        println!("✅ Python packages installed successfully");
        Ok(())
    }

    /// Start the distributed cluster
    pub async fn start_cluster(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 Starting distributed DataFusion cluster...");

        // Try to use Cargo's built-in binary path, fallback to standard debug path
        let binary_path = std::env::var("CARGO_BIN_EXE_distributed-datafusion")
            .unwrap_or_else(|_| "./target/debug/distributed-datafusion".to_string());

        // Only build if the binary doesn't exist
        if !Path::new(&binary_path).exists() {
            println!("📦 Building distributed-datafusion binary (not found)...");
            let _build_result = Self::run_cargo_command(
                &["build", "--bin", "distributed-datafusion"],
                "build distributed-datafusion",
            )?;
        } else {
            println!("✅ Using existing distributed-datafusion binary");
        }

        // Clone the path for use in subprocess commands
        let binary_path_str = binary_path.as_str();

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
            TPCH_DATA_PATH,
            TPCH_DATA_PATH,
            TPCH_DATA_PATH,
            TPCH_DATA_PATH,
            TPCH_DATA_PATH,
            TPCH_DATA_PATH,
            TPCH_DATA_PATH,
            TPCH_DATA_PATH
        );

        // Setup TPC-H views configuration (required for q15)
        let tpch_views = "CREATE VIEW revenue0 (supplier_no, total_revenue) AS SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) FROM lineitem WHERE l_shipdate >= date '1996-08-01' AND l_shipdate < date '1996-08-01' + interval '3' month GROUP BY l_suppkey";

        // Start workers first
        for (i, &port) in self.worker_ports.iter().enumerate() {
            println!("  Starting worker {} on port {}...", i + 1, port);
            let worker = Self::spawn_process(
                binary_path_str,
                &["--mode", "worker", "--port", &port.to_string()],
                &[("DFRAY_TABLES", &tpch_tables), ("DFRAY_VIEWS", &tpch_views)],
                &format!("start worker {}", i + 1),
            )?;
            self.worker_processes.push(worker);
        }

        // Give workers time to start
        thread::sleep(Duration::from_secs(WORKER_STARTUP_WAIT_SECONDS));

        // Start proxy
        println!("  Starting proxy on port {}...", PROXY_PORT);
        let worker_addresses = self
            .worker_ports
            .iter()
            .enumerate()
            .map(|(i, &port)| format!("worker{}/127.0.0.1:{}", i + 1, port))
            .collect::<Vec<_>>()
            .join(",");
        let proxy = Self::spawn_process(
            binary_path_str,
            &["--mode", "proxy", "--port", &PROXY_PORT.to_string()],
            &[
                ("DFRAY_WORKER_ADDRESSES", &worker_addresses),
                ("DFRAY_TABLES", &tpch_tables),
                ("DFRAY_VIEWS", &tpch_views),
            ],
            "start proxy",
        )?;

        self.proxy_process = Some(proxy);
        self.is_running.store(true, Ordering::Relaxed);

        println!("✅ Cluster started successfully");
        Ok(())
    }

    /// Wait for cluster to be ready
    pub async fn wait_for_cluster_ready(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("⏳ Waiting for cluster to be ready...");

        // First, give the proxy extra time to start after workers
        thread::sleep(Duration::from_secs(PROXY_STARTUP_WAIT_SECONDS));

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
                println!(
                    "  Still waiting... (attempt {} after {:.1}s)",
                    attempt,
                    start_time.elapsed().as_secs_f64()
                );
            }

            thread::sleep(Duration::from_millis(CLUSTER_READY_POLL_INTERVAL_MS));
        }

        Err(format!(
            "Cluster failed to become ready within {} seconds (tried {} times)",
            SETUP_TIMEOUT_SECONDS, attempt
        )
        .into())
    }

    /// Check if all processes are still running
    pub fn are_processes_running(&mut self) -> bool {
        // Check proxy
        if let Some(proxy) = &mut self.proxy_process {
            match proxy.try_wait() {
                Ok(Some(exit_status)) => {
                    println!("⚠️ Proxy process has exited with status: {}", exit_status);
                    Self::capture_process_output(proxy, "proxy");
                    return false;
                }
                Ok(None) => {} // Still running
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
                    println!(
                        "⚠️ Worker {} process has exited with status: {}",
                        i + 1,
                        exit_status
                    );
                    Self::capture_process_output(worker, &format!("worker {}", i + 1));
                    return false;
                }
                Ok(None) => {} // Still running
                Err(e) => {
                    println!("⚠️ Error checking worker {} process: {}", i + 1, e);
                    return false;
                }
            }
        }

        true
    }

    /// Capture and display process output for debugging
    pub fn capture_process_output(process: &mut Child, process_name: &str) {
        if let Some(ref mut stdout) = process.stdout {
            let mut stdout_content = String::new();
            if stdout.read_to_string(&mut stdout_content).is_ok() && !stdout_content.is_empty() {
                println!("📋 {} stdout:\n{}", process_name, stdout_content);
            }
        }

        if let Some(ref mut stderr) = process.stderr {
            let mut stderr_content = String::new();
            if stderr.read_to_string(&mut stderr_content).is_ok() && !stderr_content.is_empty() {
                println!("⚠️ {} stderr:\n{}", process_name, stderr_content);
            }
        }
    }

    /// Check if cluster is ready to accept queries
    pub async fn is_cluster_ready(&self) -> bool {
        if !self.is_running.load(Ordering::Relaxed) {
            return false;
        }

        // Try to connect to proxy
        if !self.can_connect_to_workers() {
            return false;
        }

        // Also check that we can connect to at least one worker
        true
    }

    /// Check if we can connect to workers
    pub fn can_connect_to_workers(&self) -> bool {
        for &port in &self.worker_ports {
            if let Ok(stream) = std::net::TcpStream::connect(format!("127.0.0.1:{}", port)) {
                drop(stream);
                return true;
            }
        }
        false
    }

    /// Get the proxy address for connections
    pub fn get_proxy_address(&self) -> String {
        format!("127.0.0.1:{}", PROXY_PORT)
    }
}

impl Drop for ClusterManager {
    fn drop(&mut self) {
        if self.is_running.load(Ordering::Relaxed) {
            println!("🧹 Cleaning up cluster processes...");

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
            println!("✅ Cluster cleanup complete");
        }

        // Clean up reusable Python script
        if let Some(script_path) = &self.reusable_python_script {
            Self::cleanup_temp_files(&[script_path]);
        }
    }
}

impl ValidationResults {
    pub fn new() -> Self {
        Self {
            total_queries: 0,
            passed_queries: 0,
            failed_queries: 0,
            results: Vec::new(),
            total_time: Duration::default(),
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_queries == 0 {
            0.0
        } else {
            self.passed_queries as f64 / self.total_queries as f64 * 100.0
        }
    }

    pub fn print_summary(&self) {
        println!("\n📊 TPC-H Validation Summary:");
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!(
            "✅ Passed: {} / {} ({:.1}%)",
            self.passed_queries,
            self.total_queries,
            self.success_rate()
        );
        println!("❌ Failed: {}", self.failed_queries);
        println!("⏱️  Total time: {:.2}s", self.total_time.as_secs_f64());

        if self.failed_queries > 0 {
            println!("\n❌ Failed queries:");
            for result in &self.results {
                if !result.matches {
                    println!(
                        "   {} - {}",
                        result.query_name,
                        result.error_message.as_deref().unwrap_or("Unknown error")
                    );
                }
            }
        }

        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }
}

/// Create a TPC-H DataFusion context
pub async fn create_tpch_context() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(8192);

    let ctx = SessionContext::new_with_config(config);

    // Register TPC-H tables
    let tables = [
        ("customer", "customer.parquet"),
        ("lineitem", "lineitem.parquet"),
        ("nation", "nation.parquet"),
        ("orders", "orders.parquet"),
        ("part", "part.parquet"),
        ("partsupp", "partsupp.parquet"),
        ("region", "region.parquet"),
        ("supplier", "supplier.parquet"),
    ];

    for (table_name, filename) in &tables {
        let path = format!("{}/{}", TPCH_DATA_PATH, filename);
        ctx.register_parquet(*table_name, &path, Default::default())
            .await
            .map_err(|e| format!("Failed to register {}: {}", table_name, e))?;
    }

    // Create revenue0 view for q15
    let revenue0_view_sql = "CREATE VIEW revenue0 (supplier_no, total_revenue) AS SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) FROM lineitem WHERE l_shipdate >= date '1996-08-01' AND l_shipdate < date '1996-08-01' + interval '3' month GROUP BY l_suppkey";

    ctx.sql(revenue0_view_sql)
        .await
        .map_err(|e| format!("Failed to create revenue0 view: {}", e))?;

    Ok(ctx)
}

/// Read a TPC-H query file
pub fn read_query_file(query_name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let path = format!("{}/{}.sql", QUERY_PATH, query_name);
    let content =
        fs::read_to_string(&path).map_err(|e| format!("Failed to read {}: {}", path, e))?;
    Ok(content)
}

/// Execute a query using regular DataFusion
pub async fn execute_query_datafusion(
    ctx: &SessionContext,
    sql: &str,
    query_name: &str,
) -> Result<(Vec<RecordBatch>, Duration), Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    let df = ctx.sql(sql).await.map_err(|e| {
        format!(
            "DataFusion SQL parsing/planning failed for {}: {}",
            query_name, e
        )
    })?;

    let batches = df.collect().await.map_err(|e| {
        format!(
            "DataFusion query execution failed for {}: {}",
            query_name, e
        )
    })?;

    let execution_time = start_time.elapsed();
    Ok((batches, execution_time))
}

/// Execute a query using distributed DataFusion
pub async fn execute_query_distributed(
    cluster: &ClusterManager,
    sql: &str,
    query_name: &str,
) -> Result<(String, Duration), Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    // Write SQL to temporary file (revenue0 view now handled via DFRAY_VIEWS env var)
    let temp_sql_file = ClusterManager::write_temp_file(
        &format!("{}_query.sql", query_name),
        sql,
        &format!("SQL for {}", query_name),
    )?;

    // Get the reusable Python script
    let python_script_path = cluster
        .reusable_python_script
        .as_ref()
        .ok_or("Reusable Python script not available")?;

    // Execute using Python Flight SQL client with SQL file as argument
    let output = ClusterManager::run_python_command(
        &[python_script_path, &temp_sql_file],
        &format!("execute distributed query for {}", query_name),
    )?;

    // Clean up only the SQL temp file (keep the reusable Python script)
    ClusterManager::cleanup_temp_files(&[&temp_sql_file]);

    let result = String::from_utf8_lossy(&output.stdout).to_string();
    let execution_time = start_time.elapsed();

    Ok((result, execution_time))
}

/// Convert record batches to sorted strings for comparison
pub fn batches_to_sorted_strings(
    batches: &[RecordBatch],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();

    if batches.is_empty() {
        return Ok(rows);
    }

    let formatted =
        pretty_format_batches(batches).map_err(|e| format!("Failed to format batches: {}", e))?;

    let formatted_str = formatted.to_string();
    let lines: Vec<&str> = formatted_str.lines().collect();

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
            } else if header_found && !rows.is_empty() {
                // End of table - we've seen header and collected some data
                break;
            } else if header_found {
                // This is the header separator line - continue processing
            }
            continue;
        }

        // Process lines within table
        if in_table && trimmed.starts_with('|') && trimmed.ends_with('|') {
            // Remove | boundaries and split into fields
            if let Some(content) = trimmed.strip_prefix('|').and_then(|s| s.strip_suffix('|')) {
                let fields: Vec<&str> = content.split('|').map(|f| f.trim()).collect();

                // Check if this is a header line (column names)
                // Header lines contain mostly alphabetic column names
                let is_header = fields.iter().all(|field| {
                    !field.is_empty()
                        && field.chars().all(|c| {
                            c.is_alphabetic() || c == '_' || c == '(' || c == ')' || c == '.'
                        })
                });

                if is_header && !header_found {
                    header_found = true;
                    continue;
                } else if header_found {
                    // This is a data row
                    let clean_fields: Vec<String> =
                        fields.iter().map(|f| f.trim().to_string()).collect();

                    // Check if all fields are empty (NULL result)
                    if clean_fields.iter().all(|f| f.is_empty()) {
                        rows.push("NULL".to_string());
                    } else {
                        let row_data = clean_fields.join("|");
                        rows.push(row_data);
                    }
                }
            }
        }
    }

    // Handle empty table with header but no data
    if in_table && header_found && rows.is_empty() {
        rows.push("NULL".to_string());
    }

    rows.sort();
    Ok(rows)
}

/// Parse distributed query output to sorted strings
pub fn distributed_output_to_sorted_strings(output: &str) -> Vec<String> {
    let mut rows = Vec::new();

    // Filter out warnings and empty lines
    let lines: Vec<&str> = output
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty()
                && !trimmed.starts_with("Warning:")
                && !trimmed.contains("warnings.warn")
        })
        .collect();

    if lines.is_empty() {
        return rows;
    }

    // Check if first line looks like column headers
    let first_line = lines[0].trim();
    let fields: Vec<&str> = first_line.split_whitespace().collect();
    let is_header = fields.len() > 1
        && fields
            .iter()
            .all(|field| field.chars().all(|c| c.is_alphabetic() || c == '_'));

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
        } else if !trimmed.is_empty()
            && (trimmed.contains(char::is_numeric) || trimmed.contains('|'))
        {
            // DataFrame format or other data rows
            rows.push(trimmed.to_string());
        }
    }

    rows.sort();
    rows
}

/// Compare two sets of results with tolerance for floating-point differences
pub fn compare_results(
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

    // Only show detailed comparison when requested or for failures
    let verbose_mode = should_be_verbose(query_name);
    if verbose_mode {
        println!(
            "\n🔍 DETAILED COMPARISON FOR {}:",
            query_name.to_uppercase()
        );

        // Print limited rows for performance
        let max_rows = MAX_ROWS_TO_PRINT.min(row_count_datafusion);
        println!(
            "📊 DataFusion Results ({} rows, showing first {}):",
            row_count_datafusion, max_rows
        );
        for (i, row) in df_rows.iter().take(max_rows).enumerate() {
            println!("  [{}] {}", i, row);
        }
        if row_count_datafusion > max_rows {
            println!("  ... and {} more rows", row_count_datafusion - max_rows);
        }

        let max_rows = MAX_ROWS_TO_PRINT.min(row_count_distributed);
        println!(
            "🌐 Distributed Results ({} rows, showing first {}):",
            row_count_distributed, max_rows
        );
        for (i, row) in dist_rows.iter().take(max_rows).enumerate() {
            println!("  [{}] {}", i, row);
        }
        if row_count_distributed > max_rows {
            println!("  ... and {} more rows", row_count_distributed - max_rows);
        }

        if SHOW_RAW_DISTRIBUTED_OUTPUT {
            println!("📈 Raw Distributed Output:");
            println!("{}", distributed_output);
        }
    }

    // Check row count first
    if row_count_datafusion != row_count_distributed {
        let mut detailed_error = format!(
            "Row count mismatch: DataFusion={}, Distributed={}\n",
            row_count_datafusion, row_count_distributed
        );

        // For debugging, show sample rows when verbose mode is off
        if !verbose_mode {
            detailed_error.push_str("\n📊 Sample DataFusion rows:\n");
            for (i, row) in df_rows.iter().take(3).enumerate() {
                detailed_error.push_str(&format!("  [{}] {}\n", i, row));
            }
            detailed_error.push_str("\n🌐 Sample Distributed rows:\n");
            for (i, row) in dist_rows.iter().take(3).enumerate() {
                detailed_error.push_str(&format!("  [{}] {}\n", i, row));
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

    // Compare each row with tolerance
    for (i, (df_row, dist_row)) in df_rows.iter().zip(dist_rows.iter()).enumerate() {
        if !compare_rows_with_tolerance(df_row, dist_row, FLOATING_POINT_TOLERANCE) {
            let error_msg = format!(
                "Row {} mismatch:\n  DataFusion: {}\n  Distributed: {}",
                i, df_row, dist_row
            );

            return ComparisonResult {
                query_name: query_name.to_string(),
                matches: false,
                row_count_datafusion,
                row_count_distributed,
                error_message: Some(error_msg),
                execution_time_datafusion: datafusion_time,
                execution_time_distributed: distributed_time,
            };
        }
    }

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

/// Compare two rows with tolerance for floating-point differences
pub fn compare_rows_with_tolerance(row1: &str, row2: &str, tolerance: f64) -> bool {
    if row1 == row2 {
        return true;
    }

    let fields1: Vec<&str> = row1.split('|').collect();
    let fields2: Vec<&str> = row2.split('|').collect();

    if fields1.len() != fields2.len() {
        return false;
    }

    for (field1, field2) in fields1.iter().zip(fields2.iter()) {
        let f1 = field1.trim();
        let f2 = field2.trim();

        if f1 == f2 {
            continue;
        }

        // Try to parse as numbers and compare with tolerance
        if let (Ok(num1), Ok(num2)) = (f1.parse::<f64>(), f2.parse::<f64>()) {
            if (num1 - num2).abs() > tolerance {
                return false;
            }
        } else {
            // Non-numeric fields must match exactly
            return false;
        }
    }

    true
}

/// Get list of TPC-H queries
pub fn get_tpch_queries() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut queries = Vec::new();

    // Read directory and filter for SQL files
    let entries = fs::read_dir(QUERY_PATH)
        .map_err(|e| format!("Failed to read query directory {}: {}", QUERY_PATH, e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
        let path = entry.path();

        if let Some(filename) = path.file_name() {
            let filename_str = filename.to_string_lossy();
            if filename_str.ends_with(".sql") && filename_str.starts_with('q') {
                let query_name = filename_str.trim_end_matches(".sql").to_string();
                queries.push(query_name);
            }
        }
    }

    queries.sort();
    Ok(queries)
}

/// Setup the test environment (cluster + context)
pub async fn setup_test_environment(
) -> Result<(ClusterManager, SessionContext), Box<dyn std::error::Error>> {
    // Setup cluster and environment
    let cluster = ClusterManager::setup()
        .await
        .map_err(|e| format!("Failed to setup test environment: {}", e))?;

    // Create DataFusion context
    let ctx = create_tpch_context()
        .await
        .map_err(|e| format!("Failed to create TPC-H context: {}", e))?;

    Ok((cluster, ctx))
}

/// Create a comparison result for an error case
pub fn create_error_comparison_result(
    query_name: &str,
    error_message: String,
    datafusion_rows: usize,
    datafusion_time: Duration,
) -> ComparisonResult {
    ComparisonResult {
        query_name: query_name.to_string(),
        matches: false,
        row_count_datafusion: datafusion_rows,
        row_count_distributed: 0,
        error_message: Some(error_message),
        execution_time_datafusion: datafusion_time,
        execution_time_distributed: Duration::default(),
    }
}

/// Execute a single query validation and return the comparison result
pub async fn execute_single_query_validation(
    cluster: &ClusterManager,
    ctx: &SessionContext,
    query_name: &str,
) -> ComparisonResult {
    // Read query SQL
    let sql = match read_query_file(query_name) {
        Ok(sql) => sql,
        Err(e) => {
            return create_error_comparison_result(
                query_name,
                format!("Failed to read query file: {}", e),
                0,
                Duration::default(),
            );
        }
    };

    // Execute with distributed system
    let (distributed_output, distributed_time) =
        match execute_query_distributed(cluster, &sql, query_name).await {
            Ok(result) => result,
            Err(e) => {
                return ComparisonResult {
                    query_name: query_name.to_string(),
                    matches: false,
                    row_count_datafusion: 0,
                    row_count_distributed: 0,
                    error_message: Some(format!("Distributed execution failed: {}", e)),
                    execution_time_datafusion: Duration::default(),
                    execution_time_distributed: Duration::default(),
                };
            }
        };

    // Execute with DataFusion
    let (datafusion_batches, datafusion_time) =
        match execute_query_datafusion(ctx, &sql, query_name).await {
            Ok(result) => result,
            Err(e) => {
                // Get estimated row count from distributed output
                let distributed_row_count =
                    distributed_output_to_sorted_strings(&distributed_output).len();
                return ComparisonResult {
                    query_name: query_name.to_string(),
                    matches: false,
                    row_count_datafusion: 0,
                    row_count_distributed: distributed_row_count,
                    error_message: Some(format!("DataFusion execution failed: {}", e)),
                    execution_time_datafusion: Duration::default(),
                    execution_time_distributed: distributed_time,
                };
            }
        };

    // Compare results
    compare_results(
        query_name,
        &datafusion_batches,
        &distributed_output,
        datafusion_time,
        distributed_time,
    )
}
