use clap::Args as ClapArgs;
use clap::{Parser, Subcommand};
use datafusion::common::{DataFusionError, Result, internal_datafusion_err};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::physical_plan::collect;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::{
    ArrowFlightEndpoint, DistributedExt, DistributedPhysicalOptimizerRule,
    DistributedSessionBuilder, DistributedSessionBuilderContext, display_plan_ascii,
    explain_analyze,
};
use distributed_datafusion_controller::channel_resolver::DistributedChannelResolver;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use tonic::async_trait;
use tonic::transport::Server;

#[derive(Parser)]
#[command(name = "DataFusion Distributed Agent")]
#[command(about = "Runs Code on a Remote Machines")]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Serialize, Deserialize)]
/// YAML containing all the endpoints (IP + port pairs) of the datafusion distributed cluster.
struct ClusterConfig {
    endpoints: Vec<String>,
}

const DEFAULT_CONFIG_FILE: &str = "/etc/datafusion-distributed/config.yaml";
const DEFAULT_FIXTURES_DIR: &str = "/var/lib/datafusion-distributed/data";

#[derive(ClapArgs, Debug)]
struct CommonArgs {
    #[arg(long, default_value = DEFAULT_CONFIG_FILE, help = "YAML config file containing cluster IPs")]
    config_path: PathBuf,

    #[arg(long, default_value = DEFAULT_FIXTURES_DIR, help = "Directory containing data files")]
    fixtures_dir: PathBuf,
}

#[derive(ClapArgs, Debug)]
struct QueryArgs {
    #[arg(long, help = "SQL Query")]
    sql: String,
    #[arg(
        short,
        long,
        default_value = "2",
        help = "Number of shuffle tasks to use"
    )]
    num_shuffle_tasks: usize,
    #[arg(
        short,
        long,
        default_value = "2",
        help = "Number of coalesce tasks to use"
    )]
    num_coalesce_tasks: usize,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Start a datafusion distributed worker on the provided port")]
    StartServer {
        #[command(flatten)]
        common: CommonArgs,
        #[arg(short, long)]
        port: u16,
    },
    #[command(about = "Run a SQL query against distributed cluster")]
    Query {
        #[command(flatten)]
        common: CommonArgs,
        #[command(flatten)]
        query: QueryArgs,
    },
    #[command(about = "Show execution plan for a SQL query")]
    Explain {
        #[command(flatten)]
        common: CommonArgs,
        #[command(flatten)]
        query: QueryArgs,
    },
    #[command(about = "Show execution plan with metrics for a SQL query")]
    ExplainAnalyze {
        #[command(flatten)]
        common: CommonArgs,
        #[command(flatten)]
        query: QueryArgs,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::StartServer { common, port } => start_server(port, common.config_path).await,
        Commands::Query { common, query } => run_query(common, query).await.map(|_| ()),
        Commands::Explain { common, query } => run_explain(common, query, false).await.map(|_| ()),
        Commands::ExplainAnalyze { common, query } => {
            run_explain(common, query, true).await.map(|_| ())
        }
    }
}

/// Starts an arrow flight endpoint server on the specified port.
async fn start_server(port: u16, config_path: PathBuf) -> Result<()> {
    let endpoint = ArrowFlightEndpoint::try_new(SessionBuilder::new(config_path, 0, 0))?;

    info!("Starting ArrowFlightEndpoint server on port {}", port);

    Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
        .await
        .map_err(|e| internal_datafusion_err!("Error staring server: {e}"))?;

    Ok(())
}

/// SessionBuilder which creates a datafusion distributed session state.
struct SessionBuilder {
    config_path: PathBuf,
    num_shuffle_tasks: usize,
    num_coalesce_tasks: usize,
}

impl SessionBuilder {
    fn new(config_path: PathBuf, num_shuffle_tasks: usize, num_coalesce_tasks: usize) -> Self {
        Self {
            config_path,
            num_shuffle_tasks,
            num_coalesce_tasks,
        }
    }
}

#[async_trait]
impl DistributedSessionBuilder for SessionBuilder {
    async fn build_session_state(
        &self,
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError> {
        let config_content = std::fs::read_to_string(self.config_path.clone())
            .map_err(|e| internal_datafusion_err!("Error reading config file: {e}"))?;
        let config: ClusterConfig = serde_yaml::from_str(&config_content)
            .map_err(|e| internal_datafusion_err!("Error parsing YAML config: {e}"))?;

        let channel_resolver = DistributedChannelResolver::try_new(config.endpoints).await?;

        Ok(SessionStateBuilder::new()
            .with_runtime_env(ctx.runtime_env.clone())
            .with_default_features()
            .with_distributed_channel_resolver(channel_resolver)
            .with_distributed_network_shuffle_tasks(self.num_shuffle_tasks)
            .with_distributed_network_coalesce_tasks(self.num_coalesce_tasks)
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .build())
    }
}

/// Creates a [SessionContext] for distributed execution.
async fn new_session_context(
    config_path: PathBuf,
    fixtures_dir: PathBuf,
    num_shuffle_tasks: usize,
    num_coalesce_tasks: usize,
) -> Result<SessionContext> {
    let state = SessionBuilder::new(config_path, num_shuffle_tasks, num_coalesce_tasks)
        .build_session_state(DistributedSessionBuilderContext::default())
        .await?;

    let ctx = SessionContext::new_with_state(state);

    register_tables_from_fixtures(&ctx, &fixtures_dir).await?;

    Ok(ctx)
}

/// Run query command on the cluster denoted by the config file.
async fn run_query(common: CommonArgs, query: QueryArgs) -> Result<impl Display> {
    let ctx = new_session_context(
        common.config_path,
        common.fixtures_dir,
        query.num_shuffle_tasks,
        query.num_coalesce_tasks,
    )
    .await?;

    let df = ctx.sql(&query.sql).await?;
    let batches = df.collect().await?;

    let results =
        datafusion::common::arrow::util::pretty::pretty_format_batches(batches.as_slice())
            .map_err(|e| internal_datafusion_err!("Error formatting batch: {e}"))?;

    Ok(results)
}

/// Run explain command on the cluster denoted by the config file.
async fn run_explain(
    common: CommonArgs,
    query: QueryArgs,
    show_metrics: bool,
) -> Result<impl Display> {
    let ctx = new_session_context(
        common.config_path,
        common.fixtures_dir,
        query.num_shuffle_tasks,
        query.num_coalesce_tasks,
    )
    .await?;

    let df = ctx.sql(&query.sql).await?;
    let physical_plan = df.create_physical_plan().await?;

    if show_metrics {
        collect(physical_plan.clone(), ctx.task_ctx()).await?;
        return explain_analyze(physical_plan);
    }

    Ok(display_plan_ascii(physical_plan.as_ref(), false))
}

async fn register_tables_from_fixtures(ctx: &SessionContext, fixtures_dir: &PathBuf) -> Result<()> {
    if !fixtures_dir.exists() {
        println!(
            "Warning: Fixtures directory {:?} does not exist",
            fixtures_dir
        );
        return Ok(());
    }

    let entries = fs::read_dir(fixtures_dir)
        .map_err(|e| internal_datafusion_err!("Error reading fixtures directory: {e}"))?;

    for entry in entries {
        let entry =
            entry.map_err(|e| internal_datafusion_err!("Error reading directory entry: {e}"))?;
        let path = entry.path();

        if path.is_file() {
            let file_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");
            let table_name = path
                .file_stem()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");

            if let Some(extension) = path.extension().and_then(|ext| ext.to_str()) {
                match extension.to_lowercase().as_str() {
                    "parquet" => {
                        info!("Registering parquet table '{}' from {:?}", table_name, path);
                        ctx.register_parquet(
                            table_name,
                            path.to_str().unwrap(),
                            ParquetReadOptions::default(),
                        )
                        .await?;
                    }
                    "csv" => {
                        info!("Registering CSV table '{}' from {:?}", table_name, path);
                        ctx.register_csv(
                            table_name,
                            path.to_str().unwrap(),
                            datafusion::prelude::CsvReadOptions::default(),
                        )
                        .await?;
                    }
                    _ => {
                        warn!("Skipping unsupported file type: {}", file_name);
                    }
                }
            }
        } else if path.is_dir() {
            // Handle directories as parquet datasets (like weather/)
            let dir_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");
            info!(
                "Registering parquet directory '{}' from {:?}",
                dir_name, path
            );
            ctx.register_parquet(
                dir_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::NamedTempFile;
    use tokio::task::JoinHandle;

    use datafusion_distributed::assert_snapshot;

    struct TestCluster {
        server_handles: Vec<JoinHandle<()>>,
        config_file: NamedTempFile,
        testdata_path: PathBuf,
    }

    async fn setup_test_cluster(base_port: u16) -> Result<TestCluster> {
        // Create YAML config file in temp directory
        let mut temp_file = NamedTempFile::new().unwrap();
        let config = ClusterConfig {
            endpoints: vec![
                format!("127.0.0.1:{}", base_port),
                format!("127.0.0.1:{}", base_port + 1),
                format!("127.0.0.1:{}", base_port + 2),
            ],
        };
        let yaml_content = serde_yaml::to_string(&config).unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        // Start 3 servers on different ports
        let ports = vec![base_port, base_port + 1, base_port + 2];
        let mut server_handles = Vec::new();

        for port in &ports {
            let config_path = temp_file.path().to_path_buf();
            let port = *port;
            let handle = tokio::spawn(async move {
                start_server(port, config_path)
                    .await
                    .expect("Server failed");
            });
            server_handles.push(handle);
        }

        // Wait a bit for servers to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let testdata_path = PathBuf::from("../testdata"); // Relative to the controller crate

        Ok(TestCluster {
            server_handles,
            config_file: temp_file,
            testdata_path,
        })
    }

    impl Drop for TestCluster {
        fn drop(&mut self) {
            // Abort server tasks when test cluster is dropped
            for handle in &self.server_handles {
                handle.abort();
            }
        }
    }

    #[tokio::test]
    async fn test_sql_command() {
        let cluster = setup_test_cluster(18080).await.unwrap();

        let result = run_query(
                    CommonArgs {
            config_path: cluster.config_file.path().to_path_buf(),
            fixtures_dir: cluster.testdata_path.clone(),
        },
            QueryArgs {
                sql: "SELECT \"RainToday\", count(*) FROM weather GROUP BY \"RainToday\" ORDER BY count(*)"
                    .to_string(),
                num_shuffle_tasks: 2,
                num_coalesce_tasks: 2,
            },
        )
        .await.unwrap();

        assert_snapshot!(result.to_string(), @r"
      +-----------+----------+
      | RainToday | count(*) |
      +-----------+----------+
      | Yes       | 66       |
      | No        | 300      |
      +-----------+----------+
      ");
    }

    #[tokio::test]
    async fn test_explain_command() {
        let cluster = setup_test_cluster(18100).await.unwrap();

        let result = run_explain(
            CommonArgs {
            config_path: cluster.config_file.path().to_path_buf(),
            fixtures_dir: cluster.testdata_path.clone(),
        },
            QueryArgs {
                sql: "SELECT \"RainToday\", count(*) FROM weather GROUP BY \"RainToday\" ORDER BY count(*)"
                    .to_string(),
                num_shuffle_tasks: 2,
                num_coalesce_tasks: 2,
            },
            false, // show_metrics = false
        )
        .await.unwrap();

        assert!(result.to_string().contains("DistributedExec"));
        assert!(!result.to_string().contains("output_rows"));
    }

    #[tokio::test]
    async fn test_explain_analyze_command() {
        let cluster = setup_test_cluster(18120).await.unwrap();

        let result = run_explain(
           CommonArgs {
            config_path: cluster.config_file.path().to_path_buf(),
            fixtures_dir: cluster.testdata_path.clone(),
        },
            QueryArgs {
                sql: "SELECT \"RainToday\", count(*) FROM weather GROUP BY \"RainToday\" ORDER BY count(*)"
                    .to_string(),
                num_shuffle_tasks: 2,
                num_coalesce_tasks: 2,
            },
            true, // show_metrics = true
        )
        .await.unwrap();

        assert!(result.to_string().contains("DistributedExec"));
        assert!(result.to_string().contains("output_rows=2"));
    }
}
