use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use datafusion::common::internal_err;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_distributed::test_utils::{
    fuzz::{FuzzConfig, FuzzDB},
    rand::rng,
    tpcds::{generate_tpcds_data, queries, register_tables},
};
use log::{debug, error, info};
use rand::Rng;
use std::process;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(name = "fuzz")]
#[command(about = "Fuzz test distributed datafusion with various workloads")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Base64-encoded seed for random number generator. This seed is used to
    /// randomize configuration options and not used to generate data.
    #[arg(long)]
    seed: Option<String>,
}

/// Trait for defining fuzz workloads
#[async_trait]
trait Workload {
    /// Do any necessary setup for the workload such as registering tables
    async fn setup(ctx: SessionContext) -> Result<()>;

    /// Iterator of (query_name, query_sql) pairs  
    async fn queries(&self) -> Result<Box<dyn Iterator<Item = (String, String)> + Send>>;

    /// Get workload name for logging
    fn name(&self) -> &'static str;
}

#[derive(Subcommand)]
enum Commands {
    /// Run TPC-DS benchmark queries
    Tpcds {
        /// Scale factor for TPCDS data generation
        #[arg(short, long, default_value = "0.01")]
        scale_factor: String,

        /// Generate data even if it already exists
        #[arg(long, default_value_t = false)]
        force_regenerate: bool,

        /// Run only specific queries (comma-separated list, e.g., "q1,q5,q10")
        #[arg(long)]
        queries: Option<String>,
    },
}

/// TPC-DS workload implementation
#[derive(Clone)]
struct TpcdsWorkload {
    queries: Option<String>,
}

impl TpcdsWorkload {
    fn try_new(
        scale_factor: String,
        force_regenerate: bool,
        queries: Option<String>,
    ) -> Result<Self> {
        if force_regenerate {
            info!(
                "Generating TPC-DS data with scale factor {}...",
                scale_factor
            );
            generate_tpcds_data(&scale_factor)?;
            info!("Done");
        } else {
            info!("Using existing TPC-DS data");
        }

        Ok(Self { queries })
    }
}

#[async_trait]
impl Workload for TpcdsWorkload {
    async fn setup(ctx: SessionContext) -> Result<()> {
        info!("Registering TPC-DS tables...");
        let registered_tables = register_tables(&ctx).await?;
        info!("Done");
        debug!("Registered tables: {:?}", registered_tables);
        Ok(())
    }

    async fn queries(&self) -> Result<Box<dyn Iterator<Item = (String, String)> + Send>> {
        let queries_vec = queries(self.queries.as_deref())?;
        Ok(Box::new(queries_vec.into_iter()))
    }

    fn name(&self) -> &'static str {
        "TPC-DS"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Tpcds {
            scale_factor,
            force_regenerate,
            queries,
        } => {
            // Validate arguments
            if let Err(e) = validate_tpcds_args(&scale_factor) {
                error!("Error: {}", e);
                process::exit(1);
            }

            info!("🌀 Starting Distributed DataFusion Fuzz Test");

            let workload = TpcdsWorkload::try_new(scale_factor.clone(), force_regenerate, queries)?;

            if let Err(e) = run_workload(workload, cli.seed).await {
                error!("❌ Fuzz testing failed: {}", e);
                process::exit(1);
            }

            info!("✅ All fuzz tests passed!");
        }
    }

    Ok(())
}

/// Validate TPC-DS command line arguments
fn validate_tpcds_args(scale_factor: &str) -> std::result::Result<(), String> {
    // Validate scale factor is numeric (decimal allowed)
    if scale_factor.parse::<f64>().is_err() {
        return Err(format!(
            "Scale factor '{}' is not a valid number",
            scale_factor
        ));
    }

    Ok(())
}

fn randomized_fuzz_config(seed: Option<String>) -> Result<(FuzzConfig, String)> {
    let (mut rng, seed_b64) = rng(seed)?;

    let config = FuzzConfig {
        num_workers: rng.gen_range(2..=8),
        files_per_task: rng.gen_range(1..=8),
        cardinality_task_count_factor: rng.gen_range(1.0..=3.0),
    };

    Ok((config, seed_b64))
}

/// Run a workload using the generic workload trait
async fn run_workload<W>(workload: W, seed: Option<String>) -> Result<()>
where
    W: Workload,
{
    info!("Starting workload: {}", workload.name());

    let (config, seed_b64) = randomized_fuzz_config(seed)?;
    info!("Fuzz config: {}", config);
    info!("Seed: {}", seed_b64);

    let fuzz_db = match FuzzDB::new(config, W::setup).await {
        Ok(db) => db,
        Err(e) => {
            return internal_err!(
                "Failed to create FuzzDB: {}",
                e
            );
        }
    };

    let queries_iter = workload.queries().await?;

    let mut successful = 0;
    let mut failed = 0;
    let mut invalid = 0; 

    for (query_name, query_sql) in queries_iter {
        debug!("Executing fuzz query: {}", query_sql.trim());

        match fuzz_db.run(&query_sql).await {
            Ok(results) => {
                info!("✅ {} completed", query_name);
                match results {
                    Some(batches) => {
                        successful += 1;
                        debug!("{}", pretty_format_batches(&batches)?,);
                    }
                    None => {
                        debug!("No results (query errored expectedly)");
                        invalid += 1;
                    }
                }
            }
            Err(e) => {
                failed += 1;
                error!("❌ {} failed: {}", query_name, e);
            }
        }
    }

    if failed > 0 {
        return internal_err!(
            "{} out of {} queries failed",
            failed,
            successful + failed
        );
    }

    info!("Success: {} Invalid: {} Valid %: {}", successful, invalid, (successful as f64 / (successful + invalid) as f64) * 100.0);

    Ok(())
}
