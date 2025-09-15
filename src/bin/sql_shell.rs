use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::{DistributedPhysicalOptimizerRule, DistributedSessionBuilder, DistributedSessionBuilderContext, ExecutionStage};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use datafusion_distributed::{MetricsCollector, df_metrics_set_to_proto, ProtoMetricsSet, StageKey};

#[derive(Clone)]
struct DistributedSessionBuilder4Partitions;

#[async_trait::async_trait]
impl DistributedSessionBuilder for DistributedSessionBuilder4Partitions {
    async fn build_session_state(
        &self,
        ctx: DistributedSessionBuilderContext,
    ) -> Result<datafusion::execution::SessionState, DataFusionError> {
        // Create distributed physical optimizer with 2 partitions per task
        let distributed_optimizer = DistributedPhysicalOptimizerRule::new()
            .with_maximum_partitions_per_task(2);

        // Configure session with 4 target partitions and distributed optimizer
        let config = SessionConfig::new()
            .with_target_partitions(4);

        Ok(SessionStateBuilder::new()
            .with_runtime_env(ctx.runtime_env)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(distributed_optimizer))
            .with_config(config)
            .build())
    }
}

async fn register_tables(ctx: &SessionContext) -> Result<(), DataFusionError> {
    // Register weather dataset if it exists
    let weather_path = "testdata/weather.parquet";
    if Path::new(weather_path).exists() {
        ctx.register_parquet("weather", weather_path, ParquetReadOptions::default())
            .await?;
        println!("✓ Registered weather table from {}", weather_path);
    } else {
        println!("⚠ Warning: {} not found", weather_path);
    }

    // Register flights dataset if it exists
    let flights_path = "testdata/flights-1m.parquet";
    if Path::new(flights_path).exists() {
        ctx.register_parquet("flights", flights_path, ParquetReadOptions::default())
            .await?;
        println!("✓ Registered flights table from {}", flights_path);
    } else {
        println!("⚠ Warning: {} not found", flights_path);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    println!("🚀 DataFusion Distributed SQL Shell");
    println!("Starting distributed query engine with 2 workers...");
    println!("Configuration: 4 partitions with 2 partitions per task");

    // Start distributed context with 2 workers
    let (ctx, mut join_set) = start_localhost_context(2, DistributedSessionBuilder4Partitions).await;

    // The context is already configured for distributed execution

    // Register parquet tables
    register_tables(&ctx).await?;

    println!("📊 Ready to execute queries!");
    println!("Available tables: weather, flights (if present)");
    println!("Commands:");
    println!("  \\q or \\quit - Exit");
    println!("  \\schema <table> - Show table schema");
    println!("  \\explain <query> - Show distributed execution plan");
    println!("  \\explain_analyze <query> - Execute query and show plan with metrics (distributed)");
    println!("  \\explain_analyze_single <query> - Execute query and show plan with metrics (single-node)");
    println!("  \\help - Show this help");
    println!();

    let stdin = io::stdin();
    loop {
        print!("datafusion-distributed> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        match stdin.read_line(&mut input) {
            Ok(0) => break, // EOF
            Ok(_) => {
                let input = input.trim();
                
                if input.is_empty() {
                    continue;
                }

                match input {
                    "\\q" | "\\quit" => {
                        println!("Goodbye!");
                        break;
                    }
                    "\\help" => {
                        println!("Available commands:");
                        println!("  \\q, \\quit - Exit the shell");
                        println!("  \\schema <table> - Show table schema");
                        println!("  \\explain <query> - Show distributed execution plan");
                        println!("  \\explain_analyze <query> - Execute query and show plan with metrics (distributed)");
                        println!("  \\explain_analyze_single <query> - Execute query and show plan with metrics (single-node)");
                        println!("  \\help - Show this help");
                        println!();
                        println!("Example queries:");
                        println!("  SELECT COUNT(*) FROM weather;");
                        println!("  SELECT * FROM weather LIMIT 10;");
                        println!("  SELECT \"RainToday\", COUNT(*) FROM weather GROUP BY \"RainToday\";");
                        println!();
                        println!("Example explain:");
                        println!("  \\explain SELECT \"RainToday\", COUNT(*) FROM weather GROUP BY \"RainToday\";");
                        println!("  \\explain_analyze SELECT COUNT(*) FROM weather;");
                        println!("  \\explain_analyze_single SELECT COUNT(*) FROM weather;");
                        continue;
                    }
                    _ if input.starts_with("\\schema ") => {
                        let table_name = input.strip_prefix("\\schema ").unwrap().trim();
                        match ctx.sql(&format!("DESCRIBE {}", table_name)).await {
                            Ok(df) => {
                                match df.collect().await {
                                    Ok(batches) => {
                                        if !batches.is_empty() {
                                            println!("{}", pretty_format_batches(&batches).unwrap());
                                        } else {
                                            println!("No schema information available.");
                                        }
                                    }
                                    Err(e) => println!("Error: {}", e),
                                }
                            }
                            Err(e) => println!("Error: {}", e),
                        }
                        continue;
                    }
                    _ if input.starts_with("\\explain ") => {
                        let query = input.strip_prefix("\\explain ").unwrap().trim();
                        match show_execution_plan(&ctx, query).await {
                            Ok(_) => {}
                            Err(e) => println!("Error: {}", e),
                        }
                        continue;
                    }
                    _ if input.starts_with("\\explain_analyze_single ") => {
                        let query = input.strip_prefix("\\explain_analyze_single ").unwrap().trim();
                        match explain_analyze_single(query).await {
                            Ok(_) => {}
                            Err(e) => println!("Error: {}", e),
                        }
                        continue;
                    }
                    _ if input.starts_with("\\explain_analyze ") => {
                        let query = input.strip_prefix("\\explain_analyze ").unwrap().trim();
                        match explain_analyze(&ctx, query).await {
                            Ok(_) => {}
                            Err(e) => println!("Error: {}", e),
                        }
                        continue;
                    }
                    _ => {
                        // Execute SQL query
                        match execute_query(&ctx, input).await {
                            Ok(_) => {}
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                }
            }
            Err(e) => {
                println!("Error reading input: {}", e);
                break;
            }
        }
    }

    // Shutdown background tasks
    join_set.shutdown().await;

    Ok(())
}

async fn execute_query(ctx: &SessionContext, sql: &str) -> Result<(), DataFusionError> {
    let start = std::time::Instant::now();
    
    // Execute the query
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    
    let elapsed = start.elapsed();

    if batches.is_empty() {
        println!("Query returned no results.");
    } else {
        // Display results
        println!("{}", pretty_format_batches(&batches)?);
        
        // Show statistics
        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        println!();
        println!("📈 Query executed in {:?}", elapsed);
        println!("📊 {} rows returned", total_rows);
    }

    Ok(())
}

async fn show_execution_plan(ctx: &SessionContext, sql: &str) -> Result<(), DataFusionError> {
    println!("🏗️  Distributed Execution Plan:");
    println!();
    
    // Create the DataFrame and get the physical plan
    let df = ctx.sql(sql).await?;
    let physical_plan = df.create_physical_plan().await?;
    
    // Display the plan with indentation
    let display = displayable(physical_plan.as_ref()).indent(true).to_string();
    println!("{}", display);
    
    Ok(())
}

async fn explain_analyze(ctx: &SessionContext, sql: &str) -> Result<(), DataFusionError> {
    println!("🔍 EXPLAIN ANALYZE - Executing query and collecting metrics...");
    println!();
    
    let start = std::time::Instant::now();
    
    // Create the DataFrame and get the physical plan FIRST (before consuming df)
    let df = ctx.sql(sql).await?;
    let physical_plan = df.create_physical_plan().await?;
    
    println!("📋 Physical Plan BEFORE Execution:");
    let display_before = displayable(physical_plan.as_ref()).indent(true).to_string();
    println!("{}", display_before);
    println!();
    
    // Execute the query using the physical plan (clone it since collect consumes it)
    println!("⚡ Executing query...");
    let task_ctx = ctx.state().task_ctx();
    let results = collect(physical_plan.clone(), task_ctx).await?;
    let execution_time = start.elapsed();
    
    println!("✅ Query executed in {:?}", execution_time);
    println!();
    
    // Show results summary
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    let total_batches = results.len();
    println!("📊 Results: {} rows in {} batches", total_rows, total_batches);
    println!();

    if let Some(stage) = physical_plan.as_any().downcast_ref::<ExecutionStage>() {
        let (task_metrics, mut child_task_metrics) = MetricsCollector::new().collect(&stage)?;
        let proto_task_metrics = task_metrics.iter()
            .filter_map(|metrics| df_metrics_set_to_proto(metrics).ok())
            .collect::<Vec<_>>(); 
        child_task_metrics.insert(StageKey {
            query_id: stage.query_id.to_string(),
            stage_id: stage.num as u64,
            task_number: 0,
        }, proto_task_metrics);
        let keys = child_task_metrics.keys().collect::<Vec<_>>();
        for key in keys {
            println!("{}", key);
        }
    }
    
    // Display the plan WITH metrics (physical_plan is still available)
    println!("📈 Physical Plan WITH Metrics:");
    let display_with_metrics = DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
        .indent(true)
        .to_string();
    println!("{}", display_with_metrics);
    println!();
    
    // Also show root node metrics directly
    println!("🔧 Root Node Metrics (direct call to .metrics()):");
    if let Some(metrics) = physical_plan.metrics() {
        if metrics.iter().count() > 0 {
            for metric in metrics.iter() {
                println!("  {:?}", metric);
            }
        } else {
            println!("  No metrics available (empty MetricsSet)");
        }
    } else {
        println!("  No metrics available (metrics() returned None)");
    }
    
    Ok(())
}

async fn explain_analyze_single(sql: &str) -> Result<(), DataFusionError> {
    println!("🔍 EXPLAIN ANALYZE SINGLE - Non-distributed execution with metrics...");
    println!();
    
    let start = std::time::Instant::now();
    
    // Create regular (non-distributed) DataFusion context
    let config = SessionConfig::new().with_target_partitions(4);
    let single_ctx = SessionContext::new_with_config(config);
    
    // Register tables in the single-node context
    register_tables(&single_ctx).await?;
    
    // Create the DataFrame and get the physical plan
    let df = single_ctx.sql(sql).await?;
    let physical_plan = df.create_physical_plan().await?;

    
    println!("📋 Physical Plan BEFORE Execution (Single-Node):");
    let display_before = displayable(physical_plan.as_ref()).indent(true).to_string();
    println!("{}", display_before);
    println!();
    
    // Execute the query using the physical plan
    println!("⚡ Executing query...");
    let task_ctx = single_ctx.state().task_ctx();
    let results = collect(physical_plan.clone(), task_ctx).await?;
    let execution_time = start.elapsed();
    
    println!("✅ Query executed in {:?}", execution_time);
    println!();
    
    // Show results summary
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    let total_batches = results.len();
    println!("📊 Results: {} rows in {} batches", total_rows, total_batches);
    println!();
    
    // Display the plan WITH metrics (should show real DataFusion metrics)
    println!("📈 Physical Plan WITH Metrics (Single-Node):");
    let display_with_metrics = DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
        .indent(true)
        .to_string();
    println!("{}", display_with_metrics);
    println!();
    
    // Also show root node metrics directly
    println!("🔧 Root Node Metrics (direct call to .metrics()):");
    if let Some(metrics) = physical_plan.metrics() {
        if metrics.iter().count() > 0 {
            println!("  Found {} metrics:", metrics.iter().count());
            for metric in metrics.iter() {
                println!("    {:?}", metric);
            }
        } else {
            println!("  No metrics available (empty MetricsSet)");
        }
    } else {
        println!("  No metrics available (metrics() returned None)");
    }
    
    Ok(())
}