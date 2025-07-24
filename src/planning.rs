use std::{
    collections::HashMap,
    env,
    sync::{Arc, LazyLock},
};

use anyhow::{anyhow, Context};
use arrow_flight::Action;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::{
        file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat, FileFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::DataFusionError,
    execution::{SessionState, SessionStateBuilder},
    logical_expr::LogicalPlan,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        analyze::AnalyzeExec, coalesce_batches::CoalesceBatchesExec, displayable,
        joins::NestedLoopJoinExec, repartition::RepartitionExec, sorts::sort::SortExec,
        ExecutionPlan, ExecutionPlanProperties,
    },
    prelude::{SQLOptions, SessionConfig, SessionContext},
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::TryStreamExt;
use itertools::Itertools;
use prost::Message;

use crate::{
    analyze::{DistributedAnalyzeExec, DistributedAnalyzeRootExec},
    isolator::PartitionIsolatorExec,
    logging::{debug, error, info, trace},
    max_rows::MaxRowsExec,
    physical::DDStageOptimizerRule,
    result::{DDError, Result},
    stage::DDStageExec,
    stage_reader::{DDStageReaderExec, QueryId},
    util::{display_plan_with_partition_counts, get_client, physical_plan_to_bytes, wait_for},
    vocab::{
        Addrs, CtxAnnotatedOutputs, CtxHost, CtxPartitionGroup, CtxStageAddrs, CtxStageId, DDTask,
        Host, Hosts, PartitionAddrs, StageAddrs,
    },
};

/// Represents a stage of distributed query execution
///
/// A stage is a unit of work that can be executed independently on worker nodes.
/// Stages are created by breaking up a physical plan at points where data needs
/// to be redistributed between nodes (e.g., joins, sorts, repartitioning).
///
/// # Stage Lifecycle
/// 1. **Creation**: Physical plan is analyzed and broken into stages
/// 2. **Distribution**: Each stage is assigned to worker nodes
/// 3. **Execution**: Workers execute their assigned stage tasks
/// 4. **Coordination**: Results are read by DDStageReaderExec nodes
///
/// # Partition Groups
/// Each stage can be divided into partition groups (aka tasks), where each group/task represents
/// a subset of partitions that will be assigned to a specific worker node.
/// This allows for fine-grained distribution of work across the cluster.
#[derive(Debug, Clone)]
pub struct DDStage {
    /// Unique identifier for this stage within a query
    pub stage_id: u64,

    /// Physical execution plan that workers will execute for this stage
    pub plan: Arc<dyn ExecutionPlan>,

    /// Groups of partition IDs, where each group is assigned to a worker
    /// Example: [[0, 1], [2, 3]] means two workers handle 2 partitions each
    pub partition_groups: Vec<Vec<u64>>,

    /// Whether this stage produces complete partitions that can be consumed directly
    /// If false, DDStageReaderExecs will merge results from multiple workers
    pub full_partitions: bool,
}

impl DDStage {
    /// Creates a new stage with the specified configuration
    ///
    /// # Arguments
    /// * `stage_id` - Unique identifier for this stage
    /// * `plan` - Execution plan that workers will run
    /// * `partition_groups` - How partitions are distributed among workers
    /// * `full_partitions` - Whether partitions are complete or need merging
    fn new(
        stage_id: u64,
        plan: Arc<dyn ExecutionPlan>,
        partition_groups: Vec<Vec<u64>>,
        full_partitions: bool,
    ) -> Self {
        Self {
            stage_id,
            plan,
            partition_groups,
            full_partitions,
        }
    }

    /// Extracts the stage IDs of all child stages that this stage depends on
    ///
    /// This method walks through the execution plan tree to find DDStageReaderExec
    /// nodes, which represent dependencies on other stages. These dependencies
    /// determine the execution order - child stages must complete before this stage.
    ///
    /// # Returns
    /// * `Vec<u64>` - List of stage IDs that this stage depends on
    ///
    /// # Usage
    /// Used during task distribution to ensure proper stage execution ordering.
    pub fn child_stage_ids(&self) -> Result<Vec<u64>> {
        let mut result = vec![];
        // Walk through the execution plan tree to find stage reader dependencies
        self.plan
            .clone()
            .transform_down(|node: Arc<dyn ExecutionPlan>| {
                if let Some(reader) = node.as_any().downcast_ref::<DDStageReaderExec>() {
                    result.push(reader.stage_id);
                }
                Ok(Transformed::no(node))
            })?;
        Ok(result)
    }
}

/// Global lazy-initialized DataFusion session state for distributed execution
///
/// This static variable holds a pre-configured SessionState that is shared across
/// all distributed query executions. It includes optimizations for distributed
/// execution and table registrations from environment variables.
///
/// # Initialization
/// The state is lazily initialized on first access using the make_state() function.
/// If initialization fails, all subsequent access attempts will return the same error.
static STATE: LazyLock<Result<SessionState>> = LazyLock::new(|| {
    let wait_result = wait_for(make_state(), "make_state");
    match wait_result {
        Ok(Ok(state)) => Ok(state),
        Ok(Err(e)) => Err(anyhow!("Failed to initialize state: {}", e).into()),
        Err(e) => Err(anyhow!("Failed to initialize state: {}", e).into()),
    }
});

/// Creates a new DataFusion session context configured for distributed execution
///
/// This function provides the primary entry point for obtaining a properly configured
/// DataFusion session context. It uses the global STATE to ensure consistent
/// configuration across all distributed executions.
///
/// # Returns
/// * `SessionContext` - Ready-to-use context with distributed-specific configuration
///
/// # Errors
/// Returns an error if the global state failed to initialize during startup.
pub fn get_ctx() -> Result<SessionContext> {
    match &*STATE {
        // Create new context from the successfully initialized global state
        Ok(state) => Ok(SessionContext::new_with_state(state.clone())),
        // Propagate initialization error from startup
        Err(e) => Err(anyhow!("Context initialization failed: {}", e).into()),
    }
}

/// Creates and configures a DataFusion session state optimized for distributed execution
///
/// This function builds a SessionState with specific optimizations required for
/// distributed DataFusion clusters. It configures join algorithms, partitioning
/// settings, and registers external tables from environment variables.
///
/// # Key Optimizations
/// - **Hash Join Configuration**: Disables single-partition optimizations to ensure
///   hash joins can work correctly across distributed partitions
/// - **Partition Thresholds**: Sets thresholds to 0 to prevent DataFusion from
///   falling back to collect-based joins that don't work in distributed mode
/// - **Target Partitions**: Configures default partition count for parallel execution
/// - **Information Schema**: Enables metadata queries like DESCRIBE TABLE
///
/// # Table Registration
/// Automatically registers tables defined in the DD_TABLES environment variable.
/// This allows queries to reference external data sources (Parquet, CSV, JSON files).
///
/// # Returns
/// * `SessionState` - Configured state ready for distributed query execution
///
/// # Environment Variables
/// - `DD_TABLES`: Comma-separated table definitions (name:format:path)
///
/// # Errors
/// Returns an error if configuration fails or table registration encounters issues.
async fn make_state() -> Result<SessionState> {
    // Start with default DataFusion configuration and enable information schema
    let mut config = SessionConfig::default().with_information_schema(true);

    // Configure hash join settings for distributed execution
    // These settings are critical - they prevent DataFusion from using
    // single-partition optimizations that break distributed joins
    let options = config.options_mut();
    options.set(
        "datafusion.optimizer.hash_join_single_partition_threshold",
        "0",
    )?;
    options.set(
        "datafusion.optimizer.hash_join_single_partition_threshold_rows",
        "0",
    )?;

    // Set default partition count for parallel execution across workers
    // TODO: Make this configurable
    options.set("datafusion.execution.target_partitions", "3")?;

    // Build the session state with default features and our custom configuration
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .build();

    // Register external tables from environment variable configuration
    // This allows queries to reference data sources like Parquet files
    add_tables_from_env(&mut state)
        .await
        .context("Failed to add tables from environment")?;

    Ok(state)
}

/// Adds distributed execution metadata as extensions to the DataFusion session context
///
/// This function populates the execution context with all the metadata needed for
/// distributed query execution. These extensions are later retrieved by execution
/// plans (like DDStageReaderExec) to connect to workers and execute tasks.
///
/// # Arguments
/// * `ctx` - DataFusion session context to modify
/// * `host` - Information about this proxy/worker node
/// * `query_id` - Unique identifier for the current query
/// * `stage_id` - Stage ID being executed
/// * `stage_addrs` - Map of worker addresses for connecting to distributed stages
/// * `partition_group` - List of partitions assigned to this execution
///
/// # Returns
/// * `Ok(())` if extensions were successfully added
pub fn add_ctx_extentions(
    ctx: &mut SessionContext,
    host: &Host,
    query_id: &str,
    stage_id: u64,
    stage_addrs: Addrs,
    partition_group: Vec<u64>,
) -> Result<()> {
    let state = ctx.state_ref();
    let mut guard = state.write();
    let config = guard.config_mut();

    // Add worker addresses so DDStageReaderExec can connect to distributed workers
    config.set_extension(Arc::new(CtxStageAddrs(stage_addrs)));

    // Add query identification for worker communication
    config.set_extension(Arc::new(QueryId(query_id.to_owned())));

    // Add host information for logging and debugging
    config.set_extension(Arc::new(CtxHost(host.clone())));

    // Add current stage ID for execution tracking
    config.set_extension(Arc::new(CtxStageId(stage_id)));

    // Add container for collecting execution analysis outputs
    config.set_extension(Arc::new(CtxAnnotatedOutputs::default()));

    trace!("Adding partition group: {:?}", partition_group);

    // Add partition group information for workers to know which partitions to execute
    config.set_extension(Arc::new(CtxPartitionGroup(partition_group)));

    Ok(())
}

/// Registers external data tables from environment variable configuration
///
/// This function reads table definitions from the DD_TABLES environment variable
/// and registers them in the DataFusion session state. This allows distributed
/// queries to reference external data sources like Parquet files, CSV files, etc.
///
/// # Environment Variable Format
/// `DD_TABLES` should contain comma-separated table definitions, where each definition
/// follows the format: `name:format:path`
///
/// # Supported Formats
/// - **parquet**: Apache Parquet columnar format
/// - **csv**: Comma-separated values with automatic schema inference
/// - **json**: JSON lines format with automatic schema inference
///
/// # Examples
/// ```bash
/// # Single table
/// DD_TABLES="customers:parquet:/data/customers.parquet"
///
/// # Multiple tables
/// DD_TABLES="customers:parquet:/data/customers.parquet,orders:csv:/data/orders.csv"
/// ```
///
/// # Table Registration Process
/// 1. Parse environment variable into individual table definitions
/// 2. Extract name, format, and path for each table
/// 3. Create appropriate FileFormat handler (Parquet, CSV, JSON)
/// 4. Infer schema from the data source
/// 5. Register table in DataFusion session state
///
/// # Arguments
/// * `state` - Mutable reference to DataFusion session state for table registration
///
/// # Returns
/// * `Ok(())` if all tables were successfully registered
/// * `Err(DDError)` if parsing fails or table registration encounters issues
///
/// # Errors
/// - Invalid format in DD_TABLES (wrong number of components)
/// - Unsupported file format
/// - File path not accessible or invalid
/// - Schema inference failures
pub async fn add_tables_from_env(state: &mut SessionState) -> Result<()> {
    // Check if DD_TABLES environment variable is set
    let table_str = env::var("DD_TABLES");
    if table_str.is_err() {
        info!("No DD_TABLES environment variable set, skipping table registration");
        return Ok(());
    }

    // Process each table definition in the comma-separated list
    for table in table_str.unwrap().split(',') {
        info!("adding table from env: {}", table);

        // Parse table definition: name:format:path
        let parts: Vec<&str> = table.split(':').collect();
        if parts.len() != 3 {
            return Err(anyhow!("Invalid format for DD_TABLES env var: {}", table).into());
        }
        let name = parts[0].to_string();
        let fmt = parts[1].to_string();
        let path = parts[2].to_string();

        // Create appropriate file format handler based on specified format
        let format: Arc<dyn FileFormat> = match fmt.as_str() {
            "parquet" => Arc::new(ParquetFormat::default()),
            "csv" => Arc::new(CsvFormat::default()),
            "json" => Arc::new(JsonFormat::default()),
            _ => {
                return Err(anyhow!(
                    "Unsupported format: {}. Supported formats are: parquet, csv, json",
                    fmt
                )
                .into());
            }
        };

        // Configure listing options for the file format
        let options = ListingOptions::new(format);

        // Parse and validate the table path
        let table_path = ListingTableUrl::parse(path)?;

        // Infer schema from the actual data source
        let resolved_schema = options.infer_schema(state, &table_path).await?;

        // Configure the listing table with path, options, and inferred schema
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let table = Arc::new(ListingTable::try_new(config)?);

        // Register the table in DataFusion's schema registry
        state
            .schema_for_ref(name.clone())?
            .register_table(name, table)?;
    }

    Ok(())
}

/// Converts SQL query to optimized DataFusion logical plan
///
/// This function handles the first phase of query planning by parsing SQL text
/// into DataFusion's internal logical plan representation and applying logical
/// optimizations. The resulting plan serves as the foundation for physical
/// and distributed planning phases.
///
/// # Planning Process
/// 1. **SQL Parsing**: Convert SQL text to logical plan AST
/// 2. **Plan Validation**: Verify schema consistency and semantic correctness
/// 3. **Logical Optimization**: Apply DataFusion's logical optimization rules
///
/// # Logical Optimizations Applied
/// - **Predicate Pushdown**: Move filters closer to data sources
/// - **Projection Pushdown**: Eliminate unnecessary columns early
/// - **Constant Folding**: Evaluate constant expressions at plan time
/// - **Join Reordering**: Optimize join order for better performance
/// - **Common Expression Elimination**: Reuse computed expressions
///
/// # Arguments
/// * `sql` - SQL query string to parse and optimize
/// * `ctx` - DataFusion session context with registered tables and configuration
///
/// # Returns
/// * `LogicalPlan` - Optimized logical plan ready for physical planning
///
/// # Errors
/// - SQL parsing errors (syntax, semantic validation)
/// - Schema resolution failures (unknown tables/columns)
/// - Type checking failures in expressions
/// - Logical optimization rule failures
pub async fn logical_planning(sql: &str, ctx: &SessionContext) -> Result<LogicalPlan> {
    let options = SQLOptions::new();

    // Parse SQL text into DataFusion's logical plan representation
    let plan = ctx.state().create_logical_plan(sql).await?;

    debug!("Logical plan:\n{}", plan.display_indent());

    // Validate the logical plan for schema consistency and semantic correctness
    options.verify_plan(&plan)?;

    // Apply DataFusion's logical optimization rules to improve query performance
    let plan = ctx.state().optimize(&plan)?;

    debug!("Optimized Logical plan:\n{}", plan.display_indent());
    Ok(plan)
}

/// Builds the physical plan from the logical plan, using the default Physical Planner from DataFusion
pub async fn physical_planning(
    logical_plan: &LogicalPlan,
    ctx: &SessionContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Use DataFusion's physical planner to convert logical to physical plan
    let physical_plan = ctx
        .state()
        .create_physical_plan(logical_plan)
        .await
        .context("Failed to create physical plan")?;

    debug!(
        "Physical plan:\n{}",
        displayable(physical_plan.as_ref()).indent(false)
    );
    Ok(physical_plan)
}

/// Returns distributed plan and execution stages for both query execution and EXPLAIN display
pub async fn distributed_physical_planning(
    physical_plan: Arc<dyn ExecutionPlan>,
    batch_size: usize,
    partitions_per_worker: Option<usize>,
) -> Result<(Arc<dyn ExecutionPlan>, Vec<DDStage>)> {
    let mut stages = vec![];

    let mut partition_groups = vec![];
    let mut full_partitions = false;

    // Walk up the physical plan tree to identify stage boundaries and convert stages
    // Stage boundaries are places where data needs to be reshuffled between workers
    // (e.g., repartitioning, sorting, joins that require data movement)
    let up = |plan: Arc<dyn ExecutionPlan>| {
        trace!(
            "Examining plan up: {}",
            displayable(plan.as_ref()).one_line()
        );

        if let Some(stage_exec) = plan.as_any().downcast_ref::<DDStageExec>() {
            // Found a stage boundary marker - convert DDStageExec to DDStageReaderExec
            // The DDStageReaderExec will read data from workers executing this stage
            trace!("stage exec. partition_groups: {:?}", partition_groups);
            let input = plan.children();
            assert!(input.len() == 1, "DDStageExec must have exactly one child");
            let input = input[0];

            // Create a reader that will connect to workers executing this stage
            let replacement = Arc::new(DDStageReaderExec::try_new(
                plan.output_partitioning().clone(),
                input.schema(),
                stage_exec.stage_id,
            )?) as Arc<dyn ExecutionPlan>;

            // Store the stage for later distribution to workers
            let stage = DDStage::new(
                stage_exec.stage_id,
                input.clone(),
                partition_groups.clone(),
                full_partitions,
            );
            full_partitions = false;

            stages.push(stage);
            Ok(Transformed::yes(replacement))
        } else if plan.as_any().downcast_ref::<RepartitionExec>().is_some() {
            // Repartitioning requires data exchange between workers
            trace!("repartition exec partition_groups: {:?}", partition_groups);
            let (calculated_partition_groups, replacement) =
                build_replacement(plan, partitions_per_worker, true, batch_size, batch_size)?;
            partition_groups = calculated_partition_groups;
            full_partitions = false;

            Ok(Transformed::yes(replacement))
        } else if plan.as_any().downcast_ref::<SortExec>().is_some() {
            // Sorting may require gathering all partitions to ensure global order
            trace!("sort exec partition_groups: {:?}", partition_groups);
            let (calculated_partition_groups, replacement) =
                build_replacement(plan, partitions_per_worker, false, batch_size, batch_size)?;
            partition_groups = calculated_partition_groups;
            full_partitions = true;

            Ok(Transformed::yes(replacement))
        } else if plan.as_any().downcast_ref::<NestedLoopJoinExec>().is_some() {
            // Nested loop joins must execute on single worker due to materializing left side
            trace!(
                "nested loop join exec partition_groups: {:?}",
                partition_groups
            );
            // NestedLoopJoinExec must be on a stage by itself as it materializes the entire
            // left side of the join and is not suitable to be executed in a
            // partitioned manner.
            let mut replacement = plan.clone();
            let partition_count = plan.output_partitioning().partition_count() as u64;
            trace!("nested join output partitioning {}", partition_count);

            replacement = Arc::new(MaxRowsExec::new(
                Arc::new(CoalesceBatchesExec::new(replacement, batch_size))
                    as Arc<dyn ExecutionPlan>,
                batch_size,
            )) as Arc<dyn ExecutionPlan>;

            partition_groups = vec![(0..partition_count).collect()];
            full_partitions = true;
            Ok(Transformed::yes(replacement))
        } else {
            trace!("not special case partition_groups: {:?}", partition_groups);

            let partition_count = plan.output_partitioning().partition_count() as u64;
            // set this back to default
            partition_groups = vec![(0..partition_count).collect()];

            Ok(Transformed::no(plan))
        }
    };

    // Walk up the plan adding DDStageExec marker nodes first
    // This identifies where stage boundaries should be placed
    let optimizer = DDStageOptimizerRule::new();
    let distributed_plan = optimizer.optimize(physical_plan, &ConfigOptions::default())?;

    // Clone the distributed plan before transformation since we need to return it
    let distributed_plan_clone = Arc::clone(&distributed_plan);

    // Transform the plan by replacing DDStageExec nodes with DDStageReaderExec nodes
    distributed_plan.transform_up(up)?;

    // Optimize the final stage by adding coalesce batching and row limits
    let mut last_stage = stages.pop().ok_or(anyhow!("No stages found"))?;

    last_stage = DDStage::new(
        last_stage.stage_id,
        Arc::new(MaxRowsExec::new(
            Arc::new(CoalesceBatchesExec::new(last_stage.plan, batch_size))
                as Arc<dyn ExecutionPlan>,
            batch_size,
        )) as Arc<dyn ExecutionPlan>,
        partition_groups,
        full_partitions,
    );

    // Put the optimized final stage back
    stages.push(last_stage);

    // If the plan contains EXPLAIN ANALYZE, add distributed analyze stages
    if contains_analyze(stages[stages.len() - 1].plan.as_ref()) {
        add_distributed_analyze(&mut stages, false, false)?;
    }

    let txt = stages
        .iter()
        .map(|stage| format!("{}", display_plan_with_partition_counts(&stage.plan)))
        .join(",\n");
    trace!("stages:\n{}", txt);

    // Return the distributed plan (with readers) and the stages (for worker execution)
    Ok((distributed_plan_clone, stages))
}

/// Checks if an execution plan tree contains any AnalyzeExec nodes
///
/// This function recursively traverses an execution plan tree to determine
/// if it contains EXPLAIN ANALYZE operations. This information is used to
/// determine whether to add distributed analysis capabilities to the stages.
///
/// # Arguments
/// * `plan` - Root of the execution plan tree to examine
///
/// # Returns
/// * `true` if the plan tree contains any AnalyzeExec nodes
/// * `false` if no analyze operations are found
///
/// # Usage
/// Used during distributed plan generation to detect EXPLAIN ANALYZE queries
/// that require special handling for collecting execution statistics across
/// distributed workers.
fn contains_analyze(plan: &dyn ExecutionPlan) -> bool {
    trace!(
        "checking stage for analyze: {}",
        displayable(plan).indent(false)
    );

    // Check if this node is an AnalyzeExec
    if plan.as_any().downcast_ref::<AnalyzeExec>().is_some() {
        true
    } else {
        // Recursively check all child nodes
        for child in plan.children() {
            if contains_analyze(child.as_ref()) {
                return true;
            }
        }
        false
    }
}

/// Transforms stages to support distributed EXPLAIN ANALYZE operations
///
/// This function modifies execution stages to collect and aggregate performance
/// statistics across distributed workers. It replaces regular AnalyzeExec nodes
/// with distributed-aware analyze nodes that can coordinate metrics collection.
///
/// # Stage Transformation
/// - **Final Stage**: Wrapped with DistributedAnalyzeRootExec that aggregates
///   statistics from all workers and produces the final EXPLAIN ANALYZE output
/// - **Intermediate Stages**: Wrapped with DistributedAnalyzeExec that collects
///   local metrics and forwards them to the root stage
///
/// # Arguments
/// * `stages` - Mutable slice of stages to transform for distributed analysis
/// * `verbose` - Whether to include verbose execution details in output
/// * `show_statistics` - Whether to display table and column statistics
///
/// # Returns
/// * `Ok(())` if transformation succeeds
/// * `Err(DDError)` if plan transformation fails
///
/// # Analysis Flow
/// 1. Each worker executes its stage wrapped in DistributedAnalyzeExec
/// 2. Workers collect local execution metrics (row counts, timing, memory usage)
/// 3. Final stage (DistributedAnalyzeRootExec) aggregates metrics from all workers
/// 4. Root stage produces formatted EXPLAIN ANALYZE output for the client
///
/// # Partition Adjustment
/// The final stage's partition groups are reset to [[0]] to account for the
/// CoalescePartitionsExec that combines all distributed results into a single stream.
pub fn add_distributed_analyze(
    stages: &mut [DDStage],
    verbose: bool,
    show_statistics: bool,
) -> Result<()> {
    trace!("Adding distributed analyze to stages");
    let len = stages.len();

    // Transform each stage to support distributed analysis
    for (i, stage) in stages.iter_mut().enumerate() {
        if i == len - 1 {
            // Final stage: create root analyzer that aggregates all worker metrics

            // Remove the original AnalyzeExec node since we'll replace it with distributed version
            let plan_without_analyze = stage
                .plan
                .clone()
                .transform_down(|plan: Arc<dyn ExecutionPlan>| {
                    if let Some(analyze) = plan.as_any().downcast_ref::<AnalyzeExec>() {
                        Ok(Transformed::yes(analyze.input().clone()))
                    } else {
                        Ok(Transformed::no(plan))
                    }
                })?
                .data;

            trace!(
                "plan without analyze: {}",
                displayable(plan_without_analyze.as_ref()).indent(false)
            );

            // Wrap with distributed analyze root that collects and formats final output
            stage.plan = Arc::new(DistributedAnalyzeRootExec::new(
                plan_without_analyze,
                verbose,
                show_statistics,
            )) as Arc<dyn ExecutionPlan>;

            // Reset partition groups to account for coalesced final output
            stage.partition_groups = vec![vec![0]];
        } else {
            // Intermediate stage: wrap with worker-level analyzer that collects local metrics
            stage.plan = Arc::new(DistributedAnalyzeExec::new(
                stage.plan.clone(),
                verbose,
                show_statistics,
            )) as Arc<dyn ExecutionPlan>;
        }
    }
    Ok(())
}

/// Distribute the stages to the workers, assigning each stage to a worker
/// Returns an Addrs containing the addresses of the workers that will execute
/// final stage only as that's all we care about from the call site
pub async fn distribute_stages(
    query_id: &str,
    stages: &[DDStage],
    worker_addrs: Vec<Host>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<(Addrs, Vec<DDTask>)> {
    // map of worker name to address
    // FIXME: use types over tuples of strings, as we can accidently swap them and
    // not know
    let mut workers: HashMap<String, Host> = worker_addrs
        .iter()
        .map(|host| (host.name.clone(), host.clone()))
        .collect();

    // Retry task distribution up to 3 times in case of worker failures
    for attempt in 0..3 {
        if workers.is_empty() {
            return Err(anyhow!("No workers available to distribute stages").into());
        }

        // Assign stages to specific workers and create task definitions
        let (task_datas, final_addrs) =
            assign_to_workers(query_id, stages, workers.values().collect(), codec)?;

        // Attempt to send all tasks to their assigned workers
        // If a worker fails, we'll retry with a reduced worker set
        match try_distribute_tasks(&task_datas).await {
            Ok(_) => return Ok((final_addrs, task_datas)),
            Err(DDError::WorkerCommunicationError(bad_worker)) => {
                error!(
                    "distribute stages for query {query_id} attempt {attempt} failed removing \
                     worker {bad_worker}. Retrying..."
                );
                // Remove failed worker and retry with remaining workers
                workers.remove(&bad_worker.name);
            }
            Err(e) => return Err(e),
        }
        if attempt == 2 {
            return Err(
                anyhow!("Failed to distribute query {query_id} stages after 3 attempts").into(),
            );
        }
    }
    unreachable!()
}

/// Try to distribute the tasks of stages to the workers, if we cannot communicate with a
/// worker return it as the element in the Err
async fn try_distribute_tasks(task_datas: &[DDTask]) -> Result<()> {
    // Send each task to its assigned worker via Arrow Flight protocol
    for task_data in task_datas {
        trace!(
            "Distributing Task: stage_id {}, pg: {:?} to worker: {:?}",
            task_data.stage_id,
            task_data.partition_group,
            task_data.assigned_host
        );

        // Prepare the task by adding addresses of dependent stages
        let mut task = task_data.clone();
        task.stage_addrs = Some(get_stage_addrs_from_tasks(
            &task.child_stage_ids,
            task_datas,
        )?);

        let host = task
            .assigned_host
            .clone()
            .context("Assigned host is missing for task data")?;

        // Create Arrow Flight client connection to the worker
        let mut client = match get_client(&host) {
            Ok(client) => client,
            Err(e) => {
                error!("Couldn't not communicate with worker {e:#?}");
                return Err(DDError::WorkerCommunicationError(
                    host.clone(), // here
                ));
            }
        };

        // Serialize the task data for network transmission
        let mut buf = vec![];
        task.encode(&mut buf)
            .context("Failed to encode task data to buf")?;

        // Send the task to worker using "add_plan" action
        // This tells the worker to store this execution plan for later execution
        let action = Action {
            r#type: "add_plan".to_string(),
            body: buf.into(),
        };

        let mut response = client
            .do_action(action)
            .await
            .context("Failed to send action to worker")?;

        // Consume the response to ensure the task was successfully received
        while let Some(_res) = response
            .try_next()
            .await
            .context("error consuming do_action response")?
        {
            // we don't care about the response, just that it was successful
        }
        trace!("do action success for stage_id: {}", task.stage_id);
    }
    Ok(())
}

/// Assigns execution stages to worker nodes and creates task definitions
///
/// This function performs the core work of distributing query stages across
/// available workers. It creates individual tasks for each partition group
/// within each stage and assigns them to workers using round-robin distribution.
///
/// # Distribution Strategy
/// - **Round-Robin Assignment**: Tasks are distributed evenly across workers
/// - **Partition Group Granularity**: Each partition group becomes a separate task
/// - **Load Balancing**: Workers receive roughly equal numbers of tasks
/// - **Final Stage Tracking**: Maintains addresses for the final stage (client results)
///
/// # Task Creation Process
/// 1. Serialize execution plan for network transmission
/// 2. Assign task to next worker in round-robin order
/// 3. Extract child stage dependencies for proper execution ordering
/// 4. Track final stage addresses for client result retrieval
///
/// # Arguments
/// * `query_id` - Unique identifier for this query execution
/// * `stages` - Execution stages to distribute across workers
/// * `worker_addrs` - Available worker nodes for task assignment
/// * `codec` - Serialization codec for execution plans
///
/// # Returns
/// * `(Vec<DDTask>, Addrs)` - Tuple of:
///   - Task definitions for each worker assignment
///   - Worker addresses for the final stage (for client result retrieval)
///
/// # Errors
/// Returns an error if execution plan serialization fails.
fn assign_to_workers(
    query_id: &str,
    stages: &[DDStage],
    worker_addrs: Vec<&Host>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<(Vec<DDTask>, Addrs)> {
    let mut task_datas = vec![];
    let mut worker_idx = 0;

    trace!(
        "assigning stages: {:?}",
        stages
            .iter()
            .map(|s| format!("stage_id: {}, pgs:{:?}", s.stage_id, s.partition_groups))
            .join(",\n")
    );

    // Track the final stage (highest stage ID) for client result coordination
    let mut max_stage_id = -1;
    let mut final_addrs = Addrs::default();

    // Process each stage and create tasks for its partition groups
    for stage in stages {
        // Create a task for each partition group within this stage
        for partition_group in stage.partition_groups.iter() {
            // Serialize the execution plan for network transmission to worker
            let plan_bytes = physical_plan_to_bytes(stage.plan.clone(), codec)?;

            // Assign this task to the next worker using round-robin distribution
            let host = worker_addrs[worker_idx].clone();
            worker_idx = (worker_idx + 1) % worker_addrs.len();

            // Track the final stage addresses for client result retrieval
            if stage.stage_id as isize > max_stage_id {
                // Found a new highest stage - this becomes the final stage
                max_stage_id = stage.stage_id as isize;
                final_addrs.clear();
            }
            if stage.stage_id as isize == max_stage_id {
                // This is the final stage - track worker addresses for each partition
                for part in partition_group.iter() {
                    // Map: stage_id -> partition_id -> [worker_hosts]
                    final_addrs
                        .entry(stage.stage_id)
                        .or_default()
                        .entry(*part)
                        .or_default()
                        .push(host.clone());
                }
            }

            // Create the task definition with all necessary metadata
            let task_data = DDTask {
                query_id: query_id.to_string(),
                stage_id: stage.stage_id,
                plan_bytes,
                partition_group: partition_group.to_vec(),
                child_stage_ids: stage.child_stage_ids().unwrap_or_default().to_vec(),
                stage_addrs: None, // Will be populated later with dependency addresses
                num_output_partitions: stage.plan.output_partitioning().partition_count() as u64,
                full_partitions: stage.full_partitions,
                assigned_host: Some(host),
            };
            task_datas.push(task_data);
        }
    }

    Ok((task_datas, final_addrs))
}

/// Extracts worker addresses for dependent stages from task assignments
///
/// This function builds a mapping of stage addresses needed for inter-stage
/// communication. When a stage depends on other stages (through DDStageReaderExec
/// nodes), it needs to know which workers are executing those dependency stages.
///
/// # Address Resolution Process
/// 1. For each target stage ID that this stage depends on
/// 2. Find all tasks executing that target stage
/// 3. Group worker addresses by partition for that stage
/// 4. Build complete address mapping for dependency resolution
///
/// # Arguments
/// * `target_stage_ids` - List of stage IDs that the current stage depends on
/// * `stages` - All task assignments across the distributed query
///
/// # Returns
/// * `StageAddrs` - Mapping of stage_id -> partition_id -> [worker_addresses]
///
/// # Usage
/// Used during task distribution to populate the `stage_addrs` field in DDTask,
/// allowing workers to connect to the correct dependency stages during execution.
///
/// # Address Structure
/// The returned StageAddrs uses this structure:
/// ```text
/// {
///   stage_1: {
///     partition_0: [worker_a, worker_b],
///     partition_1: [worker_c]
///   },
///   stage_2: { ... }
/// }
/// ```
fn get_stage_addrs_from_tasks(target_stage_ids: &[u64], stages: &[DDTask]) -> Result<StageAddrs> {
    let mut stage_addrs = StageAddrs::default();

    // this can be more efficient

    trace!(
        "getting stage addresses for target stages: {:?}",
        target_stage_ids
    );

    // Build address mapping for each dependent stage
    for &stage_id in target_stage_ids {
        let mut partition_addrs = PartitionAddrs::default();

        // Find all tasks that are executing the target stage
        for stage in stages {
            if stage.stage_id == stage_id {
                // Map each partition in this task's partition group to worker addresses
                for part in stage.partition_group.iter() {
                    if !stage.full_partitions {
                        // Partial partitions: collect all workers executing this stage
                        // DDStageReaderExec will need to fan out to read from all workers
                        let hosts = stages
                            .iter()
                            .filter(|s| s.stage_id == stage_id)
                            .map(|s| {
                                s.assigned_host.clone().ok_or_else(|| {
                                    anyhow!(
                                        "Assigned address is missing for stage_id: {}",
                                        stage_id
                                    )
                                    .into()
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;

                        partition_addrs
                            .partition_addrs
                            .insert(*part, Hosts { hosts });
                    } else {
                        // Full partitions: only need the specific worker for this partition
                        // This worker has the complete partition data
                        let host = stage
                            .assigned_host
                            .clone()
                            .context("Assigned address is missing")?;
                        partition_addrs
                            .partition_addrs
                            .insert(*part, Hosts { hosts: vec![host] });
                    }
                }
            }
        }
        stage_addrs.stage_addrs.insert(stage_id, partition_addrs);
    }
    trace!("returned stage_addrs: {:?}", stage_addrs);

    Ok(stage_addrs)
}

/// Builds a modified execution plan with partition grouping and optimization layers
///
/// This function creates a replacement execution plan that can be distributed across
/// workers with optimal batch sizes and partition isolation. It wraps the original
/// plan with additional execution nodes that improve network efficiency and worker
/// coordination.
///
/// # Partition Grouping Strategy
/// - **With partitions_per_worker**: Splits partitions into groups of specified size
/// - **Without partitions_per_worker**: All partitions in a single group
/// - Groups determine how work is distributed among workers
///
/// # Execution Plan Modifications
/// 1. **Partition Isolation**: Adds PartitionIsolatorExec when multiple groups exist
///    to ensure partitions execute independently on workers
/// 2. **Batch Optimization**: Wraps with CoalesceBatchesExec to optimize network transfer
/// 3. **Row Limiting**: Adds MaxRowsExec to prevent oversized network transmissions
///
/// # Arguments
/// * `plan` - Original execution plan to be modified for distributed execution
/// * `partitions_per_worker` - Optional limit on partitions per worker group
/// * `isolate` - Whether to add partition isolation for multi-group scenarios
/// * `max_rows` - Maximum rows per batch to prevent network overload
/// * `inner_batch_size` - Target batch size for network efficiency
///
/// # Returns
/// * `(Vec<Vec<u64>>, Arc<dyn ExecutionPlan>)` - Tuple containing:
///   - Partition groups showing how partitions are distributed
///   - Modified execution plan with optimization layers
///
/// # Optimization Layers
/// The returned plan includes these optimization layers (from innermost to outermost):
/// 1. **Original Plan**: The base execution logic
/// 2. **PartitionIsolatorExec**: Isolates partitions for independent execution (if needed)
/// 3. **CoalesceBatchesExec**: Combines small batches for network efficiency
/// 4. **MaxRowsExec**: Limits batch size to prevent network congestion
#[allow(clippy::type_complexity)]
fn build_replacement(
    plan: Arc<dyn ExecutionPlan>,
    partitions_per_worker: Option<usize>,
    isolate: bool,
    max_rows: usize,
    inner_batch_size: usize,
) -> Result<(Vec<Vec<u64>>, Arc<dyn ExecutionPlan>), DataFusionError> {
    let mut replacement = plan.clone();
    let children = plan.children();
    assert!(children.len() == 1, "Unexpected plan structure");

    let child = children[0];
    let partition_count = plan.output_partitioning().partition_count() as u64;
    trace!(
        "build_replacement for {}, partition_count: {}",
        displayable(plan.as_ref()).one_line(),
        partition_count
    );

    // Create partition groups based on the partitions_per_worker setting
    let partition_groups = match partitions_per_worker {
        Some(p) => {
            // Split partitions into groups of size p
            (0..partition_count)
                .chunks(p)
                .into_iter()
                .map(|chunk| chunk.collect())
                .collect()
        }
        None => {
            // All partitions in a single group (all on one worker)
            vec![(0..partition_count).collect()]
        }
    };

    // Add partition isolation if we have multiple groups
    if isolate && partition_groups.len() > 1 {
        let new_child = Arc::new(PartitionIsolatorExec::new(
            child.clone(),
            partitions_per_worker.unwrap(), // Safe: we know it's Some here
        ));
        replacement = replacement.clone().with_new_children(vec![new_child])?;
    }

    // Add batch optimization layers for efficient network transmission
    // This prevents sending too many small batches or too few large batches
    replacement = Arc::new(MaxRowsExec::new(
        Arc::new(CoalesceBatchesExec::new(replacement, inner_batch_size)) as Arc<dyn ExecutionPlan>,
        max_rows,
    )) as Arc<dyn ExecutionPlan>;

    Ok((partition_groups, replacement))
}
