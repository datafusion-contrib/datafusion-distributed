use std::sync::Arc;

use anyhow::anyhow;
use arrow::{compute::concat_batches, datatypes::SchemaRef};
use datafusion::{
    logical_expr::LogicalPlan,
    physical_plan::{coalesce_partitions::CoalescePartitionsExec, ExecutionPlan},
    prelude::SessionContext,
};

use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use datafusion_substrait::{logical_plan::consumer::from_substrait_plan, substrait::proto::Plan};
use tokio_stream::StreamExt;

use crate::{
    codec::DDCodec,
    customizer::Customizer,
    explain::build_explain_batch,
    planning::{
        distribute_stages, distributed_physical_planning, get_ctx, logical_planning,
        physical_planning, DDStage,
    },
    record_batch_exec::RecordBatchExec,
    result::Result,
    vocab::{Addrs, DDTask},
    worker_discovery::get_worker_addresses,
};

/// Complete query execution plan for distributed DataFusion queries
///
/// This structure contains all the information needed to execute a query in a distributed
/// DataFusion cluster. It includes the original logical plan, optimized physical plans,
/// distributed execution stages, and worker assignments. The QueryPlan is created during
/// the planning phase and used throughout the query execution lifecycle.
///
/// # Query Processing Lifecycle
/// 1. **Planning**: SQL → LogicalPlan → PhysicalPlan → DistributedPlan with stages
/// 2. **Distribution**: Stages assigned to workers, tasks sent to worker nodes
/// 3. **Execution**: Workers execute tasks, proxy coordinates result retrieval
/// 4. **Results**: Combined results streamed back to client
///
/// # Plan Variants
/// - `logical_plan`: DataFusion's optimized logical representation
/// - `physical_plan`: Local physical execution plan (for comparison/analysis)
/// - `distributed_plan`: Distributed version with stage readers replacing stages
/// - `distributed_stages`: Individual stages that get executed on workers
pub struct QueryPlan {
    /// Unique identifier for this query execution (UUID)
    pub query_id: String,

    /// DataFusion session context with distributed execution metadata
    pub session_context: SessionContext,

    /// Map of worker addresses organized by stage and partition
    /// Populated after distribute_plan() is called
    pub worker_addresses: Addrs,

    /// Stage ID of the final stage that produces results for the client
    pub final_stage_id: u64,

    /// Schema of the query results (from the final stage)
    pub schema: SchemaRef,

    /// Original logical plan after DataFusion optimizations
    pub logical_plan: LogicalPlan,

    /// Physical execution plan for local execution (used for EXPLAIN and analysis)
    pub physical_plan: Arc<dyn ExecutionPlan>,

    /// Distributed physical plan with DDStageReaderExec nodes
    /// This plan coordinates reading results from distributed workers
    pub distributed_plan: Arc<dyn ExecutionPlan>,

    /// Tasks that were sent to workers during distribution
    /// Each task represents a portion of a stage assigned to a specific worker
    pub distributed_tasks: Vec<DDTask>,

    /// Execution stages that make up the distributed query
    /// Each stage represents a unit of work that can be executed independently
    pub distributed_stages: Vec<DDStage>,
}

/// Custom Debug implementation for QueryPlan to provide readable query plan information
///
/// This implementation provides a concise debug representation of the QueryPlan without
/// overwhelming output. Complex nested structures like SessionContext and execution plans
/// are summarized to show their type and key information rather than full details.
///
/// # Debug Output
/// - Includes query_id, logical_plan structure, and plan names
/// - Shows number of distributed stages without full stage details
/// - Excludes worker addresses and detailed execution plans to keep output manageable
impl std::fmt::Debug for QueryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryPlan")
            .field("query_id", &self.query_id)
            .field("session_context", &"<SessionContext>")
            .field("logical_plan", &self.logical_plan)
            .field("physical_plan", &format!("<{}>", self.physical_plan.name()))
            .field(
                "distributed_plan",
                &format!("<{}>", self.distributed_plan.name()),
            )
            .field(
                "stages",
                &format!("<{} stages>", self.distributed_tasks.len()),
            )
            .finish()
    }
}

/// Distributed query planner for DataFusion SQL and Substrait queries
///
/// The QueryPlanner is responsible for converting SQL queries and Substrait plans into
/// distributed execution plans that can run across multiple worker nodes. It handles
/// the complete planning pipeline from SQL parsing to worker task distribution.
///
/// # Planning Pipeline
/// 1. **SQL/Substrait Parsing**: Convert input to DataFusion LogicalPlan
/// 2. **Physical Planning**: Generate optimized physical execution plan
/// 3. **Distributed Planning**: Break plan into stages with network boundaries
/// 4. **Stage Distribution**: Assign stages to workers and send execution tasks
/// 5. **Reader Creation**: Create plans that read results from distributed workers
///
/// # Query Types Supported
/// - **Regular SQL**: SELECT, JOIN, aggregations, sorting, filtering
/// - **Substrait Plans**: Pre-compiled cross-language query plans
/// - **EXPLAIN Queries**: Analysis and visualization of query execution
/// - **Local Queries**: DESCRIBE TABLE, metadata queries (executed on proxy)
///
/// # Distributed Execution Model
/// - Stages represent units of work that can execute independently
/// - Stage boundaries are inserted at operations requiring data movement
/// - Workers execute stages and store results for later retrieval
/// - Proxy coordinates execution and combines results from workers
pub struct QueryPlanner {
    /// Optional customization for DataFusion contexts, object stores, and UDFs
    customizer: Option<Arc<dyn Customizer>>,
    /// Codec for serializing/deserializing execution plans for network transmission
    codec: Arc<dyn PhysicalExtensionCodec>,
}

/// Default implementation creates a QueryPlanner with no customizations
///
/// This provides a standard QueryPlanner instance with default DataFusion settings,
/// suitable for most distributed query processing scenarios. Uses the default
/// DDCodec for plan serialization and no custom context modifications.
impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new(None)
    }
}

impl QueryPlanner {
    /// Creates a new distributed query planner with optional customizations
    ///
    /// This constructor sets up a QueryPlanner capable of processing distributed
    /// DataFusion queries. It initializes the plan serialization codec and applies
    /// any provided customizations for specialized deployment scenarios.
    ///
    /// # Arguments
    /// * `customizer` - Optional customization for:
    ///   - DataFusion context configuration
    ///   - Custom object stores (S3, GCS, etc.)
    ///   - User-defined functions (UDFs)
    ///   - Plan serialization modifications
    ///
    /// # Returns
    /// * `QueryPlanner` - Ready-to-use distributed query planner
    ///
    /// # Codec Configuration
    /// The planner automatically configures an appropriate codec for serializing
    /// execution plans for network transmission to workers:
    /// - Uses DDCodec with custom extensions if customizer provided
    /// - Falls back to DefaultPhysicalExtensionCodec for standard DataFusion plans
    pub fn new(customizer: Option<Arc<dyn Customizer>>) -> Self {
        // Configure codec for serializing execution plans across the network
        // DDCodec handles distributed-specific plan nodes like DDStageReaderExec
        let codec = Arc::new(DDCodec::new(
            customizer
                .clone()
                .map(|c| c as Arc<dyn PhysicalExtensionCodec>)
                .unwrap_or(Arc::new(DefaultPhysicalExtensionCodec {})),
        ));

        Self { customizer, codec }
    }

    /// Prepare a Distributed DataFusion plan from a sql query.
    ///
    /// This function parses the SQL, produces a logical plan, then derives the
    /// physical plan and its distributed counterpart.  
    /// The resulting `QueryPlan` includes the logical plan, physical plan,
    /// distributed plan, and distributed stages, but it does not yet contain
    /// worker addresses or tasks, as they are filled in later by `distribute_plan()`.
    pub async fn prepare(&self, sql: &str) -> Result<QueryPlan> {
        // Create DataFusion execution context with default settings
        let mut ctx = get_ctx().map_err(|e| anyhow!("Could not create context: {e}"))?;

        // Apply any custom configuration (e.g., file format)
        if let Some(customizer) = &self.customizer {
            customizer
                .customize(&mut ctx)
                .await
                .map_err(|e| anyhow!("Customization failed: {e:#?}"))?;
        }

        // Parse SQL string into DataFusion LogicalPlan
        let logical_plan = logical_planning(sql, &ctx).await?;

        // Route to appropriate preparation method based on query type
        match logical_plan {
            p @ LogicalPlan::Explain(_) => self.prepare_explain(p, ctx).await,
            // add other logical plans for local execution here following the pattern for explain
            p @ LogicalPlan::DescribeTable(_) => self.prepare_local(p, ctx).await,
            p => self.prepare_query(p, ctx).await,
        }
    }

    /// Prepare a Distributed DataFusion plan from a Substrait plan.
    ///
    /// 1. Convert the incoming Substrait plan into a `LogicalPlan` with DataFusion’s
    ///    default Substrait consumer.
    /// 2. Derive the corresponding physical plan and distributed variant.
    ///
    /// The resulting `QueryPlan` contains the logical plan, physical plan,
    /// distributed plan, and distributed stages, but it does not yet contain
    /// worker addresses or tasks, as they are filled in later by `distribute_plan()`.
    pub async fn prepare_substrait(&self, substrait_plan: Plan) -> Result<QueryPlan> {
        let mut ctx = get_ctx().map_err(|e| anyhow!("Could not create context: {e}"))?;
        if let Some(customizer) = &self.customizer {
            customizer
                .customize(&mut ctx)
                .await
                .map_err(|e| anyhow!("Customization failed: {e:#?}"))?;
        }

        let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;

        match logical_plan {
            p @ LogicalPlan::Explain(_) => self.prepare_explain(p, ctx).await,
            // add other logical plans for local execution here following the pattern for explain
            p @ LogicalPlan::DescribeTable(_) => self.prepare_local(p, ctx).await,
            p => self.prepare_query(p, ctx).await,
        }
    }

    /// Prepare a `QueryPlan` for a regular SELECT query
    async fn prepare_query(
        &self,
        logical_plan: LogicalPlan,
        ctx: SessionContext,
    ) -> Result<QueryPlan> {
        // construct the initial physical plan from the logical plan
        let physical_plan = physical_planning(&logical_plan, &ctx).await?;

        // construct the distributed physical plan and stages
        // This breaks the plan into stages that can run on different workers
        let (distributed_plan, distributed_stages) =
            distributed_physical_planning(physical_plan.clone(), 8192, Some(2)).await?;

        // Create the QueryPlan structure containing all plan variants
        // Note: worker addresses and tasks are not yet assigned
        return self
            .build_initial_plan(
                distributed_plan,
                distributed_stages,
                ctx,
                logical_plan,
                physical_plan,
            )
            .await;
    }

    /// Prepare a `QueryPlan` for statements that should run locally on the proxy
    /// node (e.g. `DESCRIBE TABLE`).
    async fn prepare_local(
        &self,
        logical_plan: LogicalPlan,
        ctx: SessionContext,
    ) -> Result<QueryPlan> {
        // Convert logical plan to physical plan for local execution
        let physical_plan = physical_planning(&logical_plan, &ctx).await?;

        // Execute the plan locally on the proxy to get immediate results
        // This is used for metadata queries that don't benefit from distribution
        let mut stream =
            Arc::new(CoalescePartitionsExec::new(physical_plan)).execute(0, ctx.task_ctx())?;
        let mut batches = vec![];

        // Collect all result batches from the local execution
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        if batches.is_empty() {
            return Err(anyhow!("No data returned from local execution").into());
        }

        // Combine all batches into a single batch for consistent output
        let combined_batch = concat_batches(&batches[0].schema(), &batches)?;

        // Create a RecordBatchExec to hold the pre-computed results
        // This allows the results to be treated like any other execution plan
        let physical_plan = Arc::new(RecordBatchExec::new(combined_batch));

        // Even though this was executed locally, still create a distributed plan structure
        // This ensures consistent QueryPlan interface regardless of execution location
        let (distributed_plan, distributed_stages) =
            distributed_physical_planning(physical_plan.clone(), 8192, Some(2)).await?;

        // Build the final QueryPlan with pre-computed results
        return self
            .build_initial_plan(
                distributed_plan,
                distributed_stages,
                ctx,
                logical_plan,
                physical_plan,
            )
            .await;
    }

    /// Prepare a `QueryPlan` for an EXPLAIN statement.
    async fn prepare_explain(
        &self,
        explain_plan: LogicalPlan,
        ctx: SessionContext,
    ) -> Result<QueryPlan> {
        // Extract the child plan that EXPLAIN is analyzing
        let child_plan = explain_plan.inputs();
        if child_plan.len() != 1 {
            return Err(anyhow!("EXPLAIN plan must have exactly one child").into());
        }

        let logical_plan = child_plan[0];

        // Create a distributed query plan for the inner query being explained
        // This generates all the planning information without actually executing
        let mut query_plan = self.prepare_query(logical_plan.clone(), ctx).await?;

        // Generate an EXPLAIN output batch showing the query execution plans
        // This includes logical, physical, and distributed plan representations
        let batch = build_explain_batch(
            &query_plan.logical_plan,
            &query_plan.physical_plan,
            &query_plan.distributed_plan,
            &query_plan.distributed_tasks,
            self.codec.as_ref(),
        )?;

        // Replace the physical plan with a RecordBatchExec containing the EXPLAIN results
        // This allows EXPLAIN output to be treated like regular query results
        let physical_plan = Arc::new(RecordBatchExec::new(batch));
        query_plan.physical_plan = physical_plan.clone();

        Ok(query_plan)
    }

    /// Constructs the initial QueryPlan structure with all plan variants
    ///
    /// This method creates a complete QueryPlan containing all the different plan
    /// representations needed for distributed execution. It generates a unique query ID,
    /// extracts metadata from the final stage, and packages everything into a consistent
    /// structure that can be used throughout the execution lifecycle.
    ///
    /// # Arguments
    /// * `distributed_plan` - Physical plan with DDStageReaderExec nodes for distributed execution
    /// * `distributed_stages` - Individual stages that will be sent to workers
    /// * `ctx` - DataFusion session context with configuration and metadata
    /// * `logical_plan` - Original logical plan for analysis and debugging
    /// * `physical_plan` - Local physical plan for comparison and EXPLAIN output
    ///
    /// # Returns
    /// * `QueryPlan` - Complete query plan ready for distribution (worker addresses not yet populated)
    ///
    /// # Plan State
    /// The returned QueryPlan has empty worker_addresses and distributed_tasks.
    /// These fields are populated later by distribute_plan() when stages are assigned to workers.
    async fn build_initial_plan(
        &self,
        distributed_plan: Arc<dyn ExecutionPlan>,
        distributed_stages: Vec<DDStage>,
        ctx: SessionContext,
        logical_plan: LogicalPlan,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<QueryPlan> {
        // Generate a unique identifier for this query execution
        let query_id = uuid::Uuid::new_v4().to_string();

        // Extract metadata from the final stage for client communication
        // The final stage contains the schema and stage ID that clients need
        let final_stage = &distributed_stages[distributed_stages.len() - 1];
        let schema = Arc::clone(&final_stage.plan.schema());
        let final_stage_id = final_stage.stage_id;

        Ok(QueryPlan {
            query_id,
            session_context: ctx,
            final_stage_id,
            schema,
            logical_plan,
            physical_plan,
            distributed_plan,
            distributed_stages,
            // Worker addresses and tasks will be populated by distribute_plan()
            worker_addresses: Addrs::default(),
            distributed_tasks: Vec::new(),
        })
    }

    /// Performs worker discovery, and distributes the query plan to workers,
    /// also sets the final worker addresses and distributed tasks in the query plan.
    pub async fn distribute_plan(&self, initial_plan: &mut QueryPlan) -> Result<()> {
        // Discover all available worker nodes in the cluster
        let worker_addrs = get_worker_addresses()?;

        // Assign stages to workers and send execution tasks to each worker
        // This breaks stages into partition groups and distributes them across workers
        // Returns the final worker addresses and the tasks that were sent
        let (final_workers, tasks) = distribute_stages(
            &initial_plan.query_id,
            &initial_plan.distributed_stages,
            worker_addrs,
            self.codec.as_ref(),
        )
        .await?;

        // Update the query plan with worker assignments and distributed tasks
        initial_plan.worker_addresses = final_workers;
        initial_plan.distributed_tasks = tasks;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::displayable;
    use insta::assert_snapshot;
    use std::io::BufReader;
    use std::{fs::File, path::Path};

    /// Tests Substrait plan preparation and distributed plan generation
    ///
    /// Validates that a simple Substrait SELECT 1 query is correctly converted
    /// to a distributed execution plan with proper schema preservation.
    #[tokio::test]
    async fn prepare_substrait_select_one() -> anyhow::Result<()> {
        // Load Substrait and parse to protobuf `Plan`.
        let file = File::open(Path::new("testdata/substrait/select_one.substrait.json"))?;
        let reader = BufReader::new(file);
        let plan: Plan = serde_json::from_reader(reader)?;

        let planner = QueryPlanner::default();
        let qp = planner.prepare_substrait(plan).await?;

        // Distributed plan schema must match logical schema.
        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "test_col",
            DataType::Int64,
            false,
        )]));
        assert_eq!(qp.distributed_plan.schema(), expected_schema);

        // Check the distributed physical plan.
        let distributed_plan_str =
            format!("{}", displayable(qp.distributed_plan.as_ref()).indent(true));
        assert_snapshot!(distributed_plan_str, @r"
        DDStageExec[0] (output_partitioning=UnknownPartitioning(1))
          ProjectionExec: expr=[1 as test_col]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");

        Ok(())
    }

    /// Tests SQL query preparation and distributed plan generation
    ///
    /// Validates that a simple SQL SELECT 1 query is correctly parsed and converted
    /// to a distributed execution plan with appropriate stage structure.
    #[tokio::test]
    async fn prepare_sql_select_one() -> Result<()> {
        let planner = QueryPlanner::default();
        let sql = "SELECT 1 AS test_col";

        let qp = planner.prepare(sql).await?;

        // Distributed plan schema must match logical schema.
        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "test_col",
            DataType::Int64,
            false,
        )]));
        assert_eq!(qp.distributed_plan.schema(), expected_schema);

        // Check the distributed physical plan.
        let distributed_plan_str =
            format!("{}", displayable(qp.distributed_plan.as_ref()).indent(true));
        assert_snapshot!(distributed_plan_str, @r"
        DDStageExec[0] (output_partitioning=UnknownPartitioning(1))
          ProjectionExec: expr=[1 as test_col]
            PlaceholderRowExec
        ");

        Ok(())
    }

    /// Tests local query execution for metadata queries
    ///
    /// Validates that DESCRIBE TABLE queries are executed locally on the proxy
    /// rather than being distributed, and that results are properly packaged
    /// into a distributed plan structure for consistency.
    #[tokio::test]
    async fn prepare_describe_table() -> Result<()> {
        std::env::set_var(
            "DD_TABLES",
            "people:parquet:testdata/parquet/people.parquet",
        );

        let planner = QueryPlanner::default();
        let sql = "DESCRIBE people";

        let qp = planner.prepare(sql).await?;

        // Check the distributed physical plan.
        let distributed_plan_str =
            format!("{}", displayable(qp.distributed_plan.as_ref()).indent(true));
        assert_snapshot!(distributed_plan_str, @r"
        DDStageExec[0] (output_partitioning=UnknownPartitioning(1))
          RecordBatchExec
        ");

        Ok(())
    }

    /// Tests multi-stage distributed query planning
    ///
    /// Validates that complex queries requiring multiple stages (due to operations
    /// like sorting that require stage boundaries) are correctly broken into
    /// separate execution stages with proper stage dependency structure.
    #[tokio::test]
    async fn two_stages_query() -> Result<()> {
        std::env::set_var(
            "DD_TABLES",
            "people:parquet:testdata/parquet/people.parquet",
        );

        let planner = QueryPlanner::default();
        let sql = "SELECT * FROM (SELECT 1 as id) a CROSS JOIN (SELECT 2 as id) b order by b.id";
        let qp = planner.prepare(sql).await?;

        // Distributed plan schema must match logical schema.
        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ]));

        assert_eq!(qp.distributed_plan.schema(), expected_schema);

        // Check the distributed physical plan.
        let distributed_plan_str =
            format!("{}", displayable(qp.distributed_plan.as_ref()).indent(true));
        assert_snapshot!(distributed_plan_str, @r"
        DDStageExec[1] (output_partitioning=UnknownPartitioning(1))
          DDStageExec[0] (output_partitioning=UnknownPartitioning(1))
            SortExec: expr=[id@1 ASC NULLS LAST], preserve_partitioning=[false]
              CrossJoinExec
                ProjectionExec: expr=[1 as id]
                  PlaceholderRowExec
                ProjectionExec: expr=[2 as id]
                  PlaceholderRowExec
        ");

        Ok(())
    }
}
