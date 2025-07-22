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

/// Result of query preparation for execution of both query and its EXPLAIN
pub struct QueryPlan {
    pub query_id: String,
    pub session_context: SessionContext,
    pub worker_addresses: Addrs,
    pub final_stage_id: u64,
    pub schema: SchemaRef,
    pub logical_plan: LogicalPlan,
    pub physical_plan: Arc<dyn ExecutionPlan>,
    pub distributed_plan: Arc<dyn ExecutionPlan>,
    pub distributed_tasks: Vec<DDTask>,
    pub distributed_stages: Vec<DDStage>,
}

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

/// Query planner responsible for preparing SQL queries for distributed execution
pub struct QueryPlanner {
    customizer: Option<Arc<dyn Customizer>>,
    codec: Arc<dyn PhysicalExtensionCodec>,
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new(None)
    }
}

impl QueryPlanner {
    pub fn new(customizer: Option<Arc<dyn Customizer>>) -> Self {
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
        let mut ctx = get_ctx().map_err(|e| anyhow!("Could not create context: {e}"))?;
        if let Some(customizer) = &self.customizer {
            customizer
                .customize(&mut ctx)
                .await
                .map_err(|e| anyhow!("Customization failed: {e:#?}"))?;
        }

        let logical_plan = logical_planning(sql, &ctx).await?;

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
        let (distributed_plan, distributed_stages) =
            distributed_physical_planning(physical_plan.clone(), 8192, Some(2)).await?;

        // build the initial plan, without the worker addresses and DDTasks
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
        let physical_plan = physical_planning(&logical_plan, &ctx).await?;

        // execute it locally
        let mut stream =
            Arc::new(CoalescePartitionsExec::new(physical_plan)).execute(0, ctx.task_ctx())?;
        let mut batches = vec![];

        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        if batches.is_empty() {
            return Err(anyhow!("No data returned from local execution").into());
        }

        let combined_batch = concat_batches(&batches[0].schema(), &batches)?;
        let physical_plan = Arc::new(RecordBatchExec::new(combined_batch));

        // construct the distributed physical plan and stages
        let (distributed_plan, distributed_stages) =
            distributed_physical_planning(physical_plan.clone(), 8192, Some(2)).await?;

        // build the initial plan, without the worker addresses and DDTasks
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
        let child_plan = explain_plan.inputs();
        if child_plan.len() != 1 {
            return Err(anyhow!("EXPLAIN plan must have exactly one child").into());
        }

        let logical_plan = child_plan[0];

        // construct the initial distributed physical plan, without the worker addresses and DDTasks
        let mut query_plan = self.prepare_query(logical_plan.clone(), ctx).await?;

        let batch = build_explain_batch(
            &query_plan.logical_plan,
            &query_plan.physical_plan,
            &query_plan.distributed_plan,
            &query_plan.distributed_tasks,
            self.codec.as_ref(),
        )?;
        let physical_plan = Arc::new(RecordBatchExec::new(batch));
        query_plan.physical_plan = physical_plan.clone();

        Ok(query_plan)
    }

    async fn build_initial_plan(
        &self,
        distributed_plan: Arc<dyn ExecutionPlan>,
        distributed_stages: Vec<DDStage>,
        ctx: SessionContext,
        logical_plan: LogicalPlan,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<QueryPlan> {
        let query_id = uuid::Uuid::new_v4().to_string();

        // gather some information we need to send back such that
        // we can send a ticket to the client
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
            // will be populated on distribute_plan
            worker_addresses: Addrs::default(),
            distributed_tasks: Vec::new(),
        })
    }

    /// Performs worker discovery, and distributes the query plan to workers,
    /// also sets the final worker addresses and distributed tasks in the query plan.
    pub async fn distribute_plan(&self, initial_plan: &mut QueryPlan) -> Result<()> {
        // Perform worker discovery
        let worker_addrs = get_worker_addresses()?;

        // Distribute the stages to workers, further dividing them up
        // into chunks of partitions (partition_groups)
        let (final_workers, tasks) = distribute_stages(
            &initial_plan.query_id,
            &initial_plan.distributed_stages,
            worker_addrs,
            self.codec.as_ref(),
        )
        .await?;

        // set the distributed tasks and final worker addresses
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
