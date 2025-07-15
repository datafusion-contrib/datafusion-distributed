use std::sync::Arc;

use anyhow::{anyhow, Context as AnyhowContext};
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
        distribute_stages, execution_planning, get_ctx, logical_planning, physical_planning,
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
                .or(Some(Arc::new(DefaultPhysicalExtensionCodec {})))
                .unwrap(),
        ));

        Self { customizer, codec }
    }

    /// Common planning steps shared by both query and its EXPLAIN
    ///
    /// Prepare a query by parsing the SQL, planning it, and distributing the
    /// physical plan into stages that can be executed by workers.
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

    pub async fn prepare_substrait(&self, substrait_plan: Plan) -> Result<QueryPlan> {
        let ctx = get_ctx().map_err(|e| anyhow!("Could not create context: {e}"))?;

        let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;

        match logical_plan {
            p @ LogicalPlan::Explain(_) => self.prepare_explain(p, ctx).await,
            // add other logical plans for local execution here following the pattern for explain
            p @ LogicalPlan::DescribeTable(_) => self.prepare_local(p, ctx).await,
            p => self.prepare_query(p, ctx).await,
        }
    }

    async fn prepare_query(
        &self,
        logical_plan: LogicalPlan,
        ctx: SessionContext,
    ) -> Result<QueryPlan> {
        let physical_plan = physical_planning(&logical_plan, &ctx).await?;

        self.send_it(logical_plan, physical_plan, ctx).await
    }

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

        self.send_it(logical_plan, physical_plan, ctx).await
    }

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

        let query_plan = self.prepare_query(logical_plan.clone(), ctx).await?;

        let batch = build_explain_batch(
            &query_plan.logical_plan,
            &query_plan.physical_plan,
            &query_plan.distributed_plan,
            &query_plan.distributed_tasks,
            self.codec.as_ref(),
        )?;
        let physical_plan = Arc::new(RecordBatchExec::new(batch));

        self.send_it(
            query_plan.logical_plan,
            physical_plan,
            query_plan.session_context,
        )
        .await
    }
    async fn send_it(
        &self,
        logical_plan: LogicalPlan,
        physical_plan: Arc<dyn ExecutionPlan>,
        ctx: SessionContext,
    ) -> Result<QueryPlan> {
        let query_id = uuid::Uuid::new_v4().to_string();

        // divide the physical plan into chunks (tasks) that we can distribute to workers
        let (distributed_plan, distributed_stages) =
            execution_planning(physical_plan.clone(), 8192, Some(2)).await?;

        let worker_addrs = get_worker_addresses()?;

        // gather some information we need to send back such that
        // we can send a ticket to the client
        let final_stage = &distributed_stages[distributed_stages.len() - 1];
        let schema = Arc::clone(&final_stage.plan.schema());
        let final_stage_id = final_stage.stage_id;

        // distribute the stages to workers, further dividing them up
        // into chunks of partitions (partition_groups)
        let (final_workers, tasks) = distribute_stages(
            &query_id,
            distributed_stages,
            worker_addrs,
            self.codec.as_ref(),
        )
        .await?;

        let qp = QueryPlan {
            query_id,
            session_context: ctx,
            worker_addresses: final_workers,
            final_stage_id,
            schema,
            logical_plan,
            physical_plan,
            distributed_plan,
            distributed_tasks: tasks,
        };

        Ok(qp)
    }
}
