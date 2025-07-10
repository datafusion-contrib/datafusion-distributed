use std::sync::Arc;

use anyhow::anyhow;
use arrow::datatypes::SchemaRef;
use datafusion::{
    logical_expr::LogicalPlan, physical_plan::ExecutionPlan, prelude::SessionContext,
};

use crate::{
    logging::debug,
    planning::{
        distribute_stages, execution_planning, get_ctx, logical_planning, physical_planning,
        DDStage,
    },
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

impl QueryPlan {
    pub fn is_explain(&self) -> bool {
        match self.logical_plan {
            LogicalPlan::Explain { .. } => true,
            _ => false,
        }
    }
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
pub struct QueryPlanner;

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryPlanner {
    pub fn new() -> Self {
        Self
    }

    /// Common planning steps shared by both query and its EXPLAIN
    ///
    /// Prepare a query by parsing the SQL, planning it, and distributing the
    /// physical plan into stages that can be executed by workers.
    pub async fn prepare_query(&self, sql: &str) -> Result<QueryPlan> {
        let query_id = uuid::Uuid::new_v4().to_string();
        let ctx = get_ctx().map_err(|e| anyhow!("Could not create context: {e}"))?;

        let logical_plan = logical_planning(sql, &ctx).await?;
        let physical_plan = physical_planning(&logical_plan, &ctx).await?;

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
        let (final_workers, tasks) =
            distribute_stages(&query_id, distributed_stages, worker_addrs).await?;

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
