use crate::DefaultSessionBuilder;
use crate::DistributedPhysicalOptimizerRule;
use crate::test_utils::localhost::start_localhost_context;
use crate::test_utils::parquet::register_parquet_tables;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::runtime::JoinSet;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::execution_plan::collect;
use regex::Regex;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use std::sync::Arc;

pub struct DatafusionDistributedDB {
    ctx: SessionContext,
    _guard: JoinSet<()>,
}

impl DatafusionDistributedDB {
    pub async fn new(num_nodes: usize) -> Self {
        let (ctx, _guard) = start_localhost_context(num_nodes, DefaultSessionBuilder).await;
        register_parquet_tables(&ctx).await.unwrap();
        Self { ctx, _guard }
    }

    pub fn optimize_distributed(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        DistributedPhysicalOptimizerRule::default()
            .with_network_shuffle_tasks(2)
            .with_network_coalesce_tasks(2)
            .optimize(plan, &Default::default())
    }

    /// Sanitize file paths in EXPLAIN output to make tests portable across machines
    /// Ex. "Users/jay/code/datafusion-distributed/....../file-0000.parquet" -> "datafusion-distributed/....../file-0000.parquet"
    fn sanitize_file_paths(plan_str: &str) -> String {
        let re = Regex::new(r"(?m)(^|[\[,]\s*)(?:[^,\[\s]*/)*datafusion-distributed/").unwrap();
        re.replace_all(plan_str, "${1}datafusion-distributed/")
            .to_string()
    }

    fn convert_batches_to_output(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<DBOutput<DefaultColumnType>, datafusion::error::DataFusionError> {
        if batches.is_empty() {
            return Ok(DBOutput::Rows {
                types: vec![],
                rows: vec![],
            });
        }

        let num_columns = batches[0].num_columns();
        let column_types = vec![DefaultColumnType::Text; num_columns]; // Everything as text

        let mut rows = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let column = batch.column(col_idx);
                    let value = array_value_to_string(column, row_idx)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    row.push(value);
                }
                rows.push(row);
            }
        }

        Ok(DBOutput::Rows {
            types: column_types,
            rows,
        })
    }

    async fn handle_explain_analyze(
        &mut self,
        _sql: &str,
    ) -> Result<DBOutput<DefaultColumnType>, datafusion::error::DataFusionError> {
        unimplemented!();
    }

    async fn handle_explain(
        &mut self,
        sql: &str,
    ) -> Result<DBOutput<DefaultColumnType>, datafusion::error::DataFusionError> {
        let query = sql.trim_start_matches("EXPLAIN").trim();
        let df = self.ctx.sql(query).await?;
        let physical_plan = df.create_physical_plan().await?;

        let physical_distributed = Self::optimize_distributed(physical_plan)?;

        let physical_distributed_str = displayable(physical_distributed.as_ref())
            .indent(true)
            .to_string();

        // Sanitize file paths to make tests portable across machines
        let sanitized_str = Self::sanitize_file_paths(&physical_distributed_str);

        let lines: Vec<String> = sanitized_str.lines().map(|s| s.to_string()).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("plan", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(lines)) as ArrayRef])?;

        self.convert_batches_to_output(vec![batch])
    }
}

#[async_trait]
impl AsyncDB for DatafusionDistributedDB {
    type Error = datafusion::error::DataFusionError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let sql = sql.trim();

        // Ignore DDL/DML
        if sql.to_uppercase().starts_with("CREATE")
            || sql.to_uppercase().starts_with("INSERT")
            || sql.to_uppercase().starts_with("DROP")
        {
            return Ok(DBOutput::StatementComplete(0));
        }

        if sql.to_uppercase().starts_with("EXPLAIN ANALYZE") {
            return self.handle_explain_analyze(sql).await;
        }

        if sql.to_uppercase().starts_with("EXPLAIN") {
            return self.handle_explain(sql).await;
        }

        // Default: Execute SELECT statement
        let df = self.ctx.sql(sql).await?;
        let task_ctx = Arc::new(df.task_ctx());
        let plan = df.create_physical_plan().await?;
        let distributed_plan = Self::optimize_distributed(plan)?;
        let batches = collect(distributed_plan, task_ctx).await?;

        self.convert_batches_to_output(batches)
    }

    fn engine_name(&self) -> &str {
        "datafusion-distributed"
    }
}
