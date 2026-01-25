//! This example demonstrates how to create a custom execution plan that works with
//! Distributed DataFusion. It implements a `numbers(start, end)` table function that
//! generates a sequence of numbers and can be distributed across multiple workers.
//!
//! This example includes:
//! - Custom TableFunction for accepting the `numbers(start, end)` in SQL
//! - Custom TableProvider for mapping the table function to an execution plan
//! - Custom ExecutionPlan for returning the requested number range
//! - Custom PhysicalExtensionCodec for serialization across the network
//! - Custom DistributedPlannerExtension to control how to scale up a custom node
//!
//! Run this example with:
//! ```bash
//! cargo run --features integration --example custom_execution_plan "SELECT DISTINCT number FROM numbers(0, 10) ORDER BY number" --show-distributed-plan
//! cargo run --features integration --example custom_execution_plan "SELECT DISTINCT number FROM numbers(0, 11) ORDER BY number" --show-distributed-plan
//! cargo run --features integration --example custom_execution_plan "SELECT DISTINCT number FROM numbers(0, 100) ORDER BY number" --show-distributed-plan
//! cargo run --features integration --example custom_execution_plan "SELECT DISTINCT number FROM numbers(0, 100) ORDER BY number" --workers 10 --show-distributed-plan
//! ```

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatchOptions;
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::common::stats::Precision;
use datafusion::common::{
    DataFusionError, Result, ScalarValue, Statistics, exec_err, internal_err, plan_err,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::SessionContext;
use datafusion_distributed::test_utils::in_memory_channel_resolver::{
    InMemoryChannelResolver, InMemoryWorkerResolver,
};
use datafusion_distributed::{
    DistributedExt, DistributedPhysicalOptimizerRule, DistributedPlannerExtension,
    DistributedTaskContext, WorkerQueryContext, display_plan_ascii,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use futures::{TryStreamExt, stream};
use prost::Message;
use std::any::Any;
use std::fmt::{self, Formatter};
use std::ops::Range;
use std::sync::Arc;
use structopt::StructOpt;

/// Table function that generates a sequence of numbers from start to end.
/// Can be called in SQL as: SELECT * FROM numbers(start, end)
#[derive(Debug)]
struct NumbersTableFunction;

impl TableFunctionImpl for NumbersTableFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.len() != 2 {
            return plan_err!(
                "numbers() requires exactly 2 arguments (start, end), got {}",
                exprs.len()
            );
        }
        fn get_number(expr: &Expr) -> Result<i64, DataFusionError> {
            match &expr {
                Expr::Literal(ScalarValue::Int64(Some(v)), _) => Ok(*v),
                Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(*v as i64),
                v => plan_err!("numbers() arguments must be integer literals, got {v:?}"),
            }
        }
        Ok(Arc::new(NumbersTableProvider {
            start: get_number(&exprs[0])?,
            end: get_number(&exprs[1])?,
        }))
    }
}

fn numbers_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::Int64,
        false,
    )]))
}

/// TableProvider that generates a sequence of numbers from start to end.
#[derive(Debug)]
struct NumbersTableProvider {
    start: i64,
    end: i64,
}

#[async_trait]
impl TableProvider for NumbersTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        numbers_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(indices) => Arc::new(self.schema().project(indices)?),
            None => self.schema(),
        };

        #[allow(clippy::single_range_in_vec_init)]
        Ok(Arc::new(NumbersExec::new([self.start..self.end], schema)))
    }
}

/// Custom execution plan that generates numbers from start to end.
/// When distributed, each task generates a subset of numbers based on its task_index.
#[derive(Debug, Clone)]
struct NumbersExec {
    ranges_per_task: Vec<Range<i64>>,
    plan_properties: PlanProperties,
}

impl NumbersExec {
    fn new(ranges_per_task: impl IntoIterator<Item = Range<i64>>, schema: SchemaRef) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            ranges_per_task: ranges_per_task.into_iter().collect(),
            plan_properties,
        }
    }
}

impl DisplayAs for NumbersExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "NumbersExec: ")?;
        for (task_i, range) in self.ranges_per_task.iter().enumerate() {
            write!(f, "t{task_i}:[{}-{})", range.start, range.end)?;
            if task_i < self.ranges_per_task.len() - 1 {
                write!(f, ", ")?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for NumbersExec {
    fn name(&self) -> &str {
        "NumbersExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Get the distributed task context to determine which subset of numbers
        // this task should generate
        let dist_ctx = DistributedTaskContext::from_ctx(&context);

        let Some(range) = self.ranges_per_task.get(dist_ctx.task_index) else {
            return exec_err!("Task index out of range");
        };

        // Calculate which numbers this task should generate
        let numbers: Vec<i64> = range.clone().collect();
        let row_count = numbers.len();

        // Create batch matching the schema (may be empty for COUNT queries)
        let batch = if self.schema().fields().is_empty() {
            // For COUNT queries, return batch with correct row count but no columns
            let mut options = RecordBatchOptions::new();
            options.row_count = Some(row_count);
            RecordBatch::try_new_with_options(self.schema(), vec![], &options)?
        } else {
            let array: ArrayRef = Arc::new(Int64Array::from(numbers));
            RecordBatch::try_new(self.schema(), vec![array])?
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream::once(async { Ok(batch) }),
        )))
    }

    /// Implementing [ExecutionPlan::partition_statistics] is essential for Distributed DataFusion
    /// to infer how much compute is going to be needed across the plan.
    fn partition_statistics(&self, _: Option<usize>) -> Result<Statistics> {
        let mut stats = Statistics::default();
        let num_rows = self
            .ranges_per_task
            .iter()
            .map(|v| v.end - v.start)
            .sum::<i64>();

        stats.num_rows = Precision::Exact(num_rows as usize);
        stats.column_statistics =
            vec![ColumnStatistics::new_unknown().with_distinct_count(stats.num_rows)];

        Ok(stats)
    }
}

/// Custom codec for serializing/deserializing NumbersExec across the network. As the NumbersExec
/// plan will be sent over the wire during distributed queries, both the SessionContext that
/// initiates the query and each Worker need to know how to (de)serialize it.
#[derive(Debug)]
struct NumbersExecCodec;

#[derive(Clone, PartialEq, ::prost::Message)]
struct NumbersExecProto {
    #[prost(message, optional, tag = "1")]
    schema: Option<protobuf::Schema>,
    #[prost(repeated, message, tag = "2")]
    ranges: Vec<RangeProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct RangeProto {
    #[prost(int64, tag = "1")]
    start: i64,
    #[prost(int64, tag = "2")]
    end: i64,
}

impl PhysicalExtensionCodec for NumbersExecCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!("NumbersExec should have no children, got {}", inputs.len());
        }

        let proto = NumbersExecProto::decode(buf)
            .map_err(|e| proto_error(format!("Failed to decode NumbersExec: {e}")))?;

        let schema: Schema = proto
            .schema
            .as_ref()
            .map(|s| s.try_into())
            .ok_or(proto_error("NumbersExec is missing schema"))??;

        Ok(Arc::new(NumbersExec::new(
            proto.ranges.iter().map(|v| v.start..v.end),
            Arc::new(schema),
        )))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(exec) = node.as_any().downcast_ref::<NumbersExec>() else {
            return internal_err!("Expected plan to be NumbersExec, but was {}", node.name());
        };

        let proto = NumbersExecProto {
            schema: Some(node.schema().try_into()?),
            ranges: exec
                .ranges_per_task
                .iter()
                .map(|v| RangeProto {
                    start: v.start,
                    end: v.end,
                })
                .collect(),
        };

        proto
            .encode(buf)
            .map_err(|e| proto_error(format!("Failed to encode NumbersExec: {e}")))
    }
}

/// Custom [DistributedPlannerExtension] that tells the planner how to distribute [NumbersExec].
#[derive(Debug)]
struct NumbersDistributedPlannerExtension;

impl DistributedPlannerExtension for NumbersDistributedPlannerExtension {
    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &datafusion::config::ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let plan = plan.as_any().downcast_ref::<NumbersExec>()?;
        let range = &plan.ranges_per_task[0];
        let chunk_size = ((range.end - range.start) as f64 / task_count as f64).ceil() as i64;

        let ranges_per_task = (0..task_count).map(|i| {
            let start = range.start + (i as i64 * chunk_size);
            let end = (start + chunk_size).min(range.end);
            start..end
        });

        Some(Arc::new(NumbersExec::new(ranges_per_task, plan.schema())))
    }
}

#[derive(StructOpt)]
#[structopt(
    name = "custom_execution_plan",
    about = "Example demonstrating custom execution plans with Distributed DataFusion"
)]
struct Args {
    /// The SQL query to run.
    #[structopt()]
    query: String,

    /// Number of distributed workers to simulate.
    #[structopt(long, default_value = "4")]
    workers: usize,

    /// Whether the distributed plan should be rendered instead of executing the query.
    #[structopt(long)]
    show_distributed_plan: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::from_args();

    let worker_resolver = InMemoryWorkerResolver::new(args.workers);
    let channel_resolver =
        InMemoryChannelResolver::from_session_builder(|ctx: WorkerQueryContext| async move {
            Ok(ctx
                .builder
                .with_distributed_user_codec(NumbersExecCodec)
                .build())
        });

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_worker_resolver(worker_resolver)
        .with_distributed_channel_resolver(channel_resolver)
        .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
        .with_distributed_user_codec(NumbersExecCodec)
        .with_distributed_planner_extension(NumbersDistributedPlannerExtension)
        .build();

    let ctx = SessionContext::from(state);
    ctx.register_udtf("numbers", Arc::new(NumbersTableFunction));

    let mut df = None;
    for query in args.query.split(';') {
        df = Some(ctx.sql(query).await?);
    }
    let df = df.unwrap();
    if args.show_distributed_plan {
        let plan = df.create_physical_plan().await?;
        println!("{}", display_plan_ascii(plan.as_ref(), false));
    } else {
        let stream = df.execute_stream().await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        let formatted = pretty_format_batches(&batches)?;
        println!("{formatted}");
    }
    Ok(())
}
