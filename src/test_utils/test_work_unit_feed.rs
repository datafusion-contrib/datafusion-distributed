use crate::{
    DistributedTaskContext, TaskEstimation, TaskEstimator, WorkUnitFeed, WorkUnitFeedProto,
    WorkUnitFeedProvider,
};
use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue, Statistics, internal_err, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::proto_error;
use futures::StreamExt;
use futures::stream::BoxStream;
use prost::Message;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RowGeneratorWorkUnit {
    #[prost(uint64, tag = "1")]
    n_rows: u64,
}

/// A scripted operation that the [`RowGeneratorFeedProvider`] performs on the
/// coordinator side while producing its per-partition work unit stream.
///
/// `WorkUnitOp` is a tiny DSL used by the test harness to drive specific
/// timing and error scenarios through the feed pipeline. Ops are written as
/// comma-separated strings in the `test_work_unit` table function:
///
/// - `rows(N)` — emit a [`RowGeneratorWorkUnit`] that produces N rows.
/// - `wait(MS)` — sleep for MS milliseconds before the next op.
/// - `err(MSG)` — yield a [`DataFusionError::Execution`] with the given
///   message and terminate the stream.
#[derive(Debug, Clone)]
pub enum WorkUnitOp {
    Rows(u64),
    Wait(Duration),
    Err(String),
}

impl Display for WorkUnitOp {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            WorkUnitOp::Rows(n) => write!(f, "rows({n})"),
            WorkUnitOp::Wait(d) => write!(f, "wait({})", d.as_millis()),
            WorkUnitOp::Err(msg) => write!(f, "err({msg})"),
        }
    }
}

fn parse_partition_ops(s: &str) -> Result<Vec<WorkUnitOp>> {
    if s.trim().is_empty() {
        return Ok(vec![]);
    }
    s.split(',').map(|item| parse_op(item.trim())).collect()
}

fn parse_op(s: &str) -> Result<WorkUnitOp> {
    let Some(open) = s.find('(') else {
        return plan_err!("expected `name(arg)` op, got {s:?}");
    };
    if !s.ends_with(')') {
        return plan_err!("expected closing `)` in op {s:?}");
    }
    let name = &s[..open];
    let arg = &s[open + 1..s.len() - 1];
    match name {
        "rows" => {
            let n: u64 = arg
                .parse()
                .map_err(|e| DataFusionError::Plan(format!("invalid rows arg in {s:?}: {e}")))?;
            Ok(WorkUnitOp::Rows(n))
        }
        "wait" => {
            let n: u64 = arg
                .parse()
                .map_err(|e| DataFusionError::Plan(format!("invalid wait arg in {s:?}: {e}")))?;
            Ok(WorkUnitOp::Wait(Duration::from_millis(n)))
        }
        "err" => Ok(WorkUnitOp::Err(arg.to_string())),
        other => plan_err!("unknown op {other:?} in {s:?}"),
    }
}

#[derive(Debug, Clone)]
pub struct RowGeneratorFeedProvider {
    per_partition_ops: Vec<Vec<WorkUnitOp>>,
    task_count: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl RowGeneratorFeedProvider {
    pub fn new(task_count: usize, per_partition_ops: Vec<Vec<WorkUnitOp>>) -> Self {
        Self {
            per_partition_ops,
            task_count,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

struct FeedStreamState {
    iter: std::vec::IntoIter<WorkUnitOp>,
    counter: Count,
    done: bool,
}

impl WorkUnitFeedProvider for RowGeneratorFeedProvider {
    type WorkUnit = RowGeneratorWorkUnit;

    fn feed(
        &self,
        partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<Self::WorkUnit>>> {
        let counter: Count =
            MetricBuilder::new(&self.metrics).counter("work_units_sent", partition);
        let ops = self
            .per_partition_ops
            .get(partition)
            .cloned()
            .unwrap_or_default();
        let state = FeedStreamState {
            iter: ops.into_iter(),
            counter,
            done: false,
        };
        let stream = futures::stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            loop {
                let op = state.iter.next()?;
                match op {
                    WorkUnitOp::Rows(n) => {
                        state.counter.add(1);
                        return Some((Ok(RowGeneratorWorkUnit { n_rows: n }), state));
                    }
                    WorkUnitOp::Wait(d) => {
                        tokio::time::sleep(d).await;
                        continue;
                    }
                    WorkUnitOp::Err(msg) => {
                        state.done = true;
                        return Some((Err(DataFusionError::Execution(msg)), state));
                    }
                }
            }
        });
        Ok(stream.boxed())
    }
}

/// Leaf execution plan that holds a [`WorkUnitFeed`] directly. During execution, it pulls
/// its per-partition stream from the feed and turns each [`RowGeneratorWorkUnit`] into a
/// [`RecordBatch`] of `n_rows` rows.
#[derive(Debug, Clone)]
pub struct RowGeneratorExec {
    pub feed: WorkUnitFeed<RowGeneratorFeedProvider>,
    properties: Arc<PlanProperties>,
    tag: String,
    projection: Option<Vec<usize>>,
    /// Total number of rows this exec will produce across all partitions.
    total_rows: usize,
}

impl RowGeneratorExec {
    pub fn new(
        feed: WorkUnitFeed<RowGeneratorFeedProvider>,
        tag: String,
        partitions: usize,
        projection: Option<Vec<usize>>,
        total_rows: usize,
    ) -> Self {
        let schema = match &projection {
            Some(indices) => Arc::new(row_generator_schema().project(indices).unwrap()),
            None => row_generator_schema(),
        };
        Self {
            feed,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            tag,
            projection,
            total_rows,
        }
    }
}

fn row_generator_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tag", DataType::Utf8, false),
        Field::new("task", DataType::Int64, false),
        Field::new("partition", DataType::Int64, false),
        Field::new("letter", DataType::Utf8, false),
    ]))
}

/// Table function that creates a [`RowGeneratorExec`].
///
/// Called in SQL as:
/// `SELECT * FROM test_work_unit('my_tag', 2, 'rows(3),rows(1)', 'rows(5)', 'rows(2)')`
/// where the first argument is a tag string, the second is the task count (integer),
/// and the remaining arguments are comma-separated [`WorkUnitOp`]s describing what
/// each partition's feed should do at runtime.
///
/// Available ops: `rows(N)` emits a work unit that generates N rows, `wait(MS)`
/// sleeps the producer for MS milliseconds, `err(MSG)` yields an error and ends
/// the stream. An empty string means an empty partition (no ops). The number of
/// partition arguments must be divisible by the task count — they are distributed
/// evenly across tasks.
///
/// String encoding is used for partitions because DataFusion 52.x has a bug where array
/// literal arguments are silently dropped by the table-function SQL planner.
#[derive(Debug)]
pub struct TestWorkUnitFeedFunction;

impl TableFunctionImpl for TestWorkUnitFeedFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.len() < 3 {
            return plan_err!(
                "test_work_unit(tag, task_count, partitions...) requires at least 3 arguments"
            );
        }
        let tag = match &exprs[0] {
            Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
            v => return plan_err!("tag must be a string literal, got {v:?}"),
        };
        let task_count = match &exprs[1] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v as usize,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as usize,
            v => return plan_err!("task_count must be an integer literal, got {v:?}"),
        };
        let partition_ops = exprs[2..]
            .iter()
            .map(|expr| match expr {
                Expr::Literal(ScalarValue::Utf8(Some(s)), _) => parse_partition_ops(s),
                v => plan_err!("partition args must be string literals, got {v:?}"),
            })
            .collect::<Result<Vec<_>>>()?;
        if partition_ops.len() % task_count != 0 {
            return plan_err!(
                "number of partitions ({}) must be divisible by task_count ({task_count})",
                partition_ops.len()
            );
        }
        Ok(Arc::new(TestWorkUnitFeedTableProvider {
            tag,
            task_count,
            partition_ops,
        }))
    }
}

/// TableProvider that creates a [`RowGeneratorExec`] in `scan()`.
#[derive(Debug)]
struct TestWorkUnitFeedTableProvider {
    tag: String,
    task_count: usize,
    partition_ops: Vec<Vec<WorkUnitOp>>,
}

#[async_trait]
impl TableProvider for TestWorkUnitFeedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        row_generator_schema()
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
        let total_rows: usize = self
            .partition_ops
            .iter()
            .flat_map(|ops| ops.iter())
            .map(|op| match op {
                WorkUnitOp::Rows(n) => *n as usize,
                _ => 0,
            })
            .sum();
        Ok(Arc::new(RowGeneratorExec::new(
            WorkUnitFeed::new(RowGeneratorFeedProvider::new(
                self.task_count,
                self.partition_ops.clone(),
            )),
            self.tag.clone(),
            self.partition_ops.len(),
            projection.cloned(),
            total_rows,
        )))
    }
}

pub struct TestWorkUnitFeedTaskEstimator;

impl TaskEstimator for TestWorkUnitFeedTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        let exec = plan.as_any().downcast_ref::<RowGeneratorExec>()?;
        let provider = exec.feed.clone().try_into_inner().ok()?;
        Some(TaskEstimation::desired(provider.task_count))
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let exec = plan.as_any().downcast_ref::<RowGeneratorExec>()?;
        let provider = exec.feed.clone().try_into_inner().ok()?;
        let partitions_per_task = provider.per_partition_ops.len() / task_count;

        // Rebuild the exec with the decided task count so its partition count matches.
        let transformed = Arc::clone(plan).transform_down(|plan| {
            if let Some(exec) = plan.as_any().downcast_ref::<RowGeneratorExec>() {
                return Ok(Transformed::yes(Arc::new(RowGeneratorExec::new(
                    exec.feed.clone(),
                    exec.tag.clone(),
                    partitions_per_task,
                    exec.projection.clone(),
                    exec.total_rows,
                ))));
            };
            Ok(Transformed::no(plan))
        });

        Some(transformed.ok()?.data)
    }
}

impl DisplayAs for RowGeneratorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RowGeneratorExec: tag={}", self.tag)?;
        let Some(provider) = self.feed.inner() else {
            return Ok(());
        };
        write!(f, ", tasks={}, partition_ops=[", provider.task_count)?;
        for (i, ops) in provider.per_partition_ops.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "[")?;
            for (j, op) in ops.iter().enumerate() {
                if j > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{op}")?;
            }
            write!(f, "]")?;
        }
        write!(f, "]")
    }
}

impl ExecutionPlan for RowGeneratorExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(self.as_ref().clone()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let work_unit_feed = self.feed.feed(partition, Arc::clone(&context))?;

        let distributed_ctx = DistributedTaskContext::from_ctx(&context);
        let task_index = distributed_ctx.task_index as i64;
        let partition_idx = partition as i64;
        let schema = self.schema();
        let tag = self.tag.clone();
        let projection = self.projection.clone();

        let stream = work_unit_feed.map(move |msg_result| {
            let msg = msg_result?;
            let n_rows = msg.n_rows as usize;
            // Build all columns, then select only the projected ones.
            let all_columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = vec![
                Arc::new(StringArray::from(vec![tag.as_str(); n_rows])),
                Arc::new(Int64Array::from(vec![task_index; n_rows])),
                Arc::new(Int64Array::from(vec![partition_idx; n_rows])),
                Arc::new(StringArray::from(
                    (0..n_rows).map(|i| ABC[i % ABC.len()]).collect::<Vec<_>>(),
                )),
            ];
            let columns = match &projection {
                Some(indices) => indices
                    .iter()
                    .map(|&i| Arc::clone(&all_columns[i]))
                    .collect(),
                None => all_columns,
            };
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    /// Compute statistics from the exact total row count known at planning time. This lets
    /// DataFusion's planner decide whether to use `CollectLeft` vs `Partitioned` mode for
    /// hash joins based on the real data size — small inputs (below DataFusion's threshold)
    /// become `CollectLeft` and can be broadcast.
    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        // Rough byte estimate: assume ~32 bytes per row (tag + task + partition + letter).
        let total_byte_size = self.total_rows.saturating_mul(32);
        Ok(Statistics {
            num_rows: Precision::Exact(self.total_rows),
            total_byte_size: Precision::Exact(total_byte_size),
            column_statistics: Statistics::unknown_column(&self.schema()),
        })
    }

    /// Exposes the metrics recorded by the local [`RowGeneratorFeedProvider`] (e.g. the
    /// `work_units_sent` counter incremented as work units are streamed out on the
    /// coordinator side). For remote feeds there are no local metrics to report.
    fn metrics(&self) -> Option<MetricsSet> {
        let provider = self.feed.inner()?;
        Some(provider.metrics.clone_inner())
    }
}

const ABC: [&str; 27] = [
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "ñ", "o", "p", "q", "r",
    "s", "t", "u", "v", "w", "x", "y", "z",
];

#[derive(Clone, PartialEq, ::prost::Message)]
struct RowGeneratorExecProto {
    #[prost(uint64, tag = "1")]
    partitions: u64,
    #[prost(uint64, repeated, tag = "2")]
    projection: Vec<u64>,
    #[prost(string, tag = "3")]
    tag: String,
    #[prost(uint64, tag = "4")]
    total_rows: u64,
    #[prost(message, optional, tag = "5")]
    feed: Option<WorkUnitFeedProto>,
}

#[derive(Debug)]
pub struct TestWorkUnitFeedExecCodec;

impl PhysicalExtensionCodec for TestWorkUnitFeedExecCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "RowGeneratorExec should have no children, got {}",
                inputs.len()
            );
        }
        let proto = RowGeneratorExecProto::decode(buf)
            .map_err(|e| proto_error(format!("Failed to decode RowGeneratorExecProto: {e}")))?;

        let projection = if proto.projection.is_empty() {
            None
        } else {
            Some(proto.projection.iter().map(|&i| i as usize).collect())
        };
        let feed_proto = proto
            .feed
            .ok_or_else(|| proto_error("RowGeneratorExecProto missing feed"))?;
        let feed = WorkUnitFeed::<RowGeneratorFeedProvider>::from_proto(feed_proto)?;
        Ok(Arc::new(RowGeneratorExec::new(
            feed,
            proto.tag,
            proto.partitions as usize,
            projection,
            proto.total_rows as usize,
        )))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(exec) = node.as_any().downcast_ref::<RowGeneratorExec>() else {
            return internal_err!("Expected RowGeneratorExec, but was {}", node.name());
        };

        let proto = RowGeneratorExecProto {
            partitions: exec.properties.partitioning.partition_count() as u64,
            projection: exec
                .projection
                .as_ref()
                .map(|p| p.iter().map(|&i| i as u64).collect())
                .unwrap_or_default(),
            tag: exec.tag.clone(),
            total_rows: exec.total_rows as u64,
            feed: Some(exec.feed.to_proto()),
        };

        proto
            .encode(buf)
            .map_err(|e| proto_error(format!("Failed to encode RowGeneratorExec: {e}")))
    }
}
