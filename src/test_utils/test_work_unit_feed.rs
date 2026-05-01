use crate::distributed_planner::DistributedPlan;
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
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RowGeneratorWorkUnit {
    #[prost(uint64, tag = "1")]
    n_rows: u64,
}

#[derive(Debug, Clone)]
pub struct RowGeneratorFeedProvider {
    per_partition_work_units: Vec<Vec<RowGeneratorWorkUnit>>,
    task_count: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl RowGeneratorFeedProvider {
    pub fn new(task_count: usize, row_count_per_partition: Vec<Vec<usize>>) -> Self {
        Self {
            per_partition_work_units: row_count_per_partition
                .into_iter()
                .map(|msgs| {
                    msgs.into_iter()
                        .map(|n_rows| RowGeneratorWorkUnit {
                            n_rows: n_rows as u64,
                        })
                        .collect()
                })
                .collect(),
            task_count,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl WorkUnitFeedProvider for RowGeneratorFeedProvider {
    type WorkUnit = RowGeneratorWorkUnit;

    fn feed(
        &self,
        partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<Self::WorkUnit>>> {
        let work_units_sent: Count =
            MetricBuilder::new(&self.metrics).counter("work_units_sent", partition);
        let units = self
            .per_partition_work_units
            .get(partition)
            .cloned()
            .unwrap_or_default();
        Ok(futures::stream::iter(units.into_iter().map(move |unit| {
            work_units_sent.add(1);
            Ok(unit)
        }))
        .boxed())
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
/// Called in SQL as: `SELECT * FROM test_work_unit('my_tag', 2, '3,1', '5', '2')`
/// where the first argument is a tag string, the second is the task count (integer),
/// and the remaining arguments are comma-separated row counts for each partition's feed
/// messages. An empty string means an empty partition (no messages). The number of
/// partition arguments must be divisible by the task count — they are distributed evenly
/// across tasks.
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
        let row_counts = exprs[2..]
            .iter()
            .map(|expr| match expr {
                Expr::Literal(ScalarValue::Utf8(Some(s)), _) => {
                    if s.is_empty() {
                        return Ok(vec![]);
                    }
                    s.split(',')
                        .map(|v| {
                            v.trim().parse::<usize>().map_err(|e| {
                                datafusion::error::DataFusionError::Plan(format!(
                                    "Invalid integer in test_work_unit_feed(): {e}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>>>()
                }
                v => plan_err!("partition args must be string literals, got {v:?}"),
            })
            .collect::<Result<Vec<_>>>()?;
        if row_counts.len() % task_count != 0 {
            return plan_err!(
                "number of partitions ({}) must be divisible by task_count ({task_count})",
                row_counts.len()
            );
        }
        Ok(Arc::new(TestWorkUnitFeedTableProvider {
            tag,
            task_count,
            row_counts,
        }))
    }
}

/// TableProvider that creates a [`RowGeneratorExec`] in `scan()`.
#[derive(Debug)]
struct TestWorkUnitFeedTableProvider {
    tag: String,
    task_count: usize,
    row_counts: Vec<Vec<usize>>,
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
            .row_counts
            .iter()
            .flat_map(|msgs| msgs.iter().copied())
            .sum();
        Ok(Arc::new(RowGeneratorExec::new(
            WorkUnitFeed::new(RowGeneratorFeedProvider::new(
                self.task_count,
                self.row_counts.clone(),
            )),
            self.tag.clone(),
            self.row_counts.len(),
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
    ) -> Result<Option<TaskEstimation>> {
        let Some(exec) = plan.as_any().downcast_ref::<RowGeneratorExec>() else {
            return Ok(None);
        };
        let Ok(provider) = exec.feed.clone().try_into_inner() else {
            return Ok(None);
        };
        Ok(Some(TaskEstimation::desired(provider.task_count)))
    }

    fn distribute_plan(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Result<Option<crate::distributed_planner::DistributedPlan>> {
        let Some(exec) = plan.as_any().downcast_ref::<RowGeneratorExec>() else {
            return Ok(None);
        };
        let Ok(provider) = exec.feed.clone().try_into_inner() else {
            return Ok(None);
        };
        let partitions_per_task = provider.per_partition_work_units.len() / task_count;

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
        })?;

        Ok(Some(DistributedPlan::from_plan(transformed.data)))
    }

    fn route_tasks(
        &self,
        _tasks: Vec<crate::stage::ExecutionTask>,
        _urls: &[url::Url],
    ) -> Result<Option<Vec<url::Url>>> {
        Ok(None)
    }
}

impl DisplayAs for RowGeneratorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RowGeneratorExec: tag={}", self.tag)?;
        let Some(provider) = self.feed.inner() else {
            return Ok(());
        };
        write!(f, ", tasks={}, rows_per_partition=[", provider.task_count)?;
        for (i, msgs) in provider.per_partition_work_units.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "[")?;
            for (j, msg) in msgs.iter().enumerate() {
                if j > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", msg.n_rows)?;
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
