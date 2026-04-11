use crate::{
    DistributedPlan, DistributedTaskContext, TaskEstimation, TaskEstimator, partition_feed,
};
use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::common::{Result, ScalarValue, internal_err, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::proto_error;
use futures::StreamExt;
use prost::Message;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TestMessage {
    #[prost(uint64, tag = "1")]
    n_rows: u64,
}

#[derive(Debug, Clone)]
pub struct TestPartitionFeedExec {
    properties: PlanProperties,
    task_count: usize,
    test_messages: Vec<Vec<TestMessage>>,
}

impl TestPartitionFeedExec {
    pub fn new(task_count: usize, row_count_per_partition: Vec<Vec<usize>>) -> Self {
        let partitions_per_task = row_count_per_partition.len() / task_count;
        Self {
            properties: PlanProperties::new(
                EquivalenceProperties::new(test_partition_feed_schema()),
                Partitioning::UnknownPartitioning(partitions_per_task),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            test_messages: row_count_per_partition
                .into_iter()
                .map(|msgs| {
                    msgs.into_iter()
                        .map(|n_rows| TestMessage {
                            n_rows: n_rows as u64,
                        })
                        .collect()
                })
                .collect(),
            task_count,
        }
    }
}

fn test_partition_feed_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("task", DataType::Int64, false),
        Field::new("partition", DataType::Int64, false),
        Field::new("string", DataType::Utf8, false),
    ]))
}

/// Table function that creates a `TestPartitionFeedExec`.
///
/// Called in SQL as: `SELECT * FROM test_partition_feed(2, '3,1', '5', '2', '')`
/// where the first argument is the task count (integer) and the remaining arguments are
/// comma-separated row counts for each partition's feed messages. An empty string means
/// an empty partition (no messages). The number of partition arguments must be divisible
/// by the task count — they are distributed evenly across tasks.
///
/// String encoding is used for partitions because DataFusion 52.x has a bug where array
/// literal arguments are silently dropped by the table-function SQL planner.
#[derive(Debug)]
pub struct TestPartitionFeedFunction;

impl TableFunctionImpl for TestPartitionFeedFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.len() < 2 {
            return plan_err!(
                "test_partition_feed(task_count, partitions...) requires at least 2 arguments"
            );
        }
        let task_count = match &exprs[0] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v as usize,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as usize,
            v => return plan_err!("task_count must be an integer literal, got {v:?}"),
        };
        let row_counts = exprs[1..]
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
                                    "Invalid integer in test_partition_feed(): {e}"
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
        Ok(Arc::new(TestPartitionFeedTableProvider {
            task_count,
            row_counts,
        }))
    }
}

/// TableProvider that creates a `TestPartitionFeedExec` in `scan()`.
#[derive(Debug)]
struct TestPartitionFeedTableProvider {
    task_count: usize,
    row_counts: Vec<Vec<usize>>,
}

#[async_trait]
impl TableProvider for TestPartitionFeedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        test_partition_feed_schema()
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
        let _ = projection; // TestPartitionFeedExec always produces the full schema
        Ok(Arc::new(TestPartitionFeedExec::new(
            self.task_count,
            self.row_counts.clone(),
        )))
    }
}

pub struct TestPartitionFeedTaskEstimator;

impl TaskEstimator for TestPartitionFeedTaskEstimator {
    type Data = ();

    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _cfg: &ConfigOptions,
    ) -> Option<TaskEstimation<Self::Data>> {
        let plan = plan.as_any().downcast_ref::<TestPartitionFeedExec>()?;
        Some(TaskEstimation::desired(plan.task_count))
    }

    fn distribute_plan(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_estimation: TaskEstimation<Self::Data>,
        _cfg: &ConfigOptions,
    ) -> Option<DistributedPlan> {
        let metadata_exec = plan.as_any().downcast_ref::<TestPartitionFeedExec>()?;
        let task_count = task_estimation.task_count.as_usize();
        let partitions_per_task = metadata_exec.test_messages.len() / task_count;

        // Rebuild the exec with the decided task count so its partition count matches.
        let new_exec = Arc::new(TestPartitionFeedExec {
            properties: PlanProperties::new(
                EquivalenceProperties::new(test_partition_feed_schema()),
                Partitioning::UnknownPartitioning(partitions_per_task),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            task_count,
            test_messages: metadata_exec.test_messages.clone(),
        });

        // P_per_task * T feeds, distributed: task 0 gets the first P_per_task, task 1 the next, etc.
        let partition_feeds: Vec<_> = metadata_exec
            .test_messages
            .iter()
            .map(|msgs| futures::stream::iter(msgs.iter().cloned().map(Ok).collect::<Vec<_>>()))
            .collect();

        Some(DistributedPlan::new(new_exec).with_partition_feeds(partition_feeds))
    }
}

impl DisplayAs for TestPartitionFeedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "TestPartitionFeedExec: tasks={}, rows_per_partition=[",
            self.task_count
        )?;
        for (i, msgs) in self.test_messages.iter().enumerate() {
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

impl ExecutionPlan for TestPartitionFeedExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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
        let partition_feed = match partition_feed::<TestMessage>(&context) {
            Some(feed) => feed,
            None => {
                // No PartitionFeedExec wrapping us (non-distributed), use own messages.
                let msgs = self
                    .test_messages
                    .get(partition)
                    .cloned()
                    .unwrap_or_default();
                futures::stream::iter(msgs.into_iter().map(Ok).collect::<Vec<_>>()).boxed()
            }
        };

        let distributed_ctx = DistributedTaskContext::from_ctx(&context);
        let task_index = distributed_ctx.task_index as i64;
        let partition_idx = partition as i64;
        let schema = self.schema();

        let stream = partition_feed.map(move |msg_result| {
            let msg = msg_result?;
            let n_rows = msg.n_rows as usize;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![task_index; n_rows])),
                    Arc::new(Int64Array::from(vec![partition_idx; n_rows])),
                    Arc::new(StringArray::from(
                        (0..n_rows).map(|i| ABC[i % ABC.len()]).collect::<Vec<_>>(),
                    )),
                ],
            )?;
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

const ABC: [&str; 27] = [
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "ñ", "o", "p", "q", "r",
    "s", "t", "u", "v", "w", "x", "y", "z",
];

#[derive(Clone, PartialEq, ::prost::Message)]
struct TestPartitionFeedExecProto {
    #[prost(uint64, tag = "1")]
    task_count: u64,
    #[prost(message, repeated, tag = "2")]
    partitions: Vec<TestPartitionMessages>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct TestPartitionMessages {
    #[prost(message, repeated, tag = "1")]
    messages: Vec<TestMessage>,
}

#[derive(Debug)]
pub struct TestPartitionFeedExecCodec;

impl PhysicalExtensionCodec for TestPartitionFeedExecCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "TestPartitionFeedExec should have no children, got {}",
                inputs.len()
            );
        }
        let proto = TestPartitionFeedExecProto::decode(buf)
            .map_err(|e| proto_error(format!("Failed to decode TestPartitionFeedExec: {e}")))?;

        let row_counts: Vec<Vec<usize>> = proto
            .partitions
            .into_iter()
            .map(|p| p.messages.into_iter().map(|m| m.n_rows as usize).collect())
            .collect();

        Ok(Arc::new(TestPartitionFeedExec::new(
            proto.task_count as usize,
            row_counts,
        )))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(exec) = node.as_any().downcast_ref::<TestPartitionFeedExec>() else {
            return internal_err!("Expected TestPartitionFeedExec, but was {}", node.name());
        };

        let proto = TestPartitionFeedExecProto {
            task_count: exec.task_count as u64,
            partitions: exec
                .test_messages
                .iter()
                .map(|msgs| TestPartitionMessages {
                    messages: msgs.clone(),
                })
                .collect(),
        };

        proto
            .encode(buf)
            .map_err(|e| proto_error(format!("Failed to encode TestPartitionFeedExec: {e}")))
    }
}
