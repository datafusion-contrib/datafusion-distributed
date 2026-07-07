//! Demonstrates **work unit feeds**: a distributed leaf node whose work is discovered on the
//! coordinator *at runtime* and streamed to the workers while the query runs, instead of being
//! known at planning time (think a paginated API, a queue, or a catalog handing out keys).
//!
//! It wires up the four pieces a feed-backed leaf needs — a `WorkUnitFeedProvider`, a custom
//! `ExecutionPlan` holding a `WorkUnitFeed`, a `PhysicalExtensionCodec`, and a `TaskEstimator` —
//! plus `with_distributed_work_unit_feed` to register the feed so the planner can find it.
//!
//! Run with:
//! ```bash
//! cargo run --features integration --example work_unit_feed "SELECT count(*) AS cnt, task FROM scan(2, '3,1', '2', '4', '1,1') GROUP BY task ORDER BY task"
//! cargo run --features integration --example work_unit_feed "SELECT * FROM scan(2, '3,1', '2', '4', '1,1') ORDER BY task, partition" --show-distributed-plan
//! ```

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_distributed::test_utils::in_memory_channel_resolver::{
    InMemoryChannelResolver, InMemoryWorkerResolver,
};
use datafusion_distributed::{
    DistributedExt, DistributedTaskContext, SessionStateBuilderExt, TaskEstimation, TaskEstimator,
    WorkUnitFeed, WorkUnitFeedProto, WorkUnitFeedProvider, WorkerQueryContext, display_plan_ascii,
};
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use datafusion_proto::protobuf::proto_error;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;

/// The work unit streamed from coordinator to worker. Any `prost` message is a valid `WorkUnit`.
/// In a real connector this might carry a file URL, an object-store key, or a page token.
#[derive(Clone, PartialEq, ::prost::Message)]
struct Chunk {
    #[prost(uint64, tag = "1")]
    n_rows: u64,
}

/// Coordinator-side producer of work units, one stream per partition. `feed` is called only on
/// the coordinator; here it "discovers" each chunk with a tiny delay to mimic polling a source.
#[derive(Debug, Clone)]
struct ChunkFeedProvider {
    per_partition_chunks: Vec<Vec<u64>>,
    task_count: usize,
}

impl WorkUnitFeedProvider for ChunkFeedProvider {
    type WorkUnit = Chunk;

    fn feed(
        &self,
        partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<Chunk>>> {
        let chunks = self
            .per_partition_chunks
            .get(partition)
            .cloned()
            .unwrap_or_default();
        Ok(futures::stream::iter(chunks)
            .then(|n_rows| async move {
                tokio::time::sleep(Duration::from_millis(1)).await; // pretend to poll a remote source
                Ok(Chunk { n_rows })
            })
            .boxed())
    }
}

fn scan_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("task", DataType::Int64, false),
        Field::new("partition", DataType::Int64, false),
    ]))
}

/// Leaf `ExecutionPlan` that holds a `WorkUnitFeed` and, in `execute`, turns the chunks it
/// receives into `RecordBatch`es.
#[derive(Debug, Clone)]
struct RemoteScanExec {
    feed: WorkUnitFeed<ChunkFeedProvider>,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl RemoteScanExec {
    fn new(
        feed: WorkUnitFeed<ChunkFeedProvider>,
        partitions: usize,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let schema = match &projection {
            Some(p) => Arc::new(scan_schema().project(p).unwrap()),
            None => scan_schema(),
        };
        Self {
            feed,
            projection,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }
}

impl DisplayAs for RemoteScanExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RemoteScanExec")?;
        if let Some(p) = self.feed.inner() {
            write!(
                f,
                ": tasks={}, partition_chunks={:?}",
                p.task_count, p.per_partition_chunks
            )?;
        }
        Ok(())
    }
}

impl ExecutionPlan for RemoteScanExec {
    fn name(&self) -> &str {
        "RemoteScanExec"
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
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // On a worker this pulls from the network (the chunks the coordinator streamed for this
        // partition); on the coordinator it pulls straight from the local provider.
        let chunks = self.feed.feed(partition, Arc::clone(&ctx))?;
        let task = DistributedTaskContext::from_ctx(&ctx).task_index as i64;
        let partition = partition as i64;
        let schema = self.schema();
        let projection = self.projection.clone();

        let stream = chunks.map(move |chunk| {
            let n = chunk?.n_rows as usize;
            // Each chunk produces `n` rows tagged with the task and partition that produced them.
            let cols: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(vec![task; n])),
                Arc::new(Int64Array::from(vec![partition; n])),
            ];
            let cols = match &projection {
                Some(p) => p.iter().map(|&i| Arc::clone(&cols[i])).collect(),
                None => cols,
            };
            Ok(RecordBatch::try_new(Arc::clone(&schema), cols)?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Codec for `RemoteScanExec`. Only the feed *handle* (a UUID) is serialised via `to_proto()` —
/// the provider stays on the coordinator, and `from_proto()` rebuilds a remote feed that reads
/// its chunks from the network.
#[derive(Debug)]
struct RemoteScanExecCodec;

#[derive(Clone, PartialEq, ::prost::Message)]
struct RemoteScanProto {
    #[prost(uint64, tag = "1")]
    partitions: u64,
    #[prost(uint64, repeated, tag = "2")]
    projection: Vec<u64>,
    #[prost(message, optional, tag = "3")]
    feed: Option<WorkUnitFeedProto>,
}

impl PhysicalExtensionCodec for RemoteScanExecCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let p = RemoteScanProto::decode(buf).map_err(|e| proto_error(format!("{e}")))?;
        let feed = WorkUnitFeed::<ChunkFeedProvider>::from_proto(
            p.feed.ok_or_else(|| proto_error("missing feed"))?,
        )?;
        let projection =
            (!p.projection.is_empty()).then(|| p.projection.iter().map(|&i| i as usize).collect());
        Ok(Arc::new(RemoteScanExec::new(
            feed,
            p.partitions as usize,
            projection,
        )))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let exec = node
            .downcast_ref::<RemoteScanExec>()
            .ok_or_else(|| proto_error(format!("expected RemoteScanExec, got {}", node.name())))?;
        RemoteScanProto {
            partitions: exec.properties.partitioning.partition_count() as u64,
            projection: exec
                .projection
                .iter()
                .flatten()
                .map(|&i| i as u64)
                .collect(),
            feed: Some(exec.feed.to_proto()),
        }
        .encode(buf)
        .map_err(|e| proto_error(format!("{e}")))
    }
}

/// Tells the planner how many tasks the leaf stage gets, and rebuilds the leaf so each task
/// advertises its share of the partitions.
#[derive(Debug)]
struct RemoteScanTaskEstimator;

impl TaskEstimator for RemoteScanTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _: &datafusion::config::ConfigOptions,
    ) -> Option<TaskEstimation> {
        let task_count = plan
            .downcast_ref::<RemoteScanExec>()?
            .feed
            .inner()?
            .task_count;
        Some(TaskEstimation::desired(task_count))
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _: &datafusion::config::ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(exec) = plan.downcast_ref::<RemoteScanExec>() else {
            return Ok(None);
        };
        let partitions_per_task = exec.feed.try_inner()?.per_partition_chunks.len() / task_count;
        Ok(Some(Arc::new(RemoteScanExec::new(
            exec.feed.clone(),
            partitions_per_task,
            exec.projection.clone(),
        ))))
    }
}

/// `scan(task_count, 'chunks_p0', 'chunks_p1', ...)` — `task_count` tasks, with one
/// comma-separated list of chunk sizes per partition. The partition count must be a multiple of
/// `task_count`.
#[derive(Debug)]
struct ScanTableFunction;

impl TableFunctionImpl for ScanTableFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.len() < 2 {
            return plan_err!("scan(task_count, partitions...) needs at least 2 arguments");
        }
        let task_count = match &exprs[0] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v as usize,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as usize,
            v => return plan_err!("task_count must be an integer literal, got {v:?}"),
        };
        let per_partition_chunks = exprs[1..]
            .iter()
            .map(|e| match e {
                Expr::Literal(ScalarValue::Utf8(Some(s)), _) => parse_chunks(s),
                v => plan_err!("partition args must be string literals, got {v:?}"),
            })
            .collect::<Result<Vec<_>>>()?;
        if task_count == 0 || per_partition_chunks.len() % task_count != 0 {
            return plan_err!(
                "partition count ({}) must be a non-zero multiple of task_count ({task_count})",
                per_partition_chunks.len()
            );
        }
        Ok(Arc::new(ScanTableProvider {
            task_count,
            per_partition_chunks,
        }))
    }
}

fn parse_chunks(s: &str) -> Result<Vec<u64>> {
    s.split(',')
        .filter(|item| !item.trim().is_empty())
        .map(|item| {
            item.trim()
                .parse::<u64>()
                .map_err(|e| DataFusionError::Plan(format!("invalid chunk size {item:?}: {e}")))
        })
        .collect()
}

#[derive(Debug)]
struct ScanTableProvider {
    task_count: usize,
    per_partition_chunks: Vec<Vec<u64>>,
}

#[async_trait]
impl TableProvider for ScanTableProvider {
    fn schema(&self) -> SchemaRef {
        scan_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let feed = WorkUnitFeed::new(ChunkFeedProvider {
            per_partition_chunks: self.per_partition_chunks.clone(),
            task_count: self.task_count,
        });
        Ok(Arc::new(RemoteScanExec::new(
            feed,
            self.per_partition_chunks.len(),
            projection.cloned(),
        )))
    }
}

#[derive(StructOpt)]
#[structopt(name = "work_unit_feed", about = "Work unit feed example")]
struct Args {
    /// The SQL query to run.
    query: String,

    /// Render the distributed plan instead of executing the query.
    #[structopt(long)]
    show_distributed_plan: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::from_args();

    // The worker session must know how to deserialize `RemoteScanExec` (and so its remote feed),
    // so the codec is registered on both the coordinator and the worker session builder.
    let channel_resolver =
        InMemoryChannelResolver::from_session_builder(|ctx: WorkerQueryContext| async move {
            Ok(ctx
                .builder
                .with_distributed_user_codec(RemoteScanExecCodec)
                .build())
        });

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(4))
        .with_distributed_channel_resolver(channel_resolver)
        .with_distributed_planner()
        .with_distributed_user_codec(RemoteScanExecCodec)
        .with_distributed_task_estimator(RemoteScanTaskEstimator)
        // For every `RemoteScanExec`, hand the planner the feed it must drive from the coordinator.
        .with_distributed_work_unit_feed(|exec: &RemoteScanExec| Some(&exec.feed))
        .build();

    let ctx = SessionContext::from(state);
    ctx.register_udtf("scan", Arc::new(ScanTableFunction));

    let df = ctx.sql(&args.query).await?;
    if args.show_distributed_plan {
        let plan = df.create_physical_plan().await?;
        println!("{}", display_plan_ascii(plan.as_ref(), false));
    } else {
        let batches = df.execute_stream().await?.try_collect::<Vec<_>>().await?;
        println!("{}", pretty_format_batches(&batches)?);
    }
    Ok(())
}
