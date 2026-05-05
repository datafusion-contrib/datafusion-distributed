use arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::{
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::{Statistics, internal_err},
    datasource::TableType,
    error::Result,
    execution::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        stream::RecordBatchStreamAdapter,
    },
    prelude::Expr,
};
use datafusion_proto::{physical_plan::PhysicalExtensionCodec, protobuf::proto_error};
use futures::stream;
use prost::Message;
use std::{any::Any, fmt::Formatter, sync::Arc};
use tonic::async_trait;
use url::Url;

use crate::{
    DistributedTaskContext, ExecutionTask, PartitionIsolatorExec, TaskEstimation, TaskEstimator,
};

// Table function that creates a `URLEmitterExec` for testing task routing.
#[derive(Debug)]
pub struct URLEmitterFunction;

impl TableFunctionImpl for URLEmitterFunction {
    fn call(
        &self,
        _args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        Ok(Arc::new(URLEmitterTableProvider))
    }
}

#[derive(Debug)]
struct URLEmitterTableProvider;

#[async_trait]
impl TableProvider for URLEmitterTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        url_emitter_schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Hardcoded to 5 partitions and 5 tasks for testing.
        Ok(Arc::new(URLEmitterExec::new(5, 5)))
    }
}

#[derive(Debug, Clone)]
pub struct URLEmitterExec {
    properties: Arc<PlanProperties>,
    task_count: usize,
}

impl URLEmitterExec {
    pub fn new(partitions: usize, task_count: usize) -> Self {
        Self {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(url_emitter_schema()),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(partitions),
                datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            )),
            task_count,
        }
    }
}

impl DisplayAs for URLEmitterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "URLEmitterExec: tasks={} partitions={}",
            self.task_count,
            self.properties.partitioning.partition_count()
        )
    }
}

impl ExecutionPlan for URLEmitterExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(self.as_ref().clone()))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let distributed_ctx = DistributedTaskContext::from_ctx(&context);
        let batch = RecordBatch::try_new(
            url_emitter_schema(),
            vec![
                Arc::new(Int64Array::from(vec![distributed_ctx.task_count as i64])),
                Arc::new(Int64Array::from(vec![distributed_ctx.task_index as i64])),
                Arc::new(StringArray::from(vec![
                    distributed_ctx
                        .task_url
                        .as_ref()
                        .map(|url| url.as_str())
                        .unwrap(),
                ])),
            ],
        )?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            url_emitter_schema(),
            stream::iter(vec![Ok(batch)]),
        )))
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion::error::Result<datafusion::common::Statistics> {
        Ok(Statistics::new_unknown(&url_emitter_schema()))
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }
}

fn url_emitter_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("task_count", DataType::Int64, false),
        Field::new("task_index", DataType::Int64, false),
        Field::new("worker_url", DataType::Utf8, false),
    ]))
}

#[derive(Clone)]
pub struct URLEmitterTaskEstimator;

impl TaskEstimator for URLEmitterTaskEstimator {
    fn task_estimation(
        &self,
        plan: &std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _cfg: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Option<TaskEstimation>> {
        if let Some(exec) = plan.as_any().downcast_ref::<URLEmitterExec>() {
            Ok(Some(TaskEstimation::desired(exec.task_count)))
        } else {
            Ok(None)
        }
    }

    fn distribute_plan(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionIsolatorExec::new(Arc::clone(plan), task_count));
        Ok(Some(plan))
    }

    fn route_tasks(
        &self,
        tasks: Vec<ExecutionTask>,
        urls: &[Url],
    ) -> datafusion::error::Result<Option<Vec<Url>>> {
        // Trivial routing policy: Assign tasks to URLs in reverse order.
        let mut routed_urls = urls.to_vec();
        routed_urls.reverse();
        routed_urls.truncate(tasks.len());
        Ok(Some(routed_urls))
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct URLEmitterExecProto {
    #[prost(uint64, tag = "1")]
    partitions: u64,
    #[prost(uint64, tag = "2")]
    task_count: u64,
}

#[derive(Debug)]
pub struct URLEmitterExtensionCodec;

impl PhysicalExtensionCodec for URLEmitterExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "URLEmitterExtensionCodec should have no children, got {}",
                inputs.len()
            );
        }
        let proto = URLEmitterExecProto::decode(buf)
            .map_err(|e| proto_error(format!("Failed to decode URLEmitterExecProto: {e}")))?;

        Ok(Arc::new(URLEmitterExec::new(
            proto.partitions as usize,
            proto.task_count as usize,
        )))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(exec) = node.as_any().downcast_ref::<URLEmitterExec>() else {
            return internal_err!("Expected URLEmitterExec, but was {}", node.name());
        };

        let proto = URLEmitterExecProto {
            partitions: exec.properties.partitioning.partition_count() as u64,
            task_count: exec.task_count as u64,
        };

        proto
            .encode(buf)
            .map_err(|e| proto_error(format!("Failed to encode URLEmitterExec: {e}")))
    }
}
