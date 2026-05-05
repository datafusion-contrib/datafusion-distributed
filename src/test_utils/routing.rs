use arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::{
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::{ScalarValue, Statistics, internal_err, plan_err},
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

use crate::worker::LocalWorkerContext;
use crate::{DistributedTaskContext, PartitionIsolatorExec, TaskEstimation, TaskEstimator};

// Table function that creates a `URLEmitterExec` for testing task routing.
#[derive(Debug)]
pub struct URLEmitterFunction;

impl TableFunctionImpl for URLEmitterFunction {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        if args.len() != 3 {
            return plan_err!(
                "url_emitter(partitions, task_count, tag) requires exactly 3 arguments"
            );
        }

        let partitions = match &args[0] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) if *v > 0 => *v as usize,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) if *v > 0 => *v as usize,
            v => return plan_err!("partitions must be a positive integer literal, got {v:?}"),
        };

        let task_count = match &args[1] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) if *v > 0 => *v as usize,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) if *v > 0 => *v as usize,
            v => return plan_err!("task_count must be a positive integer literal, got {v:?}"),
        };

        let tag = match &args[2] {
            Expr::Literal(ScalarValue::Utf8(Some(v)), _) => v.clone(),
            v => return plan_err!("tag must be a string literal, got {v:?}"),
        };

        Ok(Arc::new(URLEmitterTableProvider {
            partitions,
            task_count,
            tag,
        }))
    }
}

#[derive(Debug)]
struct URLEmitterTableProvider {
    partitions: usize,
    task_count: usize,
    tag: String,
}

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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(URLEmitterExec::new(
            self.partitions,
            self.task_count,
            self.tag.clone(),
            projection.cloned(),
        )))
    }
}

#[derive(Debug, Clone)]
pub struct URLEmitterExec {
    properties: Arc<PlanProperties>,
    task_count: usize,
    tag: String,
    projection: Option<Vec<usize>>,
}

impl URLEmitterExec {
    pub fn new(
        partitions: usize,
        task_count: usize,
        tag: String,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let schema = match &projection {
            Some(indices) => Arc::new(url_emitter_schema().project(indices).unwrap()),
            None => url_emitter_schema(),
        };
        Self {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(partitions),
                datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            )),
            task_count,
            tag,
            projection,
        }
    }
}

impl DisplayAs for URLEmitterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "URLEmitterExec: tasks={} partitions={} tag={}",
            self.task_count,
            self.properties.partitioning.partition_count(),
            self.tag,
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
        let local_worker_ctx = context
            .session_config()
            .get_extension::<LocalWorkerContext>()
            .expect("URLEmitterExec requires LocalWorkerContext during distributed execution");
        let schema = match &self.projection {
            Some(indices) => Arc::new(url_emitter_schema().project(indices).unwrap()),
            None => url_emitter_schema(),
        };
        let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(Int64Array::from(vec![distributed_ctx.task_count as i64])),
            Arc::new(Int64Array::from(vec![distributed_ctx.task_index as i64])),
            Arc::new(StringArray::from(vec![self.tag.as_str()])),
            Arc::new(StringArray::from(vec![
                local_worker_ctx.self_url.as_str().trim_end_matches('/'),
            ])),
        ];
        if let Some(indices) = &self.projection {
            columns = indices.iter().map(|&i| Arc::clone(&columns[i])).collect();
        }
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
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
        Field::new("tag", DataType::Utf8, false),
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
    ) -> Option<TaskEstimation> {
        plan.as_any()
            .downcast_ref::<URLEmitterExec>()
            .map(|exec| TaskEstimation::desired(exec.task_count))
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &datafusion::config::ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionIsolatorExec::new(Arc::clone(plan), task_count));
        Some(plan)
    }

    fn route_tasks(
        &self,
        routing_ctx: &crate::TaskRoutingContext<'_>,
    ) -> datafusion::error::Result<Option<Vec<Url>>> {
        // Trivial routing policy: Assign tasks to URLs in reverse order.
        let mut routed_urls = routing_ctx.urls.to_vec();
        routed_urls.reverse();
        routed_urls.truncate(routing_ctx.tasks.len());
        Ok(Some(routed_urls))
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct URLEmitterExecProto {
    #[prost(uint64, tag = "1")]
    partitions: u64,
    #[prost(uint64, tag = "2")]
    task_count: u64,
    #[prost(string, tag = "3")]
    tag: String,
    #[prost(uint64, repeated, tag = "4")]
    projection: Vec<u64>,
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
            proto.tag,
            if proto.projection.is_empty() {
                None
            } else {
                Some(proto.projection.into_iter().map(|v| v as usize).collect())
            },
        )))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(exec) = node.as_any().downcast_ref::<URLEmitterExec>() else {
            return internal_err!("Expected URLEmitterExec, but was {}", node.name());
        };

        let proto = URLEmitterExecProto {
            partitions: exec.properties.partitioning.partition_count() as u64,
            task_count: exec.task_count as u64,
            tag: exec.tag.clone(),
            projection: exec
                .projection
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|v| v as u64)
                .collect(),
        };

        proto
            .encode(buf)
            .map_err(|e| proto_error(format!("Failed to encode URLEmitterExec: {e}")))
    }
}
