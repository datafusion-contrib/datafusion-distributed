use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::{
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::Statistics,
    datasource::TableType,
    error::Result,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        stream::RecordBatchStreamAdapter,
    },
    prelude::Expr,
};
use futures::stream;
use std::{any::Any, fmt::Formatter, sync::Arc};
use tonic::async_trait;
use url::Url;

use crate::{DistributedPlan, ExecutionTask, PartitionIsolatorExec, TaskEstimation, TaskEstimator};

// Table function that creates a `URLEmitterExec` for testing task routing.
// Called in SQL as: `SELECT * FROM test_url_emitter()`.
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
        Ok(Arc::new(URLEmitterExec::new(1)))
    }
}

#[derive(Debug, Clone)]
pub struct URLEmitterExec {
    properties: Arc<PlanProperties>,
}

impl URLEmitterExec {
    pub fn new(partitions: usize) -> Self {
        Self {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(url_emitter_schema()),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(partitions),
                datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            )),
        }
    }
}

impl DisplayAs for URLEmitterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "URLEmitterExec: [   TODO   ]")
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
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            url_emitter_schema(),
            stream::iter(vec![Ok(RecordBatch::new_empty(url_emitter_schema()))]),
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
        if plan.as_any().downcast_ref::<URLEmitterExec>().is_some() {
            // TODO
            Ok(Some(TaskEstimation::desired(1)))
        } else {
            Ok(None)
        }
    }

    fn distribute_plan(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Option<DistributedPlan>> {
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionIsolatorExec::new(Arc::clone(plan), task_count));
        let distributed_plan = DistributedPlan::from_plan(plan);
        Ok(Some(distributed_plan))
    }

    fn route_tasks(
        &self,
        tasks: Vec<ExecutionTask>,
        urls: &[Url],
    ) -> datafusion::error::Result<Option<Vec<Url>>> {
        // Add some custom routing.
        let routed_urls = custom_routing_fn(tasks, urls.to_vec());
        Ok(Some(routed_urls))
    }
}

fn custom_routing_fn(tasks: Vec<ExecutionTask>, mut urls: Vec<Url>) -> Vec<Url> {
    // Trivial routing policy.
    urls.reverse();
    urls.truncate(tasks.len());
    urls
}
