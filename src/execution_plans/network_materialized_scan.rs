use crate::DistributedTaskContext;
use crate::common::require_one_child;
use crate::distributed_planner::{NetworkBoundary, ProducerHead};
use crate::stage::Stage;
use crate::worker::WorkerConnectionPool;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, PlanProperties,
};
use std::fmt::Formatter;
use std::sync::Arc;

/// An independently addressed reference to a shared materialized result.
///
/// The reader's partitions are divided between consumer tasks, never broadcast.
/// Reader identity is separate from materialization identity so future runtime
/// filter messages can describe individual consumers without changing result identity.
#[derive(Debug, Clone)]
pub struct NetworkMaterializedScanExec {
    pub(crate) cte_id: u64,
    pub(crate) reader_id: usize,
    pub(crate) partition_offset: usize,
    pub(crate) input_stage: Stage,
    properties: Arc<PlanProperties>,
    worker_connections: WorkerConnectionPool,
}

impl NetworkMaterializedScanExec {
    pub(crate) fn new(
        cte_id: u64,
        reader_id: usize,
        partition_offset: usize,
        partitions: usize,
        schema: SchemaRef,
        input_stage: Stage,
    ) -> Self {
        Self {
            cte_id,
            reader_id,
            partition_offset,
            worker_connections: WorkerConnectionPool::new(input_stage.task_count()),
            input_stage,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
        }
    }
}

impl NetworkBoundary for NetworkMaterializedScanExec {
    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn NetworkBoundary>> {
        Ok(Arc::new(Self::new(
            self.cte_id,
            self.reader_id,
            self.partition_offset,
            self.properties.partitioning.partition_count(),
            self.schema(),
            input_stage,
        )))
    }
    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }
    fn producer_head(&self, _: usize) -> Result<ProducerHead> {
        Ok(ProducerHead::None)
    }
}

impl DisplayAs for NetworkMaterializedScanExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "[Stage {}] => NetworkMaterializedScanExec: cte_id={}, reader_id={}, partitions={}",
            self.input_stage.num(),
            self.cte_id,
            self.reader_id,
            self.properties.partitioning.partition_count()
        )
    }
}

impl ExecutionPlan for NetworkMaterializedScanExec {
    fn name(&self) -> &str {
        "NetworkMaterializedScanExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.input_stage.local_plan().into_iter().collect()
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut stage = self.input_stage.clone();
        match &mut stage {
            Stage::Local(local) => local.plan = require_one_child(children)?,
            Stage::Remote(_) if children.is_empty() => {}
            Stage::Remote(_) => {
                return internal_err!("Remote materialized scan cannot have children");
            }
        }
        Ok(self.with_input_stage(stage)?)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.worker_connections.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task = DistributedTaskContext::from_ctx(&context);
        if partition % task.task_count != task.task_index {
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())));
        }
        let target = self.partition_offset + partition;
        match &self.input_stage {
            Stage::Local(local) => local.execute(target, context),
            Stage::Remote(remote) => {
                // Fetch only this reader partition: another partition may be behind
                // a join build barrier or never polled at all (e.g. LIMIT).
                let stream = self.worker_connections.execute(
                    remote,
                    target..target + 1,
                    0,
                    target,
                    ProducerHead::None,
                    &context,
                )?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    self.schema(),
                    stream,
                )))
            }
        }
    }
}
