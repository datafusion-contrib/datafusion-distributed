use crate::common::require_one_child;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::materialized_cte::{
    MaterializedCteBuffer, MaterializedCteExec, MaterializedCteScanExec,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::fmt::Formatter;
use std::sync::Arc;

/// One spillable, query-scoped result with independently replayable readers.
///
/// Output partitions are disjoint reader ranges. They address the same stored
/// result, not separate executions of the body. Registering every reader before
/// execution also lets DataFusion reserve memory for concurrent spill readers.
#[derive(Debug)]
pub(crate) struct MaterializeExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) id: u64,
    pub(crate) reader_partitions: Vec<usize>,
    readers: Vec<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
}

impl MaterializeExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        id: u64,
        reader_partitions: Vec<usize>,
    ) -> Self {
        let buffer = Arc::new(MaterializedCteBuffer::new(id, format!("cte_{id}")));
        let readers = reader_partitions
            .iter()
            .map(|&partitions| {
                let scan = MaterializedCteScanExec::new(
                    id,
                    format!("cte_{id}"),
                    input.schema(),
                    partitions,
                )
                .bind(Arc::clone(&buffer));
                Arc::new(MaterializedCteExec::new(
                    Arc::clone(&input),
                    Arc::new(scan),
                    Arc::clone(&buffer),
                )) as Arc<dyn ExecutionPlan>
            })
            .collect();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(reader_partitions.iter().sum()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            id,
            reader_partitions,
            readers,
            properties,
        }
    }
}

impl DisplayAs for MaterializeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "MaterializeExec: cte_id={}, readers={}",
            self.id,
            self.readers.len()
        )
    }
}

impl ExecutionPlan for MaterializeExec {
    fn name(&self) -> &str {
        "MaterializeExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
        Ok(Arc::new(Self::new(
            require_one_child(children)?,
            self.id,
            self.reader_partitions.clone(),
        )))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        self.readers.first().and_then(|reader| reader.metrics())
    }

    fn execute(
        &self,
        mut partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        for (reader, &partitions) in self.readers.iter().zip(&self.reader_partitions) {
            if partition < partitions {
                return reader.execute(partition, context);
            }
            partition -= partitions;
        }
        internal_err!("Materialization reader partition out of range")
    }
}
