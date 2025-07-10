use std::{fmt::Formatter, sync::Arc};

use arrow::array::RecordBatch;
use datafusion::{
    error::Result,
    execution::SendableRecordBatchStream,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
};

#[derive(Debug)]
pub struct RecordBatchExec {
    pub batch: RecordBatch,
    pub properties: PlanProperties,
}

impl RecordBatchExec {
    pub fn new(batch: RecordBatch) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(batch.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { batch, properties }
    }
}

impl DisplayAs for RecordBatchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RecordBatchExec")
    }
}

impl ExecutionPlan for RecordBatchExec {
    fn name(&self) -> &str {
        "RecordBatchExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: generalize this
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(self.batch.clone())))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch = self.batch.clone();

        let output = async move { Ok(batch) };

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(output),
        )) as SendableRecordBatchStream;

        Ok(stream)
    }
}
