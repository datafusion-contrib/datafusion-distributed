use std::{fmt::Formatter, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    error::Result,
    execution::SendableRecordBatchStream,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};

/// An execution plan that serves as a marker of where we want to split the
/// physical plan into stages.
#[derive(Debug)]
pub struct DDStageExec {
    /// Input plan
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Output partitioning
    properties: PlanProperties,
    pub stage_id: u64,
}

impl DDStageExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, stage_id: u64) -> Self {
        let properties = input.properties().clone();

        Self {
            input,
            properties,
            stage_id,
        }
    }

    fn new_with_properties(
        input: Arc<dyn ExecutionPlan>,
        stage_id: u64,
        properties: PlanProperties,
    ) -> Self {
        Self {
            input,
            properties,
            stage_id,
        }
    }
}
impl DisplayAs for DDStageExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "DDStageExec[{}] (output_partitioning={:?})",
            self.stage_id,
            self.properties().partitioning
        )
    }
}

impl ExecutionPlan for DDStageExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn name(&self) -> &str {
        "DDStageExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: handle more general case
        assert_eq!(children.len(), 1);
        let child = children[0].clone();

        Ok(Arc::new(DDStageExec::new_with_properties(
            child,
            self.stage_id,
            self.properties.clone(),
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // we are only using this node as a marker in the plan, so we don't execute it
        unimplemented!("DDStageExec")
    }
}
