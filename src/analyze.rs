use std::{fmt::Formatter, sync::Arc};

use datafusion::{
    error::Result,
    execution::SendableRecordBatchStream,
    physical_plan::{
        display::DisplayableExecutionPlan, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties,
    },
};

#[derive(Debug)]
pub struct DistributedAnalyzeExec {
    /// Control how much extra to print
    pub(crate) verbose: bool,
    /// If statistics should be displayed
    pub(crate) show_statistics: bool,
    /// The input plan (the plan being analyzed)
    pub(crate) input: Arc<dyn ExecutionPlan>,
}

impl DistributedAnalyzeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, verbose: bool, show_statistics: bool) -> Self {
        Self {
            input,
            verbose,
            show_statistics,
        }
    }

    pub fn annotated_plan(&self) -> String {
        DisplayableExecutionPlan::with_metrics(self.input.as_ref())
            .set_show_statistics(self.show_statistics)
            .indent(self.verbose)
            .to_string()
    }
}

impl DisplayAs for DistributedAnalyzeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "DistributedAnalyzeExec[verbose = {}, show stats = {}]",
            self.verbose, self.show_statistics
        )
    }
}

impl ExecutionPlan for DistributedAnalyzeExec {
    fn name(&self) -> &str {
        "DistributedAnalyzeExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: generalize this
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.verbose,
            self.show_statistics,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

#[derive(Debug)]
pub struct DistributedAnalyzeRootExec {
    /// Control how much extra to print
    pub(crate) verbose: bool,
    /// If statistics should be displayed
    pub(crate) show_statistics: bool,
    /// The input plan (the plan being analyzed)
    pub(crate) input: Arc<dyn ExecutionPlan>,
}

impl DistributedAnalyzeRootExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, verbose: bool, show_statistics: bool) -> Self {
        Self {
            input,
            verbose,
            show_statistics,
        }
    }

    pub fn annotated_plan(&self) -> String {
        DisplayableExecutionPlan::with_metrics(self.input.as_ref())
            .set_show_statistics(self.show_statistics)
            .indent(self.verbose)
            .to_string()
    }
}

impl DisplayAs for DistributedAnalyzeRootExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "DistributedAnalyzeExec[verbose = {}, show stats = {}]",
            self.verbose, self.show_statistics
        )
    }
}

impl ExecutionPlan for DistributedAnalyzeRootExec {
    fn name(&self) -> &str {
        "DistributedAnalyzeExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: generalize this
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.verbose,
            self.show_statistics,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
