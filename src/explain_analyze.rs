use crate::DistributedMetricsFormat;
use crate::common::require_one_child;
use crate::stage::explain_analyze;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::instant::Instant;
use datafusion::common::{DataFusionError, Result, assert_eq_or_internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
};
use futures::{StreamExt, stream};
use std::fmt::Formatter;
use std::sync::Arc;

/// Distributed counterpart of DataFusion's [`AnalyzeExec`].
///
/// It drains a [`crate::DistributedExec`], waits for metrics from every worker, and returns the
/// distributed plan with those metrics as the `EXPLAIN ANALYZE` result.
#[derive(Debug)]
pub(crate) struct DistributedAnalyzeExec {
    input: Arc<dyn ExecutionPlan>,
    verbose: bool,
    properties: Arc<PlanProperties>,
}

impl DistributedAnalyzeExec {
    pub(crate) fn new(analyze_exec: &AnalyzeExec, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            input,
            verbose: analyze_exec.verbose(),
            properties: Arc::clone(analyze_exec.properties()),
        }
    }
}

impl DisplayAs for DistributedAnalyzeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedAnalyzeExec verbose={}", self.verbose)
    }
}

impl ExecutionPlan for DistributedAnalyzeExec {
    fn name(&self) -> &str {
        "DistributedAnalyzeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: require_one_child(&children)?,
            verbose: self.verbose,
            properties: Arc::clone(&self.properties),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq_or_internal_err!(
            partition,
            0,
            "DistributedAnalyzeExec invalid partition. Expected 0, got {partition}"
        );

        let mut input_stream = self.input.execute(0, context)?;
        let input = Arc::clone(&self.input);
        let schema = self.schema();
        let verbose = self.verbose;
        let output = async move {
            let start = Instant::now();
            let mut total_rows = 0;
            while let Some(batch) = input_stream.next().await.transpose()? {
                total_rows += batch.num_rows();
            }

            let plan =
                explain_analyze(Arc::clone(&input), DistributedMetricsFormat::Aggregated).await?;
            let full_plan = match verbose {
                true => Some(explain_analyze(input, DistributedMetricsFormat::PerTask).await?),
                false => None,
            };
            create_output_batch(total_rows, start.elapsed(), plan, full_plan, schema)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream::once(output),
        )))
    }
}

fn create_output_batch(
    total_rows: usize,
    duration: std::time::Duration,
    plan: String,
    full_plan: Option<String>,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut type_builder = StringBuilder::with_capacity(1, 1024);
    let mut plan_builder = StringBuilder::with_capacity(1, 1024);

    type_builder.append_value("Plan with Metrics");
    plan_builder.append_value(&plan);

    if let Some(full_plan) = full_plan {
        type_builder.append_value("Plan with Full Metrics");
        plan_builder.append_value(full_plan);

        type_builder.append_value("Output Rows");
        plan_builder.append_value(total_rows.to_string());

        type_builder.append_value("Duration");
        plan_builder.append_value(format!("{duration:?}"));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(type_builder.finish()),
            Arc::new(plan_builder.finish()),
        ],
    )
    .map_err(DataFusionError::from)
}
