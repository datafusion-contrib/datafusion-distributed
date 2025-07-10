use std::{fmt::Formatter, sync::Arc};

use arrow::{
    array::{RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{
    error::{DataFusionError, Result},
    execution::SendableRecordBatchStream,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec,
        display::DisplayableExecutionPlan,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
};
use futures::StreamExt;

use crate::{
    logging::{debug, trace},
    protobuf::AnnotatedTaskOutput,
    vocab::{CtxAnnotatedOutputs, CtxHost, CtxPartitionGroup, CtxStageId},
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
    /// our plan properties
    properties: PlanProperties,
}

impl DistributedAnalyzeRootExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, verbose: bool, show_statistics: bool) -> Self {
        let field_a = Field::new("Task", DataType::Utf8, false);
        let field_b = Field::new("Plan", DataType::Utf8, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            input,
            verbose,
            show_statistics,
            properties,
        }
    }
}

impl DisplayAs for DistributedAnalyzeRootExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "DistributedAnalyzeRootExec[verbose = {}, show stats = {}]",
            self.verbose, self.show_statistics
        )
    }
}

impl ExecutionPlan for DistributedAnalyzeRootExec {
    fn name(&self) -> &str {
        "DistributedAnalyzeRootExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
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
        let task_outputs = context
            .session_config()
            .get_extension::<CtxAnnotatedOutputs>()
            .unwrap_or(Arc::new(CtxAnnotatedOutputs::default()))
            .0
            .clone();

        assert!(
            partition == 0,
            "DistributedAnalyzeRootExec expects only partition 0"
        );

        let host = context
            .session_config()
            .get_extension::<CtxHost>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "CtxHost not set in session config for DistributedAnalyzeRootExec".to_string(),
                )
            })?
            .0
            .clone();

        let stage_id = context
            .session_config()
            .get_extension::<CtxStageId>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "CtxStageId not set in session config for DistributedAnalyzeRootExec"
                        .to_string(),
                )
            })?
            .0;

        let partition_group = context
            .session_config()
            .get_extension::<CtxPartitionGroup>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "CtxPartitionGroup not set in session config for DistributedAnalyzeRootExec"
                        .to_string(),
                )
            })?
            .0
            .clone();

        // we want to gather all partitions
        let coalesce = CoalescePartitionsExec::new(self.input.clone());

        let mut input_stream = coalesce.execute(partition, context)?;

        let schema_capture = self.schema().clone();
        let input_capture = self.input.clone();
        let show_statistics_capture = self.show_statistics;
        let verbose_capture = self.verbose;

        let fmt_plan = move || -> String {
            DisplayableExecutionPlan::with_metrics(input_capture.as_ref())
                .set_show_statistics(show_statistics_capture)
                .indent(verbose_capture)
                .to_string()
        };

        let output = async move {
            // consume input, and we do not have to send it downstream as we are the
            // root of the distributed analyze so we can discard the results just like
            // regular AnalyzeExec
            let mut done = false;
            while !done {
                match input_stream.next().await.transpose() {
                    Ok(Some(batch)) => {
                        // we consume the batch, yum.
                        trace!("consumed {} ", batch.num_rows());
                    }
                    Ok(None) => done = true,
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            let annotated_plan = fmt_plan();
            let toutput = AnnotatedTaskOutput {
                plan: annotated_plan,
                host: Some(host),
                stage_id,
                partition_group,
            };

            let mut tasks = task_outputs.lock();
            tasks.push(toutput);

            tasks.sort_by_key(|t| (t.stage_id, t.partition_group.clone()));

            let mut task_builder = StringBuilder::with_capacity(1, 1024);
            let mut plan_builder = StringBuilder::with_capacity(1, 1024);
            task_builder.append_value("Task");
            plan_builder.append_value("Plan with Metrics");

            for task_output in tasks.iter() {
                task_builder.append_value(format!(
                    "Task: Stage {}, Partitions {:?}\nHost: {}",
                    task_output.stage_id,
                    task_output.partition_group,
                    task_output
                        .host
                        .as_ref()
                        .map(|h| h.to_string())
                        .unwrap_or("Unknown".to_string())
                ));
                plan_builder.append_value(&task_output.plan);
            }

            RecordBatch::try_new(
                schema_capture,
                vec![
                    Arc::new(task_builder.finish()),
                    Arc::new(plan_builder.finish()),
                ],
            )
            .map_err(DataFusionError::from)
            .inspect(|batch| {
                debug!("returning record batch {:?}", batch);
            })
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(output),
        )))
    }
}
