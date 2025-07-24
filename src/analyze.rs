//! Distributed query analysis and performance profiling execution plans
//!
//! This module provides execution plan nodes for analyzing distributed query performance
//! in a similar way to DataFusion's `EXPLAIN ANALYZE` functionality, but adapted for
//! distributed execution across multiple worker nodes.
//!
//! # Purpose
//!
//! When executing `EXPLAIN ANALYZE` queries in a distributed environment, we need to:
//! - Collect execution metrics from all worker nodes
//! - Aggregate performance statistics across the distributed execution
//! - Present a unified view of the distributed query execution plan with metrics
//! - Track resource usage and timing information per stage and partition
//!
//! # Architecture
//!
//! The analysis system uses a two-tier approach:
//! - **`DistributedAnalyzeExec`**: Wraps individual stages for metric collection
//! - **`DistributedAnalyzeRootExec`**: Aggregates results from all workers at the proxy
//!
//! # Execution Flow
//!
//! 1. Query planning inserts `DistributedAnalyzeExec` nodes around distributed stages
//! 2. Workers execute their stages while collecting performance metrics
//! 3. Workers send annotated execution plans back to the proxy
//! 4. `DistributedAnalyzeRootExec` at the proxy collects and formats all results
//! 5. Final output shows the complete distributed execution plan with metrics
//!
//! # Integration with Standard DataFusion
//!
//! These execution plans integrate with DataFusion's metric collection system,
//! ensuring that distributed analysis provides the same level of detail as
//! single-node `EXPLAIN ANALYZE` queries.

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

/// Distributed analysis execution plan for individual stage metric collection
///
/// This execution plan wraps another execution plan to enable metric collection
/// for distributed `EXPLAIN ANALYZE` queries. It acts as a transparent wrapper
/// that delegates execution to its child while enabling metric tracking that
/// can be collected by the distributed analysis system.
///
/// # Purpose
/// - Enables metric collection for individual distributed stages
/// - Maintains compatibility with DataFusion's analysis infrastructure
/// - Provides transparent execution with metric annotation capabilities
/// - Supports both verbose and statistics display modes
///
/// # Usage
/// Inserted automatically during distributed query planning when `EXPLAIN ANALYZE`
/// is used, wrapping stages that will execute on worker nodes.
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
    /// Creates a new distributed analysis wrapper
    pub fn new(input: Arc<dyn ExecutionPlan>, verbose: bool, show_statistics: bool) -> Self {
        Self {
            input,
            verbose,
            show_statistics,
        }
    }

    /// Generates an annotated plan string with metrics for this stage
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

/// Distributed analysis root execution plan for result aggregation and formatting
///
/// This execution plan serves as the root node for distributed `EXPLAIN ANALYZE` queries,
/// collecting annotated execution plans from all worker nodes and formatting them into
/// a unified result. It runs on the proxy node and aggregates performance data from
/// the entire distributed execution.
///
/// # Purpose
/// - Collects annotated execution plans from all distributed workers
/// - Aggregates and sorts results by stage and partition for readable output
/// - Formats the final `EXPLAIN ANALYZE` output with distributed metrics
/// - Provides a unified view of distributed query performance
///
/// # Output Schema
/// Produces a two-column result:
/// - **Task**: Stage and partition information with worker host details
/// - **Plan with Metrics**: Annotated execution plan with performance metrics
///
/// # Context Dependencies
/// Requires session context extensions for distributed execution metadata:
/// - `CtxAnnotatedOutputs`: Collection of worker analysis results
/// - `CtxHost`: Current worker host information
/// - `CtxStageId`: Current stage identifier
/// - `CtxPartitionGroup`: Partition assignment information
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
    /// Creates a new distributed analysis root execution plan
    ///
    /// Sets up the output schema for the analysis results and configures
    /// plan properties for single-partition bounded execution.
    pub fn new(input: Arc<dyn ExecutionPlan>, verbose: bool, show_statistics: bool) -> Self {
        // Define output schema for analysis results
        let field_a = Field::new("Task", DataType::Utf8, false);
        let field_b = Field::new("Plan with Metrics", DataType::Utf8, false);
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
        // Extract annotated outputs from all workers
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

        // Extract distributed execution context information
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

        // Coalesce all partitions to single stream for processing
        let coalesce = Arc::new(CoalescePartitionsExec::new(self.input.clone()));

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

        // Aggregates analysis results from all distributed workers into formatted output
        //
        // This async block handles the core logic of distributed analysis:
        // 1. Consumes input stream (discarding data like standard AnalyzeExec)
        // 2. Collects annotated execution plans from all workers
        // 3. Sorts results by stage and partition for readable output
        // 4. Formats final RecordBatch with task information and metrics
        let output = async move {
            // Consume input stream without forwarding data
            let mut done = false;
            while !done {
                match input_stream.next().await.transpose() {
                    Ok(Some(batch)) => {
                        trace!("consumed {} ", batch.num_rows());
                    }
                    Ok(None) => done = true,
                    Err(e) => {
                        return Err(e);
                    }
                }
            }

            // Generate annotated plan for current stage
            let annotated_plan = fmt_plan();
            let toutput = AnnotatedTaskOutput {
                plan: annotated_plan,
                host: Some(host),
                stage_id,
                partition_group,
            };

            // Collect and sort all worker analysis results
            let mut tasks = task_outputs.lock();
            tasks.push(toutput);

            tasks.sort_by_key(|t| (t.stage_id, t.partition_group.clone()));

            trace!("sorted tasks: {:?}", tasks);

            // Build output columns with task info and annotated plans
            let mut task_builder = StringBuilder::with_capacity(1, 1024);
            let mut plan_builder = StringBuilder::with_capacity(1, 1024);

            for task_output in tasks.iter() {
                task_builder.append_value(format!(
                    "Task: Stage {}, Partitions {:?}\nHost: {}",
                    task_output.stage_id,
                    task_output.partition_group,
                    task_output
                        .host
                        .as_ref()
                        .map(|h| format!("{} {}", h.name, h.addr))
                        .unwrap_or("Unknown".to_string())
                ));
                plan_builder.append_value(&task_output.plan);
            }

            // Create final RecordBatch with formatted analysis results
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
