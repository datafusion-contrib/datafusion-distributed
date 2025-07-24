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

/// A DataFusion execution plan that yields a single pre-computed RecordBatch
///
/// This execution plan holds a RecordBatch in memory and streams it as the query result.
/// It's primarily used for queries that have been pre-executed or for holding static
/// results like EXPLAIN output, DESCRIBE table metadata, or locally computed results.
///
/// # Use Cases
/// - **EXPLAIN Queries**: Holds formatted explanation text as a RecordBatch
/// - **DESCRIBE Queries**: Contains table schema information pre-computed on the proxy
/// - **Local Execution**: Results from queries executed locally instead of distributed
/// - **Static Data**: Any pre-computed data that needs to be treated as query results
///
/// # Execution Behavior
/// - Always produces exactly one RecordBatch containing the pre-computed data
/// - Single partition (no parallelism since data is already materialized)
/// - Bounded execution (finite, known dataset size)
/// - Final emission (all data available immediately)
///
/// # Memory Considerations
/// The entire RecordBatch is held in memory, so this is best suited for small
/// result sets like metadata queries rather than large analytical results.
#[derive(Debug)]
pub struct RecordBatchExec {
    /// Pre-computed data that will be streamed as the query result
    pub batch: RecordBatch,
    /// Execution plan properties defining partitioning, boundedness, and equivalence
    pub properties: PlanProperties,
}

impl RecordBatchExec {
    /// Creates a new RecordBatchExec with a pre-computed RecordBatch
    ///
    /// This constructor wraps a RecordBatch in an ExecutionPlan interface, allowing
    /// pre-computed data to be treated like any other query execution step. The
    /// resulting plan is configured for single-partition, bounded execution.
    pub fn new(batch: RecordBatch) -> Self {
        // Configure execution plan properties for in-memory data
        let properties = PlanProperties::new(
            EquivalenceProperties::new(batch.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { batch, properties }
    }
}

/// Display formatting for RecordBatchExec in query plan representations
impl DisplayAs for RecordBatchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RecordBatchExec")
    }
}

/// DataFusion ExecutionPlan implementation for streaming pre-computed RecordBatch data
impl ExecutionPlan for RecordBatchExec {
    /// Returns the execution plan name for identification and debugging
    fn name(&self) -> &str {
        "RecordBatchExec"
    }

    /// Provides type-erased access to the concrete RecordBatchExec type
    ///
    /// This allows DataFusion's execution engine to downcast this plan back to
    /// RecordBatchExec if needed for specialized handling.
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Returns execution plan properties defining partitioning and execution characteristics
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    /// Returns child execution plans (always empty for RecordBatchExec)
    ///
    /// RecordBatchExec is a leaf node in the execution plan tree since it holds
    /// pre-computed data rather than deriving data from other execution plans.
    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// Creates a new RecordBatchExec with different child plans
    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> Result<std::sync::Arc<dyn ExecutionPlan>> {
        // RecordBatchExec doesn't actually use children, so we ignore them
        // TODO: Consider removing this assertion and handling empty children properly
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(self.batch.clone())))
    }

    /// Executes the plan by streaming the pre-computed RecordBatch
    fn execute(
        &self,
        _partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch = self.batch.clone();

        // Create an async future that immediately resolves with the batch
        let output = async move { Ok(batch) };

        // Wrap the future in a RecordBatch stream with proper schema information
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(output),
        )) as SendableRecordBatchStream;

        Ok(stream)
    }
}
