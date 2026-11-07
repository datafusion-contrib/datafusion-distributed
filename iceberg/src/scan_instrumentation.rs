use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use std::fmt::Debug;
use std::sync::Arc;

/// Opens one scan partition, including source setup and source-level limit handling.
/// The callback is synchronous; the returned stream performs the asynchronous reads.
pub type ScanOpen<'a> = Box<dyn FnOnce() -> Result<SendableRecordBatchStream> + Send + 'a>;

/// Application instrumentation for eager and lazy Iceberg scans.
///
/// Implementations call `open` exactly once and return its stream, optionally wrapped
/// to observe batches, errors, completion, or cancellation. They must preserve its
/// schema, batches, errors, and polling behavior. An opening error must be returned
/// unchanged. Context guards can surround `open`; stream wrappers must establish
/// their context separately on each poll rather than hold a thread-local guard.
///
/// Metrics registered in `metrics` are exposed by the source's existing execution
/// plan and collected by the normal distributed metrics machinery. No instrumentation
/// or additional plan node is serialized. Install the hook in every executing session.
pub trait ScanInstrumentation: Debug + Send + Sync {
    /// Instruments one partition opening and its output stream.
    ///
    /// `partition` is local to the executing task. The stream returned
    /// by `open` has already applied any source-level limit, but not downstream
    /// operators. A stream's lifetime includes time waiting for its consumer.
    fn instrument(
        &self,
        partition: usize,
        context: &TaskContext,
        metrics: &ExecutionPlanMetricsSet,
        open: ScanOpen<'_>,
    ) -> Result<SendableRecordBatchStream>;
}

/// Session extension holding a process-local scan instrumentation hook.
///
/// Register with `SessionConfig::with_extension(Arc::new(ScanInstrumentationExtension(hook)))`
/// on both coordinators and workers. It instruments only this crate's eager and
/// lazy Iceberg scans.
///
/// # Example
///
/// This example records rows from Iceberg scans; a stream wrapper can additionally observe
/// errors, completion, and `Drop` for cancellation.
///
/// ```
/// use std::sync::Arc;
/// use datafusion::error::Result;
/// use datafusion::execution::{SendableRecordBatchStream, TaskContext};
/// use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
/// use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
/// use datafusion::prelude::SessionConfig;
/// use datafusion_distributed_iceberg::{ScanInstrumentation, ScanInstrumentationExtension, ScanOpen};
/// use futures::TryStreamExt;
///
/// #[derive(Debug)]
/// struct SqlMetrics;
///
/// impl ScanInstrumentation for SqlMetrics {
///     fn instrument(
///         &self,
///         partition: usize,
///         _context: &TaskContext,
///         metrics: &ExecutionPlanMetricsSet,
///         open: ScanOpen<'_>,
///     ) -> Result<SendableRecordBatchStream> {
///         let stream = open()?;
///         let schema = stream.schema();
///         let rows = MetricBuilder::new(metrics)
///             .with_new_label("source", "iceberg")
///             .counter("input_rows", partition);
///         let stream = stream.inspect_ok(move |batch| rows.add(batch.num_rows()));
///         Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
///     }
/// }
///
/// // Apply this extension when constructing each coordinator and worker session.
/// let config = SessionConfig::new().with_extension(Arc::new(
///     ScanInstrumentationExtension(Arc::new(SqlMetrics)),
/// ));
/// ```
#[derive(Debug, Clone)]
pub struct ScanInstrumentationExtension(pub Arc<dyn ScanInstrumentation>);

/// Invokes the session's scan instrumentation, or opens the scan directly when absent.
///
/// Data sources call this around their partition-opening logic and supply the same
/// metrics set returned by their `metrics()` method. The callback should return the
/// final source stream, after source-level limit handling.
pub(crate) fn instrument_scan(
    partition: usize,
    context: &TaskContext,
    metrics: &ExecutionPlanMetricsSet,
    open: impl FnOnce() -> Result<SendableRecordBatchStream> + Send,
) -> Result<SendableRecordBatchStream> {
    match context
        .session_config()
        .get_extension::<ScanInstrumentationExtension>()
    {
        Some(extension) => extension
            .0
            .instrument(partition, context, metrics, Box::new(open)),
        None => open(),
    }
}
