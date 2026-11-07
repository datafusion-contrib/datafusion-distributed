use crate::{IcebergConfig, common::df_err};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    DynamicFilterTracking, Partitioning, PhysicalExpr, conjunction, split_conjunction,
};
use datafusion::physical_plan::apply_expression_roots;
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory, MetricType,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::poll_fn;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{ArrowReader, ArrowReaderBuilder};
use iceberg::io::FileIO;
use iceberg::scan::FileScanTaskStream;
use iceberg::spec::SnapshotRef;
use std::fmt;
use std::sync::Arc;

/// Execution settings shared by assigned-file and feed-backed scans.
/// Reader settings are captured from the planning or decoding context. Clones
/// share the process-local reader; decoding constructs a fresh worker reader.
#[derive(Clone)]
pub(crate) struct ScanConfig {
    pub(crate) schema: SchemaRef,
    pub(crate) partitioning: Partitioning,
    pub(crate) fetch: Option<usize>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) iceberg_file_io: FileIO,
    pub(crate) iceberg_runtime: iceberg::Runtime,
    pub(crate) filters: Option<Arc<dyn PhysicalExpr>>,
    pub(crate) dynamic_filtering_enabled: bool,
    // Clones share the delete loader across this source's local partitions.
    reader: ArrowReader,
}

impl ScanConfig {
    pub(crate) fn new(
        schema: SchemaRef,
        partitioning: Partitioning,
        fetch: Option<usize>,
        iceberg_file_io: FileIO,
        iceberg_runtime: iceberg::Runtime,
        context: &TaskContext,
    ) -> Self {
        let config = IcebergConfig::from_config_options(context.session_config().options());
        let reader = ArrowReaderBuilder::new(iceberg_file_io.clone(), iceberg_runtime.clone())
            .with_batch_size(context.session_config().batch_size())
            .with_data_file_concurrency_limit(config.data_file_concurrency_limit)
            .with_row_group_filtering_enabled(config.row_group_filtering_enabled)
            .with_row_selection_enabled(config.row_selection_enabled)
            .build();
        Self {
            schema,
            partitioning,
            fetch,
            metrics: Default::default(),
            iceberg_file_io,
            iceberg_runtime,
            filters: None,
            dynamic_filtering_enabled: config.dynamic_filtering_enabled,
            reader,
        }
    }

    /// ANDs `filters` onto the scan's filters.
    pub(crate) fn push_filters(&mut self, filters: Vec<Arc<dyn PhysicalExpr>>) {
        self.filters = Some(conjunction(self.filters.take().into_iter().chain(filters)));
    }

    /// Visits the scan's filters when dynamic filtering is enabled.
    pub(crate) fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        if !self.dynamic_filtering_enabled {
            return Ok(TreeNodeRecursion::Continue);
        }
        apply_expression_roots(self.filters.iter(), f)
    }

    /// Writes the scan's dynamic filters into `f`.
    pub(crate) fn fmt_dynamic_filters(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(filters) = &self.filters else {
            return Ok(());
        };
        let dynamic_filters: Vec<_> = split_conjunction(filters)
            .into_iter()
            .filter(|filter| DynamicFilterTracking::classify(filter).contains_dynamic_filter())
            .cloned()
            .collect();
        if dynamic_filters.is_empty() {
            return Ok(());
        }
        write!(f, ", dynamic_filters={}", conjunction(dynamic_filters))
    }

    pub(crate) fn open(
        &self,
        tasks: FileScanTaskStream,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        let files_opened = MetricBuilder::new(&self.metrics)
            .with_category(MetricCategory::Rows)
            .counter("files_opened", partition);
        let bytes_scanned = MetricBuilder::new(&self.metrics)
            .with_type(MetricType::Summary)
            .with_category(MetricCategory::Bytes)
            .gauge("bytes_scanned", partition);

        // The reader pulls a task when it starts reading that task's file. It reads ahead,
        // so with a LIMIT this can count files that never produced output.
        let tasks = tasks.inspect_ok(move |_| files_opened.add(1)).boxed();
        let result = self.reader.clone().read(tasks).map_err(df_err)?;

        let reader_metrics = result.metrics().clone();
        let mut batches = result.stream().map_err(df_err);
        let stream = poll_fn(move |cx| {
            let poll = batches.poll_next_unpin(cx);
            if poll.is_ready() {
                bytes_scanned.set(reader_metrics.bytes_read() as usize);
            }
            poll
        });

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )) as SendableRecordBatchStream;

        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(LimitStream::new(stream, 0, self.fetch, metrics)))
    }
}

impl fmt::Debug for ScanConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanConfig")
            .field("schema", &self.schema)
            .field("partitioning", &self.partitioning)
            .field("fetch", &self.fetch)
            .field("metrics", &self.metrics)
            .field("iceberg_file_io", &self.iceberg_file_io)
            .field("iceberg_runtime", &self.iceberg_runtime)
            .field("filters", &self.filters)
            .field("dynamic_filtering_enabled", &self.dynamic_filtering_enabled)
            .finish_non_exhaustive()
    }
}

/// Snapshot summary keys defined by the Iceberg table spec:
/// https://iceberg.apache.org/spec/#optional-snapshot-summary-fields
///
/// iceberg-rust defines them privately:
/// https://github.com/apache/iceberg-rust/blob/4168a0b2950dc5f85588e5cb3ab6796e5228b309/crates/iceberg/src/spec/snapshot_summary.rs#L46-L47
const TOTAL_RECORDS: &str = "total-records";
const TOTAL_FILE_SIZE: &str = "total-files-size";

/// Getting stats out of snapshot's additional properties (no I/O overhead)
pub(crate) fn stats_from_snapshot(
    snapshot: Option<SnapshotRef>,
    schema: &SchemaRef,
) -> Result<Statistics> {
    let Some(snap) = snapshot else {
        // A table with no current snapshot has never had a commit. It was created, but zero data files were added
        return Ok(Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
        });
    };
    let props = &snap.summary().additional_properties;

    let num_rows = props
        .get(TOTAL_RECORDS)
        .and_then(|v| v.parse().ok())
        .map(Precision::Exact)
        .unwrap_or(Precision::Absent);
    let total_byte_size = props
        .get(TOTAL_FILE_SIZE)
        .and_then(|v| v.parse().ok())
        .map(Precision::Exact)
        .unwrap_or(Precision::Absent);

    Ok(Statistics {
        num_rows,
        total_byte_size,
        column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
    })
}
