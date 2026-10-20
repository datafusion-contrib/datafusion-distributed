use crate::{IcebergConfig, common::df_err};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTaskStream;
use iceberg::spec::SnapshotRef;
use std::sync::Arc;

/// Execution settings shared by assigned-file and feed-backed scans.
#[derive(Debug, Clone)]
pub(crate) struct ScanConfig {
    pub(crate) schema: SchemaRef,
    pub(crate) partitioning: Partitioning,
    pub(crate) fetch: Option<usize>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) iceberg_file_io: FileIO,
    pub(crate) iceberg_runtime: iceberg::Runtime,
}

impl ScanConfig {
    pub(crate) fn open(
        &self,
        tasks: FileScanTaskStream,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let config = IcebergConfig::from_task_context(&context);

        let reader =
            ArrowReaderBuilder::new(self.iceberg_file_io.clone(), self.iceberg_runtime.clone())
                .with_batch_size(context.session_config().batch_size())
                .with_data_file_concurrency_limit(config.data_file_concurrency_limit)
                .with_row_group_filtering_enabled(config.row_group_filtering_enabled)
                .with_row_selection_enabled(config.row_selection_enabled)
                .build();

        let stream = reader
            .read(tasks)
            .map(|result| result.stream())
            .map_err(df_err)?
            .map_err(df_err);

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )) as SendableRecordBatchStream;

        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(LimitStream::new(stream, 0, self.fetch, metrics)))
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

/// Use projected compressed bytes when all column sizes are known, otherwise retain file bytes.
pub(crate) fn apply_column_statistics(stats: &mut Statistics, columns: Vec<ColumnStatistics>) {
    if let Some(bytes) = columns.iter().try_fold(0_usize, |total, column| {
        total.checked_add(*column.byte_size.get_value()?)
    }) {
        stats.total_byte_size = Precision::Inexact(bytes);
    }
    stats.column_statistics = columns;
}
