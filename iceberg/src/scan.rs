use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Result, Statistics};
use iceberg::spec::SnapshotRef;

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
