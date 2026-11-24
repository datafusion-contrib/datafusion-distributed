//! In-memory size estimates for Iceberg scans, following Trino.
//!
//! Iceberg manifests and snapshot summaries record compressed on-disk sizes. Joins and other
//! operators hold Arrow data in memory, so scans report estimated Arrow sizes in their statistics.

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::ColumnStatistics;
use datafusion::common::stats::Precision;
use datafusion_distributed::default_bytes_for_datatype;

/// In-memory bytes per on-disk byte of a string column. Trino measured it on TPC-DS's `item` table.
///
/// Reference: Trino's TableStatisticsReader.java:207-216
/// https://github.com/trinodb/trino/blob/458/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/TableStatisticsReader.java#L207-L216
const STRING_BYTES_PER_DISK_BYTE: f64 = 2.7;

/// In-memory bytes per on-disk byte of a binary column. Trino measured it on alphanumeric data.
///
/// Reference: Trino's TableStatisticsReader.java:218-223
/// https://github.com/trinodb/trino/blob/458/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/TableStatisticsReader.java#L218-L223
const BINARY_BYTES_PER_DISK_BYTE: f64 = 1.4;

/// Bytes per row for the validity bitmap, rounded up as in [default_bytes_for_datatype].
const VALIDITY_BYTES_PER_ROW: usize = 1;

/// Estimates one column's in-memory Arrow size in one data file.
///
/// Strings and binary with an `on_disk` size scale it by Trino's factor, then add validity and
/// offsets or views per row. Other columns take `rows` × [default_bytes_for_datatype]. So do
/// strings and binary when the manifest has no size for this file.
pub(crate) fn column_arrow_bytes(data_type: &DataType, rows: u64, on_disk: Option<u64>) -> usize {
    let rows = usize::try_from(rows).unwrap_or(usize::MAX);
    let default = rows.saturating_mul(default_bytes_for_datatype(data_type));
    let Some(on_disk) = on_disk else {
        return default;
    };
    // `slot_bytes` is each row's entry in the offsets or views buffer.
    let (bytes_per_disk_byte, slot_bytes) = match data_type {
        DataType::Utf8 => (STRING_BYTES_PER_DISK_BYTE, size_of::<i32>()),
        DataType::LargeUtf8 => (STRING_BYTES_PER_DISK_BYTE, size_of::<i64>()),
        DataType::Utf8View => (STRING_BYTES_PER_DISK_BYTE, 16),
        DataType::Binary => (BINARY_BYTES_PER_DISK_BYTE, size_of::<i32>()),
        DataType::LargeBinary => (BINARY_BYTES_PER_DISK_BYTE, size_of::<i64>()),
        DataType::BinaryView => (BINARY_BYTES_PER_DISK_BYTE, 16),
        _ => return default,
    };
    // Float-to-int casts saturate.
    let values = (on_disk as f64 * bytes_per_disk_byte).round() as usize;
    let per_row = rows.saturating_mul(VALIDITY_BYTES_PER_ROW + slot_bytes);
    values.saturating_add(per_row)
}

/// Estimates a scan's in-memory Arrow size.
///
/// Each column of `schema` takes its `byte_size` from `columns` when known, or `num_rows` ×
/// [default_bytes_for_datatype]. The estimate is absent when a column needs `num_rows` and it is
/// unknown.
pub(crate) fn scan_arrow_bytes(
    schema: &Schema,
    num_rows: Precision<usize>,
    columns: &[ColumnStatistics],
) -> Precision<usize> {
    if num_rows == Precision::Exact(0) {
        return Precision::Exact(0);
    }
    let rows = num_rows.get_value();
    let bytes = schema
        .fields()
        .iter()
        .enumerate()
        .try_fold(0usize, |bytes, (i, field)| {
            let column = match columns
                .get(i)
                .and_then(|column| column.byte_size.get_value())
            {
                Some(column) => *column,
                None => rows?.saturating_mul(default_bytes_for_datatype(field.data_type())),
            };
            Some(bytes.saturating_add(column))
        });
    bytes.map_or(Precision::Absent, Precision::Inexact)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;

    use super::column_arrow_bytes;

    // Other writers can leave a column out of one file's sizes. The test tools always record
    // them, so this case is tested here.
    #[test]
    fn string_without_a_recorded_size_takes_the_default_width() {
        // 50 bytes per value, plus a validity byte and a 4-byte offset.
        assert_eq!(
            column_arrow_bytes(&DataType::Utf8, 4, None),
            4 * (50 + 1 + 4)
        );
    }
}
