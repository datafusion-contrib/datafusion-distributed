use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Result, exec_err};
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;
use iceberg::puffin::APACHE_DATASKETCHES_THETA_V1;
use iceberg::spec::{
    DataFile, Datum, ManifestContentType, ManifestList, NestedFieldRef, PrimitiveLiteral,
    PrimitiveType, SnapshotRef, Type,
};
use iceberg::table::Table;

use crate::common::df_err;
use iceberg::scan::FileScanTask;

/// Selected-file metrics, with snapshot NDV retained only as an inexact fallback.
/// FileScanTask does not retain manifest metrics, so this requires a metadata-only
/// second pass. Stop as soon as all selected files have been found.
pub(crate) async fn selected_column_statistics(
    table: &Table,
    snapshot: &SnapshotRef,
    fields: &[NestedFieldRef],
    files: &[FileScanTask],
    may_filter_rows: bool,
) -> Result<Vec<ColumnStatistics>> {
    if files.is_empty() || fields.is_empty() {
        return Ok(vec![
            ColumnStatistics {
                null_count: Precision::Exact(0),
                distinct_count: Precision::Exact(0),
                byte_size: Precision::Inexact(0),
                ..ColumnStatistics::new_unknown()
            };
            fields.len()
        ]);
    }
    if files
        .iter()
        .any(|task| task.start() != 0 || task.length() != task.file_size_in_bytes())
    {
        // Full-file metrics cannot describe a byte-range task.
        return Ok(vec![ColumnStatistics::new_unknown(); fields.len()]);
    }
    let mut remaining: HashSet<_> = files.iter().map(|task| task.data_file_path()).collect();
    if remaining.len() != files.len() {
        return exec_err!("Duplicate whole-file tasks in Iceberg column statistics lookup");
    }
    let field_ids: Vec<_> = fields
        .iter()
        .map(|field| {
            let mut ids = vec![];
            leaf_ids(field, &mut ids);
            ids
        })
        .collect();
    let mut statistics = vec![None; fields.len()];
    let manifests = table
        .manifest_list_reader(snapshot)
        .load()
        .await
        .map_err(df_err)?;
    for manifest in manifests
        .entries()
        .iter()
        .filter(|manifest| manifest.content == ManifestContentType::Data)
    {
        let manifest = table
            .manifest_reader()
            .read(manifest)
            .await
            .map_err(df_err)?;
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let file = entry.data_file();
            if !remaining.remove(file.file_path()) {
                continue;
            }
            for ((acc, field), ids) in statistics.iter_mut().zip(fields).zip(&field_ids) {
                // Leaf metrics cannot describe a parent struct/list/map's value or null count.
                let mut next = if matches!(field.field_type.as_ref(), Type::Primitive(_)) {
                    data_file_col_stats(file, field.id)
                } else {
                    ColumnStatistics::new_unknown()
                };
                next.byte_size = ids
                    .iter()
                    .try_fold(0_usize, |total, id| {
                        let size = usize::try_from(*file.column_sizes().get(id)?).ok()?;
                        total.checked_add(size)
                    })
                    .map(Precision::Inexact)
                    .unwrap_or(Precision::Absent);
                *acc = Some(merge_col_stats(acc.take(), next));
            }
        }
        if remaining.is_empty() {
            break;
        }
    }
    if !remaining.is_empty() {
        return exec_err!("Planned Iceberg files missing from snapshot manifests");
    }
    let rows = files.iter().try_fold(0_usize, |rows, task| {
        rows.checked_add(usize::try_from(task.record_count()?).ok()?)
    });
    let ndvs = ndv_from_metadata(table, snapshot.snapshot_id());
    Ok(statistics
        .into_iter()
        .zip(fields)
        .map(|(stats, field)| {
            let mut stats = stats.unwrap_or_else(ColumnStatistics::new_unknown);
            if may_filter_rows {
                stats.null_count = stats.null_count.to_inexact();
            }
            // Snapshot sketches cannot be restricted to selected files. Retain their
            // estimate, capped by the selected rows, without claiming exactness.
            if let Some(&ndv) = ndvs.get(&field.id) {
                stats.distinct_count = Precision::Inexact(rows.map_or(ndv, |rows| ndv.min(rows)));
            }
            stats
        })
        .collect())
}

fn leaf_ids(field: &NestedFieldRef, ids: &mut Vec<i32>) {
    match field.field_type.as_ref() {
        Type::Primitive(_) | Type::Variant(_) => ids.push(field.id),
        Type::Struct(fields) => {
            for field in fields.fields() {
                leaf_ids(field, ids);
            }
        }
        Type::List(list) => leaf_ids(&list.element_field, ids),
        Type::Map(map) => {
            leaf_ids(&map.key_field, ids);
            leaf_ids(&map.value_field, ids);
        }
    }
}

/// Getting number of distinct values for columns in a table
///
/// Note: https://iceberg.apache.org/puffin-spec/#apache-datasketches-theta-v1-blob-type
fn ndv_from_metadata(table: &Table, snapshot_id: i64) -> HashMap<i32, usize> {
    let mut ndvs = HashMap::new();
    let Some(stats) = table.metadata().statistics_for_snapshot(snapshot_id) else {
        return ndvs;
    };
    for blob in &stats.blob_metadata {
        // Getting sketch estimates of ndv in every column
        if blob.r#type != APACHE_DATASKETCHES_THETA_V1 {
            continue;
        }

        let &[field_id] = &blob.fields[..] else {
            continue;
        };
        if let Some(ndv) = blob.properties.get("ndv").and_then(|v| v.parse().ok()) {
            ndvs.insert(field_id, ndv);
        }
    }
    ndvs
}

/// Reading table statistics from data-files concurrently
pub async fn compute_column_stats(
    table: Table,
    fields_ids: Vec<i32>,
    snapshot: Option<SnapshotRef>,
) -> Result<Vec<ColumnStatistics>> {
    match snapshot {
        Some(actual_snapshot) => {
            let metadata = table.metadata();
            let ml_bytes = table
                .file_io()
                .new_input(actual_snapshot.manifest_list())
                .map_err(df_err)?
                .read()
                .await
                .map_err(df_err)?;
            let manifest_list = Arc::new(
                ManifestList::parse_with_version(&ml_bytes, metadata.format_version())
                    .map_err(df_err)?,
            );

            // Account for merge-on-read delete files later by marking the counts as inexact.
            let has_deletes = manifest_list
                .entries()
                .iter()
                .any(|f| f.content == ManifestContentType::Deletes);
            // Collect all needed manifest entries before spawning.
            let manifests: Vec<_> = manifest_list
                .entries()
                .iter()
                .filter(|manifest| manifest.content == ManifestContentType::Data)
                .cloned()
                .collect();
            let mut join_set = tokio::task::JoinSet::new();

            for manifest_file in manifests {
                let table = table.clone();
                let fields_ids = fields_ids.clone();

                join_set.spawn(async move {
                    let manifest = table.manifest_reader().read(&manifest_file).await?;

                    let mut col_stats: Vec<Option<ColumnStatistics>> = vec![None; fields_ids.len()];

                    for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                        let df = entry.data_file();

                        for (i, &id) in fields_ids.iter().enumerate() {
                            let next = data_file_col_stats(df, id);
                            col_stats[i] = Some(merge_col_stats(col_stats[i].take(), next));
                        }
                    }

                    Ok::<_, iceberg::Error>(col_stats)
                });
            }

            let mut merged: Vec<Option<ColumnStatistics>> = vec![None; fields_ids.len()];
            while let Some(result) = join_set.join_next().await {
                let manifest_stats = result
                    .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))?
                    .map_err(df_err)?;
                for (acc, next) in merged.iter_mut().zip(manifest_stats) {
                    if let Some(next) = next {
                        *acc = Some(merge_col_stats(acc.take(), next));
                    }
                }
            }

            let mut merged: Vec<ColumnStatistics> = merged
                .into_iter()
                .map(|cs| cs.unwrap_or_else(ColumnStatistics::new_unknown))
                .collect();

            let ndvs = ndv_from_metadata(&table, actual_snapshot.snapshot_id());
            for (cs, id) in merged.iter_mut().zip(fields_ids.iter()) {
                if let Some(&ndv) = ndvs.get(id) {
                    cs.distinct_count = Precision::Inexact(ndv);
                }
            }

            // TODO: probably we can do something about the delete files?
            // However it wouldn't be free of cost in terms of performance
            if has_deletes {
                for cs in &mut merged {
                    cs.null_count = cs.null_count.to_inexact();
                }
            }

            Ok(merged)
        }
        None => Ok(vec![ColumnStatistics::new_unknown(); fields_ids.len()]),
    }
}

/// Merging table's column statistics incrementally
fn merge_col_stats(acc: Option<ColumnStatistics>, next: ColumnStatistics) -> ColumnStatistics {
    match acc {
        None => next,
        Some(acc) => ColumnStatistics {
            null_count: acc.null_count.add(&next.null_count),
            min_value: acc.min_value.min(&next.min_value),
            max_value: acc.max_value.max(&next.max_value),
            byte_size: acc
                .byte_size
                .get_value()
                .zip(next.byte_size.get_value())
                .and_then(|(a, b)| a.checked_add(*b))
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
        },
    }
}

fn data_file_col_stats(df: &DataFile, id: i32) -> ColumnStatistics {
    ColumnStatistics {
        null_count: df
            .null_value_counts()
            .get(&id)
            .map(|n| Precision::Exact(*n as usize))
            .unwrap_or(Precision::Absent),
        min_value: df
            .lower_bounds()
            .get(&id)
            .and_then(datum_to_scalar)
            .map(Precision::Inexact)
            .unwrap_or(Precision::Absent),
        max_value: df
            .upper_bounds()
            .get(&id)
            .and_then(datum_to_scalar)
            .map(Precision::Inexact)
            .unwrap_or(Precision::Absent),
        byte_size: df
            .column_sizes()
            .get(&id)
            .map(|n| Precision::Inexact(*n as usize))
            .unwrap_or(Precision::Absent),
        sum_value: Precision::Absent,
        distinct_count: Precision::Absent,
    }
}

/// Conversion function of iceberg's Datum
fn datum_to_scalar(d: &Datum) -> Option<ScalarValue> {
    match (d.data_type(), d.literal()) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(v)) => {
            Some(ScalarValue::Boolean(Some(*v)))
        }
        (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Some(ScalarValue::Int32(Some(*v))),
        (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Some(ScalarValue::Int64(Some(*v))),
        (PrimitiveType::Float, PrimitiveLiteral::Float(v)) => {
            Some(ScalarValue::Float32(Some(v.into_inner())))
        }
        (PrimitiveType::Double, PrimitiveLiteral::Double(v)) => {
            Some(ScalarValue::Float64(Some(v.into_inner())))
        }
        (PrimitiveType::String, PrimitiveLiteral::String(s)) => {
            Some(ScalarValue::Utf8(Some(s.clone())))
        }
        (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Some(ScalarValue::Date32(Some(*v))),
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
            Some(ScalarValue::TimestampMicrosecond(Some(*v), None))
        }
        (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => Some(
            ScalarValue::TimestampMicrosecond(Some(*v), Some("UTC".into())),
        ),
        (PrimitiveType::Decimal { precision, scale }, PrimitiveLiteral::Int128(v)) => Some(
            ScalarValue::Decimal128(Some(*v), *precision as u8, *scale as i8),
        ),
        _ => None,
    }
}
