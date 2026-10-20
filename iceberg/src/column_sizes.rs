use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::stats::Precision;
use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use iceberg::spec::{ManifestContentType, NestedFieldRef, SnapshotRef, Type};
use iceberg::table::Table;

use crate::common::df_err;
use crate::planned_files::PlannedFiles;

/// Compressed column bytes for the selected whole files, not filtered Arrow memory.
/// FileScanTask does not retain manifest metrics, so this requires a metadata-only
/// second pass. Stop as soon as all selected files have been found.
pub(crate) async fn selected_column_sizes(
    table: &Table,
    snapshot: &SnapshotRef,
    fields: &[NestedFieldRef],
    planned: &PlannedFiles,
    context: &Arc<TaskContext>,
) -> Result<Vec<Precision<usize>>> {
    if planned
        .tasks
        .iter()
        .any(|task| task.start() != 0 || task.length() != task.file_size_in_bytes())
    {
        // Full-file metrics cannot describe a byte-range task.
        return Ok(vec![Precision::Absent; fields.len()]);
    }
    let reservation =
        MemoryConsumer::new("Iceberg column size lookup").register(context.memory_pool());
    reservation.try_grow(
        planned
            .tasks
            .len()
            .saturating_mul(size_of::<&str>())
            .saturating_mul(2),
    )?;
    let mut remaining: HashSet<_> = planned
        .tasks
        .iter()
        .map(|task| task.data_file_path())
        .collect();
    if remaining.len() != planned.tasks.len() {
        return exec_err!("Duplicate whole-file tasks in Iceberg column size lookup");
    }
    let fields: Vec<_> = fields
        .iter()
        .map(|field| {
            let mut ids = vec![];
            leaf_ids(field, &mut ids);
            ids
        })
        .collect();
    let mut sizes = vec![Some(0_usize); fields.len()];
    if !remaining.is_empty() {
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
                for (total, ids) in sizes.iter_mut().zip(&fields) {
                    for id in ids {
                        *total = total.and_then(|total| {
                            let size = usize::try_from(*file.column_sizes().get(id)?).ok()?;
                            total.checked_add(size)
                        });
                    }
                }
            }
            if remaining.is_empty() {
                break;
            }
        }
        if !remaining.is_empty() {
            return exec_err!("Planned Iceberg files missing from snapshot manifests");
        }
    }
    Ok(sizes
        .into_iter()
        .map(|size| size.map(Precision::Inexact).unwrap_or(Precision::Absent))
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
