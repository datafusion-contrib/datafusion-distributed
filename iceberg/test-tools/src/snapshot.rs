use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::FixtureTable;
use crate::error::df_err;
use datafusion::error::Result;
use iceberg::spec::{
    DataContentType, DataFile, MAIN_BRANCH, ManifestListWriter, ManifestWriterBuilder, Operation,
    Snapshot, SnapshotSummaryCollector, Summary,
};

/// Commit newly written files, retaining previous manifests and accumulating totals.
pub(crate) async fn commit_files(
    fixture: &mut FixtureTable,
    data_files: &[DataFile],
    content: DataContentType,
) -> Result<()> {
    let table = fixture.table();
    let metadata = table.metadata();
    let file_io = table.file_io();
    let schema = metadata.current_schema().clone();
    let partition_spec = metadata.default_partition_spec().clone();
    let parent_snapshot_id = metadata.current_snapshot_id();
    let snapshot_id = metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .max()
        .unwrap_or(0)
        + 1;
    let sequence_number = metadata.next_sequence_number();
    let manifest_writer = ManifestWriterBuilder::new(
        file_io
            .new_output(format!(
                "{}/metadata/snap-{snapshot_id}-m0.avro",
                metadata.location()
            ))
            .map_err(df_err)?,
        Some(snapshot_id),
        schema.clone(),
        partition_spec.as_ref().clone(),
    );
    let (mut manifest_writer, operation) = match content {
        DataContentType::Data => (manifest_writer.build_v2_data(), Operation::Append),
        DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
            (manifest_writer.build_v2_deletes(), Operation::Delete)
        }
    };
    let mut summary = SnapshotSummaryCollector::default();
    for data_file in data_files {
        manifest_writer
            .add_file(data_file.clone(), sequence_number)
            .map_err(df_err)?;
        summary.add_file(data_file, schema.clone(), partition_spec.clone());
    }
    let mut manifests = vec![
        manifest_writer
            .write_manifest_file()
            .await
            .map_err(df_err)?,
    ];
    if let Some(parent) = metadata.current_snapshot() {
        let parent_manifests = table
            .manifest_list_reader(parent)
            .load()
            .await
            .map_err(df_err)?;
        manifests.extend(parent_manifests.consume_entries());
    }

    let manifest_list = format!("{}/metadata/snap-{snapshot_id}.avro", metadata.location());
    let mut manifest_list_writer = ManifestListWriter::v2(
        file_io
            .new_output(&manifest_list)
            .map_err(df_err)?
            .writer()
            .await
            .map_err(df_err)?,
        snapshot_id,
        parent_snapshot_id,
        sequence_number,
    );
    manifest_list_writer
        .add_manifests(manifests.into_iter())
        .map_err(df_err)?;
    manifest_list_writer.close().await.map_err(df_err)?;

    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent_snapshot_id)
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(now_ms())
        .with_manifest_list(manifest_list)
        .with_summary(Summary {
            operation,
            additional_properties: with_totals(
                summary.build(),
                metadata.current_snapshot().map(|parent| parent.summary()),
            ),
        })
        .with_schema_id(schema.schema_id())
        .build();
    let metadata = metadata
        .clone()
        .into_builder(Some(fixture.metadata_location()?.to_string()))
        .set_branch_snapshot(snapshot, MAIN_BRANCH)
        .map_err(df_err)?
        .build()
        .map_err(df_err)?
        .metadata;
    fixture.commit(metadata).await?;
    Ok(())
}

/// The `total-*` summary properties paired with the `added-*` ones they accumulate.
const TOTALS: [(&str, &str); 6] = [
    ("total-data-files", "added-data-files"),
    ("total-delete-files", "added-delete-files"),
    ("total-records", "added-records"),
    ("total-files-size", "added-files-size"),
    ("total-position-deletes", "added-position-deletes"),
    ("total-equality-deletes", "added-equality-deletes"),
];

/// Adds the table totals to a commit's summary `properties`: the `parent` snapshot's
/// totals plus what the commit adds.
///
/// iceberg-rust's transactions add these, but `SnapshotSummaryCollector` doesn't and the
/// function that does isn't public. Readers use them as table statistics, e.g. for the
/// row count and size of a scan.
fn with_totals(
    mut properties: HashMap<String, String>,
    parent: Option<&Summary>,
) -> HashMap<String, String> {
    fn value(properties: &HashMap<String, String>, key: &str) -> u64 {
        properties
            .get(key)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }
    for (total, added) in TOTALS {
        let previous = parent.map_or(0, |parent| value(&parent.additional_properties, total));
        let total_value = previous + value(&properties, added);
        properties.insert(total.to_string(), total_value.to_string());
    }
    properties
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}
