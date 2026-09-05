use iceberg::spec::{Snapshot, Summary, TableMetadata, TableMetadataBuilder};

pub const TAXI_SNAPSHOT_ID: i64 = 3_167_948_105_555_765_929;
pub const TAXI_MANIFEST_LIST: &str = "s3://iceberg-test/warehouse/taxi/metadata/snap-3167948105555765929-0-019fdb82-eb66-7582-99a7-9f864b92a53f.avro";

/// The complete, checked-in taxi metadata, including its original summary.
pub fn taxi_metadata() -> TableMetadata {
    serde_json::from_str(include_str!(
        "../../../testdata/iceberg/taxi/metadata/v1.metadata.json"
    ))
    .expect("taxi metadata is valid JSON")
}

/// Starts a fixture history with the original schema and partition IDs and no snapshots.
pub fn taxi_metadata_builder() -> TableMetadataBuilder {
    let metadata = serde_json::from_str(include_str!(
        "../../../testdata/iceberg/taxi/metadata/00000-00a113a6-47e0-4c4b-9522-4a7c44d74036.metadata.json"
    ))
    .expect("initial taxi metadata is valid JSON");
    TableMetadataBuilder::new_from_metadata(metadata, None)
}

/// The taxi snapshot with an explicit summary. The manifest and data files stay fixed.
pub fn taxi_snapshot(summary: Summary) -> Snapshot {
    Snapshot::builder()
        .with_snapshot_id(TAXI_SNAPSHOT_ID)
        .with_sequence_number(1)
        .with_timestamp_ms(1_786_094_218_149)
        .with_manifest_list(TAXI_MANIFEST_LIST)
        .with_summary(summary)
        .with_schema_id(0)
        .build()
}
