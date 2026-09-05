use iceberg::spec::{TableMetadata, TableMetadataBuilder};

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
