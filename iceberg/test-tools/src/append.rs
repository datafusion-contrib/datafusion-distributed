use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::arrow::array::{RecordBatch, new_null_array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::{exec_err, not_impl_err};
use datafusion::error::Result;
use datafusion::parquet::file::properties::WriterProperties;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestListWriter,
    ManifestWriterBuilder, Operation, Snapshot, SnapshotSummaryCollector, Summary,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};

use crate::FixtureTable;
use crate::error::df_err;

/// One commit that adds data files, started by [`FixtureTable::append`].
///
/// Each [`Append::file`] becomes exactly one Parquet data file, so tests
/// control how rows are laid out across files. [`Append::commit`] adds them
/// all in one snapshot on top of the current one. Snapshots are numbered 1,
/// 2, 3, ..., unlike iceberg-rust's `fast_append`, which picks random ids, so
/// that tests can show snapshot ids. Only adds data files: no deletes, and
/// format version 2 only.
pub struct Append<'a> {
    table: &'a mut FixtureTable,
    writer_properties: WriterProperties,
    files: Vec<RecordBatch>,
}

impl<'a> Append<'a> {
    pub(crate) fn new(table: &'a mut FixtureTable) -> Self {
        Self {
            table,
            writer_properties: WriterProperties::default(),
            files: Vec::new(),
        }
    }

    /// Sets the properties every data file of this commit is written with.
    pub fn writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = writer_properties;
        self
    }

    /// Adds one data file containing `batch`.
    ///
    /// Columns are matched to the table's by name and cast to its types.
    /// Columns of the table that `batch` lacks are null. On a partitioned
    /// table every row must belong to the same partition.
    pub fn file(mut self, batch: RecordBatch) -> Self {
        self.files.push(batch);
        self
    }

    /// Writes the data files and commits them as a new snapshot of the main
    /// branch. Returns the data files in the order they were added.
    pub async fn commit(self) -> Result<Vec<DataFile>> {
        let table = self.table.table();
        let metadata = table.metadata();
        if metadata.format_version() != FormatVersion::V2 {
            return not_impl_err!("fixtures only append to format version 2 tables");
        }
        let file_io = table.file_io();
        let schema = metadata.current_schema().clone();
        let partition_spec = metadata.default_partition_spec().clone();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).map_err(df_err)?);
        let parent_snapshot_id = metadata.current_snapshot_id();
        let snapshot_id = metadata
            .snapshots()
            .map(|s| s.snapshot_id())
            .max()
            .unwrap_or(0)
            + 1;
        let sequence_number = metadata.next_sequence_number();

        let splitter = if partition_spec.is_unpartitioned() {
            None
        } else {
            Some(
                RecordBatchPartitionSplitter::try_new_with_computed_values(
                    schema.clone(),
                    partition_spec.clone(),
                )
                .map_err(df_err)?,
            )
        };
        let writer_builder = DataFileWriterBuilder::new(RollingFileWriterBuilder::new(
            ParquetWriterBuilder::new(self.writer_properties, schema.clone()),
            // Never roll over, so that each batch is written to one file.
            usize::MAX,
            file_io.clone(),
            DefaultLocationGenerator::new(metadata).map_err(df_err)?,
            DefaultFileNameGenerator::new(format!("{snapshot_id}"), None, DataFileFormat::Parquet),
        ));
        let mut data_files = Vec::with_capacity(self.files.len());
        for batch in &self.files {
            let batch = conform(batch, &arrow_schema)?;
            let partition_key = match &splitter {
                None => None,
                Some(splitter) => {
                    let mut partitions = splitter.split(&batch).map_err(df_err)?;
                    if partitions.len() != 1 {
                        return exec_err!(
                            "the rows of a data file must belong to one partition, \
                             but they belong to {}",
                            partitions.len()
                        );
                    }
                    Some(partitions.remove(0).0)
                }
            };
            let mut writer = writer_builder.build(partition_key).await.map_err(df_err)?;
            writer.write(batch).await.map_err(df_err)?;
            data_files.extend(writer.close().await.map_err(df_err)?);
        }

        let mut manifest_writer = ManifestWriterBuilder::new(
            file_io
                .new_output(format!(
                    "{}/metadata/snap-{snapshot_id}-m0.avro",
                    metadata.location()
                ))
                .map_err(df_err)?,
            Some(snapshot_id),
            schema.clone(),
            partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        let mut summary = SnapshotSummaryCollector::default();
        for data_file in &data_files {
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
                operation: Operation::Append,
                additional_properties: with_totals(
                    summary.build(),
                    metadata.current_snapshot().map(|parent| parent.summary()),
                ),
            })
            .with_schema_id(schema.schema_id())
            .build();
        let metadata = metadata
            .clone()
            .into_builder(Some(self.table.metadata_location()?.to_string()))
            .set_branch_snapshot(snapshot, MAIN_BRANCH)
            .map_err(df_err)?
            .build()
            .map_err(df_err)?
            .metadata;
        self.table.commit(metadata).await?;
        Ok(data_files)
    }
}

/// Returns `batch` laid out as `schema`: its columns matched by name, put in
/// schema order, cast to the schema's types and tagged with its field ids.
/// Missing columns are null.
///
/// The partition splitter and the Parquet encoder read columns by position,
/// and the writer matches them to the table's by field id, so batches must
/// have exactly this layout. This lets tests pass any batch instead.
fn conform(batch: &RecordBatch, schema: &ArrowSchemaRef) -> Result<RecordBatch> {
    if let Some(extra) = batch
        .schema()
        .fields()
        .iter()
        .find(|field| schema.field_with_name(field.name()).is_err())
    {
        return exec_err!("the table has no column {}", extra.name());
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| match batch.column_by_name(field.name()) {
            Some(column) => cast(column, field.data_type()),
            None => Ok(new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{AsArray, Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
    use datafusion::error::{DataFusionError, Result};
    use futures::TryStreamExt;
    use iceberg::TableIdent;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type};

    use crate::error::df_err;
    use crate::{FixtureCatalog, FixtureTable};

    async fn create_table(fixtures: &FixtureCatalog) -> Result<FixtureTable> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .map_err(df_err)?;
        fixtures
            .table(TableIdent::from_strs(["ns", "t"]).map_err(df_err)?, schema)
            .partition_by("category", Transform::Identity)
            .create()
            .await
    }

    fn batch(ids: Vec<i32>, categories: Vec<&str>) -> RecordBatch {
        let schema = ArrowSchema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("id", DataType::Int32, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(categories)),
                Arc::new(Int32Array::from(ids)),
            ],
        )
        .unwrap()
    }

    async fn scanned_ids(table: &FixtureTable) -> Result<Vec<i32>> {
        let batches: Vec<_> = table
            .table()
            .scan()
            .select(["id"])
            .build()
            .map_err(df_err)?
            .to_arrow()
            .await
            .map_err(df_err)?
            .try_collect()
            .await
            .map_err(df_err)?;
        let mut ids: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }

    #[tokio::test]
    async fn numbers_snapshots_and_keeps_earlier_files() -> Result<()> {
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = create_table(&fixtures).await?;
        let files = table
            .append()
            .file(batch(vec![1, 2], vec!["a", "a"]))
            .file(batch(vec![3], vec!["b"]))
            .commit()
            .await?;
        assert_eq!(files.len(), 2);
        assert!(files[0].file_path().contains("/data/category=a/1-"));
        table
            .append()
            .file(batch(vec![4], vec!["a"]))
            .commit()
            .await?;

        let metadata = table.table().metadata();
        assert_eq!(metadata.current_snapshot_id(), Some(2));
        assert_eq!(
            metadata.snapshot_by_id(2).unwrap().parent_snapshot_id(),
            Some(1)
        );
        assert_eq!(scanned_ids(&table).await?, vec![1, 2, 3, 4]);
        let summary = &metadata
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;
        assert_eq!(summary["total-records"], "4");
        assert_eq!(summary["total-data-files"], "3");

        // The catalog serves the committed table.
        let loaded = fixtures
            .catalog()
            .load_table(table.ident())
            .await
            .map_err(df_err)?;
        assert_eq!(loaded.metadata_location(), Some(table.metadata_location()?));
        Ok(())
    }

    #[tokio::test]
    async fn rejects_a_file_spanning_partitions() -> Result<()> {
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = create_table(&fixtures).await?;
        let error = table
            .append()
            .file(batch(vec![1, 2], vec!["a", "b"]))
            .commit()
            .await
            .unwrap_err();
        assert!(matches!(error, DataFusionError::Execution(_)), "{error}");
        assert!(table.table().metadata().current_snapshot().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn matches_columns_by_name() -> Result<()> {
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = create_table(&fixtures).await?;
        let unknown =
            RecordBatch::try_from_iter([("nope", Arc::new(Int32Array::from(vec![1])) as _)])?;
        let error = table.append().file(unknown).commit().await.unwrap_err();
        assert!(
            error.to_string().contains("the table has no column nope"),
            "{error}"
        );

        // A missing optional column is null, so the file is in the null partition.
        let ids_only =
            RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![7])) as _)])?;
        let files = table.append().file(ids_only).commit().await?;
        assert!(
            files[0].file_path().contains("/category=null/"),
            "{}",
            files[0].file_path()
        );
        assert_eq!(scanned_ids(&table).await?, vec![7]);
        Ok(())
    }
}
