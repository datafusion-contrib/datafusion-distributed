use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::common::{exec_err, not_impl_err};
use datafusion::error::Result;
use datafusion::parquet::file::properties::WriterProperties;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataContentType, DataFile, DataFileFormat, FormatVersion, PartitionKey};
use iceberg::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, position_delete_schema,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};

use crate::FixtureTable;
use crate::error::df_err;
use crate::snapshot::commit_files;

/// A version 2 position-delete commit, started by [`FixtureTable::position_deletes`].
/// Each [`Self::file`] writes one delete file. A delete file can reference multiple
/// data files in the same partition. The commit retains existing data and deletes.
pub struct PositionDeletes<'a> {
    table: &'a mut FixtureTable,
    files: Vec<Vec<(DataFile, u64)>>,
}

impl<'a> PositionDeletes<'a> {
    pub(crate) fn new(table: &'a mut FixtureTable) -> Self {
        Self {
            table,
            files: Vec::new(),
        }
    }

    /// Adds a delete file containing zero-based row positions in data files returned
    /// by [`crate::Append::commit`] on this table, using its current partition spec.
    /// Rows are sorted by path and position and deduplicated.
    /// Empty files, out-of-range positions, and mixed partitions are rejected at commit.
    pub fn file<'b>(mut self, rows: impl IntoIterator<Item = (&'b DataFile, u64)>) -> Self {
        self.files.push(
            rows.into_iter()
                .map(|(file, pos)| (file.clone(), pos))
                .collect(),
        );
        self
    }

    /// Writes the delete files and commits a new snapshot. Returns their descriptors
    /// in insertion order, e.g. to inspect or remove the files in an I/O test.
    pub async fn commit(self) -> Result<Vec<DataFile>> {
        let table = self.table.table();
        let metadata = table.metadata();
        if metadata.format_version() != FormatVersion::V2 {
            return not_impl_err!("position-delete fixtures require format version 2");
        }
        if self.files.is_empty() {
            return exec_err!("a position-delete commit requires at least one file");
        }
        // Validate every group before writing any files.
        for rows in &self.files {
            let Some((first, _)) = rows.first() else {
                return exec_err!("a position-delete file requires at least one row");
            };
            for (file, pos) in rows {
                if file.content_type() != DataContentType::Data
                    || file.partition() != first.partition()
                {
                    return exec_err!(
                        "position deletes must reference data files in one partition"
                    );
                }
                if *pos >= file.record_count() || *pos > i64::MAX as u64 {
                    return exec_err!("position {pos} is outside data file {}", file.file_path());
                }
            }
        }
        let snapshot_id = metadata
            .snapshots()
            .map(|s| s.snapshot_id())
            .max()
            .unwrap_or(0)
            + 1;
        let delete_schema = position_delete_schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&delete_schema).map_err(df_err)?);
        let writer = PositionDeleteFileWriterBuilder::new(RollingFileWriterBuilder::new(
            ParquetWriterBuilder::new(WriterProperties::default(), delete_schema),
            usize::MAX,
            table.file_io().clone(),
            DefaultLocationGenerator::new(metadata).map_err(df_err)?,
            DefaultFileNameGenerator::new(
                format!("{snapshot_id}-delete"),
                None,
                DataFileFormat::Parquet,
            ),
        ));
        let mut delete_files = Vec::new();
        for mut rows in self.files {
            let partition = PartitionKey::new(
                metadata.default_partition_spec().as_ref().clone(),
                metadata.current_schema().clone(),
                rows[0].0.partition().clone(),
            );
            rows.sort_by(|(a, ap), (b, bp)| (a.file_path(), ap).cmp(&(b.file_path(), bp)));
            rows.dedup_by(|(a, ap), (b, bp)| a.file_path() == b.file_path() && ap == bp);
            let batch = RecordBatch::try_new(
                Arc::clone(&arrow_schema),
                vec![
                    Arc::new(StringArray::from(
                        rows.iter()
                            .map(|(file, _)| file.file_path())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(_, pos)| *pos as i64).collect::<Vec<_>>(),
                    )),
                ],
            )?;
            let mut writer = writer.build(Some(partition)).await.map_err(df_err)?;
            writer.write(batch).await.map_err(df_err)?;
            delete_files.extend(writer.close().await.map_err(df_err)?);
        }
        commit_files(self.table, &delete_files, DataContentType::PositionDeletes).await?;
        Ok(delete_files)
    }
}
