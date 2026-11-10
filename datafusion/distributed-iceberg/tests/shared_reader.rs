#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::source::DataSource;
    use datafusion::execution::TaskContext;
    use datafusion_distributed_iceberg::{EagerIcebergDataSource, iceberg};
    use futures::TryStreamExt;
    use iceberg::arrow::arrow_schema_to_schema;
    use iceberg::io::FileIO;
    use iceberg::metadata_columns::{
        RESERVED_COL_NAME_DELETE_FILE_PATH, RESERVED_COL_NAME_DELETE_FILE_POS,
        RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
    };
    use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
    use iceberg::spec::{DataContentType, DataFileFormat};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use tempfile::TempDir;

    #[tokio::test]
    async fn partitions_share_loaded_position_deletes() -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let fixture = files_with_shared_deletes(&directory)?;
        let context = Arc::new(TaskContext::default());
        let source = EagerIcebergDataSource::from_file_groups(
            fixture.schema,
            fixture.groups,
            FileIO::new_with_fs(),
            iceberg::Runtime::current(),
            None,
            &context,
        )?;
        let mut batches: Vec<RecordBatch> =
            source.open(0, Arc::clone(&context))?.try_collect().await?;

        // The second partition must use the cached deletes, including its own
        // row's deletion, rather than reopening the shared delete file.
        fs::remove_file(fixture.delete_path)?;
        batches.extend(source.open(1, context)?.try_collect::<Vec<_>>().await?);
        insta::assert_snapshot!(pretty_format_batches(&batches)?.to_string(), @"
        +-------+
        | value |
        +-------+
        | 2     |
        | 4     |
        +-------+
        ");
        Ok(())
    }

    struct DeleteFixture {
        schema: SchemaRef,
        groups: Vec<Vec<FileScanTask>>,
        delete_path: String,
    }

    /// Two data files, each with its first row removed by one shared delete file.
    fn files_with_shared_deletes(directory: &TempDir) -> Result<DeleteFixture, Box<dyn Error>> {
        let schema = Arc::new(Schema::new(vec![field("value", DataType::Int64, 1)]));
        let iceberg_schema = Arc::new(arrow_schema_to_schema(&schema)?);
        let paths = ["first.parquet", "second.parquet"]
            .map(|name| directory.path().join(name).to_str().unwrap().to_owned());
        for (path, values) in paths.iter().zip([[1, 2], [3, 4]]) {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values.to_vec()))],
            )?;
            write_parquet(path, &batch)?;
        }
        let delete_schema = Arc::new(Schema::new(vec![
            field(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field(
                RESERVED_COL_NAME_DELETE_FILE_POS,
                DataType::Int64,
                RESERVED_FIELD_ID_DELETE_FILE_POS,
            ),
        ]));
        let deletes = RecordBatch::try_new(
            delete_schema,
            vec![
                Arc::new(StringArray::from(
                    paths.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vec![0, 0])),
            ],
        )?;
        let delete_path = directory
            .path()
            .join("deletes.parquet")
            .to_str()
            .unwrap()
            .to_owned();
        write_parquet(&delete_path, &deletes)?;
        let delete = FileScanTaskDeleteFile::builder()
            .with_file_path(delete_path.clone())
            .with_file_size_in_bytes(fs::metadata(&delete_path)?.len())
            .with_file_type(DataContentType::PositionDeletes)
            .with_file_format(DataFileFormat::Parquet)
            .with_partition_spec_id(0)
            .build()?;
        let mut groups = Vec::new();
        for path in paths {
            let size = fs::metadata(&path)?.len();
            groups.push(vec![
                FileScanTask::builder()
                    .with_file_size_in_bytes(size)
                    .with_start(0)
                    .with_length(size)
                    .with_record_count(Some(2))
                    .with_data_file_path(path)
                    .with_data_file_format(DataFileFormat::Parquet)
                    .with_schema(Arc::clone(&iceberg_schema))
                    .with_project_field_ids(vec![1])
                    .with_deletes(vec![delete.clone()])
                    .with_case_sensitive(true)
                    .build()?,
            ]);
        }
        Ok(DeleteFixture {
            schema,
            groups,
            delete_path,
        })
    }

    fn field(name: &str, data_type: DataType, id: i32) -> Field {
        Field::new(name, data_type, false).with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.to_owned(), id.to_string())]
                .into_iter()
                .collect(),
        )
    }

    fn write_parquet(path: &str, batch: &RecordBatch) -> Result<(), Box<dyn Error>> {
        let mut writer = ArrowWriter::try_new(fs::File::create(path)?, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }
}
