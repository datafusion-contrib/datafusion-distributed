mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion_distributed::DistributedExt;
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg_test_tools::{
        FixtureCatalog, FixtureTable, IcebergTestHarness,
    };
    use futures::TryStreamExt;
    use iceberg::TableIdent;
    use iceberg::spec::{DataFile, NestedField, PrimitiveType, Schema, Transform, Type};

    #[tokio::test]
    async fn partitions_share_loaded_position_deletes() -> Result<(), Box<dyn Error>> {
        let (mut table, files) = two_files([1, 2], [3, 4]).await?;
        let deletes = table
            .position_deletes()
            .file([(&files[0], 0), (&files[1], 0)])
            .commit()
            .await?;
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("datafusion.execution.target_partitions", 2)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let context = harness.state().task_ctx();
        let mut batches: Vec<RecordBatch> =
            plan.execute(0, Arc::clone(&context))?.try_collect().await?;
        // The next partition must apply its own deletion from the shared cache.
        harness
            .table()
            .table()
            .file_io()
            .delete(deletes[0].file_path())
            .await?;
        batches.extend(plan.execute(1, context)?.try_collect::<Vec<_>>().await?);
        let mut values: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        values.sort_unstable();
        assert_eq!(values, vec![2, 4]);
        Ok(())
    }

    #[tokio::test]
    async fn lazy_scan_applies_committed_position_deletes() -> Result<(), Box<dyn Error>> {
        let (mut table, files) = two_files([1, 2], [3, 4]).await?;
        table
            .position_deletes()
            .file([(&files[1], 0), (&files[0], 0), (&files[0], 0)])
            .commit()
            .await?;
        let (_, rows) = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table)
            .with_config_option("iceberg.eager_file_planning", false)
            .query("SELECT value FROM items ORDER BY value")
            .await?;
        insta::assert_snapshot!(rows, @"
        +-------+
        | value |
        +-------+
        | 2     |
        | 4     |
        +-------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn distributed_eager_scan_applies_deletes_across_commits() -> Result<(), Box<dyn Error>> {
        let (mut table, files) = two_files([1, 2], [3, 4]).await?;
        table
            .position_deletes()
            .file([(&files[0], 0)])
            .commit()
            .await?;
        table
            .position_deletes()
            .file([(&files[1], 0)])
            .commit()
            .await?;
        table.append().file(batch(vec![5, 6])?).commit().await?;
        let (_, rows) = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_workers(2)
            .with_config_option("datafusion.execution.target_partitions", 1)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .query("SELECT value FROM items ORDER BY value")
            .await?;
        insta::assert_snapshot!(rows, @"
        +-------+
        | value |
        +-------+
        | 2     |
        | 4     |
        | 5     |
        | 6     |
        +-------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn invalid_position_does_not_commit_a_snapshot() -> Result<(), Box<dyn Error>> {
        let (mut table, files) = two_files([1, 2], [3, 4]).await?;
        let snapshot = table.table().metadata().current_snapshot_id();
        let error = table
            .position_deletes()
            .file([(&files[0], 2)])
            .commit()
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("position 2 is outside data file")
        );
        assert_eq!(table.table().metadata().current_snapshot_id(), snapshot);
        Ok(())
    }

    #[tokio::test]
    async fn partitioned_scan_applies_position_deletes() -> Result<(), Box<dyn Error>> {
        let fixtures = FixtureCatalog::try_new().await?;
        let schema = value_schema()?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "items"])?, schema)
            .partition_by("value", Transform::Identity)
            .create()
            .await?;
        let files = table
            .append()
            .file(batch(vec![1, 1])?)
            .file(batch(vec![2, 2])?)
            .commit()
            .await?;
        let error = table
            .position_deletes()
            .file([(&files[0], 0), (&files[1], 0)])
            .commit()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("one partition"));
        table
            .position_deletes()
            .file([(&files[0], 0)])
            .file([(&files[1], 1)])
            .commit()
            .await?;
        let (_, rows) = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table)
            .with_config_option("iceberg.eager_file_planning", true)
            .query("SELECT value FROM items WHERE value = 1")
            .await?;
        insta::assert_snapshot!(rows, @"
        +-------+
        | value |
        +-------+
        | 1     |
        +-------+
        ");
        Ok(())
    }

    async fn two_files(
        first: [i64; 2],
        second: [i64; 2],
    ) -> Result<(FixtureTable, Vec<DataFile>), Box<dyn Error>> {
        let fixtures = FixtureCatalog::try_new().await?;
        let schema = value_schema()?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "items"])?, schema)
            .create()
            .await?;
        let files = table
            .append()
            .file(batch(first.to_vec())?)
            .file(batch(second.to_vec())?)
            .commit()
            .await?;
        Ok((table, files))
    }

    fn value_schema() -> iceberg::Result<Schema> {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
    }

    fn batch(values: Vec<i64>) -> datafusion::error::Result<RecordBatch> {
        Ok(RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(values)) as _,
        )])?)
    }
}
