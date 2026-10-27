#[cfg(all(test, feature = "integration"))]
mod tests {
    use std::error::Error;
    use std::fmt::Write;
    use std::sync::Arc;

    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::source::{DataSource, DataSourceExec};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_distributed::{DistributedExt, DistributedLeafExec};
    use datafusion_distributed_iceberg::EagerIcebergDataSource;
    use datafusion_distributed_iceberg::test_utils::{
        FIXTURE_URI, IcebergTestHarness, IcebergTestHarnessBuilder, taxi_metadata,
    };
    use iceberg::io::{MemoryStorage, Storage};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestListWriter,
        ManifestWriterBuilder, Struct,
    };

    #[tokio::test]
    async fn largest_files_balance_tasks_independently_of_manifest_order()
    -> Result<(), Box<dyn Error>> {
        let files: Vec<_> = [100, 90, 10, 5, 4, 3].into_iter().enumerate().collect();
        for order in [files.clone(), files.into_iter().rev().collect()] {
            let harness = file_fixture(order)
                .await?
                .with_eager_file_planning()
                .with_workers(2)
                .configure_session(|mut state| {
                    let session = state.config().get_or_insert_default();
                    session.options_mut().execution.target_partitions = 2;
                    state.with_distributed_file_scan_config_bytes_per_partition(1)
                })?
                .build()
                .await?;
            let plan = harness.physical_plan("SELECT * FROM taxi").await?;
            insta::assert_snapshot!("distributed_skew", assignments(&plan)?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn equal_size_files_have_stable_assignments() -> Result<(), Box<dyn Error>> {
        let files: Vec<_> = [10; 4].into_iter().enumerate().collect();
        for order in [files.clone(), files.into_iter().rev().collect()] {
            let harness = file_fixture(order)
                .await?
                .with_eager_file_planning()
                .with_workers(2)
                .configure_session(|mut state| {
                    let session = state.config().get_or_insert_default();
                    session.options_mut().execution.target_partitions = 2;
                    state.with_distributed_file_scan_config_bytes_per_partition(1)
                })?
                .build()
                .await?;
            let plan = harness.physical_plan("SELECT * FROM taxi").await?;
            insta::assert_snapshot!("equal_size_files", assignments(&plan)?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn local_assignment_cycles_partitions_in_size_order() -> Result<(), Box<dyn Error>> {
        let harness = file_fixture([100, 90, 10, 5, 4, 3].into_iter().enumerate().rev())
            .await?
            .with_eager_file_planning()
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                Ok(state)
            })?
            .build()
            .await?;
        insta::assert_snapshot!("local_skew", assignments(&harness.scan().await?)?);
        Ok(())
    }

    #[tokio::test]
    async fn empty_scan_keeps_local_partitions() -> Result<(), Box<dyn Error>> {
        let harness = file_fixture([])
            .await?
            .with_eager_file_planning()
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                Ok(state)
            })?
            .build()
            .await?;
        insta::assert_snapshot!("empty_files", assignments(&harness.scan().await?)?);
        Ok(())
    }

    // Inspect public source assignments in the physical plan without opening data files.
    fn assignments(plan: &Arc<dyn ExecutionPlan>) -> Result<String, Box<dyn Error>> {
        let mut scans = Vec::new();
        plan.apply(|node| {
            if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() {
                scans.extend(leaf.variants().iter().cloned());
            } else if let Some(exec) = node.downcast_ref::<DataSourceExec>()
                && exec.data_source().is::<EagerIcebergDataSource>()
            {
                scans.push(Arc::clone(node));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(!scans.is_empty(), "expected assigned Iceberg scans");
        let mut output = String::new();
        for (task, scan) in scans.iter().enumerate() {
            let source = scan
                .downcast_ref::<DataSourceExec>()
                .unwrap()
                .data_source()
                .downcast_ref::<EagerIcebergDataSource>()
                .unwrap();
            let bytes: u64 = source.planned_files().map(|file| file.length()).sum();
            writeln!(output, "task {task}: {bytes} bytes")?;
            for partition in 0..source.output_partitioning().partition_count() {
                let files = source
                    .planned_partition_tasks(partition)?
                    .map(|file| {
                        format!(
                            "{} ({} bytes)",
                            file.data_file_path().rsplit('/').next().unwrap(),
                            file.length()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                writeln!(output, "  partition {partition}: [{files}]")?;
            }
        }
        Ok(output)
    }

    // Only manifest metadata is needed: these synthetic data files are never opened.
    async fn file_fixture(
        files: impl IntoIterator<Item = (usize, u64)>,
    ) -> Result<IcebergTestHarnessBuilder, Box<dyn Error>> {
        let metadata = taxi_metadata();
        let snapshot = metadata.current_snapshot().unwrap();
        let storage = MemoryStorage::new();
        let uri = format!("{FIXTURE_URI}/metadata/skewed-files.avro");
        let mut writer = ManifestWriterBuilder::new(
            storage.new_output(&uri)?,
            Some(snapshot.snapshot_id()),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        for (index, bytes) in files {
            writer.add_file(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_format(DataFileFormat::Parquet)
                    .record_count(1)
                    .file_size_in_bytes(bytes)
                    .partition(Struct::from_iter([Some(Literal::date_from_str(
                        "2024-01-10",
                    )?)]))
                    .file_path(format!("{FIXTURE_URI}/data/file-{index}.parquet"))
                    .build()?,
                snapshot.sequence_number(),
            )?;
        }
        let manifest = writer.write_manifest_file().await?;
        let mut list = ManifestListWriter::v2(
            storage
                .new_output(snapshot.manifest_list())?
                .writer()
                .await?,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        );
        list.add_manifests([manifest].into_iter())?;
        list.close().await?;
        Ok(IcebergTestHarness::builder()
            .with_file(&uri, storage.read(&uri).await?.to_vec())
            .with_file(
                snapshot.manifest_list(),
                storage.read(snapshot.manifest_list()).await?.to_vec(),
            )
            .with_table_metadata(metadata))
    }
}
