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
        FIXTURE_URI, IcebergTestHarness, taxi_metadata,
    };
    use iceberg::io::{MemoryStorage, Storage};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestListWriter,
        ManifestWriterBuilder, Struct,
    };

    #[tokio::test]
    async fn largest_files_balance_tasks_independently_of_manifest_order()
    -> Result<(), Box<dyn Error>> {
        for reverse in [false, true] {
            let harness = file_harness(&[100, 90, 10, 5, 4, 3], reverse, true).await?;
            let plan = harness.physical_plan("SELECT * FROM taxi").await?;
            insta::assert_snapshot!("distributed_skew", assignments(&plan)?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn equal_size_files_have_stable_assignments() -> Result<(), Box<dyn Error>> {
        for reverse in [false, true] {
            let harness = file_harness(&[10; 4], reverse, true).await?;
            let plan = harness.physical_plan("SELECT * FROM taxi").await?;
            insta::assert_snapshot!("equal_size_files", assignments(&plan)?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn local_assignment_uses_the_same_file_order_and_keeps_empty_partitions()
    -> Result<(), Box<dyn Error>> {
        let harness = file_harness(&[100, 90, 10, 5, 4, 3], true, false).await?;
        insta::assert_snapshot!("local_skew", assignments(&harness.scan().await?)?);
        let empty = file_harness(&[], false, false).await?;
        insta::assert_snapshot!("empty_files", assignments(&empty.scan().await?)?);
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
                writeln!(output, "task {task}, partition {partition}: [{files}]")?;
            }
        }
        Ok(output)
    }

    async fn file_harness(
        sizes: &[u64],
        reverse: bool,
        distributed: bool,
    ) -> Result<IcebergTestHarness, Box<dyn Error>> {
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
        let mut files: Vec<_> = sizes.iter().copied().enumerate().collect();
        if reverse {
            files.reverse();
        }
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
        let harness = IcebergTestHarness::builder()
            .with_file(&uri, storage.read(&uri).await?.to_vec())
            .with_file(
                snapshot.manifest_list(),
                storage.read(snapshot.manifest_list()).await?.to_vec(),
            )
            .with_table_metadata(metadata)
            .configure_iceberg(|config| config.eager_file_planning = true)
            .configure_session(|mut state| {
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .execution
                    .target_partitions = 2;
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?;
        let harness = if distributed {
            harness.with_workers(2)
        } else {
            harness
        };
        let harness = harness.build().await?;
        Ok(harness)
    }
}
