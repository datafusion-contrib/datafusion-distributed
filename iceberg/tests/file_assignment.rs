mod tests {
    use std::error::Error;
    use std::fmt::Write;
    use std::sync::Arc;

    use datafusion::arrow::array::record_batch;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::source::{DataSource, DataSourceExec};
    use datafusion::physical_plan::ExecutionPlan;
    // `record_batch!` refers to `arrow_schema` by name.
    use datafusion::arrow::datatypes as arrow_schema;
    use datafusion_distributed::{DistributedExt, DistributedLeafExec};
    use datafusion_distributed_iceberg::{EagerIcebergDataSource, IcebergExt};
    use datafusion_distributed_iceberg_test_tools::{
        FixtureCatalog, FixtureTable, IcebergTestHarness,
    };
    use iceberg::TableIdent;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    #[tokio::test]
    async fn largest_files_balance_tasks() -> Result<(), Box<dyn Error>> {
        // Each inner array is one commit, with one file per row count.
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table_of_files([[100_000, 90_000, 10_000, 5_000, 4_000, 3_000]]).await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_workers(2)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let plan = harness.physical_plan("SELECT * FROM files").await?;
        insta::assert_snapshot!(assignments(&plan)?, @r"
        task 0: 1071294 bytes
          partition 0: [1-00000.parquet (1003586 bytes), 1-00005.parquet (29103 bytes)]
          partition 1: [1-00004.parquet (38605 bytes)]
        task 1: 1049176 bytes
          partition 0: [1-00001.parquet (902316 bytes), 1-00003.parquet (48732 bytes)]
          partition 1: [1-00002.parquet (98128 bytes)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn reversed_manifest_order_preserves_task_balance() -> Result<(), Box<dyn Error>> {
        // Each inner array is one commit, with one file per row count.
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table_of_files([[3_000, 4_000, 5_000, 10_000, 90_000, 100_000]]).await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_workers(2)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let plan = harness.physical_plan("SELECT * FROM files").await?;
        insta::assert_snapshot!(assignments(&plan)?, @r"
        task 0: 1071294 bytes
          partition 0: [1-00005.parquet (1003586 bytes), 1-00000.parquet (29103 bytes)]
          partition 1: [1-00001.parquet (38605 bytes)]
        task 1: 1049176 bytes
          partition 0: [1-00004.parquet (902316 bytes), 1-00002.parquet (48732 bytes)]
          partition 1: [1-00003.parquet (98128 bytes)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn equal_size_files_have_stable_assignments() -> Result<(), Box<dyn Error>> {
        // Each inner array is one commit, with one file per row count.
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table_of_files([[10_000; 4]]).await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_workers(2)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let plan = harness.physical_plan("SELECT * FROM files").await?;
        insta::assert_snapshot!(assignments(&plan)?, @r"
        task 0: 196256 bytes
          partition 0: [1-00000.parquet (98128 bytes)]
          partition 1: [1-00002.parquet (98128 bytes)]
        task 1: 196256 bytes
          partition 0: [1-00001.parquet (98128 bytes)]
          partition 1: [1-00003.parquet (98128 bytes)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn reversed_equal_size_files_keep_the_same_assignments() -> Result<(), Box<dyn Error>> {
        // Each inner array is one commit, with one file per row count. The manifest list
        // starts with the newest commit, so these files are listed in reverse path order.
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table_of_files([[10_000], [10_000], [10_000], [10_000]]).await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_workers(2)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let plan = harness.physical_plan("SELECT * FROM files").await?;
        insta::assert_snapshot!(assignments(&plan)?, @r"
        task 0: 196256 bytes
          partition 0: [1-00000.parquet (98128 bytes)]
          partition 1: [3-00000.parquet (98128 bytes)]
        task 1: 196256 bytes
          partition 0: [2-00000.parquet (98128 bytes)]
          partition 1: [4-00000.parquet (98128 bytes)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn local_assignment_cycles_partitions_in_size_order() -> Result<(), Box<dyn Error>> {
        // Each inner array is one commit, with one file per row count.
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table_of_files([[3_000, 4_000, 5_000, 10_000, 90_000, 100_000]]).await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                Ok(state)
            })?
            .build()
            .await?;
        let plan = harness.scan().await?;
        insta::assert_snapshot!(assignments(&plan)?, @r"
        task 0: 2120470 bytes
          partition 0: [1-00005.parquet (1003586 bytes), 1-00003.parquet (98128 bytes), 1-00001.parquet (38605 bytes)]
          partition 1: [1-00004.parquet (902316 bytes), 1-00002.parquet (48732 bytes), 1-00000.parquet (29103 bytes)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn empty_scan_keeps_local_partitions() -> Result<(), Box<dyn Error>> {
        // Each inner array is one commit, with one file per row count.
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table_of_files([[]]).await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                Ok(state)
            })?
            .build()
            .await?;
        let plan = harness.scan().await?;
        insta::assert_snapshot!(assignments(&plan)?, @"
        task 0: 0 bytes
          partition 0: []
          partition 1: []
        ");
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

    // A table with one snapshot per entry of `commits`, each adding one data file per row
    // count, in order. The manifest list starts with the newest commit's manifest. Files with
    // the same row count have the same contents, and so the same size.
    async fn table_of_files<const COMMITS: usize, const FILES: usize>(
        commits: [[usize; FILES]; COMMITS],
    ) -> Result<FixtureTable, Box<dyn Error>> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()?;
        let fixtures = FixtureCatalog::try_new().await?;
        let mut table = fixtures
            .table(TableIdent::from_strs(["default", "files"])?, schema)
            .create()
            .await?;
        for rows_per_file in commits {
            let mut append = table.append();
            for rows in rows_per_file {
                let ids: Vec<i64> = (0..rows as i64).collect();
                append = append.file(record_batch!(("id", Int64, ids))?);
            }
            append.commit().await?;
        }
        Ok(table)
    }
}
