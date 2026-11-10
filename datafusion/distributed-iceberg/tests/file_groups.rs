mod tests {
    use std::sync::Arc;

    use datafusion_distributed_iceberg_test_tools::{IcebergTestHarness, data_file_name};

    use datafusion_distributed_iceberg::IcebergExt;

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::datasource::source::{DataSource, DataSourceExec};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed_iceberg::{EagerIcebergDataSource, iceberg};
    use datafusion_distributed_iceberg_test_tools::create_taxi_table;

    #[tokio::test]
    async fn regrouping_a_subset_reads_the_selected_dates() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<EagerIcebergDataSource>()
            .unwrap();
        let subset = source.with_file_groups(vec![vec![1, 4], vec![5]])?;
        let regrouped = subset.with_file_groups(vec![vec![0, 2], vec![], vec![1]])?;
        insta::assert_snapshot!(execute_groups(regrouped).await?);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_groups_in_the_visible_file_set() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<EagerIcebergDataSource>()
            .unwrap();
        let subset = source.with_file_groups(vec![vec![1, 4], vec![5]])?;
        assert!(
            subset.with_file_groups(vec![]).is_err(),
            "no partition groups"
        );
        assert!(
            subset.with_file_groups(vec![vec![3]]).is_err(),
            "position outside the three-file subset"
        );
        assert!(
            subset.with_file_groups(vec![vec![0], vec![0]]).is_err(),
            "position repeated across partitions"
        );
        let empty = subset.with_file_groups(vec![vec![]])?;
        assert_eq!(empty.planned_files().len(), 0);
        assert_eq!(empty.output_partitioning().partition_count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn reconstruction_preserves_groups_and_reads_the_selected_dates() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<EagerIcebergDataSource>()
            .unwrap();
        let selected = source.with_file_groups(vec![vec![5, 1], vec![4]])?;
        let groups = (0..2)
            .map(|p| Ok(selected.planned_partition_tasks(p)?.cloned().collect()))
            .collect::<Result<Vec<_>>>()?;
        let context = TaskContext::default();
        let restored = EagerIcebergDataSource::from_file_groups(
            Arc::clone(source.schema()),
            groups,
            source.file_io().clone(),
            iceberg::Runtime::current(),
            None,
            &context,
        )?;
        insta::assert_snapshot!(execute_groups(restored).await?);
        Ok(())
    }

    #[tokio::test]
    async fn subsets_keep_the_reservation_until_the_last_source_is_dropped() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<EagerIcebergDataSource>()
            .unwrap();
        let context = TaskContext::default();
        let retained = EagerIcebergDataSource::from_file_groups(
            Arc::clone(source.schema()),
            vec![source.planned_files().cloned().collect()],
            source.file_io().clone(),
            iceberg::Runtime::current(),
            None,
            &context,
        )?;
        let reserved = context.memory_pool().reserved();
        assert!(reserved > 0);
        let subset = retained.with_file_groups(vec![vec![0]])?;
        assert_eq!(context.memory_pool().reserved(), reserved);
        drop(retained);
        assert_eq!(context.memory_pool().reserved(), reserved);
        drop(subset);
        assert_eq!(context.memory_pool().reserved(), 0);
        Ok(())
    }

    /// Execute the supplied source and show actual dates/counts alongside its groups.
    async fn execute_groups(source: EagerIcebergDataSource) -> Result<String> {
        let groups = describe_groups(&source)?;
        let ctx = SessionContext::new();
        let batches = collect(DataSourceExec::from_data_source(source), ctx.task_ctx()).await?;
        ctx.register_table("selected", ctx.read_batches(batches)?.into_view())?;
        let counts = ctx.sql(
            "SELECT pickup_date, COUNT(*) AS rows FROM selected GROUP BY pickup_date ORDER BY pickup_date"
        ).await?.collect().await?;
        Ok(format!("{groups}\n\n{}", pretty_format_batches(&counts)?))
    }

    fn describe_groups(source: &EagerIcebergDataSource) -> Result<String> {
        let mut lines = Vec::new();
        for partition in 0..source.output_partitioning().partition_count() {
            let files = source
                .planned_partition_tasks(partition)?
                .map(|file| data_file_name(file.data_file_path()).to_owned())
                .collect::<Vec<_>>();
            lines.push(format!("Partition {partition}: [{}]", files.join(", ")));
        }
        Ok(lines.join("\n"))
    }
}
