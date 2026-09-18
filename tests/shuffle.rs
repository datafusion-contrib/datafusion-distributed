#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::{array::RecordBatch, datatypes::DataType, util::pretty::pretty_format_batches};
    use datafusion::{
        error::Result,
        physical_plan::collect,
        prelude::{ParquetReadOptions, SessionContext, col},
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, assert_snapshot, display_plan_ascii,
        test_utils::localhost::start_localhost_context,
    };

    fn set_configs(ctx: &mut SessionContext, target_partitions: usize) {
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = target_partitions;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        // DataFusion defaults `prefer_existing_sort = false`, which causes the
        // `replace_with_order_preserving_variants` optimizer rule to insert an explicit
        // `SortExec` above the shuffle on the worker side, masking any sort-order scrambling
        // across input tasks. Enabling `prefer_existing_sort` ensures DataFusion trusts the
        // ordering advertised by `NetworkShuffleExec` without inserting a downstream `SortExec`.
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .prefer_existing_sort = true;
    }

    async fn register_tables(ctx: &SessionContext) -> Result<()> {
        let dim_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("d_dkey".to_string(), DataType::Utf8)]);
        ctx.register_parquet("dim", "testdata/join/parquet/dim", dim_options)
            .await?;

        let fact_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("f_dkey".to_string(), DataType::Utf8)])
            .file_sort_order(vec![vec![
                col("f_dkey").sort(true, false),
                col("timestamp").sort(true, false),
            ]]);
        ctx.register_parquet("fact", "testdata/join/parquet/fact", fact_options)
            .await?;
        Ok(())
    }

    async fn execute_query(
        ctx: &SessionContext,
        query: &'static str,
    ) -> Result<(String, Vec<RecordBatch>)> {
        let df = ctx.sql(query).await?;
        let (state, logical_plan) = df.into_parts();
        let physical_plan = state.create_physical_plan(&logical_plan).await?;
        let distributed_plan = display_plan_ascii(physical_plan.as_ref(), false);
        let distributed_results = collect(physical_plan, state.task_ctx()).await?;
        Ok((distributed_plan, distributed_results))
    }

    #[tokio::test]
    async fn test_sorted_network_shuffle() -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
            SELECT 
                f.f_dkey,
                f.timestamp,
                f.value,
                d.env,
                d.service,
                d.host
            FROM dim d
            INNER JOIN fact f ON d.d_dkey = f.f_dkey
            ORDER BY f_dkey, timestamp
        "#;

        // Run on single-node DataFusion for ground-truth comparison
        let mut single_node_ctx = SessionContext::default();
        set_configs(&mut single_node_ctx, 4);
        register_tables(&single_node_ctx).await?;
        let (_, single_node_results) = execute_query(&single_node_ctx, query).await?;

        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(2, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx, 2);
        register_tables(&distributed_ctx).await?;

        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        // Verify that the distributed plan contains NetworkShuffleExec with multiple input tasks
        assert_snapshot!(&distributed_plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=2, partitions=2
          │ HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[f_dkey@6, timestamp@4, value@5, env@0, service@1, host@2]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=2
          │   [Stage 2] => NetworkShuffleExec: output_partitions=2, input_tasks=2, sort_exprs=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=4
            │ RepartitionExec: partitioning=Hash([d_dkey@3], 4), input_partitions=2
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet:<int>..<int>, /testdata/join/parquet/dim/d_dkey=B/data0.parquet:<int>..<int>], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet:<int>..<int>, /testdata/join/parquet/dim/d_dkey=D/data0.parquet:<int>..<int>]]}, projection=[env, service, host, d_dkey], file_type=parquet
            │     t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet/dim/d_dkey=B/data0.parquet:<int>..<int>, /testdata/join/parquet/dim/d_dkey=C/data0.parquet:<int>..<int>], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet:<int>..<int>]]}, projection=[env, service, host, d_dkey], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=4
            │ RepartitionExec: partitioning=Hash([f_dkey@2], 4), input_partitions=1, maintains_sort_order=true
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet, /testdata/join/parquet/fact/f_dkey=B/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │     t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=C/data0.parquet, /testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");

        let single_node_formatted = pretty_format_batches(&single_node_results)?.to_string();
        let distributed_formatted = pretty_format_batches(&distributed_results)?.to_string();

        assert_eq!(
            distributed_formatted, single_node_formatted,
            "Distributed results must match single-node results exactly in content and sort order"
        );

        assert_snapshot!(distributed_formatted, @"
        +--------+---------------------+-------+------+---------+--------+
        | f_dkey | timestamp           | value | env  | service | host   |
        +--------+---------------------+-------+------+---------+--------+
        | A      | 2023-01-01T09:00:00 | 95.5  | dev  | log     | host-y |
        | A      | 2023-01-01T09:00:10 | 102.3 | dev  | log     | host-y |
        | A      | 2023-01-01T09:00:20 | 98.7  | dev  | log     | host-y |
        | A      | 2023-01-01T09:12:20 | 105.1 | dev  | log     | host-y |
        | A      | 2023-01-01T09:12:30 | 100.0 | dev  | log     | host-y |
        | A      | 2023-01-01T09:12:40 | 150.0 | dev  | log     | host-y |
        | A      | 2023-01-01T09:12:50 | 120.8 | dev  | log     | host-y |
        | B      | 2023-01-01T09:00:00 | 75.2  | prod | log     | host-x |
        | B      | 2023-01-01T09:00:10 | 82.4  | prod | log     | host-x |
        | B      | 2023-01-01T09:00:20 | 78.9  | prod | log     | host-x |
        | B      | 2023-01-01T09:00:30 | 85.6  | prod | log     | host-x |
        | B      | 2023-01-01T09:12:30 | 80.0  | prod | log     | host-x |
        | B      | 2023-01-01T09:12:40 | 120.0 | prod | log     | host-x |
        | B      | 2023-01-01T09:12:50 | 92.3  | prod | log     | host-x |
        | C      | 2023-01-01T10:00:00 | 310.5 | dev  | trace   | host-z |
        | C      | 2023-01-01T10:00:10 | 225.7 | dev  | trace   | host-z |
        | C      | 2023-01-01T10:00:20 | 380.2 | dev  | trace   | host-z |
        | C      | 2023-01-01T10:00:30 | 205.8 | dev  | trace   | host-z |
        | C      | 2023-01-01T10:00:40 | 350.0 | dev  | trace   | host-z |
        | C      | 2023-01-01T10:12:40 | 200.0 | dev  | trace   | host-z |
        | C      | 2023-01-01T10:12:50 | 205.4 | dev  | trace   | host-z |
        | D      | 2023-01-01T10:00:00 | 24.8  | prod | trace   | host-x |
        | D      | 2023-01-01T10:00:10 | 72.1  | prod | trace   | host-x |
        | D      | 2023-01-01T10:00:20 | 42.5  | prod | trace   | host-x |
        +--------+---------------------+-------+------+---------+--------+
        ");

        Ok(())
    }
}
