#[cfg(test)]
mod tests {
    use arrow::{datatypes::DataType, util::pretty};
    use datafusion::{
        assert_batches_sorted_eq,
        physical_plan::collect,
        prelude::{ParquetReadOptions, col},
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, display_plan_ascii, test_utils::localhost::start_localhost_context,
    };

    fn normalize(s: &str) -> String {
        let current_dir = std::env::current_dir().unwrap().display().to_string();
        let dir_without_slash = current_dir.trim_start_matches('/');
        s.replace(&format!("{}/", current_dir), "")
            .replace(&format!("{}/", dir_without_slash), "")
    }

    #[tokio::test]
    async fn test_join_hive() -> Result<(), Box<dyn std::error::Error>> {
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
            WHERE d.service = 'log'
        "#;

        // —————————————————————————————————————————————————————————————
        // Execute the query using distributed datafusion, 2 workers,
        // and hive-style partitioned data.
        // —————————————————————————————————————————————————————————————

        let (distributed_ctx, _guard) = start_localhost_context(2, DefaultSessionBuilder).await;

        // Preserve hive-style file partitions.
        distributed_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .preserve_file_partitions = 1;
        // Set a high threshold to encourage subset satisfaction.
        distributed_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .subset_satisfaction_partition_threshold = 999;
        // Read data from 4 hive-style partitions.
        distributed_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 4;
        // Ensure that we use a partitioned hash join.
        distributed_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        distributed_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;

        // Register hive-style partitioning for the dim table.
        let dim_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("d_dkey".to_string(), DataType::Utf8)]);
        distributed_ctx
            .register_parquet("dim", "testdata/join/parquet/dim", dim_options)
            .await?;

        // Register hive-style partitioning for the fact table.
        let fact_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("f_dkey".to_string(), DataType::Utf8)])
            // TODO: Figure out why file sort order does not display in plan.
            .file_sort_order(vec![vec![
                col("f_dkey").sort(true, true),
                col("timestamp").sort(true, true),
            ]]);
        distributed_ctx
            .register_parquet("fact", "testdata/join/parquet/fact", fact_options)
            .await?;

        let df = distributed_ctx.sql(query).await?;
        let (state, logical_plan) = df.into_parts();
        let physical_plan = state.create_physical_plan(&logical_plan).await?;
        println!("\n——————— DISTRIBUTED PLAN ———————\n");
        let distributed_plan = display_plan_ascii(physical_plan.as_ref(), false);
        println!("{}", distributed_plan);

        let distributed_results = collect(physical_plan, state.task_ctx()).await?;
        pretty::print_batches(&distributed_results)?;

        // —————————————————————————————————————————————————————————————
        // Ensure the distributed plan matches our target plan, registering
        // hive-style partitioning and avoiding data-shuffling repartitions.
        // —————————————————————————————————————————————————————————————

        let target_plan = r#"┌───── DistributedExec ── Tasks: t0:[p0] 
│ CoalescePartitionsExec
│   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
└──────────────────────────────────────────────────
  ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] 
  │ ProjectionExec: expr=[f_dkey@5 as f_dkey, timestamp@3 as timestamp, value@4 as value, env@0 as env, service@1 as service, host@2 as host]
  │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[env@0, service@1, host@2, timestamp@4, value@5, f_dkey@6]
  │     FilterExec: service@1 = log
  │       PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1] 
  │         DataSourceExec: file_groups={4 groups: [[testdata/join/parquet/dim/d_dkey=A/data0.parquet], [testdata/join/parquet/dim/d_dkey=B/data0.parquet], [testdata/join/parquet/dim/d_dkey=C/data0.parquet], [testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, host, d_dkey], file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
  │     PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1] 
  │       DataSourceExec: file_groups={4 groups: [[testdata/join/parquet/fact/f_dkey=A/data0.parquet], [testdata/join/parquet/fact/f_dkey=B/data2.parquet, testdata/join/parquet/fact/f_dkey=B/data0.parquet, testdata/join/parquet/fact/f_dkey=B/data1.parquet], [testdata/join/parquet/fact/f_dkey=C/data0.parquet, testdata/join/parquet/fact/f_dkey=C/data1.parquet], [testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], file_type=parquet, predicate=DynamicFilter [ empty ]
  └──────────────────────────────────────────────────
  "#;

        assert_eq!(
            normalize(&distributed_plan).trim(),
            target_plan.trim(),
            "Plan mismatch!\nTarget:\n{}\nActual:\n{}",
            target_plan,
            distributed_plan
        );

        // —————————————————————————————————————————————————————————————
        // Ensure distributed results are correct.
        // —————————————————————————————————————————————————————————————

        let expected = vec![
            "+--------+---------------------+-------+------+---------+--------+",
            "| f_dkey | timestamp           | value | env  | service | host   |",
            "+--------+---------------------+-------+------+---------+--------+",
            "| A      | 2023-01-01T09:00:00 | 95.5  | dev  | log     | host-y |",
            "| A      | 2023-01-01T09:00:10 | 102.3 | dev  | log     | host-y |",
            "| A      | 2023-01-01T09:00:20 | 98.7  | dev  | log     | host-y |",
            "| A      | 2023-01-01T09:12:20 | 105.1 | dev  | log     | host-y |",
            "| A      | 2023-01-01T09:12:30 | 100.0 | dev  | log     | host-y |",
            "| A      | 2023-01-01T09:12:40 | 150.0 | dev  | log     | host-y |",
            "| A      | 2023-01-01T09:12:50 | 120.8 | dev  | log     | host-y |",
            "| B      | 2023-01-01T11:00:00 | 72.8  | prod | log     | host-x |",
            "| B      | 2023-01-01T11:00:10 | 79.4  | prod | log     | host-x |",
            "| B      | 2023-01-01T11:00:20 | 76.1  | prod | log     | host-x |",
            "| B      | 2023-01-01T11:00:30 | 83.7  | prod | log     | host-x |",
            "| B      | 2023-01-01T11:12:30 | 77.2  | prod | log     | host-x |",
            "| B      | 2023-01-01T09:00:00 | 75.2  | prod | log     | host-x |",
            "| B      | 2023-01-01T09:00:10 | 82.4  | prod | log     | host-x |",
            "| B      | 2023-01-01T09:00:20 | 78.9  | prod | log     | host-x |",
            "| B      | 2023-01-01T09:00:30 | 85.6  | prod | log     | host-x |",
            "| B      | 2023-01-01T09:12:30 | 80.0  | prod | log     | host-x |",
            "| B      | 2023-01-01T09:12:40 | 120.0 | prod | log     | host-x |",
            "| B      | 2023-01-01T09:12:50 | 92.3  | prod | log     | host-x |",
            "| B      | 2023-01-01T10:00:00 | 88.5  | prod | log     | host-x |",
            "| B      | 2023-01-01T10:00:10 | 91.2  | prod | log     | host-x |",
            "| B      | 2023-01-01T10:00:20 | 87.3  | prod | log     | host-x |",
            "| B      | 2023-01-01T10:00:30 | 94.1  | prod | log     | host-x |",
            "| B      | 2023-01-01T10:12:30 | 89.5  | prod | log     | host-x |",
            "| B      | 2023-01-01T10:12:40 | 95.8  | prod | log     | host-x |",
            "+--------+---------------------+-------+------+---------+--------+",
        ];

        assert_batches_sorted_eq!(expected, &distributed_results);
        Ok(())
    }
}
