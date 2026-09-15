#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::{
        array::{RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
        util::pretty::{self, pretty_format_batches},
    };
    use datafusion::{
        catalog::memory::DataSourceExec,
        common::ScalarValue,
        datasource::{
            TableProvider,
            physical_plan::{FileGroup, FileScanConfig},
        },
        error::Result,
        logical_expr::{Expr, TableType},
        physical_expr::{
            LexOrdering, Partitioning, PhysicalSortExpr, RangePartitioning, SplitPoint,
            expressions::Column,
        },
        physical_plan::{ExecutionPlan, collect},
        prelude::{ParquetReadOptions, SessionContext, col},
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, assert_snapshot, display_plan_ascii,
        test_utils::localhost::start_localhost_context,
    };
    use std::sync::Arc;

    #[derive(Debug)]
    struct RangePartitionedTableWrapper {
        inner: Arc<dyn TableProvider>,
        col_name: String,
        col_idx: usize,
        splits: Vec<SplitPoint>,
    }

    #[async_trait::async_trait]
    impl TableProvider for RangePartitionedTableWrapper {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }
        fn table_type(&self) -> TableType {
            self.inner.table_type()
        }
        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let plan = self.inner.scan(state, projection, filters, limit).await?;
            if let Some(dse) = plan.downcast_ref::<DataSourceExec>()
                && let Some(file_scan) = dse.data_source().downcast_ref::<FileScanConfig>()
            {
                let proj_col_idx = match projection {
                    Some(proj) => proj.iter().position(|&idx| idx == self.col_idx),
                    None => Some(self.col_idx),
                };
                if let Some(proj_idx) = proj_col_idx {
                    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
                        Arc::new(Column::new(&self.col_name, proj_idx)),
                        Default::default(),
                    )])
                    .unwrap();
                    let partition_count = file_scan.file_groups.len();
                    let range = RangePartitioning::try_new_with_samples(
                        ordering,
                        self.splits.clone(),
                        partition_count,
                    )
                    .unwrap();

                    // ListingTable defaults to round-robin grouping of files across groups. For
                    // range-partitioned tables, sort the files by partition path and assign them
                    // contiguously so adjacent ranges stay together within each group.
                    let mut all_files: Vec<_> = file_scan
                        .file_groups
                        .iter()
                        .flat_map(|fg| fg.iter().cloned())
                        .collect();
                    all_files.sort_by(|a, b| a.path().cmp(b.path()));
                    let n = all_files.len();
                    let mut contiguous_groups = Vec::with_capacity(partition_count);
                    for i in 0..partition_count {
                        let start = (i * n) / partition_count;
                        let end = ((i + 1) * n) / partition_count;
                        contiguous_groups.push(FileGroup::new(all_files[start..end].to_vec()));
                    }

                    let mut new_file_scan = file_scan.clone();
                    new_file_scan.file_groups = contiguous_groups;
                    new_file_scan.output_partitioning = Some(Partitioning::Range(range));
                    return Ok(DataSourceExec::from_data_source(new_file_scan));
                }
            }
            Ok(plan)
        }
    }

    fn set_configs(ctx: &mut SessionContext) {
        set_configs_with_target_partitions(ctx, 4);
    }

    fn set_configs_with_target_partitions(ctx: &mut SessionContext, target_partitions: usize) {
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .preserve_file_partitions = 1;
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
    }

    fn split_points() -> Vec<SplitPoint> {
        vec![
            SplitPoint::new(vec![ScalarValue::Utf8(Some("B".to_string()))]),
            SplitPoint::new(vec![ScalarValue::Utf8(Some("C".to_string()))]),
            SplitPoint::new(vec![ScalarValue::Utf8(Some("D".to_string()))]),
        ]
    }

    fn set_configs_with_broadcast(ctx: &mut SessionContext) {
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .preserve_file_partitions = 1;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 4;
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
            .hash_join_single_partition_threshold_rows = 3;
    }

    async fn register_services_table(ctx: &SessionContext) -> Result<()> {
        let parquet_path = std::path::Path::new("testdata/join/parquet/services/data0.parquet");
        if !parquet_path.exists() {
            if let Some(parent) = parquet_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let schema = Arc::new(Schema::new(vec![
                Field::new("service", DataType::Utf8, false),
                Field::new("service_name", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["log", "trace"])),
                    Arc::new(StringArray::from(vec!["Logging", "Tracing"])),
                ],
            )?;
            let file = std::fs::File::create(parquet_path)?;
            let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)?;
            writer.write(&batch)?;
            writer.close()?;
        }
        ctx.register_parquet(
            "services",
            "testdata/join/parquet/services",
            ParquetReadOptions::default(),
        )
        .await?;
        Ok(())
    }

    async fn register_range_tables(ctx: &SessionContext) -> Result<()> {
        let dim_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("d_dkey".to_string(), DataType::Utf8)]);
        ctx.register_parquet("dim", "testdata/join/parquet/dim", dim_options)
            .await?;
        let dim_table = ctx.table_provider("dim").await?;
        ctx.deregister_table("dim")?;
        ctx.register_table(
            "dim",
            Arc::new(RangePartitionedTableWrapper {
                inner: dim_table,
                col_name: "d_dkey".to_string(),
                col_idx: 3,
                splits: split_points(),
            }),
        )?;

        let fact_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("f_dkey".to_string(), DataType::Utf8)])
            .file_sort_order(vec![vec![
                col("f_dkey").sort(true, false),
                col("timestamp").sort(true, false),
            ]]);
        ctx.register_parquet("fact", "testdata/join/parquet/fact", fact_options)
            .await?;
        let fact_table = ctx.table_provider("fact").await?;
        ctx.deregister_table("fact")?;
        ctx.register_table(
            "fact",
            Arc::new(RangePartitionedTableWrapper {
                inner: fact_table,
                col_name: "f_dkey".to_string(),
                col_idx: 2,
                splits: split_points(),
            }),
        )?;
        Ok(())
    }

    async fn register_unpartitioned_dim_and_range_fact(ctx: &SessionContext) -> Result<()> {
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
        let fact_table = ctx.table_provider("fact").await?;
        ctx.deregister_table("fact")?;
        ctx.register_table(
            "fact",
            Arc::new(RangePartitionedTableWrapper {
                inner: fact_table,
                col_name: "f_dkey".to_string(),
                col_idx: 2,
                splits: split_points(),
            }),
        )?;
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
        println!("\n——————— DISTRIBUTED PLAN ———————\n\n{distributed_plan}");

        let distributed_results = collect(physical_plan, state.task_ctx()).await?;
        pretty::print_batches(&distributed_results)?;
        Ok((distributed_plan, distributed_results))
    }

    #[tokio::test]
    async fn test_join_range_prepartitioned_both_sides() -> Result<(), Box<dyn std::error::Error>> {
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
            ORDER BY f_dkey, timestamp
        "#;

        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(4, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        register_range_tables(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        assert_snapshot!(&distributed_plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=4, partitions=4
          │ HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[f_dkey@6, timestamp@4, value@5, env@0, service@1, host@2]
          │   FilterExec: service@1 = log
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │       t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=B/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │       t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=C/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │       t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=B/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=C/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
        ");

        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results, @"
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
        +--------+---------------------+-------+------+---------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_join_range_unsatisfied_stream_adapts_to_range()
    -> Result<(), Box<dyn std::error::Error>> {
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
            ORDER BY f_dkey, timestamp
        "#;

        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(4, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        register_unpartitioned_dim_and_range_fact(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        assert_snapshot!(&distributed_plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=4, partitions=4
          │ HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[f_dkey@6, timestamp@4, value@5, env@0, service@1, host@2]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=4
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=B/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=C/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=4, partitions=4
            │ RepartitionExec: partitioning=Range([d_dkey@3 ASC], [(B), (C), (D)], 4), input_partitions=4
            │   FilterExec: service@1 = log
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [], [], []]}, projection=[env, service, host, d_dkey], output_partitioning=Hash([d_dkey@3], 4), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │       t1: DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [], [], []]}, projection=[env, service, host, d_dkey], output_partitioning=Hash([d_dkey@3], 4), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │       t2: DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [], [], []]}, projection=[env, service, host, d_dkey], output_partitioning=Hash([d_dkey@3], 4), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │       t3: DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=D/data0.parquet], [], [], []]}, projection=[env, service, host, d_dkey], output_partitioning=Hash([d_dkey@3], 4), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            └──────────────────────────────────────────────────
        ");

        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results, @"
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
        +--------+---------------------+-------+------+---------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_join_range_under_parallelism() -> Result<(), Box<dyn std::error::Error>> {
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
            ORDER BY f_dkey, timestamp
        "#;

        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(2, DefaultSessionBuilder).await;
        set_configs_with_target_partitions(&mut distributed_ctx, 2);
        register_range_tables(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        assert_snapshot!(&distributed_plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=2, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=2
          │ HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[f_dkey@6, timestamp@4, value@5, env@0, service@1, host@2]
          │   FilterExec: service@1 = log
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet, /testdata/join/parquet/dim/d_dkey=B/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │       t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=C/data0.parquet, /testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet, /testdata/join/parquet/fact/f_dkey=B/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=C/data0.parquet, /testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
        ");

        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results, @"
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
        +--------+---------------------+-------+------+---------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_join_range_over_parallelism() -> Result<(), Box<dyn std::error::Error>> {
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
            ORDER BY f_dkey, timestamp
        "#;

        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(8, DefaultSessionBuilder).await;
        set_configs_with_target_partitions(&mut distributed_ctx, 8);
        register_range_tables(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        assert_snapshot!(&distributed_plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=4, partitions=4
          │ HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[f_dkey@6, timestamp@4, value@5, env@0, service@1, host@2]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=4
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=B/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=C/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=4, partitions=4
            │ RepartitionExec: partitioning=Range([d_dkey@3 ASC], [(B), (C), (D)], 4), input_partitions=8
            │   FilterExec: service@1 = log
            │     RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            │       DistributedLeafExec:
            │         t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │         t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=B/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │         t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=C/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │         t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            └──────────────────────────────────────────────────
        ");

        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results, @"
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
        +--------+---------------------+-------+------+---------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_three_way_join_range_to_hash_shuffle() -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
            SELECT 
                f.f_dkey,
                f.timestamp,
                f.value,
                d.env,
                d.service,
                s.service_name
            FROM dim d
            INNER JOIN fact f ON d.d_dkey = f.f_dkey
            INNER JOIN services s ON d.service = s.service
            WHERE d.service = 'log'
            ORDER BY f_dkey, timestamp
        "#;

        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(4, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        register_range_tables(&distributed_ctx).await?;
        register_services_table(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        assert_snapshot!(&distributed_plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
        │   [Stage 4] => NetworkCoalesceExec: output_partitions=16, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 4 ── tasks=4, partitions=4
          │ HashJoinExec: mode=Partitioned, join_type=Inner, on=[(service@0, service@1)], projection=[f_dkey@6, timestamp@4, value@5, env@2, service@3, service_name@1]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=4
          │   SortExec: expr=[f_dkey@4 ASC NULLS LAST, timestamp@2 ASC NULLS LAST], preserve_partitioning=[true]
          │     [Stage 3] => NetworkShuffleExec: output_partitions=4, input_tasks=4, sort_exprs=[f_dkey@4 ASC NULLS LAST, timestamp@2 ASC NULLS LAST]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=4, partitions=16
            │ RepartitionExec: partitioning=Hash([service@0], 16), input_partitions=4
            │   FilterExec: service@0 = log
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │       DistributedLeafExec:
            │         t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/services/data0.parquet:<int>..<int>]]}, projection=[service, service_name], file_type=parquet, predicate=service@0 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │         t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/services/data0.parquet:<int>..<int>]]}, projection=[service, service_name], file_type=parquet, predicate=service@0 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │         t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/services/data0.parquet:<int>..<int>]]}, projection=[service, service_name], file_type=parquet, predicate=service@0 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │         t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/services/data0.parquet:<int>..<int>]]}, projection=[service, service_name], file_type=parquet, predicate=service@0 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            └──────────────────────────────────────────────────
            ┌───── Stage 3 ── tasks=4, partitions=16
            │ RepartitionExec: partitioning=Hash([service@1], 16), input_partitions=1, maintains_sort_order=true
            │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@2, f_dkey@2)], projection=[env@0, service@1, timestamp@3, value@4, f_dkey@5]
            │     [Stage 2] => NetworkShuffleExec: output_partitions=1, input_tasks=4
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │       t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=B/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │       t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=C/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │       t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
              ┌───── Stage 2 ── tasks=4, partitions=4
              │ RepartitionExec: partitioning=Range([d_dkey@2 ASC], [(B), (C), (D)], 4), input_partitions=1
              │   FilterExec: service@1 = log
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet]]}, projection=[env, service, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
              │       t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=B/data0.parquet]]}, projection=[env, service, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
              │       t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=C/data0.parquet]]}, projection=[env, service, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
              │       t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
              └──────────────────────────────────────────────────
        ");
        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results, @"
        +--------+---------------------+-------+------+---------+--------------+
        | f_dkey | timestamp           | value | env  | service | service_name |
        +--------+---------------------+-------+------+---------+--------------+
        | A      | 2023-01-01T09:00:00 | 95.5  | dev  | log     | Logging      |
        | A      | 2023-01-01T09:00:10 | 102.3 | dev  | log     | Logging      |
        | A      | 2023-01-01T09:00:20 | 98.7  | dev  | log     | Logging      |
        | A      | 2023-01-01T09:12:20 | 105.1 | dev  | log     | Logging      |
        | A      | 2023-01-01T09:12:30 | 100.0 | dev  | log     | Logging      |
        | A      | 2023-01-01T09:12:40 | 150.0 | dev  | log     | Logging      |
        | A      | 2023-01-01T09:12:50 | 120.8 | dev  | log     | Logging      |
        | B      | 2023-01-01T09:00:00 | 75.2  | prod | log     | Logging      |
        | B      | 2023-01-01T09:00:10 | 82.4  | prod | log     | Logging      |
        | B      | 2023-01-01T09:00:20 | 78.9  | prod | log     | Logging      |
        | B      | 2023-01-01T09:00:30 | 85.6  | prod | log     | Logging      |
        | B      | 2023-01-01T09:12:30 | 80.0  | prod | log     | Logging      |
        | B      | 2023-01-01T09:12:40 | 120.0 | prod | log     | Logging      |
        | B      | 2023-01-01T09:12:50 | 92.3  | prod | log     | Logging      |
        +--------+---------------------+-------+------+---------+--------------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_three_way_aggregation_broadcast_dimension()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
            SELECT 
                s.service_name,
                COUNT(*) as row_count,
                SUM(f.value) as total_value
            FROM dim d
            INNER JOIN fact f ON d.d_dkey = f.f_dkey
            INNER JOIN services s ON d.service = s.service
            GROUP BY s.service_name
            ORDER BY s.service_name
        "#;

        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(4, DefaultSessionBuilder).await;
        set_configs_with_broadcast(&mut distributed_ctx);
        register_range_tables(&distributed_ctx).await?;
        register_services_table(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        assert_snapshot!(&distributed_plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [service_name@0 ASC NULLS LAST]
        │   [Stage 5] => NetworkCoalesceExec: output_partitions=16, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 5 ── tasks=4, partitions=4
          │ ProjectionExec: expr=[service_name@0 as service_name, count(Int64(1))@1 as row_count, sum(f.value)@2 as total_value]
          │   SortExec: expr=[service_name@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[service_name@0 as service_name], aggr=[count(Int64(1)), sum(f.value)]
          │       [Stage 4] => NetworkShuffleExec: output_partitions=4, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 4 ── tasks=4, partitions=16
            │ RepartitionExec: partitioning=Hash([service_name@0], 16), input_partitions=4
            │   AggregateExec: mode=Partial, gby=[service_name@1 as service_name], aggr=[count(Int64(1)), sum(f.value)]
            │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(service@0, service@0)], projection=[value@3, service_name@1]
            │       CoalescePartitionsExec
            │         [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=1, stage_partitions=4, input_tasks=4
            │       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@1, f_dkey@1)], projection=[service@0, value@2]
            │         [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=4
            │         [Stage 3] => NetworkShuffleExec: output_partitions=4, input_tasks=4, sort_exprs=[f_dkey@1 ASC NULLS LAST]
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=4, partitions=16
              │ BroadcastExec: input_partitions=1, consumer_tasks=4, output_partitions=4
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/services/data0.parquet:<int>..<int>]]}, projection=[service, service_name], file_type=parquet
              │     t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/services/data0.parquet:<int>..<int>]]}, projection=[service, service_name], file_type=parquet
              │     t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/services/data0.parquet:<int>..<int>]]}, projection=[service, service_name], file_type=parquet
              │     t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/services/data0.parquet:<int>..<int>]]}, projection=[service, service_name], file_type=parquet
              └──────────────────────────────────────────────────
              ┌───── Stage 2 ── tasks=4, partitions=16
              │ RepartitionExec: partitioning=Hash([d_dkey@1], 16), input_partitions=1
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet]]}, projection=[service, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │     t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=B/data0.parquet]]}, projection=[service, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │     t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=C/data0.parquet]]}, projection=[service, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │     t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[service, d_dkey], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              └──────────────────────────────────────────────────
              ┌───── Stage 3 ── tasks=4, partitions=16
              │ RepartitionExec: partitioning=Hash([f_dkey@1], 16), input_partitions=1, maintains_sort_order=true
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet]]}, projection=[value, f_dkey], output_ordering=[f_dkey@1 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet
              │     t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=B/data0.parquet]]}, projection=[value, f_dkey], output_ordering=[f_dkey@1 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet
              │     t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=C/data0.parquet]]}, projection=[value, f_dkey], output_ordering=[f_dkey@1 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet
              │     t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[value, f_dkey], output_ordering=[f_dkey@1 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet
              └──────────────────────────────────────────────────
        ");
        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results, @"
        +--------------+-----------+-------------+
        | service_name | row_count | total_value |
        +--------------+-----------+-------------+
        | Logging      | 14        | 1386.8      |
        | Tracing      | 10        | 2017.0      |
        +--------------+-----------+-------------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_join_range_unsatisfied_stream_adapts_to_range_under_parallelism()
    -> Result<(), Box<dyn std::error::Error>> {
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
            ORDER BY f_dkey, timestamp
        "#;

        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(2, DefaultSessionBuilder).await;
        set_configs_with_target_partitions(&mut distributed_ctx, 2);
        register_unpartitioned_dim_and_range_fact(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        assert_snapshot!(&distributed_plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=2, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=2
          │ HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[f_dkey@6, timestamp@4, value@5, env@0, service@1, host@2]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=2
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet, /testdata/join/parquet/fact/f_dkey=B/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet/fact/f_dkey=C/data0.parquet, /testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=UnknownPartitioning(1), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=2
            │ RepartitionExec: partitioning=Range([d_dkey@3 ASC], [(C)], 2, max 4), input_partitions=2
            │   FilterExec: service@1 = log
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Hash([d_dkey@3], 2), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │       t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Hash([d_dkey@3], 2), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            └──────────────────────────────────────────────────
        ");

        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results, @"
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
        +--------+---------------------+-------+------+---------+--------+
        ");

        Ok(())
    }
}
