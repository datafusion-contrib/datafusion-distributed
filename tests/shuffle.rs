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

    /// Verifies that [NetworkShuffleExec] preserves sort order across multiple input worker
    /// tasks by streaming-merge sorting rows arriving over the network.
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

#[cfg(all(feature = "integration", feature = "tpch", test))]
mod tpch_two_phase_tests {
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, assert_snapshot, display_plan_ascii,
    };
    use datafusion_distributed_benchmarks::datasets::{
        output::DatasetOutput, register_tables, tpch,
    };
    use futures::TryStreamExt;
    use std::error::Error;
    use std::fmt::Display;
    use std::fs;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const NUM_WORKERS: usize = 4;
    const FILE_SCAN_CONFIG_BYTES_PER_PARTITION: usize = 1;
    const CARDINALITY_TASK_COUNT_FACTOR: f64 = 1.5;
    const TPCH_DATA_PARTS: usize = 16;

    const PLAN_PARTITIONS: usize = 3;
    const PLAN_SCALE_FACTOR: f64 = 0.02;

    const CORRECTNESS_PARTITIONS: usize = 6;
    const CORRECTNESS_SCALE_FACTOR: f64 = 1.0;

    // ── TwoPhase shuffle plan snapshots ──────────────────────────────────────

    #[tokio::test]
    async fn test_tpch_1_two_phase() -> Result<(), Box<dyn Error>> {
        let plan = run_two_phase_plan_test("q1").await?;
        assert_snapshot!(plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [l_returnflag@0 ASC NULLS LAST, l_linestatus@1 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=3
          │ ProjectionExec: expr=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus, sum(lineitem.l_quantity)@2 as sum_qty, sum(lineitem.l_extendedprice)@3 as sum_base_price, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@4 as sum_disc_price, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)@5 as sum_charge, avg(lineitem.l_quantity)@6 as avg_qty, avg(lineitem.l_extendedprice)@7 as avg_price, avg(lineitem.l_discount)@8 as avg_disc, count(Int64(1))@9 as count_order]
          │   SortExec: expr=[l_returnflag@0 ASC NULLS LAST, l_linestatus@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[sum(lineitem.l_quantity), sum(lineitem.l_extendedprice), sum(__common_expr_1) as sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount), sum(__common_expr_1 * 1 + lineitem.l_tax) as sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax), avg(lineitem.l_quantity), avg(lineitem.l_extendedprice), avg(lineitem.l_discount), count(Int64(1))]
          │       RepartitionExec: partitioning=Hash([l_returnflag@0, l_linestatus@1], 3), input_partitions=4
          │         [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=4, partitions=2
            │ RepartitionExec: partitioning=Hash([l_returnflag@0, l_linestatus@1, 5871781006564002453], 2), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[l_returnflag@5 as l_returnflag, l_linestatus@6 as l_linestatus], aggr=[sum(lineitem.l_quantity), sum(lineitem.l_extendedprice), sum(__common_expr_1) as sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount), sum(__common_expr_1 * 1 + lineitem.l_tax) as sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax), avg(lineitem.l_quantity), avg(lineitem.l_extendedprice), avg(lineitem.l_discount), count(Int64(1))]
            │     ProjectionExec: expr=[l_extendedprice@0 * (1 - l_discount@1) as __common_expr_1, l_quantity@2 as l_quantity, l_extendedprice@0 as l_extendedprice, l_discount@1 as l_discount, l_tax@3 as l_tax, l_returnflag@4 as l_returnflag, l_linestatus@5 as l_linestatus]
            │       FilterExec: l_shipdate@6 <= 1998-09-02, projection=[l_extendedprice@1, l_discount@2, l_quantity@0, l_tax@3, l_returnflag@4, l_linestatus@5]
            │         DistributedLeafExec:
            │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/5.parquet:<int>..<int>]]}, projection=[l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate], file_type=parquet, predicate=l_shipdate@10 <= 1998-09-02, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@0 <= 1998-09-02, required_guarantees=[]
            │           t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/16.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/7.parquet:<int>..<int>]]}, projection=[l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate], file_type=parquet, predicate=l_shipdate@10 <= 1998-09-02, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@0 <= 1998-09-02, required_guarantees=[]
            │           t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/8.parquet:<int>..<int>]]}, projection=[l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate], file_type=parquet, predicate=l_shipdate@10 <= 1998-09-02, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@0 <= 1998-09-02, required_guarantees=[]
            │           t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/9.parquet:<int>..<int>]]}, projection=[l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate], file_type=parquet, predicate=l_shipdate@10 <= 1998-09-02, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@0 <= 1998-09-02, required_guarantees=[]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_3_two_phase() -> Result<(), Box<dyn Error>> {
        let plan = run_two_phase_plan_test("q3").await?;
        assert_snapshot!(plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [revenue@1 DESC, o_orderdate@2 ASC NULLS LAST]
        │   [Stage 4] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 4 ── tasks=3, partitions=3
          │ ProjectionExec: expr=[l_orderkey@0 as l_orderkey, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@3 as revenue, o_orderdate@1 as o_orderdate, o_shippriority@2 as o_shippriority]
          │   SortExec: expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@3 DESC, o_orderdate@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[l_orderkey@0 as l_orderkey, o_orderdate@1 as o_orderdate, o_shippriority@2 as o_shippriority], aggr=[sum(lineitem.l_extendedprice * 1 - lineitem.l_discount) as sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
          │       RepartitionExec: partitioning=Hash([l_orderkey@0, o_orderdate@1, o_shippriority@2], 3), input_partitions=4
          │         [Stage 3] => NetworkShuffleExec: output_partitions=4, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 3 ── tasks=4, partitions=3
            │ RepartitionExec: partitioning=Hash([l_orderkey@0, o_orderdate@1, o_shippriority@2, 5871781006564002453], 3), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[l_orderkey@2 as l_orderkey, o_orderdate@0 as o_orderdate, o_shippriority@1 as o_shippriority], aggr=[sum(lineitem.l_extendedprice * 1 - lineitem.l_discount) as sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
            │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@0, l_orderkey@0)], projection=[o_orderdate@1, o_shippriority@2, l_orderkey@3, l_extendedprice@4, l_discount@5]
            │       CoalescePartitionsExec
            │         [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=12, input_tasks=4
            │       FilterExec: l_shipdate@3 > 1995-03-15, projection=[l_orderkey@0, l_extendedprice@1, l_discount@2]
            │         DistributedLeafExec:
            │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/5.parquet:<int>..<int>]]}, projection=[l_orderkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 > 1995-03-15 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 > 1995-03-15, required_guarantees=[]
            │           t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/16.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/7.parquet:<int>..<int>]]}, projection=[l_orderkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 > 1995-03-15 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 > 1995-03-15, required_guarantees=[]
            │           t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/8.parquet:<int>..<int>]]}, projection=[l_orderkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 > 1995-03-15 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 > 1995-03-15, required_guarantees=[]
            │           t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/9.parquet:<int>..<int>]]}, projection=[l_orderkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 > 1995-03-15 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 > 1995-03-15, required_guarantees=[]
            └──────────────────────────────────────────────────
              ┌───── Stage 2 ── tasks=4, partitions=48
              │ BroadcastExec: input_partitions=3, consumer_tasks=4, output_partitions=12
              │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(c_custkey@0, o_custkey@1)], projection=[o_orderkey@1, o_orderdate@3, o_shippriority@4]
              │     CoalescePartitionsExec
              │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=12, input_tasks=4
              │     FilterExec: o_orderdate@2 < 1995-03-15
              │       DistributedLeafExec:
              │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/5.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate, o_shippriority], file_type=parquet, predicate=o_orderdate@4 < 1995-03-15 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@0 < 1995-03-15, required_guarantees=[]
              │         t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/2.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/7.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate, o_shippriority], file_type=parquet, predicate=o_orderdate@4 < 1995-03-15 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@0 < 1995-03-15, required_guarantees=[]
              │         t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/8.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate, o_shippriority], file_type=parquet, predicate=o_orderdate@4 < 1995-03-15 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@0 < 1995-03-15, required_guarantees=[]
              │         t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/9.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate, o_shippriority], file_type=parquet, predicate=o_orderdate@4 < 1995-03-15 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@0 < 1995-03-15, required_guarantees=[]
              └──────────────────────────────────────────────────
                ┌───── Stage 1 ── tasks=4, partitions=48
                │ BroadcastExec: input_partitions=3, consumer_tasks=4, output_partitions=12
                │   FilterExec: c_mktsegment@1 = BUILDING, projection=[c_custkey@0]
                │     DistributedLeafExec:
                │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/6.parquet:<int>..<int>]]}, projection=[c_custkey, c_mktsegment], file_type=parquet, predicate=c_mktsegment@6 = BUILDING, pruning_predicate=c_mktsegment_null_count@2 != row_count@3 AND c_mktsegment_min@0 <= BUILDING AND BUILDING <= c_mktsegment_max@1, required_guarantees=[c_mktsegment in (BUILDING)]
                │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/16.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/7.parquet:<int>..<int>]]}, projection=[c_custkey, c_mktsegment], file_type=parquet, predicate=c_mktsegment@6 = BUILDING, pruning_predicate=c_mktsegment_null_count@2 != row_count@3 AND c_mktsegment_min@0 <= BUILDING AND BUILDING <= c_mktsegment_max@1, required_guarantees=[c_mktsegment in (BUILDING)]
                │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/8.parquet:<int>..<int>]]}, projection=[c_custkey, c_mktsegment], file_type=parquet, predicate=c_mktsegment@6 = BUILDING, pruning_predicate=c_mktsegment_null_count@2 != row_count@3 AND c_mktsegment_min@0 <= BUILDING AND BUILDING <= c_mktsegment_max@1, required_guarantees=[c_mktsegment in (BUILDING)]
                │       t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/9.parquet:<int>..<int>]]}, projection=[c_custkey, c_mktsegment], file_type=parquet, predicate=c_mktsegment@6 = BUILDING, pruning_predicate=c_mktsegment_null_count@2 != row_count@3 AND c_mktsegment_min@0 <= BUILDING AND BUILDING <= c_mktsegment_max@1, required_guarantees=[c_mktsegment in (BUILDING)]
                └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_5_two_phase() -> Result<(), Box<dyn Error>> {
        let plan = run_two_phase_plan_test("q5").await?;
        assert_snapshot!(plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [revenue@1 DESC]
        │   [Stage 7] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 7 ── tasks=3, partitions=3
          │ ProjectionExec: expr=[n_name@0 as n_name, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@1 as revenue]
          │   SortExec: expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@1 DESC], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[n_name@0 as n_name], aggr=[sum(lineitem.l_extendedprice * 1 - lineitem.l_discount) as sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
          │       RepartitionExec: partitioning=Hash([n_name@0], 3), input_partitions=4
          │         [Stage 6] => NetworkShuffleExec: output_partitions=4, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 6 ── tasks=4, partitions=3
            │ RepartitionExec: partitioning=Hash([n_name@0, 5871781006564002453], 3), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[n_name@2 as n_name], aggr=[sum(lineitem.l_extendedprice * 1 - lineitem.l_discount) as sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
            │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(r_regionkey@0, n_regionkey@3)], projection=[l_extendedprice@1, l_discount@2, n_name@3]
            │       CoalescePartitionsExec
            │         [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=12, input_tasks=4
            │       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@2)], projection=[l_extendedprice@3, l_discount@4, n_name@1, n_regionkey@2]
            │         CoalescePartitionsExec
            │           [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=12, input_tasks=4
            │         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, l_suppkey@1), (s_nationkey@1, c_nationkey@0)], projection=[l_extendedprice@4, l_discount@5, s_nationkey@1]
            │           CoalescePartitionsExec
            │             [Stage 3] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=12, input_tasks=4
            │           HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@1, l_orderkey@0)], projection=[c_nationkey@0, l_suppkey@3, l_extendedprice@4, l_discount@5]
            │             CoalescePartitionsExec
            │               [Stage 5] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=12, input_tasks=4
            │             DistributedLeafExec:
            │               t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/5.parquet:<int>..<int>]]}, projection=[l_orderkey, l_suppkey, l_extendedprice, l_discount], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │               t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/16.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/7.parquet:<int>..<int>]]}, projection=[l_orderkey, l_suppkey, l_extendedprice, l_discount], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │               t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/8.parquet:<int>..<int>]]}, projection=[l_orderkey, l_suppkey, l_extendedprice, l_discount], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │               t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/9.parquet:<int>..<int>]]}, projection=[l_orderkey, l_suppkey, l_extendedprice, l_discount], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=4, partitions=48
              │ BroadcastExec: input_partitions=3, consumer_tasks=4, output_partitions=12
              │   FilterExec: r_name@1 = ASIA, projection=[r_regionkey@0]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/region/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/region/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/region/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/6.parquet:<int>..<int>]]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = ASIA, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= ASIA AND ASIA <= r_name_max@1, required_guarantees=[r_name in (ASIA)]
              │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/region/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/region/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/2.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/region/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/7.parquet:<int>..<int>]]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = ASIA, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= ASIA AND ASIA <= r_name_max@1, required_guarantees=[r_name in (ASIA)]
              │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/region/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/region/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/region/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/8.parquet:<int>..<int>]]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = ASIA, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= ASIA AND ASIA <= r_name_max@1, required_guarantees=[r_name in (ASIA)]
              │       t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/region/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/region/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/region/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/region/9.parquet:<int>..<int>]]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = ASIA, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= ASIA AND ASIA <= r_name_max@1, required_guarantees=[r_name in (ASIA)]
              └──────────────────────────────────────────────────
              ┌───── Stage 2 ── tasks=4, partitions=48
              │ BroadcastExec: input_partitions=3, consumer_tasks=4, output_partitions=12
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/nation/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/6.parquet:<int>..<int>]]}, projection=[n_nationkey, n_name, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/nation/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/2.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/7.parquet:<int>..<int>]]}, projection=[n_nationkey, n_name, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/nation/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/8.parquet:<int>..<int>]]}, projection=[n_nationkey, n_name, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │     t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/nation/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/9.parquet:<int>..<int>]]}, projection=[n_nationkey, n_name, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              └──────────────────────────────────────────────────
              ┌───── Stage 3 ── tasks=4, partitions=48
              │ BroadcastExec: input_partitions=3, consumer_tasks=4, output_partitions=12
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/supplier/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/supplier/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/supplier/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/5.parquet:<int>..<int>]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/supplier/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/supplier/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/16.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/supplier/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/7.parquet:<int>..<int>]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/supplier/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/supplier/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/supplier/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/8.parquet:<int>..<int>]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │     t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/supplier/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/supplier/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/supplier/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/supplier/9.parquet:<int>..<int>]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              └──────────────────────────────────────────────────
              ┌───── Stage 5 ── tasks=4, partitions=48
              │ BroadcastExec: input_partitions=3, consumer_tasks=4, output_partitions=12
              │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(c_custkey@0, o_custkey@1)], projection=[c_nationkey@1, o_orderkey@2]
              │     CoalescePartitionsExec
              │       [Stage 4] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=12, input_tasks=4
              │     FilterExec: o_orderdate@2 >= 1994-01-01 AND o_orderdate@2 < 1995-01-01, projection=[o_orderkey@0, o_custkey@1]
              │       DistributedLeafExec:
              │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/5.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1994-01-01 AND o_orderdate@4 < 1995-01-01 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1994-01-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1995-01-01, required_guarantees=[]
              │         t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/2.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/7.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1994-01-01 AND o_orderdate@4 < 1995-01-01 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1994-01-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1995-01-01, required_guarantees=[]
              │         t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/8.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1994-01-01 AND o_orderdate@4 < 1995-01-01 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1994-01-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1995-01-01, required_guarantees=[]
              │         t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/9.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1994-01-01 AND o_orderdate@4 < 1995-01-01 AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1994-01-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1995-01-01, required_guarantees=[]
              └──────────────────────────────────────────────────
                ┌───── Stage 4 ── tasks=4, partitions=48
                │ BroadcastExec: input_partitions=3, consumer_tasks=4, output_partitions=12
                │   DistributedLeafExec:
                │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/6.parquet:<int>..<int>]]}, projection=[c_custkey, c_nationkey], file_type=parquet
                │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/16.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/7.parquet:<int>..<int>]]}, projection=[c_custkey, c_nationkey], file_type=parquet
                │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/8.parquet:<int>..<int>]]}, projection=[c_custkey, c_nationkey], file_type=parquet
                │     t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/9.parquet:<int>..<int>]]}, projection=[c_custkey, c_nationkey], file_type=parquet
                └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_6_two_phase() -> Result<(), Box<dyn Error>> {
        let plan = run_two_phase_plan_test("q6").await?;
        assert_snapshot!(plan, @"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[sum(lineitem.l_extendedprice * lineitem.l_discount)@0 as revenue]
        │   AggregateExec: mode=Final, gby=[], aggr=[sum(lineitem.l_extendedprice * lineitem.l_discount)]
        │     CoalescePartitionsExec
        │       [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=4, partitions=12
          │ AggregateExec: mode=Partial, gby=[], aggr=[sum(lineitem.l_extendedprice * lineitem.l_discount)]
          │   FilterExec: l_shipdate@3 >= 1994-01-01 AND l_shipdate@3 < 1995-01-01 AND l_discount@2 >= 0.05 AND l_discount@2 <= 0.07 AND l_quantity@0 < 24.00, projection=[l_extendedprice@1, l_discount@2]
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/5.parquet:<int>..<int>]]}, projection=[l_quantity, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1994-01-01 AND l_shipdate@10 < 1995-01-01 AND l_discount@6 >= 0.05 AND l_discount@6 <= 0.07 AND l_quantity@4 < 24.00, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1994-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-01-01 AND l_discount_null_count@5 != row_count@2 AND l_discount_max@4 >= 0.05 AND l_discount_null_count@5 != row_count@2 AND l_discount_min@6 <= 0.07 AND l_quantity_null_count@8 != row_count@2 AND l_quantity_min@7 < 24.00, required_guarantees=[]
          │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/16.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/7.parquet:<int>..<int>]]}, projection=[l_quantity, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1994-01-01 AND l_shipdate@10 < 1995-01-01 AND l_discount@6 >= 0.05 AND l_discount@6 <= 0.07 AND l_quantity@4 < 24.00, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1994-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-01-01 AND l_discount_null_count@5 != row_count@2 AND l_discount_max@4 >= 0.05 AND l_discount_null_count@5 != row_count@2 AND l_discount_min@6 <= 0.07 AND l_quantity_null_count@8 != row_count@2 AND l_quantity_min@7 < 24.00, required_guarantees=[]
          │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/8.parquet:<int>..<int>]]}, projection=[l_quantity, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1994-01-01 AND l_shipdate@10 < 1995-01-01 AND l_discount@6 >= 0.05 AND l_discount@6 <= 0.07 AND l_quantity@4 < 24.00, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1994-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-01-01 AND l_discount_null_count@5 != row_count@2 AND l_discount_max@4 >= 0.05 AND l_discount_null_count@5 != row_count@2 AND l_discount_min@6 <= 0.07 AND l_quantity_null_count@8 != row_count@2 AND l_quantity_min@7 < 24.00, required_guarantees=[]
          │       t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/9.parquet:<int>..<int>]]}, projection=[l_quantity, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1994-01-01 AND l_shipdate@10 < 1995-01-01 AND l_discount@6 >= 0.05 AND l_discount@6 <= 0.07 AND l_quantity@4 < 24.00, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1994-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-01-01 AND l_discount_null_count@5 != row_count@2 AND l_discount_max@4 >= 0.05 AND l_discount_null_count@5 != row_count@2 AND l_discount_min@6 <= 0.07 AND l_quantity_null_count@8 != row_count@2 AND l_quantity_min@7 < 24.00, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_10_two_phase() -> Result<(), Box<dyn Error>> {
        let plan = run_two_phase_plan_test("q10").await?;
        assert_snapshot!(plan, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [revenue@2 DESC]
        │   [Stage 5] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 5 ── tasks=3, partitions=3
          │ ProjectionExec: expr=[c_custkey@0 as c_custkey, c_name@1 as c_name, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@7 as revenue, c_acctbal@2 as c_acctbal, n_name@4 as n_name, c_address@5 as c_address, c_phone@3 as c_phone, c_comment@6 as c_comment]
          │   SortExec: expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@7 DESC], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[c_custkey@0 as c_custkey, c_name@1 as c_name, c_acctbal@2 as c_acctbal, c_phone@3 as c_phone, n_name@4 as n_name, c_address@5 as c_address, c_comment@6 as c_comment], aggr=[sum(lineitem.l_extendedprice * 1 - lineitem.l_discount) as sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
          │       RepartitionExec: partitioning=Hash([c_custkey@0, c_name@1, c_acctbal@2, c_phone@3, n_name@4, c_address@5, c_comment@6], 3), input_partitions=4
          │         [Stage 4] => NetworkShuffleExec: output_partitions=4, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 4 ── tasks=4, partitions=3
            │ RepartitionExec: partitioning=Hash([c_custkey@0, c_name@1, c_acctbal@2, c_phone@3, n_name@4, c_address@5, c_comment@6, 5871781006564002453], 3), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[c_custkey@0 as c_custkey, c_name@1 as c_name, c_acctbal@4 as c_acctbal, c_phone@3 as c_phone, n_name@8 as n_name, c_address@2 as c_address, c_comment@5 as c_comment], aggr=[sum(lineitem.l_extendedprice * 1 - lineitem.l_discount) as sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
            │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, c_nationkey@3)], projection=[c_custkey@2, c_name@3, c_address@4, c_phone@6, c_acctbal@7, c_comment@8, l_extendedprice@9, l_discount@10, n_name@1]
            │       CoalescePartitionsExec
            │         [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=12, input_tasks=4
            │       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@7, l_orderkey@0)], projection=[c_custkey@0, c_name@1, c_address@2, c_nationkey@3, c_phone@4, c_acctbal@5, c_comment@6, l_extendedprice@9, l_discount@10]
            │         CoalescePartitionsExec
            │           [Stage 3] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=12, input_tasks=4
            │         FilterExec: l_returnflag@3 = R, projection=[l_orderkey@0, l_extendedprice@1, l_discount@2]
            │           DistributedLeafExec:
            │             t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/5.parquet:<int>..<int>]]}, projection=[l_orderkey, l_extendedprice, l_discount, l_returnflag], file_type=parquet, predicate=l_returnflag@8 = R AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=l_returnflag_null_count@2 != row_count@3 AND l_returnflag_min@0 <= R AND R <= l_returnflag_max@1, required_guarantees=[l_returnflag in (R)]
            │             t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/16.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/7.parquet:<int>..<int>]]}, projection=[l_orderkey, l_extendedprice, l_discount, l_returnflag], file_type=parquet, predicate=l_returnflag@8 = R AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=l_returnflag_null_count@2 != row_count@3 AND l_returnflag_min@0 <= R AND R <= l_returnflag_max@1, required_guarantees=[l_returnflag in (R)]
            │             t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/8.parquet:<int>..<int>]]}, projection=[l_orderkey, l_extendedprice, l_discount, l_returnflag], file_type=parquet, predicate=l_returnflag@8 = R AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=l_returnflag_null_count@2 != row_count@3 AND l_returnflag_min@0 <= R AND R <= l_returnflag_max@1, required_guarantees=[l_returnflag in (R)]
            │             t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/lineitem/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/lineitem/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/lineitem/9.parquet:<int>..<int>]]}, projection=[l_orderkey, l_extendedprice, l_discount, l_returnflag], file_type=parquet, predicate=l_returnflag@8 = R AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=l_returnflag_null_count@2 != row_count@3 AND l_returnflag_min@0 <= R AND R <= l_returnflag_max@1, required_guarantees=[l_returnflag in (R)]
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=4, partitions=48
              │ BroadcastExec: input_partitions=3, consumer_tasks=4, output_partitions=12
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/nation/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/6.parquet:<int>..<int>]]}, projection=[n_nationkey, n_name], file_type=parquet
              │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/nation/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/2.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/7.parquet:<int>..<int>]]}, projection=[n_nationkey, n_name], file_type=parquet
              │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/nation/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/8.parquet:<int>..<int>]]}, projection=[n_nationkey, n_name], file_type=parquet
              │     t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/nation/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/nation/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/nation/9.parquet:<int>..<int>]]}, projection=[n_nationkey, n_name], file_type=parquet
              └──────────────────────────────────────────────────
              ┌───── Stage 3 ── tasks=4, partitions=48
              │ BroadcastExec: input_partitions=3, consumer_tasks=4, output_partitions=12
              │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_custkey@1, c_custkey@0)], projection=[c_custkey@2, c_name@3, c_address@4, c_nationkey@5, c_phone@6, c_acctbal@7, c_comment@8, o_orderkey@0]
              │     CoalescePartitionsExec
              │       [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=12, input_tasks=4
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/6.parquet:<int>..<int>]]}, projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/16.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/7.parquet:<int>..<int>]]}, projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/8.parquet:<int>..<int>]]}, projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              │       t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/customer/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/customer/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/customer/9.parquet:<int>..<int>]]}, projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              └──────────────────────────────────────────────────
                ┌───── Stage 2 ── tasks=4, partitions=48
                │ BroadcastExec: input_partitions=3, consumer_tasks=4, output_partitions=12
                │   FilterExec: o_orderdate@2 >= 1993-10-01 AND o_orderdate@2 < 1994-01-01, projection=[o_orderkey@0, o_custkey@1]
                │     DistributedLeafExec:
                │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/1.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/10.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/15.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/5.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1993-10-01 AND o_orderdate@4 < 1994-01-01, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1993-10-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1994-01-01, required_guarantees=[]
                │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/10.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/11.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/15.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/16.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/2.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/5.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/6.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/7.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1993-10-01 AND o_orderdate@4 < 1994-01-01, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1993-10-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1994-01-01, required_guarantees=[]
                │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/11.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/12.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/13.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/2.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/3.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/7.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/8.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1993-10-01 AND o_orderdate@4 < 1994-01-01, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1993-10-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1994-01-01, required_guarantees=[]
                │       t3: DataSourceExec: file_groups={3 groups: [[/testdata/tpch/plan_sf0.02/orders/13.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/14.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/14.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/3.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/4.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/4.parquet:<int>..<int>], [/testdata/tpch/plan_sf0.02/orders/8.parquet:<int>..<int>, /testdata/tpch/plan_sf0.02/orders/9.parquet:<int>..<int>]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1993-10-01 AND o_orderdate@4 < 1994-01-01, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1993-10-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1994-01-01, required_guarantees=[]
                └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    async fn run_two_phase_plan_test(query_id: &str) -> Result<String, Box<dyn Error>> {
        let d_ctx = start_in_memory_context(NUM_WORKERS, DefaultSessionBuilder).await;
        let data_dir = ensure_plan_tpch_data(PLAN_SCALE_FACTOR, TPCH_DATA_PARTS).await;
        let sql = tpch::get_query(query_id)?;
        d_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = PLAN_PARTITIONS;
        let d_ctx = d_ctx
            .with_distributed_file_scan_config_bytes_per_partition(
                FILE_SCAN_CONFIG_BYTES_PER_PARTITION,
            )?
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?
            .with_distributed_broadcast_joins(true)?
            .with_distributed_two_step_shuffle_fanout_threshold(1)?;
        register_tables(&d_ctx, &data_dir).await?;
        let df = d_ctx.sql(&sql).await?;
        Ok(display_plan_ascii(
            df.create_physical_plan().await?.as_ref(),
            false,
        ))
    }

    // ── TwoPhase shuffle correctness ─────────────────────────────────────────

    #[tokio::test]
    async fn test_tpch_1_two_phase_correctness() -> Result<(), Box<dyn Error>> {
        run_two_phase_correctness_test(tpch::get_query("q1")?).await
    }

    #[tokio::test]
    async fn test_tpch_3_two_phase_correctness() -> Result<(), Box<dyn Error>> {
        run_two_phase_correctness_test(tpch::get_query("q3")?).await
    }

    #[tokio::test]
    async fn test_tpch_5_two_phase_correctness() -> Result<(), Box<dyn Error>> {
        run_two_phase_correctness_test(tpch::get_query("q5")?).await
    }

    #[tokio::test]
    async fn test_tpch_6_two_phase_correctness() -> Result<(), Box<dyn Error>> {
        run_two_phase_correctness_test(tpch::get_query("q6")?).await
    }

    #[tokio::test]
    async fn test_tpch_10_two_phase_correctness() -> Result<(), Box<dyn Error>> {
        let sql = tpch::get_query("q10")?;
        let sql = sql.replace("revenue desc", "revenue, c_acctbal desc");
        run_two_phase_correctness_test(sql).await
    }

    async fn run_two_phase_correctness_test(sql: String) -> Result<(), Box<dyn Error>> {
        let d_ctx = start_in_memory_context(NUM_WORKERS, DefaultSessionBuilder).await;
        let d_ctx = d_ctx
            .with_distributed_file_scan_config_bytes_per_partition(
                FILE_SCAN_CONFIG_BYTES_PER_PARTITION,
            )?
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?
            .with_distributed_broadcast_joins(true)?
            .with_distributed_two_step_shuffle_fanout_threshold(1)?;
        let results_d = run_tpch_query(d_ctx, sql.clone()).await?;
        let results_s = run_tpch_query(SessionContext::new(), sql).await?;
        pretty_assertions::assert_eq!(results_d.to_string(), results_s.to_string());
        Ok(())
    }

    async fn run_tpch_query(
        ctx: SessionContext,
        sql: String,
    ) -> Result<impl Display, Box<dyn Error>> {
        let data_dir =
            ensure_correctness_tpch_data(CORRECTNESS_SCALE_FACTOR, TPCH_DATA_PARTS).await;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = CORRECTNESS_PARTITIONS;
        register_tables(&ctx, &data_dir).await?;
        let stream = {
            let df = ctx.sql(&sql).await?;
            let plan = df.create_physical_plan().await?;
            execute_stream(plan.clone(), ctx.task_ctx())?
        };
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(arrow::util::pretty::pretty_format_batches(&batches)?)
    }

    // ── Data helpers ─────────────────────────────────────────────────────────

    static INIT_PLAN_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();
    static INIT_CORRECTNESS_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    async fn ensure_plan_tpch_data(sf: f64, parts: usize) -> std::path::PathBuf {
        let data_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("testdata/tpch/plan_sf{sf}"));
        INIT_PLAN_TPCH_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap() {
                    let output = DatasetOutput::new(data_dir.to_str().unwrap())
                        .await
                        .unwrap();
                    tpch::generate_data(&output, sf, parts, false)
                        .await
                        .expect("Failed to generate TPC-H data");
                }
            })
            .await;
        data_dir
    }

    async fn ensure_correctness_tpch_data(sf: f64, parts: usize) -> std::path::PathBuf {
        let data_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("testdata/tpch/correctness_sf{sf}"));
        INIT_CORRECTNESS_TPCH_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap() {
                    let output = DatasetOutput::new(data_dir.to_str().unwrap())
                        .await
                        .unwrap();
                    tpch::generate_data(&output, sf, parts, false)
                        .await
                        .expect("Failed to generate TPC-H data");
                }
            })
            .await;
        data_dir
    }
}
