#[cfg(all(feature = "integration", feature = "tpch", test))]
mod tests {
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::{benchmarks_common, tpch};
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, assert_snapshot, display_plan_ascii,
    };
    use std::error::Error;
    use std::fs;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const PARTITIONS: usize = 6;
    const TPCH_SCALE_FACTOR: f64 = 0.02;
    const TPCH_DATA_PARTS: i32 = 16;

    #[tokio::test]
    async fn test_tpch_1() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q1").await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [l_returnflag@0 ASC NULLS LAST, l_linestatus@1 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p5] t1:[p0..p5] 
          │ SortExec: expr=[l_returnflag@0 ASC NULLS LAST, l_linestatus@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus, sum(lineitem.l_quantity)@2 as sum_qty, sum(lineitem.l_extendedprice)@3 as sum_base_price, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@4 as sum_disc_price, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)@5 as sum_charge, avg(lineitem.l_quantity)@6 as avg_qty, avg(lineitem.l_extendedprice)@7 as avg_price, avg(lineitem.l_discount)@8 as avg_disc, count(Int64(1))@9 as count_order]
          │     AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[sum(lineitem.l_quantity), sum(lineitem.l_extendedprice), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax), avg(lineitem.l_quantity), avg(lineitem.l_extendedprice), avg(lineitem.l_discount), count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=6, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([l_returnflag@0, l_linestatus@1], 12), input_partitions=4
            │   AggregateExec: mode=Partial, gby=[l_returnflag@5 as l_returnflag, l_linestatus@6 as l_linestatus], aggr=[sum(lineitem.l_quantity), sum(lineitem.l_extendedprice), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax), avg(lineitem.l_quantity), avg(lineitem.l_extendedprice), avg(lineitem.l_discount), count(Int64(1))]
            │     ProjectionExec: expr=[l_extendedprice@1 * (Some(1),20,0 - l_discount@2) as __common_expr_1, l_quantity@0 as l_quantity, l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, l_tax@3 as l_tax, l_returnflag@4 as l_returnflag, l_linestatus@5 as l_linestatus]
            │       FilterExec: l_shipdate@6 <= 1998-09-02, projection=[l_quantity@0, l_extendedprice@1, l_discount@2, l_tax@3, l_returnflag@4, l_linestatus@5]
            │         PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
            │           DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/lineitem/1.parquet], [/testdata/tpch/plan_sf0.02/lineitem/10.parquet], [/testdata/tpch/plan_sf0.02/lineitem/11.parquet], [/testdata/tpch/plan_sf0.02/lineitem/12.parquet], [/testdata/tpch/plan_sf0.02/lineitem/13.parquet], ...]}, projection=[l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate], file_type=parquet, predicate=l_shipdate@10 <= 1998-09-02, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@0 <= 1998-09-02, required_guarantees=[]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_2() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q2").await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [s_acctbal@0 DESC, n_name@2 ASC NULLS LAST, s_name@1 ASC NULLS LAST, p_partkey@3 ASC NULLS LAST]
        │   [Stage 11] => NetworkCoalesceExec: output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 11 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] t3:[p0..p5] 
          │ SortExec: expr=[s_acctbal@0 DESC, n_name@2 ASC NULLS LAST, s_name@1 ASC NULLS LAST, p_partkey@3 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[s_acctbal@5 as s_acctbal, s_name@2 as s_name, n_name@7 as n_name, p_partkey@0 as p_partkey, p_mfgr@1 as p_mfgr, s_address@3 as s_address, s_phone@4 as s_phone, s_comment@6 as s_comment]
          │     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(p_partkey@0, ps_partkey@1), (ps_supplycost@7, min(partsupp.ps_supplycost)@0)], projection=[p_partkey@0, p_mfgr@1, s_name@2, s_address@3, s_phone@4, s_acctbal@5, s_comment@6, n_name@8]
          │       [Stage 5] => NetworkShuffleExec: output_partitions=6, input_tasks=4
          │       [Stage 10] => NetworkShuffleExec: output_partitions=6, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 5 ── Tasks: t0:[p0..p23] t1:[p0..p23] t2:[p0..p23] t3:[p0..p23] 
            │ RepartitionExec: partitioning=Hash([p_partkey@0, ps_supplycost@7], 24), input_partitions=4
            │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(r_regionkey@0, n_regionkey@9)], projection=[p_partkey@1, p_mfgr@2, s_name@3, s_address@4, s_phone@5, s_acctbal@6, s_comment@7, ps_supplycost@8, n_name@9]
            │     CoalescePartitionsExec
            │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=16, input_tasks=4
            │     ProjectionExec: expr=[p_partkey@2 as p_partkey, p_mfgr@3 as p_mfgr, s_name@4 as s_name, s_address@5 as s_address, s_phone@6 as s_phone, s_acctbal@7 as s_acctbal, s_comment@8 as s_comment, ps_supplycost@9 as ps_supplycost, n_name@0 as n_name, n_regionkey@1 as n_regionkey]
            │       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@4)], projection=[n_name@1, n_regionkey@2, p_partkey@3, p_mfgr@4, s_name@5, s_address@6, s_phone@8, s_acctbal@9, s_comment@10, ps_supplycost@11]
            │         CoalescePartitionsExec
            │           [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=16, input_tasks=4
            │         ProjectionExec: expr=[p_partkey@6 as p_partkey, p_mfgr@7 as p_mfgr, s_name@0 as s_name, s_address@1 as s_address, s_nationkey@2 as s_nationkey, s_phone@3 as s_phone, s_acctbal@4 as s_acctbal, s_comment@5 as s_comment, ps_supplycost@8 as ps_supplycost]
            │           HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, ps_suppkey@2)], projection=[s_name@1, s_address@2, s_nationkey@3, s_phone@4, s_acctbal@5, s_comment@6, p_partkey@7, p_mfgr@8, ps_supplycost@10]
            │             CoalescePartitionsExec
            │               [Stage 3] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=16, input_tasks=4
            │             HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@0, ps_partkey@0)], projection=[p_partkey@0, p_mfgr@1, ps_suppkey@3, ps_supplycost@4]
            │               CoalescePartitionsExec
            │                 [Stage 4] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=16, input_tasks=4
            │               PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
            │                 DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/partsupp/1.parquet], [/testdata/tpch/plan_sf0.02/partsupp/10.parquet], [/testdata/tpch/plan_sf0.02/partsupp/11.parquet], [/testdata/tpch/plan_sf0.02/partsupp/12.parquet], [/testdata/tpch/plan_sf0.02/partsupp/13.parquet], ...]}, projection=[ps_partkey, ps_suppkey, ps_supplycost], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ]
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
              │ BroadcastExec: input_partitions=4, consumer_tasks=4, output_partitions=16
              │   FilterExec: r_name@1 = EUROPE, projection=[r_regionkey@0]
              │     PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
              │       DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/region/1.parquet], [/testdata/tpch/plan_sf0.02/region/10.parquet], [/testdata/tpch/plan_sf0.02/region/11.parquet], [/testdata/tpch/plan_sf0.02/region/12.parquet], [/testdata/tpch/plan_sf0.02/region/13.parquet], ...]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = EUROPE, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= EUROPE AND EUROPE <= r_name_max@1, required_guarantees=[r_name in (EUROPE)]
              └──────────────────────────────────────────────────
              ┌───── Stage 2 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
              │ BroadcastExec: input_partitions=4, consumer_tasks=4, output_partitions=16
              │   PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
              │     DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/nation/1.parquet], [/testdata/tpch/plan_sf0.02/nation/10.parquet], [/testdata/tpch/plan_sf0.02/nation/11.parquet], [/testdata/tpch/plan_sf0.02/nation/12.parquet], [/testdata/tpch/plan_sf0.02/nation/13.parquet], ...]}, projection=[n_nationkey, n_name, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ]
              └──────────────────────────────────────────────────
              ┌───── Stage 3 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
              │ BroadcastExec: input_partitions=4, consumer_tasks=4, output_partitions=16
              │   PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
              │     DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/supplier/1.parquet], [/testdata/tpch/plan_sf0.02/supplier/10.parquet], [/testdata/tpch/plan_sf0.02/supplier/11.parquet], [/testdata/tpch/plan_sf0.02/supplier/12.parquet], [/testdata/tpch/plan_sf0.02/supplier/13.parquet], ...]}, projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment], file_type=parquet, predicate=DynamicFilter [ empty ]
              └──────────────────────────────────────────────────
              ┌───── Stage 4 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
              │ BroadcastExec: input_partitions=4, consumer_tasks=4, output_partitions=16
              │   FilterExec: p_size@3 = 15 AND p_type@2 LIKE %BRASS, projection=[p_partkey@0, p_mfgr@1]
              │     PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
              │       DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/part/1.parquet], [/testdata/tpch/plan_sf0.02/part/10.parquet], [/testdata/tpch/plan_sf0.02/part/11.parquet], [/testdata/tpch/plan_sf0.02/part/12.parquet], [/testdata/tpch/plan_sf0.02/part/13.parquet], ...]}, projection=[p_partkey, p_mfgr, p_type, p_size], file_type=parquet, predicate=p_size@5 = 15 AND p_type@4 LIKE %BRASS, pruning_predicate=p_size_null_count@2 != row_count@3 AND p_size_min@0 <= 15 AND 15 <= p_size_max@1, required_guarantees=[p_size in (15)]
              └──────────────────────────────────────────────────
            ┌───── Stage 10 ── Tasks: t0:[p0..p23] t1:[p0..p23] t2:[p0..p23] 
            │ RepartitionExec: partitioning=Hash([ps_partkey@1, min(partsupp.ps_supplycost)@0], 24), input_partitions=6
            │   ProjectionExec: expr=[min(partsupp.ps_supplycost)@1 as min(partsupp.ps_supplycost), ps_partkey@0 as ps_partkey]
            │     AggregateExec: mode=FinalPartitioned, gby=[ps_partkey@0 as ps_partkey], aggr=[min(partsupp.ps_supplycost)]
            │       [Stage 9] => NetworkShuffleExec: output_partitions=6, input_tasks=4
            └──────────────────────────────────────────────────
              ┌───── Stage 9 ── Tasks: t0:[p0..p17] t1:[p0..p17] t2:[p0..p17] t3:[p0..p17] 
              │ RepartitionExec: partitioning=Hash([ps_partkey@0], 18), input_partitions=4
              │   AggregateExec: mode=Partial, gby=[ps_partkey@0 as ps_partkey], aggr=[min(partsupp.ps_supplycost)]
              │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(r_regionkey@0, n_regionkey@2)], projection=[ps_partkey@1, ps_supplycost@2]
              │       CoalescePartitionsExec
              │         [Stage 6] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=16, input_tasks=4
              │       ProjectionExec: expr=[ps_partkey@1 as ps_partkey, ps_supplycost@2 as ps_supplycost, n_regionkey@0 as n_regionkey]
              │         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@2)], projection=[n_regionkey@1, ps_partkey@2, ps_supplycost@3]
              │           CoalescePartitionsExec
              │             [Stage 7] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=16, input_tasks=4
              │           ProjectionExec: expr=[ps_partkey@1 as ps_partkey, ps_supplycost@2 as ps_supplycost, s_nationkey@0 as s_nationkey]
              │             HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, ps_suppkey@1)], projection=[s_nationkey@1, ps_partkey@2, ps_supplycost@4]
              │               CoalescePartitionsExec
              │                 [Stage 8] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=16, input_tasks=4
              │               PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
              │                 DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/partsupp/1.parquet], [/testdata/tpch/plan_sf0.02/partsupp/10.parquet], [/testdata/tpch/plan_sf0.02/partsupp/11.parquet], [/testdata/tpch/plan_sf0.02/partsupp/12.parquet], [/testdata/tpch/plan_sf0.02/partsupp/13.parquet], ...]}, projection=[ps_partkey, ps_suppkey, ps_supplycost], file_type=parquet, predicate=DynamicFilter [ empty ]
              └──────────────────────────────────────────────────
                ┌───── Stage 6 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
                │ BroadcastExec: input_partitions=4, consumer_tasks=4, output_partitions=16
                │   FilterExec: r_name@1 = EUROPE, projection=[r_regionkey@0]
                │     PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
                │       DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/region/1.parquet], [/testdata/tpch/plan_sf0.02/region/10.parquet], [/testdata/tpch/plan_sf0.02/region/11.parquet], [/testdata/tpch/plan_sf0.02/region/12.parquet], [/testdata/tpch/plan_sf0.02/region/13.parquet], ...]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = EUROPE, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= EUROPE AND EUROPE <= r_name_max@1, required_guarantees=[r_name in (EUROPE)]
                └──────────────────────────────────────────────────
                ┌───── Stage 7 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
                │ BroadcastExec: input_partitions=4, consumer_tasks=4, output_partitions=16
                │   PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
                │     DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/nation/1.parquet], [/testdata/tpch/plan_sf0.02/nation/10.parquet], [/testdata/tpch/plan_sf0.02/nation/11.parquet], [/testdata/tpch/plan_sf0.02/nation/12.parquet], [/testdata/tpch/plan_sf0.02/nation/13.parquet], ...]}, projection=[n_nationkey, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                └──────────────────────────────────────────────────
                ┌───── Stage 8 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
                │ BroadcastExec: input_partitions=4, consumer_tasks=4, output_partitions=16
                │   PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
                │     DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/supplier/1.parquet], [/testdata/tpch/plan_sf0.02/supplier/10.parquet], [/testdata/tpch/plan_sf0.02/supplier/11.parquet], [/testdata/tpch/plan_sf0.02/supplier/12.parquet], [/testdata/tpch/plan_sf0.02/supplier/13.parquet], ...]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_6() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q6").await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[sum(lineitem.l_extendedprice * lineitem.l_discount)@0 as revenue]
        │   AggregateExec: mode=Final, gby=[], aggr=[sum(lineitem.l_extendedprice * lineitem.l_discount)]
        │     CoalescePartitionsExec
        │       [Stage 1] => NetworkCoalesceExec: output_partitions=16, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p4..p7] t2:[p8..p11] t3:[p12..p15] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[sum(lineitem.l_extendedprice * lineitem.l_discount)]
          │   FilterExec: l_shipdate@3 >= 1994-01-01 AND l_shipdate@3 < 1995-01-01 AND l_discount@2 >= Some(5),15,2 AND l_discount@2 <= Some(7),15,2 AND l_quantity@0 < Some(2400),15,2, projection=[l_extendedprice@1, l_discount@2]
          │     PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
          │       DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/lineitem/1.parquet], [/testdata/tpch/plan_sf0.02/lineitem/10.parquet], [/testdata/tpch/plan_sf0.02/lineitem/11.parquet], [/testdata/tpch/plan_sf0.02/lineitem/12.parquet], [/testdata/tpch/plan_sf0.02/lineitem/13.parquet], ...]}, projection=[l_quantity, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1994-01-01 AND l_shipdate@10 < 1995-01-01 AND l_discount@6 >= Some(5),15,2 AND l_discount@6 <= Some(7),15,2 AND l_quantity@4 < Some(2400),15,2, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1994-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-01-01 AND l_discount_null_count@5 != row_count@2 AND l_discount_max@4 >= Some(5),15,2 AND l_discount_null_count@5 != row_count@2 AND l_discount_min@6 <= Some(7),15,2 AND l_quantity_null_count@8 != row_count@2 AND l_quantity_min@7 < Some(2400),15,2, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_21() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q21").await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [numwait@1 DESC, s_name@0 ASC NULLS LAST]
        │   SortExec: expr=[numwait@1 DESC, s_name@0 ASC NULLS LAST], preserve_partitioning=[true]
        │     ProjectionExec: expr=[s_name@0 as s_name, count(Int64(1))@1 as numwait]
        │       AggregateExec: mode=FinalPartitioned, gby=[s_name@0 as s_name], aggr=[count(Int64(1))]
        │         RepartitionExec: partitioning=Hash([s_name@0], 6), input_partitions=6
        │           AggregateExec: mode=Partial, gby=[s_name@0 as s_name], aggr=[count(Int64(1))]
        │             HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(l_orderkey@1, l_orderkey@0)], filter=l_suppkey@1 != l_suppkey@0, projection=[s_name@0]
        │               CoalescePartitionsExec
        │                 HashJoinExec: mode=CollectLeft, join_type=LeftSemi, on=[(l_orderkey@1, l_orderkey@0)], filter=l_suppkey@1 != l_suppkey@0
        │                   CoalescePartitionsExec
        │                     [Stage 4] => NetworkCoalesceExec: output_partitions=16, input_tasks=4
        │                   DataSourceExec: file_groups={6 groups: [[/testdata/tpch/plan_sf0.02/lineitem/1.parquet, /testdata/tpch/plan_sf0.02/lineitem/10.parquet, /testdata/tpch/plan_sf0.02/lineitem/11.parquet], [/testdata/tpch/plan_sf0.02/lineitem/12.parquet, /testdata/tpch/plan_sf0.02/lineitem/13.parquet, /testdata/tpch/plan_sf0.02/lineitem/14.parquet], [/testdata/tpch/plan_sf0.02/lineitem/15.parquet, /testdata/tpch/plan_sf0.02/lineitem/16.parquet, /testdata/tpch/plan_sf0.02/lineitem/2.parquet], [/testdata/tpch/plan_sf0.02/lineitem/3.parquet, /testdata/tpch/plan_sf0.02/lineitem/4.parquet, /testdata/tpch/plan_sf0.02/lineitem/5.parquet], [/testdata/tpch/plan_sf0.02/lineitem/6.parquet, /testdata/tpch/plan_sf0.02/lineitem/7.parquet, /testdata/tpch/plan_sf0.02/lineitem/8.parquet], ...]}, projection=[l_orderkey, l_suppkey], file_type=parquet
        │               FilterExec: l_receiptdate@3 > l_commitdate@2, projection=[l_orderkey@0, l_suppkey@1]
        │                 DataSourceExec: file_groups={6 groups: [[/testdata/tpch/plan_sf0.02/lineitem/1.parquet, /testdata/tpch/plan_sf0.02/lineitem/10.parquet, /testdata/tpch/plan_sf0.02/lineitem/11.parquet], [/testdata/tpch/plan_sf0.02/lineitem/12.parquet, /testdata/tpch/plan_sf0.02/lineitem/13.parquet, /testdata/tpch/plan_sf0.02/lineitem/14.parquet], [/testdata/tpch/plan_sf0.02/lineitem/15.parquet, /testdata/tpch/plan_sf0.02/lineitem/16.parquet, /testdata/tpch/plan_sf0.02/lineitem/2.parquet], [/testdata/tpch/plan_sf0.02/lineitem/3.parquet, /testdata/tpch/plan_sf0.02/lineitem/4.parquet, /testdata/tpch/plan_sf0.02/lineitem/5.parquet], [/testdata/tpch/plan_sf0.02/lineitem/6.parquet, /testdata/tpch/plan_sf0.02/lineitem/7.parquet, /testdata/tpch/plan_sf0.02/lineitem/8.parquet], ...]}, projection=[l_orderkey, l_suppkey, l_commitdate, l_receiptdate], file_type=parquet, predicate=l_receiptdate@12 > l_commitdate@11
        └──────────────────────────────────────────────────
          ┌───── Stage 4 ── Tasks: t0:[p0..p3] t1:[p4..p7] t2:[p8..p11] t3:[p12..p15] 
          │ HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@1)], projection=[s_name@1, l_orderkey@3, l_suppkey@4]
          │   CoalescePartitionsExec
          │     [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=16, input_tasks=4
          │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@0, l_orderkey@2)], projection=[s_name@1, s_nationkey@2, l_orderkey@3, l_suppkey@4]
          │     CoalescePartitionsExec
          │       [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=16, input_tasks=4
          │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, l_suppkey@1)], projection=[s_name@1, s_nationkey@2, l_orderkey@3, l_suppkey@4]
          │       CoalescePartitionsExec
          │         [Stage 3] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=16, input_tasks=4
          │       FilterExec: l_receiptdate@3 > l_commitdate@2, projection=[l_orderkey@0, l_suppkey@1]
          │         PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
          │           DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/lineitem/1.parquet], [/testdata/tpch/plan_sf0.02/lineitem/10.parquet], [/testdata/tpch/plan_sf0.02/lineitem/11.parquet], [/testdata/tpch/plan_sf0.02/lineitem/12.parquet], [/testdata/tpch/plan_sf0.02/lineitem/13.parquet], ...]}, projection=[l_orderkey, l_suppkey, l_commitdate, l_receiptdate], file_type=parquet, predicate=l_receiptdate@12 > l_commitdate@11 AND DynamicFilter [ empty ] AND DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
            │ BroadcastExec: input_partitions=4, consumer_tasks=4, output_partitions=16
            │   FilterExec: n_name@1 = SAUDI ARABIA, projection=[n_nationkey@0]
            │     PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
            │       DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/nation/1.parquet], [/testdata/tpch/plan_sf0.02/nation/10.parquet], [/testdata/tpch/plan_sf0.02/nation/11.parquet], [/testdata/tpch/plan_sf0.02/nation/12.parquet], [/testdata/tpch/plan_sf0.02/nation/13.parquet], ...]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = SAUDI ARABIA, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= SAUDI ARABIA AND SAUDI ARABIA <= n_name_max@1, required_guarantees=[n_name in (SAUDI ARABIA)]
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
            │ BroadcastExec: input_partitions=4, consumer_tasks=4, output_partitions=16
            │   FilterExec: o_orderstatus@1 = F, projection=[o_orderkey@0]
            │     PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
            │       DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/orders/1.parquet], [/testdata/tpch/plan_sf0.02/orders/10.parquet], [/testdata/tpch/plan_sf0.02/orders/11.parquet], [/testdata/tpch/plan_sf0.02/orders/12.parquet], [/testdata/tpch/plan_sf0.02/orders/13.parquet], ...]}, projection=[o_orderkey, o_orderstatus], file_type=parquet, predicate=o_orderstatus@2 = F, pruning_predicate=o_orderstatus_null_count@2 != row_count@3 AND o_orderstatus_min@0 <= F AND F <= o_orderstatus_max@1, required_guarantees=[o_orderstatus in (F)]
            └──────────────────────────────────────────────────
            ┌───── Stage 3 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
            │ BroadcastExec: input_partitions=4, consumer_tasks=4, output_partitions=16
            │   PartitionIsolatorExec: t0:[p0,p1,p2,p3,__,__,__,__,__,__,__,__,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3,__,__,__,__,__,__,__,__] t2:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,__,__,__,__] t3:[__,__,__,__,__,__,__,__,__,__,__,__,p0,p1,p2,p3]
            │     DataSourceExec: file_groups={16 groups: [[/testdata/tpch/plan_sf0.02/supplier/1.parquet], [/testdata/tpch/plan_sf0.02/supplier/10.parquet], [/testdata/tpch/plan_sf0.02/supplier/11.parquet], [/testdata/tpch/plan_sf0.02/supplier/12.parquet], [/testdata/tpch/plan_sf0.02/supplier/13.parquet], ...]}, projection=[s_suppkey, s_name, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    // test_tpch_query generates and displays a distributed plan for each TPC-H query.
    async fn test_tpch_query(query_id: &str) -> Result<String, Box<dyn Error>> {
        let (d_ctx, _guard, _) = start_localhost_context(4, DefaultSessionBuilder).await;
        let d_ctx = d_ctx.with_distributed_broadcast_joins(true)?;
        let data_dir = ensure_tpch_data(TPCH_SCALE_FACTOR, TPCH_DATA_PARTS).await;
        let sql = tpch::get_query(query_id)?;
        d_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = PARTITIONS;

        benchmarks_common::register_tables(&d_ctx, &data_dir).await?;

        // Query 15 has three queries in it, one creating the view, the second
        // executing, which we want to capture the output of, and the third
        // tearing down the view
        let plan = if query_id == "q15" {
            let queries: Vec<&str> = sql
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();

            d_ctx.sql(queries[0]).await?.collect().await?;
            let df = d_ctx.sql(queries[1]).await?;
            let plan = df.create_physical_plan().await?;
            d_ctx.sql(queries[2]).await?.collect().await?;
            plan
        } else {
            let df = d_ctx.sql(&sql).await?;
            df.create_physical_plan().await?
        };

        Ok(display_plan_ascii(plan.as_ref(), false))
    }

    // OnceCell to ensure TPCH tables are generated only once for tests
    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    // ensure_tpch_data initializes the TPCH data on disk.
    pub async fn ensure_tpch_data(sf: f64, parts: i32) -> std::path::PathBuf {
        let data_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("testdata/tpch/plan_sf{sf}"));
        INIT_TEST_TPCH_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap() {
                    tpch::generate_tpch_data(&data_dir, sf, parts);
                }
            })
            .await;
        data_dir
    }
}
