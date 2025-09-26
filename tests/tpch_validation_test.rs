#[cfg(all(feature = "integration", feature = "tpch", test))]
mod tests {
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::tpch;
    use datafusion_distributed::{
        DistributedPhysicalOptimizerRule, DistributedSessionBuilderContext, assert_snapshot,
    };
    use futures::TryStreamExt;
    use std::error::Error;
    use std::fs;
    use std::sync::Arc;
    use tokio::sync::OnceCell;

    const PARTITIONS: usize = 6;
    const SHUFFLE_TASKS: usize = 3;
    const COALESCE_TASKS: usize = 4;

    const TPCH_SCALE_FACTOR: f64 = 0.1;
    const TPCH_DATA_PARTS: i32 = 16;

    #[tokio::test]
    async fn test_tpch_1() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(1).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 3   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [l_returnflag@0 ASC NULLS LAST, l_linestatus@1 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 2, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[l_returnflag@0 ASC NULLS LAST, l_linestatus@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus, sum(lineitem.l_quantity)@2 as sum_qty, sum(lineitem.l_extendedprice)@3 as sum_base_price, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@4 as sum_disc_price, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)@5 as sum_charge, avg(lineitem.l_quantity)@6 as avg_qty, avg(lineitem.l_extendedprice)@7 as avg_price, avg(lineitem.l_discount)@8 as avg_disc, count(Int64(1))@9 as count_order]
          │       AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[sum(lineitem.l_quantity), sum(lineitem.l_extendedprice), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax), avg(lineitem.l_quantity), avg(lineitem.l_extendedprice), avg(lineitem.l_discount), count(Int64(1))]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 1, output_partitions=6, n_tasks=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([l_returnflag@0, l_linestatus@1], 24), input_partitions=2
            │     AggregateExec: mode=Partial, gby=[l_returnflag@5 as l_returnflag, l_linestatus@6 as l_linestatus], aggr=[sum(lineitem.l_quantity), sum(lineitem.l_extendedprice), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax), avg(lineitem.l_quantity), avg(lineitem.l_extendedprice), avg(lineitem.l_discount), count(Int64(1))]
            │       ProjectionExec: expr=[l_extendedprice@1 * (Some(1),20,0 - l_discount@2) as __common_expr_1, l_quantity@0 as l_quantity, l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, l_tax@3 as l_tax, l_returnflag@4 as l_returnflag, l_linestatus@5 as l_linestatus]
            │         CoalesceBatchesExec: target_batch_size=8192
            │           FilterExec: l_shipdate@6 <= 1998-09-02, projection=[l_quantity@0, l_extendedprice@1, l_discount@2, l_tax@3, l_returnflag@4, l_linestatus@5]
            │             PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
            │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate], file_type=parquet, predicate=l_shipdate@6 <= 1998-09-02, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@0 <= 1998-09-02, required_guarantees=[]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_2() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(2).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 8   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [s_acctbal@0 DESC, n_name@2 ASC NULLS LAST, s_name@1 ASC NULLS LAST, p_partkey@3 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 7, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 7   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[s_acctbal@0 DESC, n_name@2 ASC NULLS LAST, s_name@1 ASC NULLS LAST, p_partkey@3 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[s_acctbal@5 as s_acctbal, s_name@2 as s_name, n_name@7 as n_name, p_partkey@0 as p_partkey, p_mfgr@1 as p_mfgr, s_address@3 as s_address, s_phone@4 as s_phone, s_comment@6 as s_comment]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         HashJoinExec: mode=Partitioned, join_type=Inner, on=[(p_partkey@0, ps_partkey@1), (ps_supplycost@7, min(partsupp.ps_supplycost)@0)], projection=[p_partkey@0, p_mfgr@1, s_name@2, s_address@3, s_phone@4, s_acctbal@5, s_comment@6, n_name@8]
          │           CoalesceBatchesExec: target_batch_size=8192
          │             NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=4, input_tasks=1
          │           CoalesceBatchesExec: target_batch_size=8192
          │             NetworkShuffleExec read_from=Stage 6, output_partitions=6, n_tasks=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([p_partkey@0, ps_supplycost@7], 24), input_partitions=6
            │     CoalesceBatchesExec: target_batch_size=8192
            │       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(r_regionkey@0, n_regionkey@9)], projection=[p_partkey@1, p_mfgr@2, s_name@3, s_address@4, s_phone@5, s_acctbal@6, s_comment@7, ps_supplycost@8, n_name@9]
            │         CoalescePartitionsExec
            │           NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
            │         ProjectionExec: expr=[p_partkey@2 as p_partkey, p_mfgr@3 as p_mfgr, s_name@4 as s_name, s_address@5 as s_address, s_phone@6 as s_phone, s_acctbal@7 as s_acctbal, s_comment@8 as s_comment, ps_supplycost@9 as ps_supplycost, n_name@0 as n_name, n_regionkey@1 as n_regionkey]
            │           CoalesceBatchesExec: target_batch_size=8192
            │             HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@4)], projection=[n_name@1, n_regionkey@2, p_partkey@3, p_mfgr@4, s_name@5, s_address@6, s_phone@8, s_acctbal@9, s_comment@10, ps_supplycost@11]
            │               CoalescePartitionsExec
            │                 DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name, n_regionkey], file_type=parquet
            │               ProjectionExec: expr=[p_partkey@6 as p_partkey, p_mfgr@7 as p_mfgr, s_name@0 as s_name, s_address@1 as s_address, s_nationkey@2 as s_nationkey, s_phone@3 as s_phone, s_acctbal@4 as s_acctbal, s_comment@5 as s_comment, ps_supplycost@8 as ps_supplycost]
            │                 CoalesceBatchesExec: target_batch_size=8192
            │                   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, ps_suppkey@2)], projection=[s_name@1, s_address@2, s_nationkey@3, s_phone@4, s_acctbal@5, s_comment@6, p_partkey@7, p_mfgr@8, ps_supplycost@10]
            │                     CoalescePartitionsExec
            │                       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment], file_type=parquet
            │                     CoalesceBatchesExec: target_batch_size=8192
            │                       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@0, ps_partkey@0)], projection=[p_partkey@0, p_mfgr@1, ps_suppkey@3, ps_supplycost@4]
            │                         CoalescePartitionsExec
            │                           NetworkCoalesceExec read_from=Stage 2, output_partitions=8, input_tasks=4
            │                         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/partsupp/1.parquet:0..753508, /testdata/tpch/data/partsupp/10.parquet:0..748125, /testdata/tpch/data/partsupp/11.parquet:0..496934], [/testdata/tpch/data/partsupp/11.parquet:496934..744689, /testdata/tpch/data/partsupp/12.parquet:0..749068, /testdata/tpch/data/partsupp/13.parquet:0..750296, /testdata/tpch/data/partsupp/14.parquet:0..251448], [/testdata/tpch/data/partsupp/14.parquet:251448..748674, /testdata/tpch/data/partsupp/15.parquet:0..744298, /testdata/tpch/data/partsupp/16.parquet:0..748035, /testdata/tpch/data/partsupp/2.parquet:0..9008], [/testdata/tpch/data/partsupp/2.parquet:9008..753010, /testdata/tpch/data/partsupp/3.parquet:0..756405, /testdata/tpch/data/partsupp/4.parquet:0..498160], [/testdata/tpch/data/partsupp/4.parquet:498160..746724, /testdata/tpch/data/partsupp/5.parquet:0..746720, /testdata/tpch/data/partsupp/6.parquet:0..750184, /testdata/tpch/data/partsupp/7.parquet:0..253099], ...]}, projection=[ps_partkey, ps_suppkey, ps_supplycost], file_type=parquet
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: r_name@1 = EUROPE, projection=[r_regionkey@0]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/region/1.parquet, /testdata/tpch/data/region/10.parquet, /testdata/tpch/data/region/11.parquet], [/testdata/tpch/data/region/12.parquet, /testdata/tpch/data/region/13.parquet, /testdata/tpch/data/region/14.parquet], [/testdata/tpch/data/region/15.parquet, /testdata/tpch/data/region/16.parquet, /testdata/tpch/data/region/2.parquet], [/testdata/tpch/data/region/3.parquet, /testdata/tpch/data/region/4.parquet, /testdata/tpch/data/region/5.parquet], [/testdata/tpch/data/region/6.parquet, /testdata/tpch/data/region/7.parquet, /testdata/tpch/data/region/8.parquet], ...]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = EUROPE, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= EUROPE AND EUROPE <= r_name_max@1, required_guarantees=[r_name in (EUROPE)]
              └──────────────────────────────────────────────────
              ┌───── Stage 2   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: p_size@3 = 15 AND p_type@2 LIKE %BRASS, projection=[p_partkey@0, p_mfgr@1]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/part/1.parquet, /testdata/tpch/data/part/10.parquet, /testdata/tpch/data/part/11.parquet], [/testdata/tpch/data/part/12.parquet, /testdata/tpch/data/part/13.parquet, /testdata/tpch/data/part/14.parquet], [/testdata/tpch/data/part/15.parquet, /testdata/tpch/data/part/16.parquet, /testdata/tpch/data/part/2.parquet], [/testdata/tpch/data/part/3.parquet, /testdata/tpch/data/part/4.parquet, /testdata/tpch/data/part/5.parquet], [/testdata/tpch/data/part/6.parquet, /testdata/tpch/data/part/7.parquet, /testdata/tpch/data/part/8.parquet], ...]}, projection=[p_partkey, p_mfgr, p_type, p_size], file_type=parquet, predicate=p_size@3 = 15 AND p_type@2 LIKE %BRASS, pruning_predicate=p_size_null_count@2 != row_count@3 AND p_size_min@0 <= 15 AND 15 <= p_size_max@1, required_guarantees=[p_size in (15)]
              └──────────────────────────────────────────────────
            ┌───── Stage 6   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([ps_partkey@1, min(partsupp.ps_supplycost)@0], 24), input_partitions=6
            │     ProjectionExec: expr=[min(partsupp.ps_supplycost)@1 as min(partsupp.ps_supplycost), ps_partkey@0 as ps_partkey]
            │       AggregateExec: mode=FinalPartitioned, gby=[ps_partkey@0 as ps_partkey], aggr=[min(partsupp.ps_supplycost)]
            │         CoalesceBatchesExec: target_batch_size=8192
            │           NetworkShuffleExec read_from=Stage 5, output_partitions=6, n_tasks=3, input_tasks=1
            └──────────────────────────────────────────────────
              ┌───── Stage 5   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([ps_partkey@0], 18), input_partitions=6
              │     AggregateExec: mode=Partial, gby=[ps_partkey@0 as ps_partkey], aggr=[min(partsupp.ps_supplycost)]
              │       CoalesceBatchesExec: target_batch_size=8192
              │         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(r_regionkey@0, n_regionkey@2)], projection=[ps_partkey@1, ps_supplycost@2]
              │           CoalescePartitionsExec
              │             NetworkCoalesceExec read_from=Stage 4, output_partitions=8, input_tasks=4
              │           ProjectionExec: expr=[ps_partkey@1 as ps_partkey, ps_supplycost@2 as ps_supplycost, n_regionkey@0 as n_regionkey]
              │             CoalesceBatchesExec: target_batch_size=8192
              │               HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@2)], projection=[n_regionkey@1, ps_partkey@2, ps_supplycost@3]
              │                 CoalescePartitionsExec
              │                   DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_regionkey], file_type=parquet
              │                 ProjectionExec: expr=[ps_partkey@1 as ps_partkey, ps_supplycost@2 as ps_supplycost, s_nationkey@0 as s_nationkey]
              │                   CoalesceBatchesExec: target_batch_size=8192
              │                     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, ps_suppkey@1)], projection=[s_nationkey@1, ps_partkey@2, ps_supplycost@4]
              │                       CoalescePartitionsExec
              │                         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_nationkey], file_type=parquet
              │                       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/partsupp/1.parquet:0..753508, /testdata/tpch/data/partsupp/10.parquet:0..748125, /testdata/tpch/data/partsupp/11.parquet:0..496934], [/testdata/tpch/data/partsupp/11.parquet:496934..744689, /testdata/tpch/data/partsupp/12.parquet:0..749068, /testdata/tpch/data/partsupp/13.parquet:0..750296, /testdata/tpch/data/partsupp/14.parquet:0..251448], [/testdata/tpch/data/partsupp/14.parquet:251448..748674, /testdata/tpch/data/partsupp/15.parquet:0..744298, /testdata/tpch/data/partsupp/16.parquet:0..748035, /testdata/tpch/data/partsupp/2.parquet:0..9008], [/testdata/tpch/data/partsupp/2.parquet:9008..753010, /testdata/tpch/data/partsupp/3.parquet:0..756405, /testdata/tpch/data/partsupp/4.parquet:0..498160], [/testdata/tpch/data/partsupp/4.parquet:498160..746724, /testdata/tpch/data/partsupp/5.parquet:0..746720, /testdata/tpch/data/partsupp/6.parquet:0..750184, /testdata/tpch/data/partsupp/7.parquet:0..253099], ...]}, projection=[ps_partkey, ps_suppkey, ps_supplycost], file_type=parquet
              └──────────────────────────────────────────────────
                ┌───── Stage 4   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   CoalesceBatchesExec: target_batch_size=8192
                │     FilterExec: r_name@1 = EUROPE, projection=[r_regionkey@0]
                │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
                │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/region/1.parquet, /testdata/tpch/data/region/10.parquet, /testdata/tpch/data/region/11.parquet], [/testdata/tpch/data/region/12.parquet, /testdata/tpch/data/region/13.parquet, /testdata/tpch/data/region/14.parquet], [/testdata/tpch/data/region/15.parquet, /testdata/tpch/data/region/16.parquet, /testdata/tpch/data/region/2.parquet], [/testdata/tpch/data/region/3.parquet, /testdata/tpch/data/region/4.parquet, /testdata/tpch/data/region/5.parquet], [/testdata/tpch/data/region/6.parquet, /testdata/tpch/data/region/7.parquet, /testdata/tpch/data/region/8.parquet], ...]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = EUROPE, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= EUROPE AND EUROPE <= r_name_max@1, required_guarantees=[r_name in (EUROPE)]
                └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_3() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(3).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 4   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [revenue@1 DESC, o_orderdate@2 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 3, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[revenue@1 DESC, o_orderdate@2 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[l_orderkey@0 as l_orderkey, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@3 as revenue, o_orderdate@1 as o_orderdate, o_shippriority@2 as o_shippriority]
          │       AggregateExec: mode=FinalPartitioned, gby=[l_orderkey@0 as l_orderkey, o_orderdate@1 as o_orderdate, o_shippriority@2 as o_shippriority], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 2, output_partitions=6, n_tasks=4, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([l_orderkey@0, o_orderdate@1, o_shippriority@2], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[l_orderkey@2 as l_orderkey, o_orderdate@0 as o_orderdate, o_shippriority@1 as o_shippriority], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
            │       CoalesceBatchesExec: target_batch_size=8192
            │         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@0, l_orderkey@0)], projection=[o_orderdate@1, o_shippriority@2, l_orderkey@3, l_extendedprice@4, l_discount@5]
            │           CoalescePartitionsExec
            │             CoalesceBatchesExec: target_batch_size=8192
            │               HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(c_custkey@0, o_custkey@1)], projection=[o_orderkey@1, o_orderdate@3, o_shippriority@4]
            │                 CoalescePartitionsExec
            │                   NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
            │                 CoalesceBatchesExec: target_batch_size=8192
            │                   FilterExec: o_orderdate@2 < 1995-03-15
            │                     DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_custkey, o_orderdate, o_shippriority], file_type=parquet, predicate=o_orderdate@2 < 1995-03-15, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@0 < 1995-03-15, required_guarantees=[]
            │           CoalesceBatchesExec: target_batch_size=8192
            │             FilterExec: l_shipdate@3 > 1995-03-15, projection=[l_orderkey@0, l_extendedprice@1, l_discount@2]
            │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@3 > 1995-03-15, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 > 1995-03-15, required_guarantees=[]
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: c_mktsegment@1 = BUILDING, projection=[c_custkey@0]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/customer/1.parquet, /testdata/tpch/data/customer/10.parquet, /testdata/tpch/data/customer/11.parquet], [/testdata/tpch/data/customer/12.parquet, /testdata/tpch/data/customer/13.parquet, /testdata/tpch/data/customer/14.parquet], [/testdata/tpch/data/customer/15.parquet, /testdata/tpch/data/customer/16.parquet, /testdata/tpch/data/customer/2.parquet], [/testdata/tpch/data/customer/3.parquet, /testdata/tpch/data/customer/4.parquet, /testdata/tpch/data/customer/5.parquet], [/testdata/tpch/data/customer/6.parquet, /testdata/tpch/data/customer/7.parquet, /testdata/tpch/data/customer/8.parquet], ...]}, projection=[c_custkey, c_mktsegment], file_type=parquet, predicate=c_mktsegment@1 = BUILDING, pruning_predicate=c_mktsegment_null_count@2 != row_count@3 AND c_mktsegment_min@0 <= BUILDING AND BUILDING <= c_mktsegment_max@1, required_guarantees=[c_mktsegment in (BUILDING)]
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_4() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(4).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 4   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [o_orderpriority@0 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 3, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[o_orderpriority@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[o_orderpriority@0 as o_orderpriority, count(Int64(1))@1 as order_count]
          │       AggregateExec: mode=FinalPartitioned, gby=[o_orderpriority@0 as o_orderpriority], aggr=[count(Int64(1))]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 2, output_partitions=6, n_tasks=4, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([o_orderpriority@0], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[o_orderpriority@0 as o_orderpriority], aggr=[count(Int64(1))]
            │       CoalesceBatchesExec: target_batch_size=8192
            │         HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(l_orderkey@0, o_orderkey@0)], projection=[o_orderpriority@1]
            │           CoalescePartitionsExec
            │             NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
            │           CoalesceBatchesExec: target_batch_size=8192
            │             FilterExec: o_orderdate@1 >= 1993-07-01 AND o_orderdate@1 < 1993-10-01, projection=[o_orderkey@0, o_orderpriority@2]
            │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_orderdate, o_orderpriority], file_type=parquet, predicate=o_orderdate@1 >= 1993-07-01 AND o_orderdate@1 < 1993-10-01, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1993-07-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1993-10-01, required_guarantees=[]
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: l_receiptdate@2 > l_commitdate@1, projection=[l_orderkey@0]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_commitdate, l_receiptdate], file_type=parquet, predicate=l_receiptdate@2 > l_commitdate@1
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_5() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(5).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 5   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [revenue@1 DESC]
        │   NetworkCoalesceExec read_from=Stage 4, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[revenue@1 DESC], preserve_partitioning=[true]
          │     ProjectionExec: expr=[n_name@0 as n_name, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@1 as revenue]
          │       AggregateExec: mode=FinalPartitioned, gby=[n_name@0 as n_name], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=4, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([n_name@0], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[n_name@2 as n_name], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
            │       CoalesceBatchesExec: target_batch_size=8192
            │         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(r_regionkey@0, n_regionkey@3)], projection=[l_extendedprice@1, l_discount@2, n_name@3]
            │           CoalescePartitionsExec
            │             NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
            │           ProjectionExec: expr=[l_extendedprice@2 as l_extendedprice, l_discount@3 as l_discount, n_name@0 as n_name, n_regionkey@1 as n_regionkey]
            │             CoalesceBatchesExec: target_batch_size=8192
            │               HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@2)], projection=[n_name@1, n_regionkey@2, l_extendedprice@3, l_discount@4]
            │                 CoalescePartitionsExec
            │                   DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name, n_regionkey], file_type=parquet
            │                 ProjectionExec: expr=[l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, s_nationkey@0 as s_nationkey]
            │                   CoalesceBatchesExec: target_batch_size=8192
            │                     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, l_suppkey@1), (s_nationkey@1, c_nationkey@0)], projection=[s_nationkey@1, l_extendedprice@4, l_discount@5]
            │                       CoalescePartitionsExec
            │                         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_nationkey], file_type=parquet
            │                       CoalesceBatchesExec: target_batch_size=8192
            │                         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@1, l_orderkey@0)], projection=[c_nationkey@0, l_suppkey@3, l_extendedprice@4, l_discount@5]
            │                           CoalescePartitionsExec
            │                             ProjectionExec: expr=[c_nationkey@1 as c_nationkey, o_orderkey@0 as o_orderkey]
            │                               CoalesceBatchesExec: target_batch_size=8192
            │                                 HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_custkey@1, c_custkey@0)], projection=[o_orderkey@0, c_nationkey@3]
            │                                   CoalescePartitionsExec
            │                                     NetworkCoalesceExec read_from=Stage 2, output_partitions=8, input_tasks=4
            │                                   DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/customer/1.parquet, /testdata/tpch/data/customer/10.parquet, /testdata/tpch/data/customer/11.parquet], [/testdata/tpch/data/customer/12.parquet, /testdata/tpch/data/customer/13.parquet, /testdata/tpch/data/customer/14.parquet], [/testdata/tpch/data/customer/15.parquet, /testdata/tpch/data/customer/16.parquet, /testdata/tpch/data/customer/2.parquet], [/testdata/tpch/data/customer/3.parquet, /testdata/tpch/data/customer/4.parquet, /testdata/tpch/data/customer/5.parquet], [/testdata/tpch/data/customer/6.parquet, /testdata/tpch/data/customer/7.parquet, /testdata/tpch/data/customer/8.parquet], ...]}, projection=[c_custkey, c_nationkey], file_type=parquet
            │                           DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_suppkey, l_extendedprice, l_discount], file_type=parquet
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: r_name@1 = ASIA, projection=[r_regionkey@0]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/region/1.parquet, /testdata/tpch/data/region/10.parquet, /testdata/tpch/data/region/11.parquet], [/testdata/tpch/data/region/12.parquet, /testdata/tpch/data/region/13.parquet, /testdata/tpch/data/region/14.parquet], [/testdata/tpch/data/region/15.parquet, /testdata/tpch/data/region/16.parquet, /testdata/tpch/data/region/2.parquet], [/testdata/tpch/data/region/3.parquet, /testdata/tpch/data/region/4.parquet, /testdata/tpch/data/region/5.parquet], [/testdata/tpch/data/region/6.parquet, /testdata/tpch/data/region/7.parquet, /testdata/tpch/data/region/8.parquet], ...]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = ASIA, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= ASIA AND ASIA <= r_name_max@1, required_guarantees=[r_name in (ASIA)]
              └──────────────────────────────────────────────────
              ┌───── Stage 2   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: o_orderdate@2 >= 1994-01-01 AND o_orderdate@2 < 1995-01-01, projection=[o_orderkey@0, o_custkey@1]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@2 >= 1994-01-01 AND o_orderdate@2 < 1995-01-01, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1994-01-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1995-01-01, required_guarantees=[]
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_6() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(6).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 2   Tasks: t0:[p0] 
        │ ProjectionExec: expr=[sum(lineitem.l_extendedprice * lineitem.l_discount)@0 as revenue]
        │   AggregateExec: mode=Final, gby=[], aggr=[sum(lineitem.l_extendedprice * lineitem.l_discount)]
        │     CoalescePartitionsExec
        │       NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   AggregateExec: mode=Partial, gby=[], aggr=[sum(lineitem.l_extendedprice * lineitem.l_discount)]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: l_shipdate@3 >= 1994-01-01 AND l_shipdate@3 < 1995-01-01 AND l_discount@2 >= Some(5),15,2 AND l_discount@2 <= Some(7),15,2 AND l_quantity@0 < Some(2400),15,2, projection=[l_extendedprice@1, l_discount@2]
          │         PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
          │           DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_quantity, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@3 >= 1994-01-01 AND l_shipdate@3 < 1995-01-01 AND l_discount@2 >= Some(5),15,2 AND l_discount@2 <= Some(7),15,2 AND l_quantity@0 < Some(2400),15,2, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1994-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-01-01 AND l_discount_null_count@5 != row_count@2 AND l_discount_max@4 >= Some(5),15,2 AND l_discount_null_count@5 != row_count@2 AND l_discount_min@6 <= Some(7),15,2 AND l_quantity_null_count@8 != row_count@2 AND l_quantity_min@7 < Some(2400),15,2, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_7() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(7).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 5   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [supp_nation@0 ASC NULLS LAST, cust_nation@1 ASC NULLS LAST, l_year@2 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 4, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[supp_nation@0 ASC NULLS LAST, cust_nation@1 ASC NULLS LAST, l_year@2 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[supp_nation@0 as supp_nation, cust_nation@1 as cust_nation, l_year@2 as l_year, sum(shipping.volume)@3 as revenue]
          │       AggregateExec: mode=FinalPartitioned, gby=[supp_nation@0 as supp_nation, cust_nation@1 as cust_nation, l_year@2 as l_year], aggr=[sum(shipping.volume)]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=4, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([supp_nation@0, cust_nation@1, l_year@2], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[supp_nation@0 as supp_nation, cust_nation@1 as cust_nation, l_year@2 as l_year], aggr=[sum(shipping.volume)]
            │       ProjectionExec: expr=[n_name@4 as supp_nation, n_name@0 as cust_nation, date_part(YEAR, l_shipdate@3) as l_year, l_extendedprice@1 * (Some(1),20,0 - l_discount@2) as volume]
            │         CoalesceBatchesExec: target_batch_size=8192
            │           HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, c_nationkey@3)], filter=n_name@0 = FRANCE AND n_name@1 = GERMANY OR n_name@0 = GERMANY AND n_name@1 = FRANCE, projection=[n_name@1, l_extendedprice@2, l_discount@3, l_shipdate@4, n_name@6]
            │             CoalescePartitionsExec
            │               NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
            │             ProjectionExec: expr=[l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, l_shipdate@3 as l_shipdate, c_nationkey@4 as c_nationkey, n_name@0 as n_name]
            │               CoalesceBatchesExec: target_batch_size=8192
            │                 HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@0)], projection=[n_name@1, l_extendedprice@3, l_discount@4, l_shipdate@5, c_nationkey@6]
            │                   CoalescePartitionsExec
            │                     NetworkCoalesceExec read_from=Stage 2, output_partitions=8, input_tasks=4
            │                   CoalesceBatchesExec: target_batch_size=8192
            │                     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_custkey@4, c_custkey@0)], projection=[s_nationkey@0, l_extendedprice@1, l_discount@2, l_shipdate@3, c_nationkey@6]
            │                       CoalescePartitionsExec
            │                         CoalesceBatchesExec: target_batch_size=8192
            │                           HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(l_orderkey@1, o_orderkey@0)], projection=[s_nationkey@0, l_extendedprice@2, l_discount@3, l_shipdate@4, o_custkey@6]
            │                             CoalescePartitionsExec
            │                               CoalesceBatchesExec: target_batch_size=8192
            │                                 HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, l_suppkey@1)], projection=[s_nationkey@1, l_orderkey@2, l_extendedprice@4, l_discount@5, l_shipdate@6]
            │                                   CoalescePartitionsExec
            │                                     DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_nationkey], file_type=parquet
            │                                   CoalesceBatchesExec: target_batch_size=8192
            │                                     FilterExec: l_shipdate@4 >= 1995-01-01 AND l_shipdate@4 <= 1996-12-31
            │                                       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@4 >= 1995-01-01 AND l_shipdate@4 <= 1996-12-31, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1995-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 <= 1996-12-31, required_guarantees=[]
            │                             DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_custkey], file_type=parquet
            │                       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/customer/1.parquet, /testdata/tpch/data/customer/10.parquet, /testdata/tpch/data/customer/11.parquet], [/testdata/tpch/data/customer/12.parquet, /testdata/tpch/data/customer/13.parquet, /testdata/tpch/data/customer/14.parquet], [/testdata/tpch/data/customer/15.parquet, /testdata/tpch/data/customer/16.parquet, /testdata/tpch/data/customer/2.parquet], [/testdata/tpch/data/customer/3.parquet, /testdata/tpch/data/customer/4.parquet, /testdata/tpch/data/customer/5.parquet], [/testdata/tpch/data/customer/6.parquet, /testdata/tpch/data/customer/7.parquet, /testdata/tpch/data/customer/8.parquet], ...]}, projection=[c_custkey, c_nationkey], file_type=parquet
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: n_name@1 = GERMANY OR n_name@1 = FRANCE
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = GERMANY OR n_name@1 = FRANCE, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= GERMANY AND GERMANY <= n_name_max@1 OR n_name_null_count@2 != row_count@3 AND n_name_min@0 <= FRANCE AND FRANCE <= n_name_max@1, required_guarantees=[n_name in (FRANCE, GERMANY)]
              └──────────────────────────────────────────────────
              ┌───── Stage 2   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: n_name@1 = FRANCE OR n_name@1 = GERMANY
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = FRANCE OR n_name@1 = GERMANY, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= FRANCE AND FRANCE <= n_name_max@1 OR n_name_null_count@2 != row_count@3 AND n_name_min@0 <= GERMANY AND GERMANY <= n_name_max@1, required_guarantees=[n_name in (FRANCE, GERMANY)]
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_8() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(8).await?;
        assert_snapshot!(plan, @r#"
        ┌───── Stage 9   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [o_year@0 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 8, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 8   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[o_year@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[o_year@0 as o_year, sum(CASE WHEN all_nations.nation = Utf8("BRAZIL") THEN all_nations.volume ELSE Int64(0) END)@1 / sum(all_nations.volume)@2 as mkt_share]
          │       AggregateExec: mode=FinalPartitioned, gby=[o_year@0 as o_year], aggr=[sum(CASE WHEN all_nations.nation = Utf8("BRAZIL") THEN all_nations.volume ELSE Int64(0) END), sum(all_nations.volume)]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 7, output_partitions=6, n_tasks=4, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 7   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([o_year@0], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[o_year@0 as o_year], aggr=[sum(CASE WHEN all_nations.nation = Utf8("BRAZIL") THEN all_nations.volume ELSE Int64(0) END), sum(all_nations.volume)]
            │       ProjectionExec: expr=[date_part(YEAR, o_orderdate@2) as o_year, l_extendedprice@0 * (Some(1),20,0 - l_discount@1) as volume, n_name@3 as nation]
            │         CoalesceBatchesExec: target_batch_size=8192
            │           HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(r_regionkey@0, n_regionkey@3)], projection=[l_extendedprice@1, l_discount@2, o_orderdate@3, n_name@5]
            │             CoalescePartitionsExec
            │               NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
            │             ProjectionExec: expr=[l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, o_orderdate@3 as o_orderdate, n_regionkey@4 as n_regionkey, n_name@0 as n_name]
            │               CoalesceBatchesExec: target_batch_size=8192
            │                 HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@2)], projection=[n_name@1, l_extendedprice@2, l_discount@3, o_orderdate@5, n_regionkey@6]
            │                   CoalescePartitionsExec
            │                     DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name], file_type=parquet
            │                   ProjectionExec: expr=[l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, s_nationkey@3 as s_nationkey, o_orderdate@4 as o_orderdate, n_regionkey@0 as n_regionkey]
            │                     CoalesceBatchesExec: target_batch_size=8192
            │                       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, c_nationkey@4)], projection=[n_regionkey@1, l_extendedprice@2, l_discount@3, s_nationkey@4, o_orderdate@5]
            │                         CoalescePartitionsExec
            │                           DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_regionkey], file_type=parquet
            │                         CoalesceBatchesExec: target_batch_size=8192
            │                           HashJoinExec: mode=Partitioned, join_type=Inner, on=[(o_custkey@3, c_custkey@0)], projection=[l_extendedprice@0, l_discount@1, s_nationkey@2, o_orderdate@4, c_nationkey@6]
            │                             CoalesceBatchesExec: target_batch_size=8192
            │                               NetworkShuffleExec read_from=Stage 5, output_partitions=6, n_tasks=1, input_tasks=3
            │                             CoalesceBatchesExec: target_batch_size=8192
            │                               NetworkShuffleExec read_from=Stage 6, output_partitions=6, n_tasks=1, input_tasks=3
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: r_name@1 = AMERICA, projection=[r_regionkey@0]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/region/1.parquet, /testdata/tpch/data/region/10.parquet, /testdata/tpch/data/region/11.parquet], [/testdata/tpch/data/region/12.parquet, /testdata/tpch/data/region/13.parquet, /testdata/tpch/data/region/14.parquet], [/testdata/tpch/data/region/15.parquet, /testdata/tpch/data/region/16.parquet, /testdata/tpch/data/region/2.parquet], [/testdata/tpch/data/region/3.parquet, /testdata/tpch/data/region/4.parquet, /testdata/tpch/data/region/5.parquet], [/testdata/tpch/data/region/6.parquet, /testdata/tpch/data/region/7.parquet, /testdata/tpch/data/region/8.parquet], ...]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = AMERICA, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= AMERICA AND AMERICA <= r_name_max@1, required_guarantees=[r_name in (AMERICA)]
              └──────────────────────────────────────────────────
              ┌───── Stage 5   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([o_custkey@3], 6), input_partitions=6
              │     ProjectionExec: expr=[l_extendedprice@2 as l_extendedprice, l_discount@3 as l_discount, s_nationkey@4 as s_nationkey, o_custkey@0 as o_custkey, o_orderdate@1 as o_orderdate]
              │       CoalesceBatchesExec: target_batch_size=8192
              │         HashJoinExec: mode=Partitioned, join_type=Inner, on=[(o_orderkey@0, l_orderkey@0)], projection=[o_custkey@1, o_orderdate@2, l_extendedprice@4, l_discount@5, s_nationkey@6]
              │           CoalesceBatchesExec: target_batch_size=8192
              │             NetworkShuffleExec read_from=Stage 2, output_partitions=6, n_tasks=3, input_tasks=3
              │           CoalesceBatchesExec: target_batch_size=8192
              │             NetworkShuffleExec read_from=Stage 4, output_partitions=6, n_tasks=3, input_tasks=1
              └──────────────────────────────────────────────────
                ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   RepartitionExec: partitioning=Hash([o_orderkey@0], 18), input_partitions=2
                │     CoalesceBatchesExec: target_batch_size=8192
                │       FilterExec: o_orderdate@2 >= 1995-01-01 AND o_orderdate@2 <= 1996-12-31
                │         PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
                │           DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@2 >= 1995-01-01 AND o_orderdate@2 <= 1996-12-31, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1995-01-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 <= 1996-12-31, required_guarantees=[]
                └──────────────────────────────────────────────────
                ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   RepartitionExec: partitioning=Hash([l_orderkey@0], 18), input_partitions=6
                │     ProjectionExec: expr=[l_orderkey@1 as l_orderkey, l_extendedprice@2 as l_extendedprice, l_discount@3 as l_discount, s_nationkey@0 as s_nationkey]
                │       CoalesceBatchesExec: target_batch_size=8192
                │         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, l_suppkey@1)], projection=[s_nationkey@1, l_orderkey@2, l_extendedprice@4, l_discount@5]
                │           CoalescePartitionsExec
                │             DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_nationkey], file_type=parquet
                │           CoalesceBatchesExec: target_batch_size=8192
                │             HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@0, l_partkey@1)], projection=[l_orderkey@1, l_suppkey@3, l_extendedprice@4, l_discount@5]
                │               CoalescePartitionsExec
                │                 NetworkCoalesceExec read_from=Stage 3, output_partitions=8, input_tasks=4
                │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount], file_type=parquet
                └──────────────────────────────────────────────────
                  ┌───── Stage 3   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
                  │ CoalesceBatchesExec: target_batch_size=8194
                  │   CoalesceBatchesExec: target_batch_size=8192
                  │     FilterExec: p_type@1 = ECONOMY ANODIZED STEEL, projection=[p_partkey@0]
                  │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
                  │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/part/1.parquet, /testdata/tpch/data/part/10.parquet, /testdata/tpch/data/part/11.parquet], [/testdata/tpch/data/part/12.parquet, /testdata/tpch/data/part/13.parquet, /testdata/tpch/data/part/14.parquet], [/testdata/tpch/data/part/15.parquet, /testdata/tpch/data/part/16.parquet, /testdata/tpch/data/part/2.parquet], [/testdata/tpch/data/part/3.parquet, /testdata/tpch/data/part/4.parquet, /testdata/tpch/data/part/5.parquet], [/testdata/tpch/data/part/6.parquet, /testdata/tpch/data/part/7.parquet, /testdata/tpch/data/part/8.parquet], ...]}, projection=[p_partkey, p_type], file_type=parquet, predicate=p_type@1 = ECONOMY ANODIZED STEEL, pruning_predicate=p_type_null_count@2 != row_count@3 AND p_type_min@0 <= ECONOMY ANODIZED STEEL AND ECONOMY ANODIZED STEEL <= p_type_max@1, required_guarantees=[p_type in (ECONOMY ANODIZED STEEL)]
                  └──────────────────────────────────────────────────
              ┌───── Stage 6   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([c_custkey@0], 6), input_partitions=2
              │     PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
              │       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/customer/1.parquet, /testdata/tpch/data/customer/10.parquet, /testdata/tpch/data/customer/11.parquet], [/testdata/tpch/data/customer/12.parquet, /testdata/tpch/data/customer/13.parquet, /testdata/tpch/data/customer/14.parquet], [/testdata/tpch/data/customer/15.parquet, /testdata/tpch/data/customer/16.parquet, /testdata/tpch/data/customer/2.parquet], [/testdata/tpch/data/customer/3.parquet, /testdata/tpch/data/customer/4.parquet, /testdata/tpch/data/customer/5.parquet], [/testdata/tpch/data/customer/6.parquet, /testdata/tpch/data/customer/7.parquet, /testdata/tpch/data/customer/8.parquet], ...]}, projection=[c_custkey, c_nationkey], file_type=parquet
              └──────────────────────────────────────────────────
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_9() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(9).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 8   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [nation@0 ASC NULLS LAST, o_year@1 DESC]
        │   NetworkCoalesceExec read_from=Stage 7, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 7   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[nation@0 ASC NULLS LAST, o_year@1 DESC], preserve_partitioning=[true]
          │     ProjectionExec: expr=[nation@0 as nation, o_year@1 as o_year, sum(profit.amount)@2 as sum_profit]
          │       AggregateExec: mode=FinalPartitioned, gby=[nation@0 as nation, o_year@1 as o_year], aggr=[sum(profit.amount)]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 6, output_partitions=6, n_tasks=4, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 6   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([nation@0, o_year@1], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[nation@0 as nation, o_year@1 as o_year], aggr=[sum(profit.amount)]
            │       ProjectionExec: expr=[n_name@0 as nation, date_part(YEAR, o_orderdate@5) as o_year, l_extendedprice@2 * (Some(1),20,0 - l_discount@3) - ps_supplycost@4 * l_quantity@1 as amount]
            │         CoalesceBatchesExec: target_batch_size=8192
            │           HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@3)], projection=[n_name@1, l_quantity@2, l_extendedprice@3, l_discount@4, ps_supplycost@6, o_orderdate@7]
            │             CoalescePartitionsExec
            │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name], file_type=parquet
            │             ProjectionExec: expr=[l_quantity@1 as l_quantity, l_extendedprice@2 as l_extendedprice, l_discount@3 as l_discount, s_nationkey@4 as s_nationkey, ps_supplycost@5 as ps_supplycost, o_orderdate@0 as o_orderdate]
            │               CoalesceBatchesExec: target_batch_size=8192
            │                 HashJoinExec: mode=Partitioned, join_type=Inner, on=[(o_orderkey@0, l_orderkey@0)], projection=[o_orderdate@1, l_quantity@3, l_extendedprice@4, l_discount@5, s_nationkey@6, ps_supplycost@7]
            │                   CoalesceBatchesExec: target_batch_size=8192
            │                     NetworkShuffleExec read_from=Stage 1, output_partitions=6, n_tasks=1, input_tasks=3
            │                   CoalesceBatchesExec: target_batch_size=8192
            │                     NetworkShuffleExec read_from=Stage 5, output_partitions=6, n_tasks=1, input_tasks=3
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([o_orderkey@0], 6), input_partitions=2
              │     PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
              │       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_orderdate], file_type=parquet
              └──────────────────────────────────────────────────
              ┌───── Stage 5   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([l_orderkey@0], 6), input_partitions=6
              │     CoalesceBatchesExec: target_batch_size=8192
              │       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(l_suppkey@2, ps_suppkey@1), (l_partkey@1, ps_partkey@0)], projection=[l_orderkey@0, l_quantity@3, l_extendedprice@4, l_discount@5, s_nationkey@6, ps_supplycost@9]
              │         CoalesceBatchesExec: target_batch_size=8192
              │           NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=3, input_tasks=1
              │         CoalesceBatchesExec: target_batch_size=8192
              │           NetworkShuffleExec read_from=Stage 4, output_partitions=6, n_tasks=3, input_tasks=3
              └──────────────────────────────────────────────────
                ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   RepartitionExec: partitioning=Hash([l_suppkey@2, l_partkey@1], 18), input_partitions=6
                │     ProjectionExec: expr=[l_orderkey@1 as l_orderkey, l_partkey@2 as l_partkey, l_suppkey@3 as l_suppkey, l_quantity@4 as l_quantity, l_extendedprice@5 as l_extendedprice, l_discount@6 as l_discount, s_nationkey@0 as s_nationkey]
                │       CoalesceBatchesExec: target_batch_size=8192
                │         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, l_suppkey@2)], projection=[s_nationkey@1, l_orderkey@2, l_partkey@3, l_suppkey@4, l_quantity@5, l_extendedprice@6, l_discount@7]
                │           CoalescePartitionsExec
                │             DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_nationkey], file_type=parquet
                │           CoalesceBatchesExec: target_batch_size=8192
                │             HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@0, l_partkey@1)], projection=[l_orderkey@1, l_partkey@2, l_suppkey@3, l_quantity@4, l_extendedprice@5, l_discount@6]
                │               CoalescePartitionsExec
                │                 NetworkCoalesceExec read_from=Stage 2, output_partitions=8, input_tasks=4
                │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount], file_type=parquet
                └──────────────────────────────────────────────────
                  ┌───── Stage 2   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
                  │ CoalesceBatchesExec: target_batch_size=8194
                  │   CoalesceBatchesExec: target_batch_size=8192
                  │     FilterExec: p_name@1 LIKE %green%, projection=[p_partkey@0]
                  │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
                  │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/part/1.parquet, /testdata/tpch/data/part/10.parquet, /testdata/tpch/data/part/11.parquet], [/testdata/tpch/data/part/12.parquet, /testdata/tpch/data/part/13.parquet, /testdata/tpch/data/part/14.parquet], [/testdata/tpch/data/part/15.parquet, /testdata/tpch/data/part/16.parquet, /testdata/tpch/data/part/2.parquet], [/testdata/tpch/data/part/3.parquet, /testdata/tpch/data/part/4.parquet, /testdata/tpch/data/part/5.parquet], [/testdata/tpch/data/part/6.parquet, /testdata/tpch/data/part/7.parquet, /testdata/tpch/data/part/8.parquet], ...]}, projection=[p_partkey, p_name], file_type=parquet, predicate=p_name@1 LIKE %green%
                  └──────────────────────────────────────────────────
                ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   RepartitionExec: partitioning=Hash([ps_suppkey@1, ps_partkey@0], 18), input_partitions=2
                │     PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
                │       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/partsupp/1.parquet:0..753508, /testdata/tpch/data/partsupp/10.parquet:0..748125, /testdata/tpch/data/partsupp/11.parquet:0..496934], [/testdata/tpch/data/partsupp/11.parquet:496934..744689, /testdata/tpch/data/partsupp/12.parquet:0..749068, /testdata/tpch/data/partsupp/13.parquet:0..750296, /testdata/tpch/data/partsupp/14.parquet:0..251448], [/testdata/tpch/data/partsupp/14.parquet:251448..748674, /testdata/tpch/data/partsupp/15.parquet:0..744298, /testdata/tpch/data/partsupp/16.parquet:0..748035, /testdata/tpch/data/partsupp/2.parquet:0..9008], [/testdata/tpch/data/partsupp/2.parquet:9008..753010, /testdata/tpch/data/partsupp/3.parquet:0..756405, /testdata/tpch/data/partsupp/4.parquet:0..498160], [/testdata/tpch/data/partsupp/4.parquet:498160..746724, /testdata/tpch/data/partsupp/5.parquet:0..746720, /testdata/tpch/data/partsupp/6.parquet:0..750184, /testdata/tpch/data/partsupp/7.parquet:0..253099], ...]}, projection=[ps_partkey, ps_suppkey, ps_supplycost], file_type=parquet
                └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_10() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(10).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 4   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [revenue@2 DESC]
        │   NetworkCoalesceExec read_from=Stage 3, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[revenue@2 DESC], preserve_partitioning=[true]
          │     ProjectionExec: expr=[c_custkey@0 as c_custkey, c_name@1 as c_name, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@7 as revenue, c_acctbal@2 as c_acctbal, n_name@4 as n_name, c_address@5 as c_address, c_phone@3 as c_phone, c_comment@6 as c_comment]
          │       AggregateExec: mode=FinalPartitioned, gby=[c_custkey@0 as c_custkey, c_name@1 as c_name, c_acctbal@2 as c_acctbal, c_phone@3 as c_phone, n_name@4 as n_name, c_address@5 as c_address, c_comment@6 as c_comment], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 2, output_partitions=6, n_tasks=4, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([c_custkey@0, c_name@1, c_acctbal@2, c_phone@3, n_name@4, c_address@5, c_comment@6], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[c_custkey@0 as c_custkey, c_name@1 as c_name, c_acctbal@4 as c_acctbal, c_phone@3 as c_phone, n_name@8 as n_name, c_address@2 as c_address, c_comment@5 as c_comment], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
            │       ProjectionExec: expr=[c_custkey@1 as c_custkey, c_name@2 as c_name, c_address@3 as c_address, c_phone@4 as c_phone, c_acctbal@5 as c_acctbal, c_comment@6 as c_comment, l_extendedprice@7 as l_extendedprice, l_discount@8 as l_discount, n_name@0 as n_name]
            │         CoalesceBatchesExec: target_batch_size=8192
            │           HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, c_nationkey@3)], projection=[n_name@1, c_custkey@2, c_name@3, c_address@4, c_phone@6, c_acctbal@7, c_comment@8, l_extendedprice@9, l_discount@10]
            │             CoalescePartitionsExec
            │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name], file_type=parquet
            │             CoalesceBatchesExec: target_batch_size=8192
            │               HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@7, l_orderkey@0)], projection=[c_custkey@0, c_name@1, c_address@2, c_nationkey@3, c_phone@4, c_acctbal@5, c_comment@6, l_extendedprice@9, l_discount@10]
            │                 CoalescePartitionsExec
            │                   ProjectionExec: expr=[c_custkey@1 as c_custkey, c_name@2 as c_name, c_address@3 as c_address, c_nationkey@4 as c_nationkey, c_phone@5 as c_phone, c_acctbal@6 as c_acctbal, c_comment@7 as c_comment, o_orderkey@0 as o_orderkey]
            │                     CoalesceBatchesExec: target_batch_size=8192
            │                       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_custkey@1, c_custkey@0)], projection=[o_orderkey@0, c_custkey@2, c_name@3, c_address@4, c_nationkey@5, c_phone@6, c_acctbal@7, c_comment@8]
            │                         CoalescePartitionsExec
            │                           NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
            │                         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/customer/1.parquet, /testdata/tpch/data/customer/10.parquet, /testdata/tpch/data/customer/11.parquet], [/testdata/tpch/data/customer/12.parquet, /testdata/tpch/data/customer/13.parquet, /testdata/tpch/data/customer/14.parquet], [/testdata/tpch/data/customer/15.parquet, /testdata/tpch/data/customer/16.parquet, /testdata/tpch/data/customer/2.parquet], [/testdata/tpch/data/customer/3.parquet, /testdata/tpch/data/customer/4.parquet, /testdata/tpch/data/customer/5.parquet], [/testdata/tpch/data/customer/6.parquet, /testdata/tpch/data/customer/7.parquet, /testdata/tpch/data/customer/8.parquet], ...]}, projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment], file_type=parquet
            │                 CoalesceBatchesExec: target_batch_size=8192
            │                   FilterExec: l_returnflag@3 = R, projection=[l_orderkey@0, l_extendedprice@1, l_discount@2]
            │                     DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_extendedprice, l_discount, l_returnflag], file_type=parquet, predicate=l_returnflag@3 = R, pruning_predicate=l_returnflag_null_count@2 != row_count@3 AND l_returnflag_min@0 <= R AND R <= l_returnflag_max@1, required_guarantees=[l_returnflag in (R)]
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: o_orderdate@2 >= 1993-10-01 AND o_orderdate@2 < 1994-01-01, projection=[o_orderkey@0, o_custkey@1]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@2 >= 1993-10-01 AND o_orderdate@2 < 1994-01-01, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1993-10-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1994-01-01, required_guarantees=[]
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_11() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(11).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 3   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [value@1 DESC]
        │   SortExec: expr=[value@1 DESC], preserve_partitioning=[true]
        │     ProjectionExec: expr=[ps_partkey@0 as ps_partkey, sum(partsupp.ps_supplycost * partsupp.ps_availqty)@1 as value]
        │       NestedLoopJoinExec: join_type=Inner, filter=CAST(sum(partsupp.ps_supplycost * partsupp.ps_availqty)@0 AS Decimal128(38, 15)) > sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)@1, projection=[ps_partkey@1, sum(partsupp.ps_supplycost * partsupp.ps_availqty)@2]
        │         ProjectionExec: expr=[CAST(CAST(sum(partsupp.ps_supplycost * partsupp.ps_availqty)@0 AS Float64) * 0.0001 AS Decimal128(38, 15)) as sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)]
        │           AggregateExec: mode=Final, gby=[], aggr=[sum(partsupp.ps_supplycost * partsupp.ps_availqty)]
        │             CoalescePartitionsExec
        │               AggregateExec: mode=Partial, gby=[], aggr=[sum(partsupp.ps_supplycost * partsupp.ps_availqty)]
        │                 CoalesceBatchesExec: target_batch_size=8192
        │                   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@2)], projection=[ps_availqty@1, ps_supplycost@2]
        │                     CoalescePartitionsExec
        │                       NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
        │                     ProjectionExec: expr=[ps_availqty@1 as ps_availqty, ps_supplycost@2 as ps_supplycost, s_nationkey@0 as s_nationkey]
        │                       CoalesceBatchesExec: target_batch_size=8192
        │                         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, ps_suppkey@0)], projection=[s_nationkey@1, ps_availqty@3, ps_supplycost@4]
        │                           CoalescePartitionsExec
        │                             DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_nationkey], file_type=parquet
        │                           DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/partsupp/1.parquet:0..753508, /testdata/tpch/data/partsupp/10.parquet:0..748125, /testdata/tpch/data/partsupp/11.parquet:0..496934], [/testdata/tpch/data/partsupp/11.parquet:496934..744689, /testdata/tpch/data/partsupp/12.parquet:0..749068, /testdata/tpch/data/partsupp/13.parquet:0..750296, /testdata/tpch/data/partsupp/14.parquet:0..251448], [/testdata/tpch/data/partsupp/14.parquet:251448..748674, /testdata/tpch/data/partsupp/15.parquet:0..744298, /testdata/tpch/data/partsupp/16.parquet:0..748035, /testdata/tpch/data/partsupp/2.parquet:0..9008], [/testdata/tpch/data/partsupp/2.parquet:9008..753010, /testdata/tpch/data/partsupp/3.parquet:0..756405, /testdata/tpch/data/partsupp/4.parquet:0..498160], [/testdata/tpch/data/partsupp/4.parquet:498160..746724, /testdata/tpch/data/partsupp/5.parquet:0..746720, /testdata/tpch/data/partsupp/6.parquet:0..750184, /testdata/tpch/data/partsupp/7.parquet:0..253099], ...]}, projection=[ps_suppkey, ps_availqty, ps_supplycost], file_type=parquet
        │         AggregateExec: mode=FinalPartitioned, gby=[ps_partkey@0 as ps_partkey], aggr=[sum(partsupp.ps_supplycost * partsupp.ps_availqty)]
        │           CoalesceBatchesExec: target_batch_size=8192
        │             RepartitionExec: partitioning=Hash([ps_partkey@0], 6), input_partitions=6
        │               AggregateExec: mode=Partial, gby=[ps_partkey@0 as ps_partkey], aggr=[sum(partsupp.ps_supplycost * partsupp.ps_availqty)]
        │                 CoalesceBatchesExec: target_batch_size=8192
        │                   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@3)], projection=[ps_partkey@1, ps_availqty@2, ps_supplycost@3]
        │                     CoalescePartitionsExec
        │                       NetworkCoalesceExec read_from=Stage 2, output_partitions=8, input_tasks=4
        │                     ProjectionExec: expr=[ps_partkey@1 as ps_partkey, ps_availqty@2 as ps_availqty, ps_supplycost@3 as ps_supplycost, s_nationkey@0 as s_nationkey]
        │                       CoalesceBatchesExec: target_batch_size=8192
        │                         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, ps_suppkey@1)], projection=[s_nationkey@1, ps_partkey@2, ps_availqty@4, ps_supplycost@5]
        │                           CoalescePartitionsExec
        │                             DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_nationkey], file_type=parquet
        │                           DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/partsupp/1.parquet:0..753508, /testdata/tpch/data/partsupp/10.parquet:0..748125, /testdata/tpch/data/partsupp/11.parquet:0..496934], [/testdata/tpch/data/partsupp/11.parquet:496934..744689, /testdata/tpch/data/partsupp/12.parquet:0..749068, /testdata/tpch/data/partsupp/13.parquet:0..750296, /testdata/tpch/data/partsupp/14.parquet:0..251448], [/testdata/tpch/data/partsupp/14.parquet:251448..748674, /testdata/tpch/data/partsupp/15.parquet:0..744298, /testdata/tpch/data/partsupp/16.parquet:0..748035, /testdata/tpch/data/partsupp/2.parquet:0..9008], [/testdata/tpch/data/partsupp/2.parquet:9008..753010, /testdata/tpch/data/partsupp/3.parquet:0..756405, /testdata/tpch/data/partsupp/4.parquet:0..498160], [/testdata/tpch/data/partsupp/4.parquet:498160..746724, /testdata/tpch/data/partsupp/5.parquet:0..746720, /testdata/tpch/data/partsupp/6.parquet:0..750184, /testdata/tpch/data/partsupp/7.parquet:0..253099], ...]}, projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost], file_type=parquet
        └──────────────────────────────────────────────────
          ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   CoalesceBatchesExec: target_batch_size=8192
          │     FilterExec: n_name@1 = GERMANY, projection=[n_nationkey@0]
          │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
          │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = GERMANY, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= GERMANY AND GERMANY <= n_name_max@1, required_guarantees=[n_name in (GERMANY)]
          └──────────────────────────────────────────────────
          ┌───── Stage 2   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   CoalesceBatchesExec: target_batch_size=8192
          │     FilterExec: n_name@1 = GERMANY, projection=[n_nationkey@0]
          │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
          │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = GERMANY, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= GERMANY AND GERMANY <= n_name_max@1, required_guarantees=[n_name in (GERMANY)]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_12() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(12).await?;
        assert_snapshot!(plan, @r#"
        ┌───── Stage 5   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [l_shipmode@0 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 4, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[l_shipmode@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[l_shipmode@0 as l_shipmode, sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)@1 as high_line_count, sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)@2 as low_line_count]
          │       AggregateExec: mode=FinalPartitioned, gby=[l_shipmode@0 as l_shipmode], aggr=[sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([l_shipmode@0], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[l_shipmode@0 as l_shipmode], aggr=[sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)]
            │       CoalesceBatchesExec: target_batch_size=8192
            │         HashJoinExec: mode=Partitioned, join_type=Inner, on=[(l_orderkey@0, o_orderkey@0)], projection=[l_shipmode@1, o_orderpriority@3]
            │           CoalesceBatchesExec: target_batch_size=8192
            │             NetworkShuffleExec read_from=Stage 1, output_partitions=6, n_tasks=3, input_tasks=3
            │           CoalesceBatchesExec: target_batch_size=8192
            │             NetworkShuffleExec read_from=Stage 2, output_partitions=6, n_tasks=3, input_tasks=3
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([l_orderkey@0], 18), input_partitions=2
              │     CoalesceBatchesExec: target_batch_size=8192
              │       FilterExec: (l_shipmode@4 = MAIL OR l_shipmode@4 = SHIP) AND l_receiptdate@3 > l_commitdate@2 AND l_shipdate@1 < l_commitdate@2 AND l_receiptdate@3 >= 1994-01-01 AND l_receiptdate@3 < 1995-01-01, projection=[l_orderkey@0, l_shipmode@4]
              │         PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
              │           DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode], file_type=parquet, predicate=(l_shipmode@4 = MAIL OR l_shipmode@4 = SHIP) AND l_receiptdate@3 > l_commitdate@2 AND l_shipdate@1 < l_commitdate@2 AND l_receiptdate@3 >= 1994-01-01 AND l_receiptdate@3 < 1995-01-01, pruning_predicate=(l_shipmode_null_count@2 != row_count@3 AND l_shipmode_min@0 <= MAIL AND MAIL <= l_shipmode_max@1 OR l_shipmode_null_count@2 != row_count@3 AND l_shipmode_min@0 <= SHIP AND SHIP <= l_shipmode_max@1) AND l_receiptdate_null_count@5 != row_count@3 AND l_receiptdate_max@4 >= 1994-01-01 AND l_receiptdate_null_count@5 != row_count@3 AND l_receiptdate_min@6 < 1995-01-01, required_guarantees=[l_shipmode in (MAIL, SHIP)]
              └──────────────────────────────────────────────────
              ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([o_orderkey@0], 18), input_partitions=2
              │     PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
              │       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_orderpriority], file_type=parquet
              └──────────────────────────────────────────────────
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_13() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(13).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 5   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [custdist@1 DESC, c_count@0 DESC]
        │   NetworkCoalesceExec read_from=Stage 4, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[custdist@1 DESC, c_count@0 DESC], preserve_partitioning=[true]
          │     ProjectionExec: expr=[c_count@0 as c_count, count(Int64(1))@1 as custdist]
          │       AggregateExec: mode=FinalPartitioned, gby=[c_count@0 as c_count], aggr=[count(Int64(1))]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([c_count@0], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[c_count@0 as c_count], aggr=[count(Int64(1))]
            │       ProjectionExec: expr=[count(orders.o_orderkey)@1 as c_count]
            │         AggregateExec: mode=FinalPartitioned, gby=[c_custkey@0 as c_custkey], aggr=[count(orders.o_orderkey)]
            │           CoalesceBatchesExec: target_batch_size=8192
            │             NetworkShuffleExec read_from=Stage 2, output_partitions=6, n_tasks=3, input_tasks=1
            └──────────────────────────────────────────────────
              ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([c_custkey@0], 18), input_partitions=6
              │     AggregateExec: mode=Partial, gby=[c_custkey@0 as c_custkey], aggr=[count(orders.o_orderkey)]
              │       ProjectionExec: expr=[c_custkey@1 as c_custkey, o_orderkey@0 as o_orderkey]
              │         CoalesceBatchesExec: target_batch_size=8192
              │           HashJoinExec: mode=CollectLeft, join_type=Right, on=[(o_custkey@1, c_custkey@0)], projection=[o_orderkey@0, c_custkey@2]
              │             CoalescePartitionsExec
              │               NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
              │             DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/customer/1.parquet, /testdata/tpch/data/customer/10.parquet, /testdata/tpch/data/customer/11.parquet], [/testdata/tpch/data/customer/12.parquet, /testdata/tpch/data/customer/13.parquet, /testdata/tpch/data/customer/14.parquet], [/testdata/tpch/data/customer/15.parquet, /testdata/tpch/data/customer/16.parquet, /testdata/tpch/data/customer/2.parquet], [/testdata/tpch/data/customer/3.parquet, /testdata/tpch/data/customer/4.parquet, /testdata/tpch/data/customer/5.parquet], [/testdata/tpch/data/customer/6.parquet, /testdata/tpch/data/customer/7.parquet, /testdata/tpch/data/customer/8.parquet], ...]}, projection=[c_custkey], file_type=parquet
              └──────────────────────────────────────────────────
                ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   CoalesceBatchesExec: target_batch_size=8192
                │     FilterExec: o_comment@2 NOT LIKE %special%requests%, projection=[o_orderkey@0, o_custkey@1]
                │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
                │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_custkey, o_comment], file_type=parquet, predicate=o_comment@2 NOT LIKE %special%requests%
                └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_14() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(14).await?;
        assert_snapshot!(plan, @r#"
        ┌───── Stage 4   Tasks: t0:[p0] 
        │ ProjectionExec: expr=[100 * CAST(sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END)@0 AS Float64) / CAST(sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@1 AS Float64) as promo_revenue]
        │   AggregateExec: mode=Final, gby=[], aggr=[sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
        │     CoalescePartitionsExec
        │       NetworkCoalesceExec read_from=Stage 3, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   AggregateExec: mode=Partial, gby=[], aggr=[sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
          │     ProjectionExec: expr=[l_extendedprice@1 * (Some(1),20,0 - l_discount@2) as __common_expr_1, p_type@0 as p_type]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         HashJoinExec: mode=Partitioned, join_type=Inner, on=[(p_partkey@0, l_partkey@0)], projection=[p_type@1, l_extendedprice@3, l_discount@4]
          │           CoalesceBatchesExec: target_batch_size=8192
          │             NetworkShuffleExec read_from=Stage 1, output_partitions=6, n_tasks=4, input_tasks=3
          │           CoalesceBatchesExec: target_batch_size=8192
          │             NetworkShuffleExec read_from=Stage 2, output_partitions=6, n_tasks=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([p_partkey@0], 24), input_partitions=2
            │     PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
            │       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/part/1.parquet, /testdata/tpch/data/part/10.parquet, /testdata/tpch/data/part/11.parquet], [/testdata/tpch/data/part/12.parquet, /testdata/tpch/data/part/13.parquet, /testdata/tpch/data/part/14.parquet], [/testdata/tpch/data/part/15.parquet, /testdata/tpch/data/part/16.parquet, /testdata/tpch/data/part/2.parquet], [/testdata/tpch/data/part/3.parquet, /testdata/tpch/data/part/4.parquet, /testdata/tpch/data/part/5.parquet], [/testdata/tpch/data/part/6.parquet, /testdata/tpch/data/part/7.parquet, /testdata/tpch/data/part/8.parquet], ...]}, projection=[p_partkey, p_type], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([l_partkey@0], 24), input_partitions=2
            │     CoalesceBatchesExec: target_batch_size=8192
            │       FilterExec: l_shipdate@3 >= 1995-09-01 AND l_shipdate@3 < 1995-10-01, projection=[l_partkey@0, l_extendedprice@1, l_discount@2]
            │         PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
            │           DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_partkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@3 >= 1995-09-01 AND l_shipdate@3 < 1995-10-01, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1995-09-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-10-01, required_guarantees=[]
            └──────────────────────────────────────────────────
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_15() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(15).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 4   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [s_suppkey@0 ASC NULLS LAST]
        │   SortExec: expr=[s_suppkey@0 ASC NULLS LAST], preserve_partitioning=[true]
        │     CoalesceBatchesExec: target_batch_size=8192
        │       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(max(revenue0.total_revenue)@0, total_revenue@4)], projection=[s_suppkey@1, s_name@2, s_address@3, s_phone@4, total_revenue@5]
        │         AggregateExec: mode=Final, gby=[], aggr=[max(revenue0.total_revenue)]
        │           CoalescePartitionsExec
        │             NetworkCoalesceExec read_from=Stage 2, output_partitions=24, input_tasks=4
        │         CoalesceBatchesExec: target_batch_size=8192
        │           HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, supplier_no@0)], projection=[s_suppkey@0, s_name@1, s_address@2, s_phone@3, total_revenue@5]
        │             CoalescePartitionsExec
        │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_name, s_address, s_phone], file_type=parquet
        │             ProjectionExec: expr=[l_suppkey@0 as supplier_no, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@1 as total_revenue]
        │               AggregateExec: mode=FinalPartitioned, gby=[l_suppkey@0 as l_suppkey], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
        │                 CoalesceBatchesExec: target_batch_size=8192
        │                   NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=1, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   AggregateExec: mode=Partial, gby=[], aggr=[max(revenue0.total_revenue)]
          │     ProjectionExec: expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@1 as total_revenue]
          │       AggregateExec: mode=FinalPartitioned, gby=[l_suppkey@0 as l_suppkey], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 1, output_partitions=6, n_tasks=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([l_suppkey@0], 24), input_partitions=2
            │     AggregateExec: mode=Partial, gby=[l_suppkey@0 as l_suppkey], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
            │       CoalesceBatchesExec: target_batch_size=8192
            │         FilterExec: l_shipdate@3 >= 1996-01-01 AND l_shipdate@3 < 1996-04-01, projection=[l_suppkey@0, l_extendedprice@1, l_discount@2]
            │           PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
            │             DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_suppkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@3 >= 1996-01-01 AND l_shipdate@3 < 1996-04-01, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1996-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1996-04-01, required_guarantees=[]
            └──────────────────────────────────────────────────
          ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   RepartitionExec: partitioning=Hash([l_suppkey@0], 6), input_partitions=2
          │     AggregateExec: mode=Partial, gby=[l_suppkey@0 as l_suppkey], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         FilterExec: l_shipdate@3 >= 1996-01-01 AND l_shipdate@3 < 1996-04-01, projection=[l_suppkey@0, l_extendedprice@1, l_discount@2]
          │           PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
          │             DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_suppkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@3 >= 1996-01-01 AND l_shipdate@3 < 1996-04-01, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1996-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1996-04-01, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_16() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(16).await?;
        assert_snapshot!(plan, @r#"
        ┌───── Stage 6   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [supplier_cnt@3 DESC, p_brand@0 ASC NULLS LAST, p_type@1 ASC NULLS LAST, p_size@2 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 5, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 5   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[supplier_cnt@3 DESC, p_brand@0 ASC NULLS LAST, p_type@1 ASC NULLS LAST, p_size@2 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[p_brand@0 as p_brand, p_type@1 as p_type, p_size@2 as p_size, count(alias1)@3 as supplier_cnt]
          │       AggregateExec: mode=FinalPartitioned, gby=[p_brand@0 as p_brand, p_type@1 as p_type, p_size@2 as p_size], aggr=[count(alias1)]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 4, output_partitions=6, n_tasks=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([p_brand@0, p_type@1, p_size@2], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[p_brand@0 as p_brand, p_type@1 as p_type, p_size@2 as p_size], aggr=[count(alias1)]
            │       AggregateExec: mode=FinalPartitioned, gby=[p_brand@0 as p_brand, p_type@1 as p_type, p_size@2 as p_size, alias1@3 as alias1], aggr=[]
            │         CoalesceBatchesExec: target_batch_size=8192
            │           NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=3, input_tasks=1
            └──────────────────────────────────────────────────
              ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([p_brand@0, p_type@1, p_size@2, alias1@3], 18), input_partitions=6
              │     AggregateExec: mode=Partial, gby=[p_brand@1 as p_brand, p_type@2 as p_type, p_size@3 as p_size, ps_suppkey@0 as alias1], aggr=[]
              │       CoalesceBatchesExec: target_batch_size=8192
              │         HashJoinExec: mode=CollectLeft, join_type=RightAnti, on=[(s_suppkey@0, ps_suppkey@0)]
              │           CoalescePartitionsExec
              │             NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
              │           ProjectionExec: expr=[ps_suppkey@3 as ps_suppkey, p_brand@0 as p_brand, p_type@1 as p_type, p_size@2 as p_size]
              │             CoalesceBatchesExec: target_batch_size=8192
              │               HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@0, ps_partkey@0)], projection=[p_brand@1, p_type@2, p_size@3, ps_suppkey@5]
              │                 CoalescePartitionsExec
              │                   NetworkCoalesceExec read_from=Stage 2, output_partitions=8, input_tasks=4
              │                 DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/partsupp/1.parquet:0..753508, /testdata/tpch/data/partsupp/10.parquet:0..748125, /testdata/tpch/data/partsupp/11.parquet:0..496934], [/testdata/tpch/data/partsupp/11.parquet:496934..744689, /testdata/tpch/data/partsupp/12.parquet:0..749068, /testdata/tpch/data/partsupp/13.parquet:0..750296, /testdata/tpch/data/partsupp/14.parquet:0..251448], [/testdata/tpch/data/partsupp/14.parquet:251448..748674, /testdata/tpch/data/partsupp/15.parquet:0..744298, /testdata/tpch/data/partsupp/16.parquet:0..748035, /testdata/tpch/data/partsupp/2.parquet:0..9008], [/testdata/tpch/data/partsupp/2.parquet:9008..753010, /testdata/tpch/data/partsupp/3.parquet:0..756405, /testdata/tpch/data/partsupp/4.parquet:0..498160], [/testdata/tpch/data/partsupp/4.parquet:498160..746724, /testdata/tpch/data/partsupp/5.parquet:0..746720, /testdata/tpch/data/partsupp/6.parquet:0..750184, /testdata/tpch/data/partsupp/7.parquet:0..253099], ...]}, projection=[ps_partkey, ps_suppkey], file_type=parquet
              └──────────────────────────────────────────────────
                ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   CoalesceBatchesExec: target_batch_size=8192
                │     FilterExec: s_comment@1 LIKE %Customer%Complaints%, projection=[s_suppkey@0]
                │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
                │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_comment], file_type=parquet, predicate=s_comment@1 LIKE %Customer%Complaints%
                └──────────────────────────────────────────────────
                ┌───── Stage 2   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   CoalesceBatchesExec: target_batch_size=8192
                │     FilterExec: p_brand@1 != Brand#45 AND p_type@2 NOT LIKE MEDIUM POLISHED% AND Use p_size@3 IN (SET) ([Literal { value: Int32(49), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(14), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(23), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(45), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(19), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(3), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(36), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(9), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }])
                │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
                │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/part/1.parquet, /testdata/tpch/data/part/10.parquet, /testdata/tpch/data/part/11.parquet], [/testdata/tpch/data/part/12.parquet, /testdata/tpch/data/part/13.parquet, /testdata/tpch/data/part/14.parquet], [/testdata/tpch/data/part/15.parquet, /testdata/tpch/data/part/16.parquet, /testdata/tpch/data/part/2.parquet], [/testdata/tpch/data/part/3.parquet, /testdata/tpch/data/part/4.parquet, /testdata/tpch/data/part/5.parquet], [/testdata/tpch/data/part/6.parquet, /testdata/tpch/data/part/7.parquet, /testdata/tpch/data/part/8.parquet], ...]}, projection=[p_partkey, p_brand, p_type, p_size], file_type=parquet, predicate=p_brand@1 != Brand#45 AND p_type@2 NOT LIKE MEDIUM POLISHED% AND Use p_size@3 IN (SET) ([Literal { value: Int32(49), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(14), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(23), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(45), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(19), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(3), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(36), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Int32(9), field: Field { name: "lit", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]), pruning_predicate=p_brand_null_count@2 != row_count@3 AND (p_brand_min@0 != Brand#45 OR Brand#45 != p_brand_max@1) AND p_type_null_count@6 != row_count@3 AND (p_type_min@4 NOT LIKE MEDIUM POLISHED% OR p_type_max@5 NOT LIKE MEDIUM POLISHED%) AND (p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 49 AND 49 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 14 AND 14 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 23 AND 23 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 45 AND 45 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 19 AND 19 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 3 AND 3 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 36 AND 36 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 9 AND 9 <= p_size_max@8), required_guarantees=[p_brand not in (Brand#45), p_size in (14, 19, 23, 3, 36, 45, 49, 9)]
                └──────────────────────────────────────────────────
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_17() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(17).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 5   Tasks: t0:[p0] 
        │ ProjectionExec: expr=[CAST(sum(lineitem.l_extendedprice)@0 AS Float64) / 7 as avg_yearly]
        │   AggregateExec: mode=Final, gby=[], aggr=[sum(lineitem.l_extendedprice)]
        │     CoalescePartitionsExec
        │       NetworkCoalesceExec read_from=Stage 4, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   AggregateExec: mode=Partial, gby=[], aggr=[sum(lineitem.l_extendedprice)]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(p_partkey@2, l_partkey@1)], filter=CAST(l_quantity@0 AS Decimal128(30, 15)) < Float64(0.2) * avg(lineitem.l_quantity)@1, projection=[l_extendedprice@1]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 2, output_partitions=6, n_tasks=4, input_tasks=1
          │         ProjectionExec: expr=[CAST(0.2 * CAST(avg(lineitem.l_quantity)@1 AS Float64) AS Decimal128(30, 15)) as Float64(0.2) * avg(lineitem.l_quantity), l_partkey@0 as l_partkey]
          │           AggregateExec: mode=FinalPartitioned, gby=[l_partkey@0 as l_partkey], aggr=[avg(lineitem.l_quantity)]
          │             CoalesceBatchesExec: target_batch_size=8192
          │               NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([p_partkey@2], 24), input_partitions=6
            │     ProjectionExec: expr=[l_quantity@1 as l_quantity, l_extendedprice@2 as l_extendedprice, p_partkey@0 as p_partkey]
            │       CoalesceBatchesExec: target_batch_size=8192
            │         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@0, l_partkey@0)], projection=[p_partkey@0, l_quantity@2, l_extendedprice@3]
            │           CoalescePartitionsExec
            │             NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
            │           DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_partkey, l_quantity, l_extendedprice], file_type=parquet
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: p_brand@1 = Brand#23 AND p_container@2 = MED BOX, projection=[p_partkey@0]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/part/1.parquet, /testdata/tpch/data/part/10.parquet, /testdata/tpch/data/part/11.parquet], [/testdata/tpch/data/part/12.parquet, /testdata/tpch/data/part/13.parquet, /testdata/tpch/data/part/14.parquet], [/testdata/tpch/data/part/15.parquet, /testdata/tpch/data/part/16.parquet, /testdata/tpch/data/part/2.parquet], [/testdata/tpch/data/part/3.parquet, /testdata/tpch/data/part/4.parquet, /testdata/tpch/data/part/5.parquet], [/testdata/tpch/data/part/6.parquet, /testdata/tpch/data/part/7.parquet, /testdata/tpch/data/part/8.parquet], ...]}, projection=[p_partkey, p_brand, p_container], file_type=parquet, predicate=p_brand@1 = Brand#23 AND p_container@2 = MED BOX, pruning_predicate=p_brand_null_count@2 != row_count@3 AND p_brand_min@0 <= Brand#23 AND Brand#23 <= p_brand_max@1 AND p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED BOX AND MED BOX <= p_container_max@5, required_guarantees=[p_brand in (Brand#23), p_container in (MED BOX)]
              └──────────────────────────────────────────────────
            ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([l_partkey@0], 24), input_partitions=2
            │     AggregateExec: mode=Partial, gby=[l_partkey@0 as l_partkey], aggr=[avg(lineitem.l_quantity)]
            │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
            │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_partkey, l_quantity], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_18() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(18).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 9   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [o_totalprice@4 DESC, o_orderdate@3 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 8, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 8   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[o_totalprice@4 DESC, o_orderdate@3 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[c_name@0 as c_name, c_custkey@1 as c_custkey, o_orderkey@2 as o_orderkey, o_orderdate@3 as o_orderdate, o_totalprice@4 as o_totalprice], aggr=[sum(lineitem.l_quantity)]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         NetworkShuffleExec read_from=Stage 7, output_partitions=6, n_tasks=4, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 7   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([c_name@0, c_custkey@1, o_orderkey@2, o_orderdate@3, o_totalprice@4], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[c_name@1 as c_name, c_custkey@0 as c_custkey, o_orderkey@2 as o_orderkey, o_orderdate@4 as o_orderdate, o_totalprice@3 as o_totalprice], aggr=[sum(lineitem.l_quantity)]
            │       CoalesceBatchesExec: target_batch_size=8192
            │         HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(l_orderkey@0, o_orderkey@2)]
            │           CoalescePartitionsExec
            │             NetworkCoalesceExec read_from=Stage 2, output_partitions=24, input_tasks=4
            │           CoalesceBatchesExec: target_batch_size=8192
            │             HashJoinExec: mode=Partitioned, join_type=Inner, on=[(o_orderkey@2, l_orderkey@0)], projection=[c_custkey@0, c_name@1, o_orderkey@2, o_totalprice@3, o_orderdate@4, l_quantity@6]
            │               CoalesceBatchesExec: target_batch_size=8192
            │                 NetworkShuffleExec read_from=Stage 5, output_partitions=6, n_tasks=1, input_tasks=3
            │               CoalesceBatchesExec: target_batch_size=8192
            │                 NetworkShuffleExec read_from=Stage 6, output_partitions=6, n_tasks=1, input_tasks=3
            └──────────────────────────────────────────────────
              ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: sum(lineitem.l_quantity)@1 > Some(30000),25,2, projection=[l_orderkey@0]
              │       AggregateExec: mode=FinalPartitioned, gby=[l_orderkey@0 as l_orderkey], aggr=[sum(lineitem.l_quantity)]
              │         CoalesceBatchesExec: target_batch_size=8192
              │           NetworkShuffleExec read_from=Stage 1, output_partitions=6, n_tasks=4, input_tasks=3
              └──────────────────────────────────────────────────
                ┌───── Stage 1   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   RepartitionExec: partitioning=Hash([l_orderkey@0], 24), input_partitions=2
                │     AggregateExec: mode=Partial, gby=[l_orderkey@0 as l_orderkey], aggr=[sum(lineitem.l_quantity)]
                │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
                │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_quantity], file_type=parquet
                └──────────────────────────────────────────────────
              ┌───── Stage 5   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([o_orderkey@2], 6), input_partitions=6
              │     CoalesceBatchesExec: target_batch_size=8192
              │       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c_custkey@0, o_custkey@1)], projection=[c_custkey@0, c_name@1, o_orderkey@2, o_totalprice@4, o_orderdate@5]
              │         CoalesceBatchesExec: target_batch_size=8192
              │           NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=3, input_tasks=3
              │         CoalesceBatchesExec: target_batch_size=8192
              │           NetworkShuffleExec read_from=Stage 4, output_partitions=6, n_tasks=3, input_tasks=3
              └──────────────────────────────────────────────────
                ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   RepartitionExec: partitioning=Hash([c_custkey@0], 18), input_partitions=2
                │     PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
                │       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/customer/1.parquet, /testdata/tpch/data/customer/10.parquet, /testdata/tpch/data/customer/11.parquet], [/testdata/tpch/data/customer/12.parquet, /testdata/tpch/data/customer/13.parquet, /testdata/tpch/data/customer/14.parquet], [/testdata/tpch/data/customer/15.parquet, /testdata/tpch/data/customer/16.parquet, /testdata/tpch/data/customer/2.parquet], [/testdata/tpch/data/customer/3.parquet, /testdata/tpch/data/customer/4.parquet, /testdata/tpch/data/customer/5.parquet], [/testdata/tpch/data/customer/6.parquet, /testdata/tpch/data/customer/7.parquet, /testdata/tpch/data/customer/8.parquet], ...]}, projection=[c_custkey, c_name], file_type=parquet
                └──────────────────────────────────────────────────
                ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17] 
                │ CoalesceBatchesExec: target_batch_size=8194
                │   RepartitionExec: partitioning=Hash([o_custkey@1], 18), input_partitions=2
                │     PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
                │       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_custkey, o_totalprice, o_orderdate], file_type=parquet
                └──────────────────────────────────────────────────
              ┌───── Stage 6   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   RepartitionExec: partitioning=Hash([l_orderkey@0], 6), input_partitions=2
              │     PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
              │       DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_quantity], file_type=parquet
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_19() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(19).await?;
        assert_snapshot!(plan, @r#"
        ┌───── Stage 2   Tasks: t0:[p0] 
        │ ProjectionExec: expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@0 as revenue]
        │   AggregateExec: mode=Final, gby=[], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
        │     CoalescePartitionsExec
        │       AggregateExec: mode=Partial, gby=[], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
        │         CoalesceBatchesExec: target_batch_size=8192
        │           HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@0, l_partkey@0)], filter=p_brand@1 = Brand#12 AND p_container@3 IN ([Literal { value: Utf8View("SM CASE"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("SM BOX"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("SM PACK"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("SM PKG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]) AND l_quantity@0 >= Some(100),15,2 AND l_quantity@0 <= Some(1100),15,2 AND p_size@2 <= 5 OR p_brand@1 = Brand#23 AND p_container@3 IN ([Literal { value: Utf8View("MED BAG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("MED BOX"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("MED PKG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("MED PACK"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]) AND l_quantity@0 >= Some(1000),15,2 AND l_quantity@0 <= Some(2000),15,2 AND p_size@2 <= 10 OR p_brand@1 = Brand#34 AND p_container@3 IN ([Literal { value: Utf8View("LG CASE"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("LG BOX"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("LG PACK"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("LG PKG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]) AND l_quantity@0 >= Some(2000),15,2 AND l_quantity@0 <= Some(3000),15,2 AND p_size@2 <= 15, projection=[l_extendedprice@6, l_discount@7]
        │             CoalescePartitionsExec
        │               NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
        │             CoalesceBatchesExec: target_batch_size=8192
        │               FilterExec: (l_quantity@1 >= Some(100),15,2 AND l_quantity@1 <= Some(1100),15,2 OR l_quantity@1 >= Some(1000),15,2 AND l_quantity@1 <= Some(2000),15,2 OR l_quantity@1 >= Some(2000),15,2 AND l_quantity@1 <= Some(3000),15,2) AND (l_shipmode@5 = AIR OR l_shipmode@5 = AIR REG) AND l_shipinstruct@4 = DELIVER IN PERSON, projection=[l_partkey@0, l_quantity@1, l_extendedprice@2, l_discount@3]
        │                 DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_partkey, l_quantity, l_extendedprice, l_discount, l_shipinstruct, l_shipmode], file_type=parquet, predicate=(l_quantity@1 >= Some(100),15,2 AND l_quantity@1 <= Some(1100),15,2 OR l_quantity@1 >= Some(1000),15,2 AND l_quantity@1 <= Some(2000),15,2 OR l_quantity@1 >= Some(2000),15,2 AND l_quantity@1 <= Some(3000),15,2) AND (l_shipmode@5 = AIR OR l_shipmode@5 = AIR REG) AND l_shipinstruct@4 = DELIVER IN PERSON, pruning_predicate=(l_quantity_null_count@1 != row_count@2 AND l_quantity_max@0 >= Some(100),15,2 AND l_quantity_null_count@1 != row_count@2 AND l_quantity_min@3 <= Some(1100),15,2 OR l_quantity_null_count@1 != row_count@2 AND l_quantity_max@0 >= Some(1000),15,2 AND l_quantity_null_count@1 != row_count@2 AND l_quantity_min@3 <= Some(2000),15,2 OR l_quantity_null_count@1 != row_count@2 AND l_quantity_max@0 >= Some(2000),15,2 AND l_quantity_null_count@1 != row_count@2 AND l_quantity_min@3 <= Some(3000),15,2) AND (l_shipmode_null_count@6 != row_count@2 AND l_shipmode_min@4 <= AIR AND AIR <= l_shipmode_max@5 OR l_shipmode_null_count@6 != row_count@2 AND l_shipmode_min@4 <= AIR REG AND AIR REG <= l_shipmode_max@5) AND l_shipinstruct_null_count@9 != row_count@2 AND l_shipinstruct_min@7 <= DELIVER IN PERSON AND DELIVER IN PERSON <= l_shipinstruct_max@8, required_guarantees=[l_shipinstruct in (DELIVER IN PERSON), l_shipmode in (AIR, AIR REG)]
        └──────────────────────────────────────────────────
          ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   CoalesceBatchesExec: target_batch_size=8192
          │     FilterExec: (p_brand@1 = Brand#12 AND p_container@3 IN ([Literal { value: Utf8View("SM CASE"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("SM BOX"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("SM PACK"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("SM PKG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]) AND p_size@2 <= 5 OR p_brand@1 = Brand#23 AND p_container@3 IN ([Literal { value: Utf8View("MED BAG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("MED BOX"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("MED PKG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("MED PACK"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]) AND p_size@2 <= 10 OR p_brand@1 = Brand#34 AND p_container@3 IN ([Literal { value: Utf8View("LG CASE"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("LG BOX"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("LG PACK"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("LG PKG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]) AND p_size@2 <= 15) AND p_size@2 >= 1
          │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
          │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/part/1.parquet, /testdata/tpch/data/part/10.parquet, /testdata/tpch/data/part/11.parquet], [/testdata/tpch/data/part/12.parquet, /testdata/tpch/data/part/13.parquet, /testdata/tpch/data/part/14.parquet], [/testdata/tpch/data/part/15.parquet, /testdata/tpch/data/part/16.parquet, /testdata/tpch/data/part/2.parquet], [/testdata/tpch/data/part/3.parquet, /testdata/tpch/data/part/4.parquet, /testdata/tpch/data/part/5.parquet], [/testdata/tpch/data/part/6.parquet, /testdata/tpch/data/part/7.parquet, /testdata/tpch/data/part/8.parquet], ...]}, projection=[p_partkey, p_brand, p_size, p_container], file_type=parquet, predicate=(p_brand@1 = Brand#12 AND p_container@3 IN ([Literal { value: Utf8View("SM CASE"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("SM BOX"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("SM PACK"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("SM PKG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]) AND p_size@2 <= 5 OR p_brand@1 = Brand#23 AND p_container@3 IN ([Literal { value: Utf8View("MED BAG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("MED BOX"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("MED PKG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("MED PACK"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]) AND p_size@2 <= 10 OR p_brand@1 = Brand#34 AND p_container@3 IN ([Literal { value: Utf8View("LG CASE"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("LG BOX"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("LG PACK"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("LG PKG"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]) AND p_size@2 <= 15) AND p_size@2 >= 1, pruning_predicate=(p_brand_null_count@2 != row_count@3 AND p_brand_min@0 <= Brand#12 AND Brand#12 <= p_brand_max@1 AND (p_container_null_count@6 != row_count@3 AND p_container_min@4 <= SM CASE AND SM CASE <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= SM BOX AND SM BOX <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= SM PACK AND SM PACK <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= SM PKG AND SM PKG <= p_container_max@5) AND p_size_null_count@8 != row_count@3 AND p_size_min@7 <= 5 OR p_brand_null_count@2 != row_count@3 AND p_brand_min@0 <= Brand#23 AND Brand#23 <= p_brand_max@1 AND (p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED BAG AND MED BAG <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED BOX AND MED BOX <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED PKG AND MED PKG <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED PACK AND MED PACK <= p_container_max@5) AND p_size_null_count@8 != row_count@3 AND p_size_min@7 <= 10 OR p_brand_null_count@2 != row_count@3 AND p_brand_min@0 <= Brand#34 AND Brand#34 <= p_brand_max@1 AND (p_container_null_count@6 != row_count@3 AND p_container_min@4 <= LG CASE AND LG CASE <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= LG BOX AND LG BOX <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= LG PACK AND LG PACK <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= LG PKG AND LG PKG <= p_container_max@5) AND p_size_null_count@8 != row_count@3 AND p_size_min@7 <= 15) AND p_size_null_count@8 != row_count@3 AND p_size_max@9 >= 1, required_guarantees=[]
          └──────────────────────────────────────────────────
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_20() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(20).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 4   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [s_name@0 ASC NULLS LAST]
        │   SortExec: expr=[s_name@0 ASC NULLS LAST], preserve_partitioning=[true]
        │     CoalesceBatchesExec: target_batch_size=8192
        │       HashJoinExec: mode=CollectLeft, join_type=LeftSemi, on=[(s_suppkey@0, ps_suppkey@0)], projection=[s_name@1, s_address@2]
        │         CoalescePartitionsExec
        │           CoalesceBatchesExec: target_batch_size=8192
        │             HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@3)], projection=[s_suppkey@1, s_name@2, s_address@3]
        │               CoalescePartitionsExec
        │                 NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
        │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_name, s_address, s_nationkey], file_type=parquet
        │         CoalesceBatchesExec: target_batch_size=8192
        │           HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(ps_partkey@0, l_partkey@1), (ps_suppkey@1, l_suppkey@2)], filter=CAST(ps_availqty@0 AS Float64) > Float64(0.5) * sum(lineitem.l_quantity)@1, projection=[ps_suppkey@1]
        │             CoalescePartitionsExec
        │               CoalesceBatchesExec: target_batch_size=8192
        │                 HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(p_partkey@0, ps_partkey@0)]
        │                   CoalescePartitionsExec
        │                     NetworkCoalesceExec read_from=Stage 2, output_partitions=8, input_tasks=4
        │                   DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/partsupp/1.parquet:0..753508, /testdata/tpch/data/partsupp/10.parquet:0..748125, /testdata/tpch/data/partsupp/11.parquet:0..496934], [/testdata/tpch/data/partsupp/11.parquet:496934..744689, /testdata/tpch/data/partsupp/12.parquet:0..749068, /testdata/tpch/data/partsupp/13.parquet:0..750296, /testdata/tpch/data/partsupp/14.parquet:0..251448], [/testdata/tpch/data/partsupp/14.parquet:251448..748674, /testdata/tpch/data/partsupp/15.parquet:0..744298, /testdata/tpch/data/partsupp/16.parquet:0..748035, /testdata/tpch/data/partsupp/2.parquet:0..9008], [/testdata/tpch/data/partsupp/2.parquet:9008..753010, /testdata/tpch/data/partsupp/3.parquet:0..756405, /testdata/tpch/data/partsupp/4.parquet:0..498160], [/testdata/tpch/data/partsupp/4.parquet:498160..746724, /testdata/tpch/data/partsupp/5.parquet:0..746720, /testdata/tpch/data/partsupp/6.parquet:0..750184, /testdata/tpch/data/partsupp/7.parquet:0..253099], ...]}, projection=[ps_partkey, ps_suppkey, ps_availqty], file_type=parquet
        │             ProjectionExec: expr=[0.5 * CAST(sum(lineitem.l_quantity)@2 AS Float64) as Float64(0.5) * sum(lineitem.l_quantity), l_partkey@0 as l_partkey, l_suppkey@1 as l_suppkey]
        │               AggregateExec: mode=FinalPartitioned, gby=[l_partkey@0 as l_partkey, l_suppkey@1 as l_suppkey], aggr=[sum(lineitem.l_quantity)]
        │                 CoalesceBatchesExec: target_batch_size=8192
        │                   NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=1, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   CoalesceBatchesExec: target_batch_size=8192
          │     FilterExec: n_name@1 = CANADA, projection=[n_nationkey@0]
          │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
          │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = CANADA, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= CANADA AND CANADA <= n_name_max@1, required_guarantees=[n_name in (CANADA)]
          └──────────────────────────────────────────────────
          ┌───── Stage 2   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   CoalesceBatchesExec: target_batch_size=8192
          │     FilterExec: p_name@1 LIKE forest%, projection=[p_partkey@0]
          │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
          │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/part/1.parquet, /testdata/tpch/data/part/10.parquet, /testdata/tpch/data/part/11.parquet], [/testdata/tpch/data/part/12.parquet, /testdata/tpch/data/part/13.parquet, /testdata/tpch/data/part/14.parquet], [/testdata/tpch/data/part/15.parquet, /testdata/tpch/data/part/16.parquet, /testdata/tpch/data/part/2.parquet], [/testdata/tpch/data/part/3.parquet, /testdata/tpch/data/part/4.parquet, /testdata/tpch/data/part/5.parquet], [/testdata/tpch/data/part/6.parquet, /testdata/tpch/data/part/7.parquet, /testdata/tpch/data/part/8.parquet], ...]}, projection=[p_partkey, p_name], file_type=parquet, predicate=p_name@1 LIKE forest%, pruning_predicate=p_name_null_count@2 != row_count@3 AND p_name_min@0 <= foresu AND forest <= p_name_max@1, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   RepartitionExec: partitioning=Hash([l_partkey@0, l_suppkey@1], 6), input_partitions=2
          │     AggregateExec: mode=Partial, gby=[l_partkey@0 as l_partkey, l_suppkey@1 as l_suppkey], aggr=[sum(lineitem.l_quantity)]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         FilterExec: l_shipdate@3 >= 1994-01-01 AND l_shipdate@3 < 1995-01-01, projection=[l_partkey@0, l_suppkey@1, l_quantity@2]
          │           PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1] 
          │             DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_partkey, l_suppkey, l_quantity, l_shipdate], file_type=parquet, predicate=l_shipdate@3 >= 1994-01-01 AND l_shipdate@3 < 1995-01-01, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1994-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-01-01, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_21() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(21).await?;
        assert_snapshot!(plan, @r"
        ┌───── Stage 5   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [numwait@1 DESC, s_name@0 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 4, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[numwait@1 DESC, s_name@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[s_name@0 as s_name, count(Int64(1))@1 as numwait]
          │       AggregateExec: mode=FinalPartitioned, gby=[s_name@0 as s_name], aggr=[count(Int64(1))]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=4, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([s_name@0], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[s_name@0 as s_name], aggr=[count(Int64(1))]
            │       CoalesceBatchesExec: target_batch_size=8192
            │         HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(l_orderkey@1, l_orderkey@0)], filter=l_suppkey@1 != l_suppkey@0, projection=[s_name@0]
            │           CoalescePartitionsExec
            │             CoalesceBatchesExec: target_batch_size=8192
            │               HashJoinExec: mode=CollectLeft, join_type=LeftSemi, on=[(l_orderkey@1, l_orderkey@0)], filter=l_suppkey@1 != l_suppkey@0
            │                 CoalescePartitionsExec
            │                   CoalesceBatchesExec: target_batch_size=8192
            │                     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@1)], projection=[s_name@1, l_orderkey@3, l_suppkey@4]
            │                       CoalescePartitionsExec
            │                         NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
            │                       CoalesceBatchesExec: target_batch_size=8192
            │                         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@0, l_orderkey@2)], projection=[s_name@1, s_nationkey@2, l_orderkey@3, l_suppkey@4]
            │                           CoalescePartitionsExec
            │                             NetworkCoalesceExec read_from=Stage 2, output_partitions=8, input_tasks=4
            │                           CoalesceBatchesExec: target_batch_size=8192
            │                             HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, l_suppkey@1)], projection=[s_name@1, s_nationkey@2, l_orderkey@3, l_suppkey@4]
            │                               CoalescePartitionsExec
            │                                 DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/supplier/1.parquet, /testdata/tpch/data/supplier/10.parquet, /testdata/tpch/data/supplier/11.parquet], [/testdata/tpch/data/supplier/12.parquet, /testdata/tpch/data/supplier/13.parquet, /testdata/tpch/data/supplier/14.parquet], [/testdata/tpch/data/supplier/15.parquet, /testdata/tpch/data/supplier/16.parquet, /testdata/tpch/data/supplier/2.parquet], [/testdata/tpch/data/supplier/3.parquet, /testdata/tpch/data/supplier/4.parquet, /testdata/tpch/data/supplier/5.parquet], [/testdata/tpch/data/supplier/6.parquet, /testdata/tpch/data/supplier/7.parquet, /testdata/tpch/data/supplier/8.parquet], ...]}, projection=[s_suppkey, s_name, s_nationkey], file_type=parquet
            │                               CoalesceBatchesExec: target_batch_size=8192
            │                                 FilterExec: l_receiptdate@3 > l_commitdate@2, projection=[l_orderkey@0, l_suppkey@1]
            │                                   DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_suppkey, l_commitdate, l_receiptdate], file_type=parquet, predicate=l_receiptdate@3 > l_commitdate@2
            │                 DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_suppkey], file_type=parquet
            │           CoalesceBatchesExec: target_batch_size=8192
            │             FilterExec: l_receiptdate@3 > l_commitdate@2, projection=[l_orderkey@0, l_suppkey@1]
            │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/lineitem/1.parquet:0..2263630, /testdata/tpch/data/lineitem/10.parquet:0..2247315, /testdata/tpch/data/lineitem/11.parquet:0..1497293], [/testdata/tpch/data/lineitem/11.parquet:1497293..2257665, /testdata/tpch/data/lineitem/12.parquet:0..2246562, /testdata/tpch/data/lineitem/13.parquet:0..2280549, /testdata/tpch/data/lineitem/14.parquet:0..720755], [/testdata/tpch/data/lineitem/14.parquet:720755..2262883, /testdata/tpch/data/lineitem/15.parquet:0..2235696, /testdata/tpch/data/lineitem/16.parquet:0..2230414], [/testdata/tpch/data/lineitem/16.parquet:2230414..2266608, /testdata/tpch/data/lineitem/2.parquet:0..2246598, /testdata/tpch/data/lineitem/3.parquet:0..2273582, /testdata/tpch/data/lineitem/4.parquet:0..1451864], [/testdata/tpch/data/lineitem/4.parquet:1451864..2243067, /testdata/tpch/data/lineitem/5.parquet:0..2258249, /testdata/tpch/data/lineitem/6.parquet:0..2240773, /testdata/tpch/data/lineitem/7.parquet:0..718013], ...]}, projection=[l_orderkey, l_suppkey, l_commitdate, l_receiptdate], file_type=parquet, predicate=l_receiptdate@3 > l_commitdate@2
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: n_name@1 = SAUDI ARABIA, projection=[n_nationkey@0]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/nation/1.parquet, /testdata/tpch/data/nation/10.parquet, /testdata/tpch/data/nation/11.parquet], [/testdata/tpch/data/nation/12.parquet, /testdata/tpch/data/nation/13.parquet, /testdata/tpch/data/nation/14.parquet], [/testdata/tpch/data/nation/15.parquet, /testdata/tpch/data/nation/16.parquet, /testdata/tpch/data/nation/2.parquet], [/testdata/tpch/data/nation/3.parquet, /testdata/tpch/data/nation/4.parquet, /testdata/tpch/data/nation/5.parquet], [/testdata/tpch/data/nation/6.parquet, /testdata/tpch/data/nation/7.parquet, /testdata/tpch/data/nation/8.parquet], ...]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = SAUDI ARABIA, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= SAUDI ARABIA AND SAUDI ARABIA <= n_name_max@1, required_guarantees=[n_name in (SAUDI ARABIA)]
              └──────────────────────────────────────────────────
              ┌───── Stage 2   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: o_orderstatus@1 = F, projection=[o_orderkey@0]
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_orderkey, o_orderstatus], file_type=parquet, predicate=o_orderstatus@1 = F, pruning_predicate=o_orderstatus_null_count@2 != row_count@3 AND o_orderstatus_min@0 <= F AND F <= o_orderstatus_max@1, required_guarantees=[o_orderstatus in (F)]
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_22() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query(22).await?;
        assert_snapshot!(plan, @r#"
        ┌───── Stage 5   Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [cntrycode@0 ASC NULLS LAST]
        │   NetworkCoalesceExec read_from=Stage 4, output_partitions=24, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3,p4,p5] t1:[p0,p1,p2,p3,p4,p5] t2:[p0,p1,p2,p3,p4,p5] t3:[p0,p1,p2,p3,p4,p5] 
          │ CoalesceBatchesExec: target_batch_size=8194
          │   SortExec: expr=[cntrycode@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[cntrycode@0 as cntrycode, count(Int64(1))@1 as numcust, sum(custsale.c_acctbal)@2 as totacctbal]
          │       AggregateExec: mode=FinalPartitioned, gby=[cntrycode@0 as cntrycode], aggr=[count(Int64(1)), sum(custsale.c_acctbal)]
          │         CoalesceBatchesExec: target_batch_size=8192
          │           NetworkShuffleExec read_from=Stage 3, output_partitions=6, n_tasks=4, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23] 
            │ CoalesceBatchesExec: target_batch_size=8194
            │   RepartitionExec: partitioning=Hash([cntrycode@0], 24), input_partitions=6
            │     AggregateExec: mode=Partial, gby=[cntrycode@0 as cntrycode], aggr=[count(Int64(1)), sum(custsale.c_acctbal)]
            │       ProjectionExec: expr=[substr(c_phone@0, 1, 2) as cntrycode, c_acctbal@1 as c_acctbal]
            │         NestedLoopJoinExec: join_type=Inner, filter=CAST(c_acctbal@0 AS Decimal128(19, 6)) > avg(customer.c_acctbal)@1, projection=[c_phone@1, c_acctbal@2]
            │           AggregateExec: mode=Final, gby=[], aggr=[avg(customer.c_acctbal)]
            │             CoalescePartitionsExec
            │               NetworkCoalesceExec read_from=Stage 1, output_partitions=8, input_tasks=4
            │           CoalesceBatchesExec: target_batch_size=8192
            │             HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(c_custkey@0, o_custkey@0)], projection=[c_phone@1, c_acctbal@2]
            │               CoalescePartitionsExec
            │                 NetworkCoalesceExec read_from=Stage 2, output_partitions=8, input_tasks=4
            │               DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/orders/1.parquet:0..818270, /testdata/tpch/data/orders/10.parquet:0..817463, /testdata/tpch/data/orders/11.parquet:0..545002], [/testdata/tpch/data/orders/11.parquet:545002..816070, /testdata/tpch/data/orders/12.parquet:0..820827, /testdata/tpch/data/orders/13.parquet:0..821995, /testdata/tpch/data/orders/14.parquet:0..266845], [/testdata/tpch/data/orders/14.parquet:266845..815509, /testdata/tpch/data/orders/15.parquet:0..815233, /testdata/tpch/data/orders/16.parquet:0..816838], [/testdata/tpch/data/orders/16.parquet:816838..817706, /testdata/tpch/data/orders/2.parquet:0..817737, /testdata/tpch/data/orders/3.parquet:0..817566, /testdata/tpch/data/orders/4.parquet:0..544564], [/testdata/tpch/data/orders/4.parquet:544564..821332, /testdata/tpch/data/orders/5.parquet:0..815888, /testdata/tpch/data/orders/6.parquet:0..815311, /testdata/tpch/data/orders/7.parquet:0..272768], ...]}, projection=[o_custkey], file_type=parquet
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   AggregateExec: mode=Partial, gby=[], aggr=[avg(customer.c_acctbal)]
              │     CoalesceBatchesExec: target_batch_size=8192
              │       FilterExec: c_acctbal@1 > Some(0),15,2 AND substr(c_phone@0, 1, 2) IN ([Literal { value: Utf8View("13"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("31"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("23"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("29"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("30"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("18"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("17"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]), projection=[c_acctbal@1]
              │         PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │           DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/customer/1.parquet, /testdata/tpch/data/customer/10.parquet, /testdata/tpch/data/customer/11.parquet], [/testdata/tpch/data/customer/12.parquet, /testdata/tpch/data/customer/13.parquet, /testdata/tpch/data/customer/14.parquet], [/testdata/tpch/data/customer/15.parquet, /testdata/tpch/data/customer/16.parquet, /testdata/tpch/data/customer/2.parquet], [/testdata/tpch/data/customer/3.parquet, /testdata/tpch/data/customer/4.parquet, /testdata/tpch/data/customer/5.parquet], [/testdata/tpch/data/customer/6.parquet, /testdata/tpch/data/customer/7.parquet, /testdata/tpch/data/customer/8.parquet], ...]}, projection=[c_phone, c_acctbal], file_type=parquet, predicate=c_acctbal@1 > Some(0),15,2 AND substr(c_phone@0, 1, 2) IN ([Literal { value: Utf8View("13"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("31"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("23"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("29"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("30"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("18"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("17"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }]), pruning_predicate=c_acctbal_null_count@1 != row_count@2 AND c_acctbal_max@0 > Some(0),15,2, required_guarantees=[]
              └──────────────────────────────────────────────────
              ┌───── Stage 2   Tasks: t0:[p0,p1] t1:[p2,p3] t2:[p4,p5] t3:[p6,p7] 
              │ CoalesceBatchesExec: target_batch_size=8194
              │   CoalesceBatchesExec: target_batch_size=8192
              │     FilterExec: substr(c_phone@1, 1, 2) IN ([Literal { value: Utf8View("13"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("31"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("23"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("29"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("30"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("18"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("17"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }])
              │       PartitionIsolatorExec Tasks: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,__] t3:[__,__,__,__,__,p0] 
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpch/data/customer/1.parquet, /testdata/tpch/data/customer/10.parquet, /testdata/tpch/data/customer/11.parquet], [/testdata/tpch/data/customer/12.parquet, /testdata/tpch/data/customer/13.parquet, /testdata/tpch/data/customer/14.parquet], [/testdata/tpch/data/customer/15.parquet, /testdata/tpch/data/customer/16.parquet, /testdata/tpch/data/customer/2.parquet], [/testdata/tpch/data/customer/3.parquet, /testdata/tpch/data/customer/4.parquet, /testdata/tpch/data/customer/5.parquet], [/testdata/tpch/data/customer/6.parquet, /testdata/tpch/data/customer/7.parquet, /testdata/tpch/data/customer/8.parquet], ...]}, projection=[c_custkey, c_phone, c_acctbal], file_type=parquet, predicate=substr(c_phone@1, 1, 2) IN ([Literal { value: Utf8View("13"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("31"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("23"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("29"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("30"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("18"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }, Literal { value: Utf8View("17"), field: Field { name: "lit", data_type: Utf8View, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} } }])
              └──────────────────────────────────────────────────
        "#);
        Ok(())
    }

    async fn test_tpch_query(query_id: u8) -> Result<String, Box<dyn Error>> {
        let (ctx, _guard) = start_localhost_context(2, build_state).await;
        run_tpch_query(ctx, query_id).await
    }

    async fn build_state(
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError> {
        let rule = DistributedPhysicalOptimizerRule::new()
            .with_network_shuffle_tasks(SHUFFLE_TASKS)
            .with_network_coalesce_tasks(COALESCE_TASKS);
        Ok(SessionStateBuilder::new()
            .with_runtime_env(ctx.runtime_env)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(rule))
            .build())
    }

    // test_non_distributed_consistency runs each TPC-H query twice - once in a distributed manner
    // and once in a non-distributed manner. For each query, it asserts that the results are identical.
    async fn run_tpch_query(ctx_d: SessionContext, query_id: u8) -> Result<String, Box<dyn Error>> {
        ensure_tpch_data(TPCH_SCALE_FACTOR, TPCH_DATA_PARTS).await;
        let sql = get_test_tpch_query(query_id);
        ctx_d
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = PARTITIONS;

        // Context 1: Non-distributed execution.
        let config_s = SessionConfig::new().with_target_partitions(PARTITIONS);
        let state_s = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config_s)
            .build();
        let ctx_s = SessionContext::new_with_state(state_s);

        // Register tables for first context
        for table_name in [
            "lineitem", "orders", "part", "partsupp", "customer", "nation", "region", "supplier",
        ] {
            let query_path = get_test_data_dir().join(table_name);
            ctx_s
                .register_parquet(
                    table_name,
                    query_path.to_string_lossy().as_ref(),
                    datafusion::prelude::ParquetReadOptions::default(),
                )
                .await?;

            ctx_d
                .register_parquet(
                    table_name,
                    query_path.to_string_lossy().as_ref(),
                    datafusion::prelude::ParquetReadOptions::default(),
                )
                .await?;
        }

        // Query 15 has three queries in it, one creating the view, the second
        // executing, which we want to capture the output of, and the third
        // tearing down the view
        let (stream_s, stream_d, plan_d) = if query_id == 15 {
            let queries: Vec<&str> = sql
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();

            ctx_s.sql(queries[0]).await?.collect().await?;
            ctx_d.sql(queries[0]).await?.collect().await?;
            let df_s = ctx_s.sql(queries[1]).await?;
            let df_d = ctx_d.sql(queries[1]).await?;

            let plan_d = df_d.create_physical_plan().await?;

            let stream_s = df_s.execute_stream().await?;
            let stream_d = execute_stream(plan_d.clone(), ctx_d.task_ctx())?;

            ctx_s.sql(queries[2]).await?.collect().await?;
            ctx_d.sql(queries[2]).await?.collect().await?;
            (stream_s, stream_d, plan_d)
        } else {
            let stream_s = ctx_s.sql(&sql).await?.execute_stream().await?;
            let df_d = ctx_d.sql(&sql).await?;

            let plan_d = df_d.create_physical_plan().await?;

            let stream_d = execute_stream(plan_d.clone(), ctx_d.task_ctx())?;

            (stream_s, stream_d, plan_d)
        };

        let batches_s = stream_s.try_collect::<Vec<_>>().await?;
        let batches_d = stream_d.try_collect::<Vec<_>>().await?;

        let formatted_s = arrow::util::pretty::pretty_format_batches(&batches_s)?;
        let formatted_d = arrow::util::pretty::pretty_format_batches(&batches_d)?;

        assert_eq!(
            formatted_d.to_string(),
            formatted_s.to_string(),
            "Query {} results differ between executions",
            query_id
        );
        let plan_display = displayable(plan_d.as_ref()).indent(true).to_string();
        Ok(plan_display)
    }

    pub fn get_test_data_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/tpch/data")
    }

    pub fn get_test_queries_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/tpch/queries")
    }

    pub fn get_test_tpch_query(num: u8) -> String {
        let queries_dir = get_test_queries_dir();
        tpch::tpch_query_from_dir(&queries_dir, num)
    }

    // OnceCell to ensure TPCH tables are generated only once for tests
    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    // ensure_tpch_data initializes the TPCH data on disk.
    pub async fn ensure_tpch_data(sf: f64, parts: i32) {
        INIT_TEST_TPCH_TABLES
            .get_or_init(|| async {
                if !fs::exists(get_test_data_dir()).unwrap() {
                    tpch::generate_tpch_data(&get_test_data_dir(), sf, parts);
                }
            })
            .await;
    }
}
