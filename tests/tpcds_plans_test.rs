#[cfg(all(feature = "integration", feature = "tpcds", test))]
mod tests {
    use datafusion::error::Result;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::{benchmarks_common, tpcds};
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, assert_snapshot, display_plan_ascii,
    };
    use std::env;
    use std::fs;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const NUM_WORKERS: usize = 4;
    const FILES_PER_TASK: usize = 2;
    const CARDINALITY_TASK_COUNT_FACTOR: f64 = 2.0;
    const SF: f64 = 1.0;
    const PARQUET_PARTITIONS: usize = 4;

    #[tokio::test]
    async fn test_tpcds_1() -> Result<()> {
        let display = test_tpcds_query("q1").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c_customer_id@0 ASC NULLS LAST], fetch=100
        │   [Stage 9] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 9 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=100), expr=[c_customer_id@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(ctr_store_sk@0, ctr_store_sk@1)], filter=CAST(ctr_total_return@0 AS Decimal128(30, 15)) > avg(ctr2.ctr_total_return) * Float64(1.2)@1, projection=[c_customer_id@2]
          │     CoalescePartitionsExec
          │       [Stage 5] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=6, input_tasks=2
          │     ProjectionExec: expr=[CAST(CAST(avg(ctr2.ctr_total_return)@1 AS Float64) * 1.2 AS Decimal128(30, 15)) as avg(ctr2.ctr_total_return) * Float64(1.2), ctr_store_sk@0 as ctr_store_sk]
          │       AggregateExec: mode=FinalPartitioned, gby=[ctr_store_sk@0 as ctr_store_sk], aggr=[avg(ctr2.ctr_total_return)]
          │         [Stage 8] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 5 ── Tasks: t0:[p0..p5] t1:[p6..p11] 
            │ BroadcastExec: input_partitions=3, consumer_tasks=2, output_partitions=6
            │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(ctr_customer_sk@0, CAST(customer.c_customer_sk AS Float64)@2)], projection=[ctr_store_sk@1, ctr_total_return@2, c_customer_id@4]
            │     CoalescePartitionsExec
            │       [Stage 4] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=6, input_tasks=2
            │     ProjectionExec: expr=[c_customer_sk@0 as c_customer_sk, c_customer_id@1 as c_customer_id, CAST(c_customer_sk@0 AS Float64) as CAST(customer.c_customer_sk AS Float64)]
            │       RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
            │         PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
            │           DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/customer/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/customer/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/customer/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/customer/part-3.parquet]]}, projection=[c_customer_sk, c_customer_id], file_type=parquet, predicate=DynamicFilter [ empty ]
            └──────────────────────────────────────────────────
              ┌───── Stage 4 ── Tasks: t0:[p0..p5] t1:[p6..p11] 
              │ BroadcastExec: input_partitions=3, consumer_tasks=2, output_partitions=6
              │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(CAST(store.s_store_sk AS Float64)@1, ctr_store_sk@1)], projection=[ctr_customer_sk@2, ctr_store_sk@3, ctr_total_return@4]
              │     CoalescePartitionsExec
              │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=6, input_tasks=2
              │     ProjectionExec: expr=[sr_customer_sk@0 as ctr_customer_sk, sr_store_sk@1 as ctr_store_sk, sum(store_returns.sr_return_amt)@2 as ctr_total_return]
              │       AggregateExec: mode=FinalPartitioned, gby=[sr_customer_sk@0 as sr_customer_sk, sr_store_sk@1 as sr_store_sk], aggr=[sum(store_returns.sr_return_amt)]
              │         [Stage 3] => NetworkShuffleExec: output_partitions=3, input_tasks=3
              └──────────────────────────────────────────────────
                ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p6..p11] 
                │ BroadcastExec: input_partitions=3, consumer_tasks=2, output_partitions=6
                │   ProjectionExec: expr=[s_store_sk@0 as s_store_sk, CAST(s_store_sk@0 AS Float64) as CAST(store.s_store_sk AS Float64)]
                │     FilterExec: s_state@1 = TN, projection=[s_store_sk@0]
                │       RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
                │         PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
                │           DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/store/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/store/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/store/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/store/part-3.parquet]]}, projection=[s_store_sk, s_state], file_type=parquet, predicate=s_state@24 = TN, pruning_predicate=s_state_null_count@2 != row_count@3 AND s_state_min@0 <= TN AND TN <= s_state_max@1, required_guarantees=[s_state in (TN)]
                └──────────────────────────────────────────────────
                ┌───── Stage 3 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
                │ RepartitionExec: partitioning=Hash([sr_customer_sk@0, sr_store_sk@1], 6), input_partitions=2
                │   AggregateExec: mode=Partial, gby=[sr_customer_sk@0 as sr_customer_sk, sr_store_sk@1 as sr_store_sk], aggr=[sum(store_returns.sr_return_amt)]
                │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(CAST(date_dim.d_date_sk AS Float64)@1, sr_returned_date_sk@0)], projection=[sr_customer_sk@3, sr_store_sk@4, sr_return_amt@5]
                │       CoalescePartitionsExec
                │         [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=2
                │       PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
                │         DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_returns/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_returns/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_returns/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_returns/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_returns/part-2.parquet:<int>..<int>], ...]}, projection=[sr_returned_date_sk, sr_customer_sk, sr_store_sk, sr_return_amt], file_type=parquet, predicate=DynamicFilter [ empty ]
                └──────────────────────────────────────────────────
                  ┌───── Stage 2 ── Tasks: t0:[p0..p8] t1:[p9..p17] 
                  │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
                  │   ProjectionExec: expr=[d_date_sk@0 as d_date_sk, CAST(d_date_sk@0 AS Float64) as CAST(date_dim.d_date_sk AS Float64)]
                  │     FilterExec: d_year@1 = 2000, projection=[d_date_sk@0]
                  │       RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
                  │         PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
                  │           DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/date_dim/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-3.parquet]]}, projection=[d_date_sk, d_year], file_type=parquet, predicate=d_year@6 = 2000, pruning_predicate=d_year_null_count@2 != row_count@3 AND d_year_min@0 <= 2000 AND 2000 <= d_year_max@1, required_guarantees=[d_year in (2000)]
                  └──────────────────────────────────────────────────
            ┌───── Stage 8 ── Tasks: t0:[p0..p5] t1:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([ctr_store_sk@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[ctr_store_sk@0 as ctr_store_sk], aggr=[avg(ctr2.ctr_total_return)]
            │     ProjectionExec: expr=[sr_store_sk@1 as ctr_store_sk, sum(store_returns.sr_return_amt)@2 as ctr_total_return]
            │       AggregateExec: mode=FinalPartitioned, gby=[sr_customer_sk@0 as sr_customer_sk, sr_store_sk@1 as sr_store_sk], aggr=[sum(store_returns.sr_return_amt)]
            │         [Stage 7] => NetworkShuffleExec: output_partitions=3, input_tasks=3
            └──────────────────────────────────────────────────
              ┌───── Stage 7 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
              │ RepartitionExec: partitioning=Hash([sr_customer_sk@0, sr_store_sk@1], 6), input_partitions=2
              │   AggregateExec: mode=Partial, gby=[sr_customer_sk@0 as sr_customer_sk, sr_store_sk@1 as sr_store_sk], aggr=[sum(store_returns.sr_return_amt)]
              │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(CAST(date_dim.d_date_sk AS Float64)@1, sr_returned_date_sk@0)], projection=[sr_customer_sk@3, sr_store_sk@4, sr_return_amt@5]
              │       CoalescePartitionsExec
              │         [Stage 6] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=2
              │       PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
              │         DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_returns/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_returns/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_returns/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_returns/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_returns/part-2.parquet:<int>..<int>], ...]}, projection=[sr_returned_date_sk, sr_customer_sk, sr_store_sk, sr_return_amt], file_type=parquet, predicate=DynamicFilter [ empty ]
              └──────────────────────────────────────────────────
                ┌───── Stage 6 ── Tasks: t0:[p0..p8] t1:[p9..p17] 
                │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
                │   ProjectionExec: expr=[d_date_sk@0 as d_date_sk, CAST(d_date_sk@0 AS Float64) as CAST(date_dim.d_date_sk AS Float64)]
                │     FilterExec: d_year@1 = 2000, projection=[d_date_sk@0]
                │       RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
                │         PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
                │           DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/date_dim/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-3.parquet]]}, projection=[d_date_sk, d_year], file_type=parquet, predicate=d_year@6 = 2000, pruning_predicate=d_year_null_count@2 != row_count@3 AND d_year_min@0 <= 2000 AND 2000 <= d_year_max@1, required_guarantees=[d_year in (2000)]
                └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpcds_9() -> Result<()> {
        let display = test_tpcds_query("q9").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   ProjectionExec: expr=[CASE WHEN count(*)@0 > 74129 THEN avg(store_sales.ss_ext_discount_amt)@1 ELSE avg(store_sales.ss_net_paid)@2 END as bucket1, CASE WHEN count(*)@3 > 122840 THEN avg(store_sales.ss_ext_discount_amt)@4 ELSE avg(store_sales.ss_net_paid)@5 END as bucket2, CASE WHEN count(*)@6 > 56580 THEN avg(store_sales.ss_ext_discount_amt)@7 ELSE avg(store_sales.ss_net_paid)@8 END as bucket3, CASE WHEN count(*)@9 > 10097 THEN avg(store_sales.ss_ext_discount_amt)@10 ELSE avg(store_sales.ss_net_paid)@11 END as bucket4, CASE WHEN count(*)@12 > 165306 THEN avg(store_sales.ss_ext_discount_amt)@13 ELSE avg(store_sales.ss_net_paid)@14 END as bucket5]
        │     RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1
        │       NestedLoopJoinExec: join_type=Left
        │         NestedLoopJoinExec: join_type=Left
        │           NestedLoopJoinExec: join_type=Left
        │             NestedLoopJoinExec: join_type=Left
        │               NestedLoopJoinExec: join_type=Left
        │                 NestedLoopJoinExec: join_type=Left
        │                   NestedLoopJoinExec: join_type=Left
        │                     NestedLoopJoinExec: join_type=Left
        │                       NestedLoopJoinExec: join_type=Left
        │                         NestedLoopJoinExec: join_type=Left
        │                           NestedLoopJoinExec: join_type=Left
        │                             NestedLoopJoinExec: join_type=Left
        │                               NestedLoopJoinExec: join_type=Left
        │                                 NestedLoopJoinExec: join_type=Left
        │                                   NestedLoopJoinExec: join_type=Left
        │                                     CoalescePartitionsExec
        │                                       [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        │                                     ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │                                       AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │                                         CoalescePartitionsExec
        │                                           [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │                                   AggregateExec: mode=Final, gby=[], aggr=[avg(store_sales.ss_ext_discount_amt)]
        │                                     CoalescePartitionsExec
        │                                       [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │                                 AggregateExec: mode=Final, gby=[], aggr=[avg(store_sales.ss_net_paid)]
        │                                   CoalescePartitionsExec
        │                                     [Stage 4] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │                               ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │                                 AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │                                   CoalescePartitionsExec
        │                                     [Stage 5] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │                             AggregateExec: mode=Final, gby=[], aggr=[avg(store_sales.ss_ext_discount_amt)]
        │                               CoalescePartitionsExec
        │                                 [Stage 6] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │                           AggregateExec: mode=Final, gby=[], aggr=[avg(store_sales.ss_net_paid)]
        │                             CoalescePartitionsExec
        │                               [Stage 7] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │                         ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │                           AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │                             CoalescePartitionsExec
        │                               [Stage 8] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │                       AggregateExec: mode=Final, gby=[], aggr=[avg(store_sales.ss_ext_discount_amt)]
        │                         CoalescePartitionsExec
        │                           [Stage 9] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │                     AggregateExec: mode=Final, gby=[], aggr=[avg(store_sales.ss_net_paid)]
        │                       CoalescePartitionsExec
        │                         [Stage 10] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │                   ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │                     AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │                       CoalescePartitionsExec
        │                         [Stage 11] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │                 AggregateExec: mode=Final, gby=[], aggr=[avg(store_sales.ss_ext_discount_amt)]
        │                   CoalescePartitionsExec
        │                     [Stage 12] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │               AggregateExec: mode=Final, gby=[], aggr=[avg(store_sales.ss_net_paid)]
        │                 CoalescePartitionsExec
        │                   [Stage 13] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │             ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │               AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │                 CoalescePartitionsExec
        │                   [Stage 14] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │           AggregateExec: mode=Final, gby=[], aggr=[avg(store_sales.ss_ext_discount_amt)]
        │             CoalescePartitionsExec
        │               [Stage 15] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        │         AggregateExec: mode=Final, gby=[], aggr=[avg(store_sales.ss_net_paid)]
        │           CoalescePartitionsExec
        │             [Stage 16] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5] 
          │ ProjectionExec: expr=[]
          │   FilterExec: r_reason_sk@0 = 1
          │     RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
          │       PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
          │         DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/reason/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/reason/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/reason/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/reason/part-3.parquet]]}, projection=[r_reason_sk], file_type=parquet, predicate=r_reason_sk@0 = 1, pruning_predicate=r_reason_sk_null_count@2 != row_count@3 AND r_reason_sk_min@0 <= 1 AND 1 <= r_reason_sk_max@1, required_guarantees=[r_reason_sk in (1)]
          └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   ProjectionExec: expr=[]
          │     FilterExec: ss_quantity@0 >= 1 AND ss_quantity@0 <= 20
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │         DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity], file_type=parquet, predicate=ss_quantity@10 >= 1 AND ss_quantity@10 <= 20, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 1 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 20, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(store_sales.ss_ext_discount_amt)]
          │   FilterExec: ss_quantity@0 >= 1 AND ss_quantity@0 <= 20, projection=[ss_ext_discount_amt@1]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │       DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity, ss_ext_discount_amt], file_type=parquet, predicate=ss_quantity@10 >= 1 AND ss_quantity@10 <= 20, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 1 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 20, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 4 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(store_sales.ss_net_paid)]
          │   FilterExec: ss_quantity@0 >= 1 AND ss_quantity@0 <= 20, projection=[ss_net_paid@1]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │       DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity, ss_net_paid], file_type=parquet, predicate=ss_quantity@10 >= 1 AND ss_quantity@10 <= 20, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 1 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 20, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 5 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   ProjectionExec: expr=[]
          │     FilterExec: ss_quantity@0 >= 21 AND ss_quantity@0 <= 40
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │         DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity], file_type=parquet, predicate=ss_quantity@10 >= 21 AND ss_quantity@10 <= 40, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 21 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 40, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 6 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(store_sales.ss_ext_discount_amt)]
          │   FilterExec: ss_quantity@0 >= 21 AND ss_quantity@0 <= 40, projection=[ss_ext_discount_amt@1]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │       DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity, ss_ext_discount_amt], file_type=parquet, predicate=ss_quantity@10 >= 21 AND ss_quantity@10 <= 40, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 21 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 40, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 7 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(store_sales.ss_net_paid)]
          │   FilterExec: ss_quantity@0 >= 21 AND ss_quantity@0 <= 40, projection=[ss_net_paid@1]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │       DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity, ss_net_paid], file_type=parquet, predicate=ss_quantity@10 >= 21 AND ss_quantity@10 <= 40, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 21 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 40, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 8 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   ProjectionExec: expr=[]
          │     FilterExec: ss_quantity@0 >= 41 AND ss_quantity@0 <= 60
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │         DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity], file_type=parquet, predicate=ss_quantity@10 >= 41 AND ss_quantity@10 <= 60, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 41 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 60, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 9 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(store_sales.ss_ext_discount_amt)]
          │   FilterExec: ss_quantity@0 >= 41 AND ss_quantity@0 <= 60, projection=[ss_ext_discount_amt@1]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │       DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity, ss_ext_discount_amt], file_type=parquet, predicate=ss_quantity@10 >= 41 AND ss_quantity@10 <= 60, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 41 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 60, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 10 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(store_sales.ss_net_paid)]
          │   FilterExec: ss_quantity@0 >= 41 AND ss_quantity@0 <= 60, projection=[ss_net_paid@1]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │       DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity, ss_net_paid], file_type=parquet, predicate=ss_quantity@10 >= 41 AND ss_quantity@10 <= 60, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 41 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 60, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 11 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   ProjectionExec: expr=[]
          │     FilterExec: ss_quantity@0 >= 61 AND ss_quantity@0 <= 80
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │         DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity], file_type=parquet, predicate=ss_quantity@10 >= 61 AND ss_quantity@10 <= 80, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 61 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 80, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 12 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(store_sales.ss_ext_discount_amt)]
          │   FilterExec: ss_quantity@0 >= 61 AND ss_quantity@0 <= 80, projection=[ss_ext_discount_amt@1]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │       DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity, ss_ext_discount_amt], file_type=parquet, predicate=ss_quantity@10 >= 61 AND ss_quantity@10 <= 80, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 61 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 80, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 13 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(store_sales.ss_net_paid)]
          │   FilterExec: ss_quantity@0 >= 61 AND ss_quantity@0 <= 80, projection=[ss_net_paid@1]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │       DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity, ss_net_paid], file_type=parquet, predicate=ss_quantity@10 >= 61 AND ss_quantity@10 <= 80, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 61 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 80, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 14 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   ProjectionExec: expr=[]
          │     FilterExec: ss_quantity@0 >= 81 AND ss_quantity@0 <= 100
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │         DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity], file_type=parquet, predicate=ss_quantity@10 >= 81 AND ss_quantity@10 <= 100, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 81 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 100, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 15 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(store_sales.ss_ext_discount_amt)]
          │   FilterExec: ss_quantity@0 >= 81 AND ss_quantity@0 <= 100, projection=[ss_ext_discount_amt@1]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │       DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity, ss_ext_discount_amt], file_type=parquet, predicate=ss_quantity@10 >= 81 AND ss_quantity@10 <= 100, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 81 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 100, required_guarantees=[]
          └──────────────────────────────────────────────────
          ┌───── Stage 16 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(store_sales.ss_net_paid)]
          │   FilterExec: ss_quantity@0 >= 81 AND ss_quantity@0 <= 100, projection=[ss_net_paid@1]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
          │       DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/store_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/store_sales/part-2.parquet:<int>..<int>], ...]}, projection=[ss_quantity, ss_net_paid], file_type=parquet, predicate=ss_quantity@10 >= 81 AND ss_quantity@10 <= 100, pruning_predicate=ss_quantity_null_count@1 != row_count@2 AND ss_quantity_max@0 >= 81 AND ss_quantity_null_count@1 != row_count@2 AND ss_quantity_min@3 <= 100, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpcds_72() -> Result<()> {
        let display = test_tpcds_query("q72").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [total_cnt@5 DESC, i_item_desc@0 ASC, w_warehouse_name@1 ASC, d_week_seq@2 ASC], fetch=100
        │   SortExec: TopK(fetch=100), expr=[total_cnt@5 DESC, i_item_desc@0 ASC, w_warehouse_name@1 ASC, d_week_seq@2 ASC], preserve_partitioning=[true]
        │     ProjectionExec: expr=[i_item_desc@0 as i_item_desc, w_warehouse_name@1 as w_warehouse_name, d_week_seq@2 as d_week_seq, sum(CASE WHEN promotion.p_promo_sk IS NULL THEN Int64(1) ELSE Int64(0) END)@3 as no_promo, sum(CASE WHEN promotion.p_promo_sk IS NOT NULL THEN Int64(1) ELSE Int64(0) END)@4 as promo, count(Int64(1))@5 as total_cnt]
        │       AggregateExec: mode=FinalPartitioned, gby=[i_item_desc@0 as i_item_desc, w_warehouse_name@1 as w_warehouse_name, d_week_seq@2 as d_week_seq], aggr=[sum(CASE WHEN promotion.p_promo_sk IS NULL THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN promotion.p_promo_sk IS NOT NULL THEN Int64(1) ELSE Int64(0) END), count(Int64(1))]
        │         RepartitionExec: partitioning=Hash([i_item_desc@0, w_warehouse_name@1, d_week_seq@2], 3), input_partitions=3
        │           AggregateExec: mode=Partial, gby=[i_item_desc@1 as i_item_desc, w_warehouse_name@0 as w_warehouse_name, d_week_seq@2 as d_week_seq], aggr=[sum(CASE WHEN promotion.p_promo_sk IS NULL THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN promotion.p_promo_sk IS NOT NULL THEN Int64(1) ELSE Int64(0) END), count(Int64(1))]
        │             HashJoinExec: mode=CollectLeft, join_type=Left, on=[(cs_item_sk@0, cr_item_sk@0), (cs_order_number@1, cr_order_number@1)], projection=[w_warehouse_name@2, i_item_desc@3, d_week_seq@4, p_promo_sk@5]
        │               CoalescePartitionsExec
        │                 [Stage 12] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        │               DataSourceExec: file_groups={3 groups: [[/testdata/tpcds/plans_sf1_partitions4/catalog_returns/part-0.parquet:<int>..<int>, /testdata/tpcds/plans_sf1_partitions4/catalog_returns/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/catalog_returns/part-1.parquet:<int>..<int>, /testdata/tpcds/plans_sf1_partitions4/catalog_returns/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/catalog_returns/part-2.parquet:<int>..<int>, /testdata/tpcds/plans_sf1_partitions4/catalog_returns/part-3.parquet:<int>..<int>]]}, projection=[cr_item_sk, cr_order_number], file_type=parquet
        └──────────────────────────────────────────────────
          ┌───── Stage 12 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8] 
          │ ProjectionExec: expr=[cs_item_sk@1 as cs_item_sk, cs_order_number@2 as cs_order_number, w_warehouse_name@3 as w_warehouse_name, i_item_desc@4 as i_item_desc, d_week_seq@5 as d_week_seq, p_promo_sk@0 as p_promo_sk]
          │   HashJoinExec: mode=CollectLeft, join_type=Right, on=[(CAST(promotion.p_promo_sk AS Float64)@1, cs_promo_sk@1)], projection=[p_promo_sk@0, cs_item_sk@2, cs_order_number@4, w_warehouse_name@5, i_item_desc@6, d_week_seq@7]
          │     CoalescePartitionsExec
          │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=2, stage_partitions=6, input_tasks=2
          │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(cs_ship_date_sk@0, CAST(d3.d_date_sk AS Float64)@2)], filter=d_date@1 > d_date@0 + IntervalMonthDayNano { months: 0, days: 5, nanoseconds: 0 }, projection=[cs_item_sk@1, cs_promo_sk@2, cs_order_number@3, w_warehouse_name@4, i_item_desc@5, d_week_seq@7]
          │       CoalescePartitionsExec
          │         [Stage 11] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=3
          │       ProjectionExec: expr=[d_date_sk@0 as d_date_sk, d_date@1 as d_date, CAST(d_date_sk@0 AS Float64) as CAST(d3.d_date_sk AS Float64)]
          │         RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
          │           PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,__] t2:[__,__,__,p0]
          │             DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/date_dim/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-3.parquet]]}, projection=[d_date_sk, d_date], file_type=parquet
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p6..p11] 
            │ BroadcastExec: input_partitions=2, consumer_tasks=3, output_partitions=6
            │   PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
            │     DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/promotion/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/promotion/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/promotion/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/promotion/part-3.parquet]]}, projection=[p_promo_sk, CAST(p_promo_sk@0 AS Float64) as CAST(promotion.p_promo_sk AS Float64)], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 11 ── Tasks: t0:[p0..p8] t1:[p9..p17] t2:[p18..p26] 
            │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
            │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(inv_date_sk@4, d_date_sk@0), (d_week_seq@8, d_week_seq@1)], projection=[cs_ship_date_sk@0, cs_item_sk@1, cs_promo_sk@2, cs_order_number@3, w_warehouse_name@5, i_item_desc@6, d_date@7, d_week_seq@8]
            │     CoalescePartitionsExec
            │       [Stage 10] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=3
            │     RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
            │       PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,__] t2:[__,__,__,p0]
            │         DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/date_dim/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-3.parquet]]}, projection=[d_date_sk, d_week_seq], file_type=parquet, predicate=DynamicFilter [ empty ]
            └──────────────────────────────────────────────────
              ┌───── Stage 10 ── Tasks: t0:[p0..p8] t1:[p9..p17] t2:[p18..p26] 
              │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
              │   ProjectionExec: expr=[cs_ship_date_sk@2 as cs_ship_date_sk, cs_item_sk@3 as cs_item_sk, cs_promo_sk@4 as cs_promo_sk, cs_order_number@5 as cs_order_number, inv_date_sk@6 as inv_date_sk, w_warehouse_name@7 as w_warehouse_name, i_item_desc@8 as i_item_desc, d_date@0 as d_date, d_week_seq@1 as d_week_seq]
              │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(CAST(d1.d_date_sk AS Float64)@3, cs_sold_date_sk@0)], projection=[d_date@1, d_week_seq@2, cs_ship_date_sk@5, cs_item_sk@6, cs_promo_sk@7, cs_order_number@8, inv_date_sk@9, w_warehouse_name@10, i_item_desc@11]
              │       CoalescePartitionsExec
              │         [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=2
              │       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(CAST(household_demographics.hd_demo_sk AS Float64)@1, cs_bill_hdemo_sk@2)], projection=[cs_sold_date_sk@2, cs_ship_date_sk@3, cs_item_sk@5, cs_promo_sk@6, cs_order_number@7, inv_date_sk@8, w_warehouse_name@9, i_item_desc@10]
              │         CoalescePartitionsExec
              │           [Stage 3] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=2
              │         HashJoinExec: mode=Partitioned, join_type=Inner, on=[(CAST(customer_demographics.cd_demo_sk AS Float64)@1, cs_bill_cdemo_sk@2)], projection=[cs_sold_date_sk@2, cs_ship_date_sk@3, cs_bill_hdemo_sk@5, cs_item_sk@6, cs_promo_sk@7, cs_order_number@8, inv_date_sk@9, w_warehouse_name@10, i_item_desc@11]
              │           [Stage 4] => NetworkShuffleExec: output_partitions=3, input_tasks=3
              │           [Stage 9] => NetworkShuffleExec: output_partitions=3, input_tasks=3
              └──────────────────────────────────────────────────
                ┌───── Stage 2 ── Tasks: t0:[p0..p8] t1:[p9..p17] 
                │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
                │   ProjectionExec: expr=[d_date_sk@0 as d_date_sk, d_date@1 as d_date, d_week_seq@2 as d_week_seq, CAST(d_date_sk@0 AS Float64) as CAST(d1.d_date_sk AS Float64)]
                │     FilterExec: d_year@3 = 1999, projection=[d_date_sk@0, d_date@1, d_week_seq@2]
                │       RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
                │         PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
                │           DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/date_dim/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-3.parquet]]}, projection=[d_date_sk, d_date, d_week_seq, d_year], file_type=parquet, predicate=d_year@6 = 1999, pruning_predicate=d_year_null_count@2 != row_count@3 AND d_year_min@0 <= 1999 AND 1999 <= d_year_max@1, required_guarantees=[d_year in (1999)]
                └──────────────────────────────────────────────────
                ┌───── Stage 3 ── Tasks: t0:[p0..p8] t1:[p9..p17] 
                │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
                │   ProjectionExec: expr=[hd_demo_sk@0 as hd_demo_sk, CAST(hd_demo_sk@0 AS Float64) as CAST(household_demographics.hd_demo_sk AS Float64)]
                │     FilterExec: hd_buy_potential@1 = >10000, projection=[hd_demo_sk@0]
                │       RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
                │         PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
                │           DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/household_demographics/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/household_demographics/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/household_demographics/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/household_demographics/part-3.parquet]]}, projection=[hd_demo_sk, hd_buy_potential], file_type=parquet, predicate=hd_buy_potential@2 = >10000, pruning_predicate=hd_buy_potential_null_count@2 != row_count@3 AND hd_buy_potential_min@0 <= >10000 AND >10000 <= hd_buy_potential_max@1, required_guarantees=[hd_buy_potential in (>10000)]
                └──────────────────────────────────────────────────
                ┌───── Stage 4 ── Tasks: t0:[p0..p8] t1:[p0..p8] t2:[p0..p8] 
                │ RepartitionExec: partitioning=Hash([CAST(customer_demographics.cd_demo_sk AS Float64)@1], 9), input_partitions=2
                │   ProjectionExec: expr=[cd_demo_sk@0 as cd_demo_sk, CAST(cd_demo_sk@0 AS Float64) as CAST(customer_demographics.cd_demo_sk AS Float64)]
                │     FilterExec: cd_marital_status@1 = D, projection=[cd_demo_sk@0]
                │       PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
                │         DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/customer_demographics/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/customer_demographics/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/customer_demographics/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/customer_demographics/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/customer_demographics/part-2.parquet:<int>..<int>], ...]}, projection=[cd_demo_sk, cd_marital_status], file_type=parquet, predicate=cd_marital_status@2 = D, pruning_predicate=cd_marital_status_null_count@2 != row_count@3 AND cd_marital_status_min@0 <= D AND D <= cd_marital_status_max@1, required_guarantees=[cd_marital_status in (D)]
                └──────────────────────────────────────────────────
                ┌───── Stage 9 ── Tasks: t0:[p0..p8] t1:[p0..p8] t2:[p0..p8] 
                │ RepartitionExec: partitioning=Hash([cs_bill_cdemo_sk@2], 9), input_partitions=3
                │   ProjectionExec: expr=[cs_sold_date_sk@1 as cs_sold_date_sk, cs_ship_date_sk@2 as cs_ship_date_sk, cs_bill_cdemo_sk@3 as cs_bill_cdemo_sk, cs_bill_hdemo_sk@4 as cs_bill_hdemo_sk, cs_item_sk@5 as cs_item_sk, cs_promo_sk@6 as cs_promo_sk, cs_order_number@7 as cs_order_number, inv_date_sk@8 as inv_date_sk, w_warehouse_name@9 as w_warehouse_name, i_item_desc@0 as i_item_desc]
                │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(i_item_sk@0, cs_item_sk@4)], projection=[i_item_desc@1, cs_sold_date_sk@2, cs_ship_date_sk@3, cs_bill_cdemo_sk@4, cs_bill_hdemo_sk@5, cs_item_sk@6, cs_promo_sk@7, cs_order_number@8, inv_date_sk@9, w_warehouse_name@10]
                │       CoalescePartitionsExec
                │         [Stage 5] => NetworkBroadcastExec: partitions_per_consumer=2, stage_partitions=6, input_tasks=2
                │       ProjectionExec: expr=[cs_sold_date_sk@1 as cs_sold_date_sk, cs_ship_date_sk@2 as cs_ship_date_sk, cs_bill_cdemo_sk@3 as cs_bill_cdemo_sk, cs_bill_hdemo_sk@4 as cs_bill_hdemo_sk, cs_item_sk@5 as cs_item_sk, cs_promo_sk@6 as cs_promo_sk, cs_order_number@7 as cs_order_number, inv_date_sk@8 as inv_date_sk, w_warehouse_name@0 as w_warehouse_name]
                │         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(w_warehouse_sk@0, inv_warehouse_sk@8)], projection=[w_warehouse_name@1, cs_sold_date_sk@2, cs_ship_date_sk@3, cs_bill_cdemo_sk@4, cs_bill_hdemo_sk@5, cs_item_sk@6, cs_promo_sk@7, cs_order_number@8, inv_date_sk@9]
                │           CoalescePartitionsExec
                │             [Stage 6] => NetworkBroadcastExec: partitions_per_consumer=2, stage_partitions=6, input_tasks=2
                │           HashJoinExec: mode=Partitioned, join_type=Inner, on=[(cs_item_sk@4, inv_item_sk@1)], filter=inv_quantity_on_hand@1 < cs_quantity@0, projection=[cs_sold_date_sk@0, cs_ship_date_sk@1, cs_bill_cdemo_sk@2, cs_bill_hdemo_sk@3, cs_item_sk@4, cs_promo_sk@5, cs_order_number@6, inv_date_sk@8, inv_warehouse_sk@10]
                │             [Stage 7] => NetworkShuffleExec: output_partitions=3, input_tasks=3
                │             [Stage 8] => NetworkShuffleExec: output_partitions=3, input_tasks=3
                └──────────────────────────────────────────────────
                  ┌───── Stage 5 ── Tasks: t0:[p0..p5] t1:[p6..p11] 
                  │ BroadcastExec: input_partitions=2, consumer_tasks=3, output_partitions=6
                  │   PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
                  │     DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/item/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/item/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/item/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/item/part-3.parquet]]}, projection=[i_item_sk, i_item_desc], file_type=parquet
                  └──────────────────────────────────────────────────
                  ┌───── Stage 6 ── Tasks: t0:[p0..p5] t1:[p6..p11] 
                  │ BroadcastExec: input_partitions=2, consumer_tasks=3, output_partitions=6
                  │   PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
                  │     DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/warehouse/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/warehouse/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/warehouse/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/warehouse/part-3.parquet]]}, projection=[w_warehouse_sk, w_warehouse_name], file_type=parquet
                  └──────────────────────────────────────────────────
                  ┌───── Stage 7 ── Tasks: t0:[p0..p8] t1:[p0..p8] t2:[p0..p8] 
                  │ RepartitionExec: partitioning=Hash([cs_item_sk@4], 9), input_partitions=2
                  │   PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
                  │     DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/catalog_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/catalog_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/catalog_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/catalog_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/catalog_sales/part-2.parquet:<int>..<int>], ...]}, projection=[cs_sold_date_sk, cs_ship_date_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ] AND DynamicFilter [ empty ] AND DynamicFilter [ empty ]
                  └──────────────────────────────────────────────────
                  ┌───── Stage 8 ── Tasks: t0:[p0..p8] t1:[p0..p8] t2:[p0..p8] 
                  │ RepartitionExec: partitioning=Hash([inv_item_sk@1], 9), input_partitions=2
                  │   PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
                  │     DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/inventory/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/inventory/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/inventory/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/inventory/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/inventory/part-2.parquet:<int>..<int>], ...]}, projection=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ]
                  └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpcds_99() -> Result<()> {
        let display = test_tpcds_query("q99").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [w_substr@0 ASC, sm_type@1 ASC, cc_name_lower@2 ASC], fetch=100
        │   [Stage 6] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 6 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=100), expr=[w_substr@0 ASC, sm_type@1 ASC, cc_name_lower@2 ASC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[w_substr@0 as w_substr, sm_type@1 as sm_type, lower(cc_name@2) as cc_name_lower, sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(30) THEN Int64(1) ELSE Int64(0) END)@3 as 30 days, sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(30) AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(60) THEN Int64(1) ELSE Int64(0) END)@4 as 31-60 days, sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(60) AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(90) THEN Int64(1) ELSE Int64(0) END)@5 as 61-90 days, sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(90) AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(120) THEN Int64(1) ELSE Int64(0) END)@6 as 91-120 days, sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(120) THEN Int64(1) ELSE Int64(0) END)@7 as >120 days]
          │     AggregateExec: mode=FinalPartitioned, gby=[w_substr@0 as w_substr, sm_type@1 as sm_type, cc_name@2 as cc_name], aggr=[sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(30) THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(30) AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(60) THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(60) AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(90) THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(90) AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(120) THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(120) THEN Int64(1) ELSE Int64(0) END)]
          │       [Stage 5] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 5 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([w_substr@0, sm_type@1, cc_name@2], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[w_substr@1 as w_substr, sm_type@2 as sm_type, cc_name@3 as cc_name], aggr=[sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(30) THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(30) AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(60) THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(60) AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(90) THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(90) AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= Int64(120) THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > Int64(120) THEN Int64(1) ELSE Int64(0) END)]
            │     ProjectionExec: expr=[cs_ship_date_sk@1 - cs_sold_date_sk@0 as __common_expr_1, w_substr@2 as w_substr, sm_type@3 as sm_type, cc_name@4 as cc_name]
            │       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(cs_ship_date_sk@1, CAST(date_dim.d_date_sk AS Float64)@1)], projection=[cs_sold_date_sk@0, cs_ship_date_sk@1, w_substr@2, sm_type@3, cc_name@4]
            │         CoalescePartitionsExec
            │           [Stage 4] => NetworkBroadcastExec: partitions_per_consumer=2, stage_partitions=6, input_tasks=3
            │         ProjectionExec: expr=[d_date_sk@0 as d_date_sk, CAST(d_date_sk@0 AS Float64) as CAST(date_dim.d_date_sk AS Float64)]
            │           FilterExec: d_month_seq@1 >= 1200 AND d_month_seq@1 <= 1211, projection=[d_date_sk@0]
            │             RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=2
            │               PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,__] t2:[__,__,__,p0]
            │                 DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/date_dim/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/date_dim/part-3.parquet]]}, projection=[d_date_sk, d_month_seq], file_type=parquet, predicate=d_month_seq@3 >= 1200 AND d_month_seq@3 <= 1211, pruning_predicate=d_month_seq_null_count@1 != row_count@2 AND d_month_seq_max@0 >= 1200 AND d_month_seq_null_count@1 != row_count@2 AND d_month_seq_min@3 <= 1211, required_guarantees=[]
            └──────────────────────────────────────────────────
              ┌───── Stage 4 ── Tasks: t0:[p0..p5] t1:[p6..p11] t2:[p12..p17] 
              │ BroadcastExec: input_partitions=2, consumer_tasks=3, output_partitions=6
              │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(cs_call_center_sk@2, CAST(call_center.cc_call_center_sk AS Float64)@2)], projection=[cs_sold_date_sk@0, cs_ship_date_sk@1, w_substr@3, sm_type@4, cc_name@6]
              │     CoalescePartitionsExec
              │       [Stage 3] => NetworkBroadcastExec: partitions_per_consumer=2, stage_partitions=6, input_tasks=3
              │     PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,__] t2:[__,__,__,p0]
              │       DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/call_center/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/call_center/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/call_center/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/call_center/part-3.parquet]]}, projection=[cc_call_center_sk, cc_name, CAST(cc_call_center_sk@0 AS Float64) as CAST(call_center.cc_call_center_sk AS Float64)], file_type=parquet, predicate=DynamicFilter [ empty ]
              └──────────────────────────────────────────────────
                ┌───── Stage 3 ── Tasks: t0:[p0..p5] t1:[p6..p11] t2:[p12..p17] 
                │ BroadcastExec: input_partitions=2, consumer_tasks=3, output_partitions=6
                │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(cs_ship_mode_sk@3, CAST(ship_mode.sm_ship_mode_sk AS Float64)@2)], projection=[cs_sold_date_sk@0, cs_ship_date_sk@1, cs_call_center_sk@2, w_substr@4, sm_type@6]
                │     CoalescePartitionsExec
                │       [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=2, stage_partitions=6, input_tasks=3
                │     PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,__] t2:[__,__,__,p0]
                │       DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/ship_mode/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/ship_mode/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/ship_mode/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/ship_mode/part-3.parquet]]}, projection=[sm_ship_mode_sk, sm_type, CAST(sm_ship_mode_sk@0 AS Float64) as CAST(ship_mode.sm_ship_mode_sk AS Float64)], file_type=parquet, predicate=DynamicFilter [ empty ]
                └──────────────────────────────────────────────────
                  ┌───── Stage 2 ── Tasks: t0:[p0..p5] t1:[p6..p11] t2:[p12..p17] 
                  │ BroadcastExec: input_partitions=2, consumer_tasks=3, output_partitions=6
                  │   ProjectionExec: expr=[cs_sold_date_sk@1 as cs_sold_date_sk, cs_ship_date_sk@2 as cs_ship_date_sk, cs_call_center_sk@3 as cs_call_center_sk, cs_ship_mode_sk@4 as cs_ship_mode_sk, w_substr@0 as w_substr]
                  │     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(CAST(sq1.w_warehouse_sk AS Float64)@2, cs_warehouse_sk@4)], projection=[w_substr@0, cs_sold_date_sk@3, cs_ship_date_sk@4, cs_call_center_sk@5, cs_ship_mode_sk@6]
                  │       CoalescePartitionsExec
                  │         [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=2, stage_partitions=6, input_tasks=2
                  │       PartitionIsolatorExec: t0:[p0,p1,__,__,__,__] t1:[__,__,p0,p1,__,__] t2:[__,__,__,__,p0,p1]
                  │         DataSourceExec: file_groups={6 groups: [[/testdata/tpcds/plans_sf1_partitions4/catalog_sales/part-0.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/catalog_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/catalog_sales/part-1.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/catalog_sales/part-2.parquet:<int>..<int>], [/testdata/tpcds/plans_sf1_partitions4/catalog_sales/part-2.parquet:<int>..<int>], ...]}, projection=[cs_sold_date_sk, cs_ship_date_sk, cs_call_center_sk, cs_ship_mode_sk, cs_warehouse_sk], file_type=parquet, predicate=DynamicFilter [ empty ]
                  └──────────────────────────────────────────────────
                    ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p6..p11] 
                    │ BroadcastExec: input_partitions=2, consumer_tasks=3, output_partitions=6
                    │   PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
                    │     DataSourceExec: file_groups={4 groups: [[/testdata/tpcds/plans_sf1_partitions4/warehouse/part-0.parquet], [/testdata/tpcds/plans_sf1_partitions4/warehouse/part-1.parquet], [/testdata/tpcds/plans_sf1_partitions4/warehouse/part-2.parquet], [/testdata/tpcds/plans_sf1_partitions4/warehouse/part-3.parquet]]}, projection=[substr(w_warehouse_name@2, 1, 20) as w_substr, w_warehouse_sk, CAST(w_warehouse_sk@0 AS Float64) as CAST(sq1.w_warehouse_sk AS Float64)], file_type=parquet
                    └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    static INIT_TEST_TPCDS_TABLES: OnceCell<()> = OnceCell::const_new();

    async fn test_tpcds_query(query_id: &str) -> Result<String> {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "testdata/tpcds/plans_sf{SF}_partitions{PARQUET_PARTITIONS}"
        ));
        INIT_TEST_TPCDS_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap_or(false) {
                    tpcds::generate_data(&data_dir, SF, PARQUET_PARTITIONS)
                        .await
                        .unwrap();
                }
            })
            .await;

        let query_sql = tpcds::get_query(query_id)?;
        // Make distributed localhost context to run queries
        let (d_ctx, _guard, _) = start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
        let d_ctx = d_ctx
            .with_distributed_files_per_task(FILES_PER_TASK)?
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?
            .with_distributed_broadcast_joins(true)?;

        benchmarks_common::register_tables(&d_ctx, &data_dir).await?;

        let df = d_ctx.sql(&query_sql).await?;
        let plan = df.create_physical_plan().await?;
        if plan.as_any().is::<DistributedExec>() {
            Ok(display_plan_ascii(plan.as_ref(), false))
        } else {
            Ok("".to_string())
        }
    }
}
