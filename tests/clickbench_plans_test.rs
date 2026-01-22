#[cfg(all(feature = "integration", feature = "clickbench", test))]
mod tests {
    use datafusion::error::Result;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::{benchmarks_common, clickbench};
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, assert_snapshot, display_plan_ascii,
    };
    use std::ops::Range;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const NUM_WORKERS: usize = 4;
    const FILES_PER_TASK: usize = 2;
    const CARDINALITY_TASK_COUNT_FACTOR: f64 = 1.5;
    const FILE_RANGE: Range<usize> = 0..3;

    #[tokio::test]
    #[ignore = "Query 0 did not get distributed"]
    async fn test_clickbench_0() -> Result<()> {
        let display = test_clickbench_query("q0").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_1() -> Result<()> {
        let display = test_clickbench_query("q1").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   ProjectionExec: expr=[]
          │     FilterExec: AdvEngineID@0 != 0
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
          │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[AdvEngineID], file_type=parquet, predicate=AdvEngineID@40 != 0, pruning_predicate=AdvEngineID_null_count@2 != row_count@3 AND (AdvEngineID_min@0 != 0 OR 0 != AdvEngineID_max@1), required_guarantees=[AdvEngineID not in (0)]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_2() -> Result<()> {
        let display = test_clickbench_query("q2").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[sum(hits.AdvEngineID)@0 as sum(hits.AdvEngineID), count(Int64(1))@1 as count(*), avg(hits.ResolutionWidth)@2 as avg(hits.ResolutionWidth)]
        │   AggregateExec: mode=Final, gby=[], aggr=[sum(hits.AdvEngineID), count(Int64(1)), avg(hits.ResolutionWidth)]
        │     CoalescePartitionsExec
        │       [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[sum(hits.AdvEngineID), count(Int64(1)), avg(hits.ResolutionWidth)]
          │   PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
          │     DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[ResolutionWidth, AdvEngineID], file_type=parquet
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 1, Right set size: 1\n\nRows only in left (1 total):\n  2533767602294735360.00\n\nRows only in right (1 total):\n  2533767602294735872.00.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_3() -> Result<()> {
        let display = test_clickbench_query("q3").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ AggregateExec: mode=Final, gby=[], aggr=[avg(hits.UserID)]
        │   CoalescePartitionsExec
        │     [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(hits.UserID)]
          │   PartitionIsolatorExec: t0:[p0,p1,p2,__,__] t1:[__,__,__,p0,p1] 
          │     DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID], file_type=parquet
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_4() -> Result<()> {
        let display = test_clickbench_query("q4").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(alias1)@0 as count(DISTINCT hits.UserID)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(alias1)]
        │     CoalescePartitionsExec
        │       [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(alias1)]
          │   AggregateExec: mode=FinalPartitioned, gby=[alias1@0 as alias1], aggr=[]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([alias1@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[UserID@0 as alias1], aggr=[]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_5() -> Result<()> {
        let display = test_clickbench_query("q5").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(alias1)@0 as count(DISTINCT hits.SearchPhrase)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(alias1)]
        │     CoalescePartitionsExec
        │       [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(alias1)]
          │   AggregateExec: mode=FinalPartitioned, gby=[alias1@0 as alias1], aggr=[]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([alias1@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchPhrase@0 as alias1], aggr=[]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[SearchPhrase], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Query 6 did not get distributed"]
    async fn test_clickbench_6() -> Result<()> {
        let display = test_clickbench_query("q6").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_7() -> Result<()> {
        let display = test_clickbench_query("q7").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[AdvEngineID@0 as AdvEngineID, count(*)@1 as count(*)]
        │   SortPreservingMergeExec: [count(Int64(1))@2 DESC]
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: expr=[count(*)@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[AdvEngineID@0 as AdvEngineID, count(Int64(1))@1 as count(*), count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[AdvEngineID@0 as AdvEngineID], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([AdvEngineID@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[AdvEngineID@0 as AdvEngineID], aggr=[count(Int64(1))]
            │     FilterExec: AdvEngineID@0 != 0
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[AdvEngineID], file_type=parquet, predicate=AdvEngineID@40 != 0, pruning_predicate=AdvEngineID_null_count@2 != row_count@3 AND (AdvEngineID_min@0 != 0 OR 0 != AdvEngineID_max@1), required_guarantees=[AdvEngineID not in (0)]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_8() -> Result<()> {
        let display = test_clickbench_query("q8").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [u@1 DESC], fetch=10
        │   SortExec: TopK(fetch=10), expr=[u@1 DESC], preserve_partitioning=[true]
        │     ProjectionExec: expr=[RegionID@0 as RegionID, count(alias1)@1 as u]
        │       AggregateExec: mode=FinalPartitioned, gby=[RegionID@0 as RegionID], aggr=[count(alias1)]
        │         [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([RegionID@0], 3), input_partitions=3
          │   AggregateExec: mode=Partial, gby=[RegionID@0 as RegionID], aggr=[count(alias1)]
          │     AggregateExec: mode=FinalPartitioned, gby=[RegionID@0 as RegionID, alias1@1 as alias1], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([RegionID@0, alias1@1], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[RegionID@0 as RegionID, UserID@1 as alias1], aggr=[]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[RegionID, UserID], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_9() -> Result<()> {
        let display = test_clickbench_query("q9").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@2 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[RegionID@0 as RegionID, sum(hits.AdvEngineID)@1 as sum(hits.AdvEngineID), count(Int64(1))@2 as c, avg(hits.ResolutionWidth)@3 as avg(hits.ResolutionWidth), count(DISTINCT hits.UserID)@4 as count(DISTINCT hits.UserID)]
          │     AggregateExec: mode=FinalPartitioned, gby=[RegionID@0 as RegionID], aggr=[sum(hits.AdvEngineID), count(Int64(1)), avg(hits.ResolutionWidth), count(DISTINCT hits.UserID)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([RegionID@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[RegionID@0 as RegionID], aggr=[sum(hits.AdvEngineID), count(Int64(1)), avg(hits.ResolutionWidth), count(DISTINCT hits.UserID)]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[RegionID, UserID, ResolutionWidth, AdvEngineID], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_10() -> Result<()> {
        let display = test_clickbench_query("q10").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [u@1 DESC], fetch=10
        │   SortExec: TopK(fetch=10), expr=[u@1 DESC], preserve_partitioning=[true]
        │     ProjectionExec: expr=[MobilePhoneModel@0 as MobilePhoneModel, count(alias1)@1 as u]
        │       AggregateExec: mode=FinalPartitioned, gby=[MobilePhoneModel@0 as MobilePhoneModel], aggr=[count(alias1)]
        │         [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([MobilePhoneModel@0], 3), input_partitions=3
          │   AggregateExec: mode=Partial, gby=[MobilePhoneModel@0 as MobilePhoneModel], aggr=[count(alias1)]
          │     AggregateExec: mode=FinalPartitioned, gby=[MobilePhoneModel@0 as MobilePhoneModel, alias1@1 as alias1], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([MobilePhoneModel@0, alias1@1], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[MobilePhoneModel@1 as MobilePhoneModel, UserID@0 as alias1], aggr=[]
            │     FilterExec: MobilePhoneModel@1 != 
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, MobilePhoneModel], file_type=parquet, predicate=MobilePhoneModel@34 != , pruning_predicate=MobilePhoneModel_null_count@2 != row_count@3 AND (MobilePhoneModel_min@0 !=  OR  != MobilePhoneModel_max@1), required_guarantees=[MobilePhoneModel not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_11() -> Result<()> {
        let display = test_clickbench_query("q11").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [u@2 DESC], fetch=10
        │   SortExec: TopK(fetch=10), expr=[u@2 DESC], preserve_partitioning=[true]
        │     ProjectionExec: expr=[MobilePhone@0 as MobilePhone, MobilePhoneModel@1 as MobilePhoneModel, count(alias1)@2 as u]
        │       AggregateExec: mode=FinalPartitioned, gby=[MobilePhone@0 as MobilePhone, MobilePhoneModel@1 as MobilePhoneModel], aggr=[count(alias1)]
        │         [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([MobilePhone@0, MobilePhoneModel@1], 3), input_partitions=3
          │   AggregateExec: mode=Partial, gby=[MobilePhone@0 as MobilePhone, MobilePhoneModel@1 as MobilePhoneModel], aggr=[count(alias1)]
          │     AggregateExec: mode=FinalPartitioned, gby=[MobilePhone@0 as MobilePhone, MobilePhoneModel@1 as MobilePhoneModel, alias1@2 as alias1], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([MobilePhone@0, MobilePhoneModel@1, alias1@2], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[MobilePhone@1 as MobilePhone, MobilePhoneModel@2 as MobilePhoneModel, UserID@0 as alias1], aggr=[]
            │     FilterExec: MobilePhoneModel@2 != 
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, MobilePhone, MobilePhoneModel], file_type=parquet, predicate=MobilePhoneModel@34 != , pruning_predicate=MobilePhoneModel_null_count@2 != row_count@3 AND (MobilePhoneModel_min@0 !=  OR  != MobilePhoneModel_max@1), required_guarantees=[MobilePhoneModel not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_12() -> Result<()> {
        let display = test_clickbench_query("q12").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@1 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, count(Int64(1))@1 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[SearchPhrase@0 as SearchPhrase], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([SearchPhrase@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchPhrase@0 as SearchPhrase], aggr=[count(Int64(1))]
            │     FilterExec: SearchPhrase@0 != 
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_13() -> Result<()> {
        let display = test_clickbench_query("q13").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [u@1 DESC], fetch=10
        │   SortExec: TopK(fetch=10), expr=[u@1 DESC], preserve_partitioning=[true]
        │     ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, count(alias1)@1 as u]
        │       AggregateExec: mode=FinalPartitioned, gby=[SearchPhrase@0 as SearchPhrase], aggr=[count(alias1)]
        │         [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([SearchPhrase@0], 3), input_partitions=3
          │   AggregateExec: mode=Partial, gby=[SearchPhrase@0 as SearchPhrase], aggr=[count(alias1)]
          │     AggregateExec: mode=FinalPartitioned, gby=[SearchPhrase@0 as SearchPhrase, alias1@1 as alias1], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([SearchPhrase@0, alias1@1], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchPhrase@1 as SearchPhrase, UserID@0 as alias1], aggr=[]
            │     FilterExec: SearchPhrase@1 != 
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_14() -> Result<()> {
        let display = test_clickbench_query("q14").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@2 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchEngineID@0 as SearchEngineID, SearchPhrase@1 as SearchPhrase, count(Int64(1))@2 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[SearchEngineID@0 as SearchEngineID, SearchPhrase@1 as SearchPhrase], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([SearchEngineID@0, SearchPhrase@1], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchEngineID@0 as SearchEngineID, SearchPhrase@1 as SearchPhrase], aggr=[count(Int64(1))]
            │     FilterExec: SearchPhrase@1 != 
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[SearchEngineID, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_15() -> Result<()> {
        let display = test_clickbench_query("q15").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[UserID@0 as UserID, count(*)@1 as count(*)]
        │   SortPreservingMergeExec: [count(Int64(1))@2 DESC], fetch=10
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[count(*)@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[UserID@0 as UserID, count(Int64(1))@1 as count(*), count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[UserID@0 as UserID], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([UserID@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[UserID@0 as UserID], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_16() -> Result<()> {
        let display = test_clickbench_query("q16").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase, count(*)@2 as count(*)]
        │   SortPreservingMergeExec: [count(Int64(1))@3 DESC], fetch=10
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[count(*)@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase, count(Int64(1))@2 as count(*), count(Int64(1))@2 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([UserID@0, SearchPhrase@1], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, SearchPhrase], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (10 total):\n  3219866204653196665||4\n  3220056705148678697||11\n  3221898002592879542||1\n  3223026783585713477||23\n  3223839745005575457||116\n  3223839745005575457|d0bcd0bed0b6d0bdd0be20d0bbd0b820d0b220d0bad180d0bed0bad0bed0b4d0b8d180d0bed0b2d0b5d0bbd18cd18820d0b1d180d0bed0b4d18b20d0bdd0b020d181d182d0bed0bbd18b20d0b2d0be20d0b2d0bbd0b0d0b4d0b8d0b2d0bed181d182d0bed0ba20d0b2d0b2d0be|1\n  3223949769615485893||1\n  3226415756450197918||24\n  3226664959488084815||62\n  3227160743723019373||71\n\nRows only in right (10 total):\n  700182585509527889||2\n  724127359630680276|d0b8d0b3d180d18b20d0b820d181d0b5d0b3d0bed0b4d0bdd18f3f|1\n  766120398574852544||1\n  766739966065297239||1\n  783205612738304865||3\n  797289180007803204||2\n  804968013253615745||1\n  830548852254311605||1\n  849024737642146119||1\n  849169469997862534||1.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_17() -> Result<()> {
        let display = test_clickbench_query("q17").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_18() -> Result<()> {
        let display = test_clickbench_query("q18").await?;
        assert_snapshot!(display, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[UserID@0 as UserID, m@1 as m, SearchPhrase@2 as SearchPhrase, count(*)@3 as count(*)]
        │   SortPreservingMergeExec: [count(Int64(1))@4 DESC], fetch=10
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[count(*)@3 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[UserID@0 as UserID, date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime))@1 as m, SearchPhrase@2 as SearchPhrase, count(Int64(1))@3 as count(*), count(Int64(1))@3 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[UserID@0 as UserID, date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime))@1 as date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime)), SearchPhrase@2 as SearchPhrase], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([UserID@0, date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime))@1, SearchPhrase@2], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[UserID@1 as UserID, date_part(MINUTE, to_timestamp_seconds(EventTime@0)) as date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime)), SearchPhrase@2 as SearchPhrase], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventTime, UserID, SearchPhrase], file_type=parquet
            └──────────────────────────────────────────────────
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_19() -> Result<()> {
        let display = test_clickbench_query("q19").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ FilterExec: UserID@0 = 435090932899640449
          │   PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
          │     DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID], file_type=parquet, predicate=UserID@9 = 435090932899640449, pruning_predicate=UserID_null_count@2 != row_count@3 AND UserID_min@0 <= 435090932899640449 AND 435090932899640449 <= UserID_max@1, required_guarantees=[UserID in (435090932899640449)]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_20() -> Result<()> {
        let display = test_clickbench_query("q20").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   ProjectionExec: expr=[]
          │     FilterExec: CAST(URL@0 AS Utf8View) LIKE %google%
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
          │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[URL], file_type=parquet, predicate=CAST(URL@13 AS Utf8View) LIKE %google%
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (5 total):\n  d181d0bbd0b0d0b2d0bbd18fd182d18c20d0bfd0bed180d0bed0b4d0b8d182d181d18f20d0bed182d0b5d0bbd0b8203230313320d181d0bcd0bed182d180d0b5d182d18c|687474703a253246253246766b2e636f6d2e75612f676f6f676c652d6a61726b6f76736b6179612d4c697065636b64|1\n  d0b1d0b0d0bdd0bad0bed0bcd0b0d182d0b5d180d0b8d0b0d0bbd18b20d181d0bcd0bed182d180d0b5d182d18c|687474703a2f2f6f72656e627572672e6972722e72752532466b7572746b692532462532467777772e676f6f676c652e72752f6d617a64612d332d6b6f6d6e2d6b762d4b617a616e2e74757475746f72736b2f64657461696c|1\n  d0bcd0bed0bdd0b8d182d18c20d0bad0b0d0bad0bed0b520d0bed0b7d0b5d180d0b0|687474703a2f2f6175746f2e7269612e75612f6175746f5f69643d30266f726465723d46616c7365266d696e707269782e72752f6b617465676f726979612f767369652d646c69612d647275676f652f6d61746572696e7374766f2f676f6f676c652d706f6c697331343334343532|1\n  d181d0bad0b0d187d0b0d182d18c20d0b4d0b5d0bdd0b5d0b320d181d183d180d0b3d183d182|687474703a2f2f7469656e736b6169612d6d6f64612d627269657469656c6b612d6b6f736b6f76736b2f64657461696c2e676f6f676c65|1\n  d0b220d0b0d0b2d0b3d183d181d1822032343720d0b3d180d183d181d182d0b8d0bcd0bed188d0bad0b020d0bdd0b020d0bad180d0b8d181d182d180d0b0d182|687474703a2f2f7469656e736b6169612d6d6f64612d627269756b692f676f6f676c652e72752f7e61706f6b2e72752f635f312d755f313138383839352c39373536|1\n\nRows only in right (5 total):\n  d0bcd0bed0b4d0b5d0bad18120d183d0bbd0b8d186d0b5d0bdd0b7d0b8d0bdd0bed0b2d0b020d0b3d0bed0b2d18fd0b4d0b8d0bdd0b0|687474703a2f2f73616d6172612e6972722e72752f636174616c6f675f676f6f676c652d636865726e796a2d393233353636363635372f3f64617465|1\n  d0bbd0b0d0b2d0bfd0bbd0b0d0bdd188d0b5d182d0bdd0b8d18520d183d181d0bbd0bed0b2d0b0d0bcd0b820d0b2d181d0b520d181d0b5d180d0b8d0b820d0b4d0b0d182d0b020d186d0b5d0bcd0b5d0bdd0b8|687474703a2f2f73616d6172612e6972722e72752f636174616c6f675f676f6f676c654d425225323661642533443930253236707a|1\n  d0bad0b0d0ba20d0bfd180d0bed0b4d0b0d0bcd0b820d0b4d0bbd18f20d0b4d0b5d0b2d183d188d0bad0b8|687474703a253246253246777777772e626f6e707269782e7275253235326625323532663737363925323532663131303931392d6c65766f652d676f6f676c652d7368746f72792e72752f666f72756d2f666f72756d2e6d617465722e72752f6461696c792f63616c63756c61746f72|1\n  d0b6d0b0d180d0b5d0bdd18cd18f20d0b32ed181d183d180d0bed0b2d0b0d0bdd0b8d0b520d0b2d0bed180d0bed0bdd0b5d0b6d181d0bad0b0d18f20d0bed0b1d0bbd0b0d181d182d0bed0bfd180d0b8d0bbd0b520d0bfd0bed181d0bbd0b5d0b4d0bdd0b8d0b520d0bad0bed181d18b|687474703a2f2f756b7261696e627572672f65636f2d6d6c656b2f65636f6e646172792f73686f77746f7069632e7068703f69643d3436333837362e68746d6c3f69643d32303634313333363631253246676f6f676c652d4170706c655765624b69742532463533372e333620284b48544d4c2c206c696b65|1\n  d180d0b8d0be20d0bdd0b020d0bad0b0d180d182d0bed187d0bdd0b8d186d0b020d181d0bcd0bed182d180d0b5d182d18c20d0bed0bdd0bbd0b0d0b9d0bd|687474703a2f2f73616d6172612e6972722e72752f636174616c6f675f676f6f676c654d425225323661642533443930253236707a|1.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_21() -> Result<()> {
        let display = test_clickbench_query("q21").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (1 total):\n  d0bad0b0d0bad0bed0b920d0bfd0bbd0bed189d0b0d0b4d0bad0b8d0bcd0b820d0b4d0bed181d182d0b0d0b2d0bad0b8|687474703a253246253246766b2e636f6d2f696672616d652d6f77612e68746d6c3f313d31266369643d353737266f6b693d31266f705f63617465676f72795f69645d3d332673656c656374|d092d0b0d0bad0b0d0bdd181d0b8d18f20d091d0a0d090d09ad090d09d20d090d09dd094d0a0d095d0a1202d20d0bfd0bed0bfd0b0d0bbd0b820d0bad183d0bfd0b8d182d18c20d0b4d0bed0bcd0bed0b5d187d0bdd18bd0b520d188d0bad0b0d184d0b020476f6f676c652e636f6d203a3a20d0bad0bed182d182d0b5d0bad181d1822c20d091d183d180d18fd182d0bdd0b8d0bad0b820d0b4d0bbd18f20d0bfd0b5d187d18c20d0bcd0b5d0b1d0b5d0bbd18cd0b520d0b4d0bbd18f20d0b4d0b5d0b2d183d188d0bad0b0|5|1\n\nRows only in right (1 total):\n  d0bad0bed0bfd182d0b8d0bcd0b8d0bad0b2d0b8d0b4d18b20d18ed180d0b8d0b920d0bfd0bed181d0bbd0b5d0b4d0bdd18fd18f|68747470733a2f2f70726f64756b747925324670756c6f76652e72752f626f6f6b6c79617474696f6e2d7761722d73696e696a2d393430343139342c3936323435332f666f746f|d09bd0b5d0b3d0bad0be20d0bdd0b020d183d187d0b0d181d182d0bdd18bd0b520d183d187d0b0d181d182d0bdd0b8d0bad0bed0b22e2c20d0a6d0b5d0bdd18b202d20d0a1d182d0b8d0bbd18cd0bdd0b0d18f20d0bfd0b0d180d0bdd0b5d0bc2e20d0a1d0b0d0b3d0b0d0bdd180d0bed0b320d0b4d0bed0b3d0b0d0b4d0b5d0bdd0b8d18f203a20d0a2d183d180d186d0b8d0b82c20d0bad183d0bfd0b8d182d18c20d18320313020d0b4d0bdd0b520d0bad0bed0bbd18cd0bdd18bd0b520d0bcd0b0d188d0b8d0bdd0bad0b820d0bdd0b520d0bfd180d0b5d0b4d181d182d0b0d0b2d0bad0b8202d20d09dd0bed0b2d0b0d18f20d18120d0b8d0b7d0b1d0b8d0b5d0bdd0b8d0b520d181d0bfd180d0bed0b4d0b0d0b6d0b03a20d0bad0bed182d18fd182d0b0203230313420d0b32ed0b22e20d0a6d0b5d0bdd0b03a2034373530302d313045434f30363020e28093202d2d2d2d2d2d2d2d20d0bad183d0bfd0b8d182d18c20d0bad0b2d0b0d180d182d0b8d180d18320d09ed180d0b5d0bdd0b1d183d180d0b32028d0a0d0bed181d181d0b8d0b82047616c616e7472617820466c616d696c6961646120476f6f676c652c204ed0be20313820d184d0bed182d0bed0bad0bed0bdd0b2d0b5d180d0ba20d0a1d183d0bfd0b5d18020d09ad0b0d180d0b4d0b8d0b3d0b0d0bd|5|1.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_22() -> Result<()> {
        let display = test_clickbench_query("q22").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_23() -> Result<()> {
        let display = test_clickbench_query("q23").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [EventTime@4 ASC NULLS LAST], fetch=10
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ SortExec: TopK(fetch=10), expr=[EventTime@4 ASC NULLS LAST], preserve_partitioning=[true]
          │   FilterExec: CAST(URL@13 AS Utf8View) LIKE %google%
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
          │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[WatchID, JavaEnable, Title, GoodEvent, EventTime, EventDate, CounterID, ClientIP, RegionID, UserID, CounterClass, OS, UserAgent, URL, Referer, IsRefresh, RefererCategoryID, RefererRegionID, URLCategoryID, URLRegionID, ResolutionWidth, ResolutionHeight, ResolutionDepth, FlashMajor, FlashMinor, FlashMinor2, NetMajor, NetMinor, UserAgentMajor, UserAgentMinor, CookieEnable, JavascriptEnable, IsMobile, MobilePhone, MobilePhoneModel, Params, IPNetworkID, TraficSourceID, SearchEngineID, SearchPhrase, AdvEngineID, IsArtifical, WindowClientWidth, WindowClientHeight, ClientTimeZone, ClientEventTime, SilverlightVersion1, SilverlightVersion2, SilverlightVersion3, SilverlightVersion4, PageCharset, CodeVersion, IsLink, IsDownload, IsNotBounce, FUniqID, OriginalURL, HID, IsOldCounter, IsEvent, IsParameter, DontCountHits, WithHash, HitColor, LocalEventTime, Age, Sex, Income, Interests, Robotness, RemoteIP, WindowName, OpenerName, HistoryLength, BrowserLanguage, BrowserCountry, SocialNetwork, SocialAction, HTTPError, SendTiming, DNSTiming, ConnectTiming, ResponseStartTiming, ResponseEndTiming, FetchTiming, SocialSourceNetworkID, SocialSourcePage, ParamPrice, ParamOrderID, ParamCurrency, ParamCurrencyID, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID, UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash, URLHash, CLID], file_type=parquet, predicate=CAST(URL@13 AS Utf8View) LIKE %google% AND DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (1 total):\n  d0b2d181d0bfd0bed0bcd0bdd0b8d182d18c20d181d0bed0bbd0bdd0b5d0bdd0b8d0b520d0b1d0b0d0bdd0bad0b020d0bbd0b0d0b420d184d0b8d0bbd18cd0bc\n\nRows only in right (1 total):\n  d0bed182d0b2d0bed0b4d0b020d0b4d0bbd18f20d0bfd0b8d180d0bed0b6d0bad0b820d0bbd0b5d187d0b5d0bdd0bdd18b20d0b2d181d0b520d181d0b5d180d196d197.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_24() -> Result<()> {
        let display = test_clickbench_query("q24").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_25() -> Result<()> {
        let display = test_clickbench_query("q25").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [SearchPhrase@0 ASC NULLS LAST], fetch=10
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ SortExec: TopK(fetch=10), expr=[SearchPhrase@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   FilterExec: SearchPhrase@0 != 
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
          │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 !=  AND DynamicFilter [ empty ], pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_26() -> Result<()> {
        let display = test_clickbench_query("q26").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase]
        │   SortPreservingMergeExec: [EventTime@1 ASC NULLS LAST, SearchPhrase@0 ASC NULLS LAST], fetch=10
        │     [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ SortExec: TopK(fetch=10), expr=[EventTime@1 ASC NULLS LAST, SearchPhrase@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchPhrase@1 as SearchPhrase, EventTime@0 as EventTime]
          │     FilterExec: SearchPhrase@1 != 
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
          │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventTime, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 !=  AND DynamicFilter [ empty ], pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_27() -> Result<()> {
        let display = test_clickbench_query("q27").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [l@1 DESC], fetch=25
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=25), expr=[l@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[CounterID@0 as CounterID, avg(length(hits.URL))@1 as l, count(Int64(1))@2 as c]
          │     FilterExec: count(Int64(1))@2 > 100000
          │       AggregateExec: mode=FinalPartitioned, gby=[CounterID@0 as CounterID], aggr=[avg(length(hits.URL)), count(Int64(1))]
          │         [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([CounterID@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[CounterID@0 as CounterID], aggr=[avg(length(hits.URL)), count(Int64(1))]
            │     FilterExec: URL@1 != 
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[CounterID, URL], file_type=parquet, predicate=URL@13 != , pruning_predicate=URL_null_count@2 != row_count@3 AND (URL_min@0 !=  OR  != URL_max@1), required_guarantees=[URL not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_28() -> Result<()> {
        let display = test_clickbench_query("q28").await?;
        assert_snapshot!(display, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [l@1 DESC], fetch=25
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=25), expr=[l@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))@0 as k, avg(length(hits.Referer))@1 as l, count(Int64(1))@2 as c, min(hits.Referer)@3 as min(hits.Referer)]
          │     FilterExec: count(Int64(1))@2 > 100000
          │       AggregateExec: mode=FinalPartitioned, gby=[regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))@0 as regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))], aggr=[avg(length(hits.Referer)), count(Int64(1)), min(hits.Referer)]
          │         [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[regexp_replace(CAST(Referer@0 AS LargeUtf8), ^https?://(?:www\.)?([^/]+)/.*$, \1) as regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))], aggr=[avg(length(hits.Referer)), count(Int64(1)), min(hits.Referer)]
            │     FilterExec: Referer@0 != 
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[Referer], file_type=parquet, predicate=Referer@14 != , pruning_predicate=Referer_null_count@2 != row_count@3 AND (Referer_min@0 !=  OR  != Referer_max@1), required_guarantees=[Referer not in ()]
            └──────────────────────────────────────────────────
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_29() -> Result<()> {
        let display = test_clickbench_query("q29").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ AggregateExec: mode=Final, gby=[], aggr=[sum(hits.ResolutionWidth), sum(hits.ResolutionWidth + Int64(1)), sum(hits.ResolutionWidth + Int64(2)), sum(hits.ResolutionWidth + Int64(3)), sum(hits.ResolutionWidth + Int64(4)), sum(hits.ResolutionWidth + Int64(5)), sum(hits.ResolutionWidth + Int64(6)), sum(hits.ResolutionWidth + Int64(7)), sum(hits.ResolutionWidth + Int64(8)), sum(hits.ResolutionWidth + Int64(9)), sum(hits.ResolutionWidth + Int64(10)), sum(hits.ResolutionWidth + Int64(11)), sum(hits.ResolutionWidth + Int64(12)), sum(hits.ResolutionWidth + Int64(13)), sum(hits.ResolutionWidth + Int64(14)), sum(hits.ResolutionWidth + Int64(15)), sum(hits.ResolutionWidth + Int64(16)), sum(hits.ResolutionWidth + Int64(17)), sum(hits.ResolutionWidth + Int64(18)), sum(hits.ResolutionWidth + Int64(19)), sum(hits.ResolutionWidth + Int64(20)), sum(hits.ResolutionWidth + Int64(21)), sum(hits.ResolutionWidth + Int64(22)), sum(hits.ResolutionWidth + Int64(23)), sum(hits.ResolutionWidth + Int64(24)), sum(hits.ResolutionWidth + Int64(25)), sum(hits.ResolutionWidth + Int64(26)), sum(hits.ResolutionWidth + Int64(27)), sum(hits.ResolutionWidth + Int64(28)), sum(hits.ResolutionWidth + Int64(29)), sum(hits.ResolutionWidth + Int64(30)), sum(hits.ResolutionWidth + Int64(31)), sum(hits.ResolutionWidth + Int64(32)), sum(hits.ResolutionWidth + Int64(33)), sum(hits.ResolutionWidth + Int64(34)), sum(hits.ResolutionWidth + Int64(35)), sum(hits.ResolutionWidth + Int64(36)), sum(hits.ResolutionWidth + Int64(37)), sum(hits.ResolutionWidth + Int64(38)), sum(hits.ResolutionWidth + Int64(39)), sum(hits.ResolutionWidth + Int64(40)), sum(hits.ResolutionWidth + Int64(41)), sum(hits.ResolutionWidth + Int64(42)), sum(hits.ResolutionWidth + Int64(43)), sum(hits.ResolutionWidth + Int64(44)), sum(hits.ResolutionWidth + Int64(45)), sum(hits.ResolutionWidth + Int64(46)), sum(hits.ResolutionWidth + Int64(47)), sum(hits.ResolutionWidth + Int64(48)), sum(hits.ResolutionWidth + Int64(49)), sum(hits.ResolutionWidth + Int64(50)), sum(hits.ResolutionWidth + Int64(51)), sum(hits.ResolutionWidth + Int64(52)), sum(hits.ResolutionWidth + Int64(53)), sum(hits.ResolutionWidth + Int64(54)), sum(hits.ResolutionWidth + Int64(55)), sum(hits.ResolutionWidth + Int64(56)), sum(hits.ResolutionWidth + Int64(57)), sum(hits.ResolutionWidth + Int64(58)), sum(hits.ResolutionWidth + Int64(59)), sum(hits.ResolutionWidth + Int64(60)), sum(hits.ResolutionWidth + Int64(61)), sum(hits.ResolutionWidth + Int64(62)), sum(hits.ResolutionWidth + Int64(63)), sum(hits.ResolutionWidth + Int64(64)), sum(hits.ResolutionWidth + Int64(65)), sum(hits.ResolutionWidth + Int64(66)), sum(hits.ResolutionWidth + Int64(67)), sum(hits.ResolutionWidth + Int64(68)), sum(hits.ResolutionWidth + Int64(69)), sum(hits.ResolutionWidth + Int64(70)), sum(hits.ResolutionWidth + Int64(71)), sum(hits.ResolutionWidth + Int64(72)), sum(hits.ResolutionWidth + Int64(73)), sum(hits.ResolutionWidth + Int64(74)), sum(hits.ResolutionWidth + Int64(75)), sum(hits.ResolutionWidth + Int64(76)), sum(hits.ResolutionWidth + Int64(77)), sum(hits.ResolutionWidth + Int64(78)), sum(hits.ResolutionWidth + Int64(79)), sum(hits.ResolutionWidth + Int64(80)), sum(hits.ResolutionWidth + Int64(81)), sum(hits.ResolutionWidth + Int64(82)), sum(hits.ResolutionWidth + Int64(83)), sum(hits.ResolutionWidth + Int64(84)), sum(hits.ResolutionWidth + Int64(85)), sum(hits.ResolutionWidth + Int64(86)), sum(hits.ResolutionWidth + Int64(87)), sum(hits.ResolutionWidth + Int64(88)), sum(hits.ResolutionWidth + Int64(89))]
        │   CoalescePartitionsExec
        │     [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[sum(hits.ResolutionWidth), sum(hits.ResolutionWidth + Int64(1)), sum(hits.ResolutionWidth + Int64(2)), sum(hits.ResolutionWidth + Int64(3)), sum(hits.ResolutionWidth + Int64(4)), sum(hits.ResolutionWidth + Int64(5)), sum(hits.ResolutionWidth + Int64(6)), sum(hits.ResolutionWidth + Int64(7)), sum(hits.ResolutionWidth + Int64(8)), sum(hits.ResolutionWidth + Int64(9)), sum(hits.ResolutionWidth + Int64(10)), sum(hits.ResolutionWidth + Int64(11)), sum(hits.ResolutionWidth + Int64(12)), sum(hits.ResolutionWidth + Int64(13)), sum(hits.ResolutionWidth + Int64(14)), sum(hits.ResolutionWidth + Int64(15)), sum(hits.ResolutionWidth + Int64(16)), sum(hits.ResolutionWidth + Int64(17)), sum(hits.ResolutionWidth + Int64(18)), sum(hits.ResolutionWidth + Int64(19)), sum(hits.ResolutionWidth + Int64(20)), sum(hits.ResolutionWidth + Int64(21)), sum(hits.ResolutionWidth + Int64(22)), sum(hits.ResolutionWidth + Int64(23)), sum(hits.ResolutionWidth + Int64(24)), sum(hits.ResolutionWidth + Int64(25)), sum(hits.ResolutionWidth + Int64(26)), sum(hits.ResolutionWidth + Int64(27)), sum(hits.ResolutionWidth + Int64(28)), sum(hits.ResolutionWidth + Int64(29)), sum(hits.ResolutionWidth + Int64(30)), sum(hits.ResolutionWidth + Int64(31)), sum(hits.ResolutionWidth + Int64(32)), sum(hits.ResolutionWidth + Int64(33)), sum(hits.ResolutionWidth + Int64(34)), sum(hits.ResolutionWidth + Int64(35)), sum(hits.ResolutionWidth + Int64(36)), sum(hits.ResolutionWidth + Int64(37)), sum(hits.ResolutionWidth + Int64(38)), sum(hits.ResolutionWidth + Int64(39)), sum(hits.ResolutionWidth + Int64(40)), sum(hits.ResolutionWidth + Int64(41)), sum(hits.ResolutionWidth + Int64(42)), sum(hits.ResolutionWidth + Int64(43)), sum(hits.ResolutionWidth + Int64(44)), sum(hits.ResolutionWidth + Int64(45)), sum(hits.ResolutionWidth + Int64(46)), sum(hits.ResolutionWidth + Int64(47)), sum(hits.ResolutionWidth + Int64(48)), sum(hits.ResolutionWidth + Int64(49)), sum(hits.ResolutionWidth + Int64(50)), sum(hits.ResolutionWidth + Int64(51)), sum(hits.ResolutionWidth + Int64(52)), sum(hits.ResolutionWidth + Int64(53)), sum(hits.ResolutionWidth + Int64(54)), sum(hits.ResolutionWidth + Int64(55)), sum(hits.ResolutionWidth + Int64(56)), sum(hits.ResolutionWidth + Int64(57)), sum(hits.ResolutionWidth + Int64(58)), sum(hits.ResolutionWidth + Int64(59)), sum(hits.ResolutionWidth + Int64(60)), sum(hits.ResolutionWidth + Int64(61)), sum(hits.ResolutionWidth + Int64(62)), sum(hits.ResolutionWidth + Int64(63)), sum(hits.ResolutionWidth + Int64(64)), sum(hits.ResolutionWidth + Int64(65)), sum(hits.ResolutionWidth + Int64(66)), sum(hits.ResolutionWidth + Int64(67)), sum(hits.ResolutionWidth + Int64(68)), sum(hits.ResolutionWidth + Int64(69)), sum(hits.ResolutionWidth + Int64(70)), sum(hits.ResolutionWidth + Int64(71)), sum(hits.ResolutionWidth + Int64(72)), sum(hits.ResolutionWidth + Int64(73)), sum(hits.ResolutionWidth + Int64(74)), sum(hits.ResolutionWidth + Int64(75)), sum(hits.ResolutionWidth + Int64(76)), sum(hits.ResolutionWidth + Int64(77)), sum(hits.ResolutionWidth + Int64(78)), sum(hits.ResolutionWidth + Int64(79)), sum(hits.ResolutionWidth + Int64(80)), sum(hits.ResolutionWidth + Int64(81)), sum(hits.ResolutionWidth + Int64(82)), sum(hits.ResolutionWidth + Int64(83)), sum(hits.ResolutionWidth + Int64(84)), sum(hits.ResolutionWidth + Int64(85)), sum(hits.ResolutionWidth + Int64(86)), sum(hits.ResolutionWidth + Int64(87)), sum(hits.ResolutionWidth + Int64(88)), sum(hits.ResolutionWidth + Int64(89))]
          │   PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
          │     DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[CAST(ResolutionWidth@20 AS Int64) as __common_expr_1], file_type=parquet
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_30() -> Result<()> {
        let display = test_clickbench_query("q30").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@2 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchEngineID@0 as SearchEngineID, ClientIP@1 as ClientIP, count(Int64(1))@2 as c, sum(hits.IsRefresh)@3 as sum(hits.IsRefresh), avg(hits.ResolutionWidth)@4 as avg(hits.ResolutionWidth)]
          │     AggregateExec: mode=FinalPartitioned, gby=[SearchEngineID@0 as SearchEngineID, ClientIP@1 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([SearchEngineID@0, ClientIP@1], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchEngineID@3 as SearchEngineID, ClientIP@0 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
            │     FilterExec: SearchPhrase@4 != , projection=[ClientIP@0, IsRefresh@1, ResolutionWidth@2, SearchEngineID@3]
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[ClientIP, IsRefresh, ResolutionWidth, SearchEngineID, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (10 total):\n  8673025726158767406|1264438551|1|0|1990.00\n  5320052218057629211|-1703087277|1|0|1996.00\n  6244273852606083750|1554672832|1|0|1638.00\n  8628753750962053665|1215278356|1|0|1087.00\n  7035318163404387241|1326714320|1|0|1638.00\n  8431857775494210873|1237512945|1|0|1996.00\n  5110752526539992124|37611695|1|0|1917.00\n  8986794334343068049|1860752926|1|0|1638.00\n  8044147848299485837|1382122372|1|0|1368.00\n  7936057634954670727|1897481896|1|0|1638.00\n\nRows only in right (10 total):\n  5132615111782210132|-50313020|1|0|1368.00\n  5783789691451717551|-1310327384|1|0|1638.00\n  5756260993772351383|1484317883|1|0|375.00\n  7739310142000732364|991864113|1|0|1368.00\n  7593472904893539271|-151291403|1|0|1087.00\n  6339599967989898410|1543815587|1|0|1638.00\n  7794346560421945218|1645556180|1|0|1368.00\n  6112645108657361792|593586188|1|0|1638.00\n  6675910710751922756|-816379256|1|0|1368.00\n  5802727636196431835|1986422271|1|0|1996.00.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_31() -> Result<()> {
        let display = test_clickbench_query("q31").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (10 total):\n  7643059318918524417|1767085700|1|0|0.00\n  5437163248266133938|-1465369615|1|0|0.00\n  9142541582422390102|-1465369615|1|0|0.00\n  8438994503411842126|-1465369615|1|0|0.00\n  7362096505818029859|-565678477|1|0|0.00\n  4928022308880516715|1699955284|1|0|0.00\n  5269769817689282522|1699955284|1|0|0.00\n  9081648050908046886|1699955284|1|0|0.00\n  6824181869275536503|1699955284|1|0|0.00\n  6905712404475757487|1552811156|1|0|0.00\n\nRows only in right (10 total):\n  6967277596165459879|-941091661|1|0|1368.00\n  5796237228224217668|-1310327384|1|0|1638.00\n  7218628137278606666|1511490240|1|0|1638.00\n  8314760197723815280|1566105210|1|0|1996.00\n  7053263954762394007|757778490|1|0|339.00\n  6283334114093174531|1216031795|1|0|1368.00\n  8818295356247036741|83042182|1|0|1638.00\n  6620528864937282562|-862894777|1|0|1996.00\n  8466121050002905379|83042182|1|0|1638.00\n  7554844936512227411|-1746904856|1|0|1368.00.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_32() -> Result<()> {
        let display = test_clickbench_query("q32").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_33() -> Result<()> {
        let display = test_clickbench_query("q33").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@1 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[URL@0 as URL, count(Int64(1))@1 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[URL@0 as URL], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([URL@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[URL@0 as URL], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[URL], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_34() -> Result<()> {
        let display = test_clickbench_query("q34").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@2 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[1 as Int64(1), URL@0 as URL, count(Int64(1))@1 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[URL@0 as URL], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([URL@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[URL@0 as URL], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[URL], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_35() -> Result<()> {
        let display = test_clickbench_query("q35").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@4 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@4 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[ClientIP@0 as ClientIP, hits.ClientIP - Int64(1)@1 as hits.ClientIP - Int64(1), hits.ClientIP - Int64(2)@2 as hits.ClientIP - Int64(2), hits.ClientIP - Int64(3)@3 as hits.ClientIP - Int64(3), count(Int64(1))@4 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[ClientIP@0 as ClientIP, hits.ClientIP - Int64(1)@1 as hits.ClientIP - Int64(1), hits.ClientIP - Int64(2)@2 as hits.ClientIP - Int64(2), hits.ClientIP - Int64(3)@3 as hits.ClientIP - Int64(3)], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([ClientIP@0, hits.ClientIP - Int64(1)@1, hits.ClientIP - Int64(2)@2, hits.ClientIP - Int64(3)@3], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[ClientIP@1 as ClientIP, __common_expr_1@0 - 1 as hits.ClientIP - Int64(1), __common_expr_1@0 - 2 as hits.ClientIP - Int64(2), __common_expr_1@0 - 3 as hits.ClientIP - Int64(3)], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[CAST(ClientIP@7 AS Int64) as __common_expr_1, ClientIP], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_36() -> Result<()> {
        let display = test_clickbench_query("q36").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [pageviews@1 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[pageviews@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[URL@0 as URL, count(Int64(1))@1 as pageviews]
          │     AggregateExec: mode=FinalPartitioned, gby=[URL@0 as URL], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([URL@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[URL@0 as URL], aggr=[count(Int64(1))]
            │     FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND DontCountHits@4 = 0 AND IsRefresh@3 = 0 AND URL@2 != , projection=[URL@2]
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, URL, IsRefresh, DontCountHits], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND DontCountHits@61 = 0 AND IsRefresh@15 = 0 AND URL@13 != , pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND DontCountHits_null_count@6 != row_count@3 AND DontCountHits_min@4 <= 0 AND 0 <= DontCountHits_max@5 AND IsRefresh_null_count@9 != row_count@3 AND IsRefresh_min@7 <= 0 AND 0 <= IsRefresh_max@8 AND URL_null_count@12 != row_count@3 AND (URL_min@10 !=  OR  != URL_max@11), required_guarantees=[CounterID in (62), DontCountHits in (0), IsRefresh in (0), URL not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_37() -> Result<()> {
        let display = test_clickbench_query("q37").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [pageviews@1 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[pageviews@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[Title@0 as Title, count(Int64(1))@1 as pageviews]
          │     AggregateExec: mode=FinalPartitioned, gby=[Title@0 as Title], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([Title@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[Title@0 as Title], aggr=[count(Int64(1))]
            │     FilterExec: CounterID@2 = 62 AND CAST(EventDate@1 AS Utf8) >= 2013-07-01 AND CAST(EventDate@1 AS Utf8) <= 2013-07-31 AND DontCountHits@4 = 0 AND IsRefresh@3 = 0 AND Title@0 != , projection=[Title@0]
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[Title, EventDate, CounterID, IsRefresh, DontCountHits], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND DontCountHits@61 = 0 AND IsRefresh@15 = 0 AND Title@2 != , pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND DontCountHits_null_count@6 != row_count@3 AND DontCountHits_min@4 <= 0 AND 0 <= DontCountHits_max@5 AND IsRefresh_null_count@9 != row_count@3 AND IsRefresh_min@7 <= 0 AND 0 <= IsRefresh_max@8 AND Title_null_count@12 != row_count@3 AND (Title_min@10 !=  OR  != Title_max@11), required_guarantees=[CounterID in (62), DontCountHits in (0), IsRefresh in (0), Title not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_38() -> Result<()> {
        let display = test_clickbench_query("q38").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ GlobalLimitExec: skip=1000, fetch=10
        │   SortPreservingMergeExec: [pageviews@1 DESC], fetch=1010
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=1010), expr=[pageviews@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[URL@0 as URL, count(Int64(1))@1 as pageviews]
          │     AggregateExec: mode=FinalPartitioned, gby=[URL@0 as URL], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([URL@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[URL@0 as URL], aggr=[count(Int64(1))]
            │     FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@3 = 0 AND IsLink@4 != 0 AND IsDownload@5 = 0, projection=[URL@2]
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, URL, IsRefresh, IsLink, IsDownload], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND IsRefresh@15 = 0 AND IsLink@52 != 0 AND IsDownload@53 = 0, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5 AND IsLink_null_count@9 != row_count@3 AND (IsLink_min@7 != 0 OR 0 != IsLink_max@8) AND IsDownload_null_count@12 != row_count@3 AND IsDownload_min@10 <= 0 AND 0 <= IsDownload_max@11, required_guarantees=[CounterID in (62), IsDownload in (0), IsLink not in (0), IsRefresh in (0)]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_39() -> Result<()> {
        let display = test_clickbench_query("q39").await?;
        assert_snapshot!(display, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ GlobalLimitExec: skip=1000, fetch=10
        │   SortPreservingMergeExec: [pageviews@5 DESC], fetch=1010
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=1010), expr=[pageviews@5 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[TraficSourceID@0 as TraficSourceID, SearchEngineID@1 as SearchEngineID, AdvEngineID@2 as AdvEngineID, CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END@3 as src, URL@4 as dst, count(Int64(1))@5 as pageviews]
          │     AggregateExec: mode=FinalPartitioned, gby=[TraficSourceID@0 as TraficSourceID, SearchEngineID@1 as SearchEngineID, AdvEngineID@2 as AdvEngineID, CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END@3 as CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END, URL@4 as URL], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([TraficSourceID@0, SearchEngineID@1, AdvEngineID@2, CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END@3, URL@4], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[TraficSourceID@2 as TraficSourceID, SearchEngineID@3 as SearchEngineID, AdvEngineID@4 as AdvEngineID, CASE WHEN SearchEngineID@3 = 0 AND AdvEngineID@4 = 0 THEN Referer@1 ELSE  END as CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END, URL@0 as URL], aggr=[count(Int64(1))]
            │     FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@4 = 0, projection=[URL@2, Referer@3, TraficSourceID@5, SearchEngineID@6, AdvEngineID@7]
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, URL, Referer, IsRefresh, TraficSourceID, SearchEngineID, AdvEngineID], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND IsRefresh@15 = 0, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5, required_guarantees=[CounterID in (62), IsRefresh in (0)]
            └──────────────────────────────────────────────────
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_40() -> Result<()> {
        let display = test_clickbench_query("q40").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ GlobalLimitExec: skip=100, fetch=10
        │   SortPreservingMergeExec: [pageviews@2 DESC], fetch=110
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=110), expr=[pageviews@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[URLHash@0 as URLHash, EventDate@1 as EventDate, count(Int64(1))@2 as pageviews]
          │     AggregateExec: mode=FinalPartitioned, gby=[URLHash@0 as URLHash, EventDate@1 as EventDate], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([URLHash@0, EventDate@1], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[URLHash@1 as URLHash, EventDate@0 as EventDate], aggr=[count(Int64(1))]
            │     FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@2 = 0 AND (TraficSourceID@3 = -1 OR TraficSourceID@3 = 6) AND RefererHash@4 = 3594120000172545465, projection=[EventDate@0, URLHash@5]
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, IsRefresh, TraficSourceID, RefererHash, URLHash], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND IsRefresh@15 = 0 AND (TraficSourceID@37 = -1 OR TraficSourceID@37 = 6) AND RefererHash@102 = 3594120000172545465, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5 AND (TraficSourceID_null_count@9 != row_count@3 AND TraficSourceID_min@7 <= -1 AND -1 <= TraficSourceID_max@8 OR TraficSourceID_null_count@9 != row_count@3 AND TraficSourceID_min@7 <= 6 AND 6 <= TraficSourceID_max@8) AND RefererHash_null_count@12 != row_count@3 AND RefererHash_min@10 <= 3594120000172545465 AND 3594120000172545465 <= RefererHash_max@11, required_guarantees=[CounterID in (62), IsRefresh in (0), RefererHash in (3594120000172545465), TraficSourceID in (-1, 6)]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_41() -> Result<()> {
        let display = test_clickbench_query("q41").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ GlobalLimitExec: skip=10000, fetch=10
        │   SortPreservingMergeExec: [pageviews@2 DESC], fetch=10010
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: TopK(fetch=10010), expr=[pageviews@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[WindowClientWidth@0 as WindowClientWidth, WindowClientHeight@1 as WindowClientHeight, count(Int64(1))@2 as pageviews]
          │     AggregateExec: mode=FinalPartitioned, gby=[WindowClientWidth@0 as WindowClientWidth, WindowClientHeight@1 as WindowClientHeight], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([WindowClientWidth@0, WindowClientHeight@1], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[WindowClientWidth@0 as WindowClientWidth, WindowClientHeight@1 as WindowClientHeight], aggr=[count(Int64(1))]
            │     FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@2 = 0 AND DontCountHits@5 = 0 AND URLHash@6 = 2868770270353813622, projection=[WindowClientWidth@3, WindowClientHeight@4]
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0] 
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, IsRefresh, WindowClientWidth, WindowClientHeight, DontCountHits, URLHash], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND IsRefresh@15 = 0 AND DontCountHits@61 = 0 AND URLHash@103 = 2868770270353813622, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5 AND DontCountHits_null_count@9 != row_count@3 AND DontCountHits_min@7 <= 0 AND 0 <= DontCountHits_max@8 AND URLHash_null_count@12 != row_count@3 AND URLHash_min@10 <= 2868770270353813622 AND 2868770270353813622 <= URLHash_max@11, required_guarantees=[CounterID in (62), DontCountHits in (0), IsRefresh in (0), URLHash in (2868770270353813622)]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "ordering mismatch: expected ordering: Some(LexOrdering { exprs: [PhysicalSortExpr { expr: ScalarFunctionExpr { fun: \"<FUNC>\", name: \"date_trunc\", args: [Literal { value: Utf8(\"minute\"), field: Field { name: \"lit\", data_type: Utf8 } }, Column { name: \"m\", index: 0 }], return_field: Field { name: \"date_trunc\", data_type: Timestamp(Second, None), nullable: true } }, options: SortOptions { descending: false, nulls_first: false } }], set: {ScalarFunctionExpr { fun: \"<FUNC>\", name: \"date_trunc\", args: [Literal { value: Utf8(\"minute\"), field: Field { name: \"lit\", data_type: Utf8 } }, Column { name: \"m\", index: 0 }], return_field: Field { name: \"date_trunc\", data_type: Timestamp(Second, None), nullable: true } }} }), actual ordering: Some(LexOrdering { exprs: [PhysicalSortExpr { expr: ScalarFunctionExpr { fun: \"<FUNC>\", name: \"date_trunc\", args: [Literal { value: Utf8(\"minute\"), field: Field { name: \"lit\", data_type: Utf8 } }, Column { name: \"m\", index: 0 }], return_field: Field { name: \"date_trunc\", data_type: Timestamp(Second, None), nullable: true } }, options: SortOptions { descending: false, nulls_first: false } }], set: {ScalarFunctionExpr { fun: \"<FUNC>\", name: \"date_trunc\", args: [Literal { value: Utf8(\"minute\"), field: Field { name: \"lit\", data_type: Utf8 } }, Column { name: \"m\", index: 0 }], return_field: Field { name: \"date_trunc\", data_type: Timestamp(Second, None), nullable: true } }} })"]
    async fn test_clickbench_42() -> Result<()> {
        let display = test_clickbench_query("q42").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    static INIT_TEST_TPCDS_TABLES: OnceCell<()> = OnceCell::const_new();

    async fn test_clickbench_query(query_id: &str) -> Result<String> {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "testdata/clickbench/plans_range{}-{}",
            FILE_RANGE.start, FILE_RANGE.end
        ));
        INIT_TEST_TPCDS_TABLES
            .get_or_init(|| async {
                clickbench::generate_clickbench_data(&data_dir, FILE_RANGE)
                    .await
                    .unwrap();
            })
            .await;

        let query_sql = clickbench::get_query(query_id)?;

        let (d_ctx, _guard) = start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
        let d_ctx = d_ctx
            .with_distributed_files_per_task(FILES_PER_TASK)?
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?;

        benchmarks_common::register_tables(&d_ctx, &data_dir).await?;

        let df = d_ctx.sql(&query_sql).await?;
        let plan = df.create_physical_plan().await?;

        if !plan.as_any().is::<DistributedExec>() {
            Ok("".to_string())
        } else {
            Ok(display_plan_ascii(plan.as_ref(), false))
        }
    }
}
