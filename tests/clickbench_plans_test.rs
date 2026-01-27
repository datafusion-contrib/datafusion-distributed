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
    const BYTES_PROCESSED_PER_PARTITION: usize = 8 * 1024 * 1024;
    const FILE_RANGE: Range<usize> = 0..3;

    #[tokio::test]
    async fn test_clickbench_0() -> Result<()> {
        let display = test_clickbench_query("q0").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_1() -> Result<()> {
        let display = test_clickbench_query("q1").await?;
        assert_snapshot!(display, @"");
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
        │       [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[sum(hits.AdvEngineID), count(Int64(1)), avg(hits.ResolutionWidth)]
          │   PartitionIsolatorExec: t0:[p0,p1,p2,__,__] t1:[__,__,__,p0,p1]
          │     DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[ResolutionWidth, AdvEngineID], file_type=parquet
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
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
        │       [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(alias1)]
          │   AggregateExec: mode=FinalPartitioned, gby=[alias1@0 as alias1], aggr=[]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p8] t1:[p0..p8] t2:[p0..p8] 
            │ RepartitionExec: partitioning=Hash([alias1@0], 9), input_partitions=2
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
        │       [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(alias1)]
          │   AggregateExec: mode=FinalPartitioned, gby=[alias1@0 as alias1], aggr=[]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p8] t1:[p0..p8] t2:[p0..p8] t3:[p0..p8] 
            │ RepartitionExec: partitioning=Hash([alias1@0], 9), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchPhrase@0 as alias1], aggr=[]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[SearchPhrase], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_6() -> Result<()> {
        let display = test_clickbench_query("q6").await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_7() -> Result<()> {
        let display = test_clickbench_query("q7").await?;
        assert_snapshot!(display, @"");
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
        │         [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([RegionID@0], 3), input_partitions=3
          │   AggregateExec: mode=Partial, gby=[RegionID@0 as RegionID], aggr=[count(alias1)]
          │     AggregateExec: mode=FinalPartitioned, gby=[RegionID@0 as RegionID, alias1@1 as alias1], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([RegionID@0, alias1@1], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[RegionID@0 as RegionID, UserID@1 as alias1], aggr=[]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] t3:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([RegionID@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[RegionID@0 as RegionID], aggr=[sum(hits.AdvEngineID), count(Int64(1)), avg(hits.ResolutionWidth), count(DISTINCT hits.UserID)]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[u@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[MobilePhoneModel@0 as MobilePhoneModel, count(alias1)@1 as u]
          │     AggregateExec: mode=FinalPartitioned, gby=[MobilePhoneModel@0 as MobilePhoneModel], aggr=[count(alias1)]
          │       [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([MobilePhoneModel@0], 12), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[MobilePhoneModel@0 as MobilePhoneModel], aggr=[count(alias1)]
            │     AggregateExec: mode=FinalPartitioned, gby=[MobilePhoneModel@0 as MobilePhoneModel, alias1@1 as alias1], aggr=[]
            │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
              │ RepartitionExec: partitioning=Hash([MobilePhoneModel@0, alias1@1], 12), input_partitions=2
              │   AggregateExec: mode=Partial, gby=[MobilePhoneModel@1 as MobilePhoneModel, UserID@0 as alias1], aggr=[]
              │     FilterExec: MobilePhoneModel@1 !=
              │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[u@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[MobilePhone@0 as MobilePhone, MobilePhoneModel@1 as MobilePhoneModel, count(alias1)@2 as u]
          │     AggregateExec: mode=FinalPartitioned, gby=[MobilePhone@0 as MobilePhone, MobilePhoneModel@1 as MobilePhoneModel], aggr=[count(alias1)]
          │       [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([MobilePhone@0, MobilePhoneModel@1], 12), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[MobilePhone@0 as MobilePhone, MobilePhoneModel@1 as MobilePhoneModel], aggr=[count(alias1)]
            │     AggregateExec: mode=FinalPartitioned, gby=[MobilePhone@0 as MobilePhone, MobilePhoneModel@1 as MobilePhoneModel, alias1@2 as alias1], aggr=[]
            │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
              │ RepartitionExec: partitioning=Hash([MobilePhone@0, MobilePhoneModel@1, alias1@2], 12), input_partitions=2
              │   AggregateExec: mode=Partial, gby=[MobilePhone@1 as MobilePhone, MobilePhoneModel@2 as MobilePhoneModel, UserID@0 as alias1], aggr=[]
              │     FilterExec: MobilePhoneModel@2 !=
              │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, count(Int64(1))@1 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[SearchPhrase@0 as SearchPhrase], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([SearchPhrase@0], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchPhrase@0 as SearchPhrase], aggr=[count(Int64(1))]
            │     FilterExec: SearchPhrase@0 !=
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[u@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, count(alias1)@1 as u]
          │     AggregateExec: mode=FinalPartitioned, gby=[SearchPhrase@0 as SearchPhrase], aggr=[count(alias1)]
          │       [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([SearchPhrase@0], 12), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[SearchPhrase@0 as SearchPhrase], aggr=[count(alias1)]
            │     AggregateExec: mode=FinalPartitioned, gby=[SearchPhrase@0 as SearchPhrase, alias1@1 as alias1], aggr=[]
            │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
              │ RepartitionExec: partitioning=Hash([SearchPhrase@0, alias1@1], 12), input_partitions=2
              │   AggregateExec: mode=Partial, gby=[SearchPhrase@1 as SearchPhrase, UserID@0 as alias1], aggr=[]
              │     FilterExec: SearchPhrase@1 !=
              │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchEngineID@0 as SearchEngineID, SearchPhrase@1 as SearchPhrase, count(Int64(1))@2 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[SearchEngineID@0 as SearchEngineID, SearchPhrase@1 as SearchPhrase], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([SearchEngineID@0, SearchPhrase@1], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchEngineID@0 as SearchEngineID, SearchPhrase@1 as SearchPhrase], aggr=[count(Int64(1))]
            │     FilterExec: SearchPhrase@1 !=
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[count(*)@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[UserID@0 as UserID, count(Int64(1))@1 as count(*), count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[UserID@0 as UserID], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([UserID@0], 12), input_partitions=2
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
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[count(*)@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase, count(Int64(1))@2 as count(*), count(Int64(1))@2 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([UserID@0, SearchPhrase@1], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, SearchPhrase], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_17() -> Result<()> {
        let display = test_clickbench_query("q17").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase, count(Int64(1))@2 as count(*)]
        │   CoalescePartitionsExec: fetch=10
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ AggregateExec: mode=FinalPartitioned, gby=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase], aggr=[count(Int64(1))]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([UserID@0, SearchPhrase@1], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, SearchPhrase], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_18() -> Result<()> {
        let display = test_clickbench_query("q18").await?;
        assert_snapshot!(display, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[UserID@0 as UserID, m@1 as m, SearchPhrase@2 as SearchPhrase, count(*)@3 as count(*)]
        │   SortPreservingMergeExec: [count(Int64(1))@4 DESC], fetch=10
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[count(*)@3 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[UserID@0 as UserID, date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime))@1 as m, SearchPhrase@2 as SearchPhrase, count(Int64(1))@3 as count(*), count(Int64(1))@3 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[UserID@0 as UserID, date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime))@1 as date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime)), SearchPhrase@2 as SearchPhrase], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([UserID@0, date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime))@1, SearchPhrase@2], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[UserID@1 as UserID, date_part(MINUTE, to_timestamp_seconds(EventTime@0)) as date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime)), SearchPhrase@2 as SearchPhrase], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5] 
          │ FilterExec: UserID@0 = 435090932899640449
          │   PartitionIsolatorExec: t0:[p0,p1,p2,__,__] t1:[__,__,__,p0,p1]
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
    async fn test_clickbench_21() -> Result<()> {
        let display = test_clickbench_query("q21").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@2 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, min(hits.URL)@1 as min(hits.URL), count(Int64(1))@2 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[SearchPhrase@0 as SearchPhrase], aggr=[min(hits.URL), count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([SearchPhrase@0], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchPhrase@1 as SearchPhrase], aggr=[min(hits.URL), count(Int64(1))]
            │     FilterExec: CAST(URL@0 AS Utf8View) LIKE %google% AND SearchPhrase@1 !=
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[URL, SearchPhrase], file_type=parquet, predicate=CAST(URL@13 AS Utf8View) LIKE %google% AND SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_22() -> Result<()> {
        let display = test_clickbench_query("q22").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@3 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@3 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, min(hits.URL)@1 as min(hits.URL), min(hits.Title)@2 as min(hits.Title), count(Int64(1))@3 as c, count(DISTINCT hits.UserID)@4 as count(DISTINCT hits.UserID)]
          │     AggregateExec: mode=FinalPartitioned, gby=[SearchPhrase@0 as SearchPhrase], aggr=[min(hits.URL), min(hits.Title), count(Int64(1)), count(DISTINCT hits.UserID)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([SearchPhrase@0], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchPhrase@3 as SearchPhrase], aggr=[min(hits.URL), min(hits.Title), count(Int64(1)), count(DISTINCT hits.UserID)]
            │     FilterExec: CAST(Title@0 AS Utf8View) LIKE %Google% AND CAST(URL@2 AS Utf8View) NOT LIKE %.google.% AND SearchPhrase@3 !=
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[Title, UserID, URL, SearchPhrase], file_type=parquet, predicate=CAST(Title@2 AS Utf8View) LIKE %Google% AND CAST(URL@13 AS Utf8View) NOT LIKE %.google.% AND SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_23() -> Result<()> {
        let display = test_clickbench_query("q23").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [EventTime@4 ASC NULLS LAST], fetch=10
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=8, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] t3:[p6..p7] 
          │ SortExec: TopK(fetch=10), expr=[EventTime@4 ASC NULLS LAST], preserve_partitioning=[true]
          │   FilterExec: CAST(URL@13 AS Utf8View) LIKE %google%
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
          │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[WatchID, JavaEnable, Title, GoodEvent, EventTime, EventDate, CounterID, ClientIP, RegionID, UserID, CounterClass, OS, UserAgent, URL, Referer, IsRefresh, RefererCategoryID, RefererRegionID, URLCategoryID, URLRegionID, ResolutionWidth, ResolutionHeight, ResolutionDepth, FlashMajor, FlashMinor, FlashMinor2, NetMajor, NetMinor, UserAgentMajor, UserAgentMinor, CookieEnable, JavascriptEnable, IsMobile, MobilePhone, MobilePhoneModel, Params, IPNetworkID, TraficSourceID, SearchEngineID, SearchPhrase, AdvEngineID, IsArtifical, WindowClientWidth, WindowClientHeight, ClientTimeZone, ClientEventTime, SilverlightVersion1, SilverlightVersion2, SilverlightVersion3, SilverlightVersion4, PageCharset, CodeVersion, IsLink, IsDownload, IsNotBounce, FUniqID, OriginalURL, HID, IsOldCounter, IsEvent, IsParameter, DontCountHits, WithHash, HitColor, LocalEventTime, Age, Sex, Income, Interests, Robotness, RemoteIP, WindowName, OpenerName, HistoryLength, BrowserLanguage, BrowserCountry, SocialNetwork, SocialAction, HTTPError, SendTiming, DNSTiming, ConnectTiming, ResponseStartTiming, ResponseEndTiming, FetchTiming, SocialSourceNetworkID, SocialSourcePage, ParamPrice, ParamOrderID, ParamCurrency, ParamCurrencyID, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID, UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash, URLHash, CLID], file_type=parquet, predicate=CAST(URL@13 AS Utf8View) LIKE %google% AND DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_24() -> Result<()> {
        let display = test_clickbench_query("q24").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase]
        │   SortPreservingMergeExec: [EventTime@1 ASC NULLS LAST], fetch=10
        │     [Stage 1] => NetworkCoalesceExec: output_partitions=8, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] t3:[p6..p7] 
          │ SortExec: TopK(fetch=10), expr=[EventTime@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchPhrase@1 as SearchPhrase, EventTime@0 as EventTime]
          │     FilterExec: SearchPhrase@1 !=
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
          │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventTime, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 !=  AND DynamicFilter [ empty ], pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_25() -> Result<()> {
        let display = test_clickbench_query("q25").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [SearchPhrase@0 ASC NULLS LAST], fetch=10
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=8, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] t3:[p6..p7] 
          │ SortExec: TopK(fetch=10), expr=[SearchPhrase@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   FilterExec: SearchPhrase@0 !=
          │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │     [Stage 1] => NetworkCoalesceExec: output_partitions=8, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] t3:[p6..p7] 
          │ SortExec: TopK(fetch=10), expr=[EventTime@1 ASC NULLS LAST, SearchPhrase@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchPhrase@1 as SearchPhrase, EventTime@0 as EventTime]
          │     FilterExec: SearchPhrase@1 !=
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   SortExec: TopK(fetch=25), expr=[l@1 DESC], preserve_partitioning=[true]
        │     ProjectionExec: expr=[CounterID@0 as CounterID, avg(length(hits.URL))@1 as l, count(Int64(1))@2 as c]
        │       FilterExec: count(Int64(1))@2 > 100000
        │         AggregateExec: mode=FinalPartitioned, gby=[CounterID@0 as CounterID], aggr=[avg(length(hits.URL)), count(Int64(1))]
        │           [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([CounterID@0], 3), input_partitions=2
          │   AggregateExec: mode=Partial, gby=[CounterID@0 as CounterID], aggr=[avg(length(hits.URL)), count(Int64(1))]
          │     FilterExec: URL@1 !=
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=25), expr=[l@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))@0 as k, avg(length(hits.Referer))@1 as l, count(Int64(1))@2 as c, min(hits.Referer)@3 as min(hits.Referer)]
          │     FilterExec: count(Int64(1))@2 > 100000
          │       AggregateExec: mode=FinalPartitioned, gby=[regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))@0 as regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))], aggr=[avg(length(hits.Referer)), count(Int64(1)), min(hits.Referer)]
          │         [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))@0], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[regexp_replace(CAST(Referer@0 AS LargeUtf8), ^https?://(?:www\.)?([^/]+)/.*$, \1) as regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))], aggr=[avg(length(hits.Referer)), count(Int64(1)), min(hits.Referer)]
            │     FilterExec: Referer@0 !=
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │     [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[sum(hits.ResolutionWidth), sum(hits.ResolutionWidth + Int64(1)), sum(hits.ResolutionWidth + Int64(2)), sum(hits.ResolutionWidth + Int64(3)), sum(hits.ResolutionWidth + Int64(4)), sum(hits.ResolutionWidth + Int64(5)), sum(hits.ResolutionWidth + Int64(6)), sum(hits.ResolutionWidth + Int64(7)), sum(hits.ResolutionWidth + Int64(8)), sum(hits.ResolutionWidth + Int64(9)), sum(hits.ResolutionWidth + Int64(10)), sum(hits.ResolutionWidth + Int64(11)), sum(hits.ResolutionWidth + Int64(12)), sum(hits.ResolutionWidth + Int64(13)), sum(hits.ResolutionWidth + Int64(14)), sum(hits.ResolutionWidth + Int64(15)), sum(hits.ResolutionWidth + Int64(16)), sum(hits.ResolutionWidth + Int64(17)), sum(hits.ResolutionWidth + Int64(18)), sum(hits.ResolutionWidth + Int64(19)), sum(hits.ResolutionWidth + Int64(20)), sum(hits.ResolutionWidth + Int64(21)), sum(hits.ResolutionWidth + Int64(22)), sum(hits.ResolutionWidth + Int64(23)), sum(hits.ResolutionWidth + Int64(24)), sum(hits.ResolutionWidth + Int64(25)), sum(hits.ResolutionWidth + Int64(26)), sum(hits.ResolutionWidth + Int64(27)), sum(hits.ResolutionWidth + Int64(28)), sum(hits.ResolutionWidth + Int64(29)), sum(hits.ResolutionWidth + Int64(30)), sum(hits.ResolutionWidth + Int64(31)), sum(hits.ResolutionWidth + Int64(32)), sum(hits.ResolutionWidth + Int64(33)), sum(hits.ResolutionWidth + Int64(34)), sum(hits.ResolutionWidth + Int64(35)), sum(hits.ResolutionWidth + Int64(36)), sum(hits.ResolutionWidth + Int64(37)), sum(hits.ResolutionWidth + Int64(38)), sum(hits.ResolutionWidth + Int64(39)), sum(hits.ResolutionWidth + Int64(40)), sum(hits.ResolutionWidth + Int64(41)), sum(hits.ResolutionWidth + Int64(42)), sum(hits.ResolutionWidth + Int64(43)), sum(hits.ResolutionWidth + Int64(44)), sum(hits.ResolutionWidth + Int64(45)), sum(hits.ResolutionWidth + Int64(46)), sum(hits.ResolutionWidth + Int64(47)), sum(hits.ResolutionWidth + Int64(48)), sum(hits.ResolutionWidth + Int64(49)), sum(hits.ResolutionWidth + Int64(50)), sum(hits.ResolutionWidth + Int64(51)), sum(hits.ResolutionWidth + Int64(52)), sum(hits.ResolutionWidth + Int64(53)), sum(hits.ResolutionWidth + Int64(54)), sum(hits.ResolutionWidth + Int64(55)), sum(hits.ResolutionWidth + Int64(56)), sum(hits.ResolutionWidth + Int64(57)), sum(hits.ResolutionWidth + Int64(58)), sum(hits.ResolutionWidth + Int64(59)), sum(hits.ResolutionWidth + Int64(60)), sum(hits.ResolutionWidth + Int64(61)), sum(hits.ResolutionWidth + Int64(62)), sum(hits.ResolutionWidth + Int64(63)), sum(hits.ResolutionWidth + Int64(64)), sum(hits.ResolutionWidth + Int64(65)), sum(hits.ResolutionWidth + Int64(66)), sum(hits.ResolutionWidth + Int64(67)), sum(hits.ResolutionWidth + Int64(68)), sum(hits.ResolutionWidth + Int64(69)), sum(hits.ResolutionWidth + Int64(70)), sum(hits.ResolutionWidth + Int64(71)), sum(hits.ResolutionWidth + Int64(72)), sum(hits.ResolutionWidth + Int64(73)), sum(hits.ResolutionWidth + Int64(74)), sum(hits.ResolutionWidth + Int64(75)), sum(hits.ResolutionWidth + Int64(76)), sum(hits.ResolutionWidth + Int64(77)), sum(hits.ResolutionWidth + Int64(78)), sum(hits.ResolutionWidth + Int64(79)), sum(hits.ResolutionWidth + Int64(80)), sum(hits.ResolutionWidth + Int64(81)), sum(hits.ResolutionWidth + Int64(82)), sum(hits.ResolutionWidth + Int64(83)), sum(hits.ResolutionWidth + Int64(84)), sum(hits.ResolutionWidth + Int64(85)), sum(hits.ResolutionWidth + Int64(86)), sum(hits.ResolutionWidth + Int64(87)), sum(hits.ResolutionWidth + Int64(88)), sum(hits.ResolutionWidth + Int64(89))]
          │   PartitionIsolatorExec: t0:[p0,p1,p2,__,__] t1:[__,__,__,p0,p1]
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
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[SearchEngineID@0 as SearchEngineID, ClientIP@1 as ClientIP, count(Int64(1))@2 as c, sum(hits.IsRefresh)@3 as sum(hits.IsRefresh), avg(hits.ResolutionWidth)@4 as avg(hits.ResolutionWidth)]
          │     AggregateExec: mode=FinalPartitioned, gby=[SearchEngineID@0 as SearchEngineID, ClientIP@1 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([SearchEngineID@0, ClientIP@1], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[SearchEngineID@3 as SearchEngineID, ClientIP@0 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
            │     FilterExec: SearchPhrase@4 != , projection=[ClientIP@0, IsRefresh@1, ResolutionWidth@2, SearchEngineID@3]
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[ClientIP, IsRefresh, ResolutionWidth, SearchEngineID, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_31() -> Result<()> {
        let display = test_clickbench_query("q31").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@2 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[WatchID@0 as WatchID, ClientIP@1 as ClientIP, count(Int64(1))@2 as c, sum(hits.IsRefresh)@3 as sum(hits.IsRefresh), avg(hits.ResolutionWidth)@4 as avg(hits.ResolutionWidth)]
          │     AggregateExec: mode=FinalPartitioned, gby=[WatchID@0 as WatchID, ClientIP@1 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([WatchID@0, ClientIP@1], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[WatchID@0 as WatchID, ClientIP@1 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
            │     FilterExec: SearchPhrase@4 != , projection=[WatchID@0, ClientIP@1, IsRefresh@2, ResolutionWidth@3]
            │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
            │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[WatchID, ClientIP, IsRefresh, ResolutionWidth, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_32() -> Result<()> {
        let display = test_clickbench_query("q32").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@2 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[WatchID@0 as WatchID, ClientIP@1 as ClientIP, count(Int64(1))@2 as c, sum(hits.IsRefresh)@3 as sum(hits.IsRefresh), avg(hits.ResolutionWidth)@4 as avg(hits.ResolutionWidth)]
          │     AggregateExec: mode=FinalPartitioned, gby=[WatchID@0 as WatchID, ClientIP@1 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([WatchID@0, ClientIP@1], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[WatchID@0 as WatchID, ClientIP@1 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
            │       DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[WatchID, ClientIP, IsRefresh, ResolutionWidth], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_33() -> Result<()> {
        let display = test_clickbench_query("q33").await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [c@1 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@1 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[URL@0 as URL, count(Int64(1))@1 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[URL@0 as URL], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([URL@0], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[URL@0 as URL], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[1 as Int64(1), URL@0 as URL, count(Int64(1))@1 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[URL@0 as URL], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([URL@0], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[URL@0 as URL], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ SortExec: TopK(fetch=10), expr=[c@4 DESC], preserve_partitioning=[true]
          │   ProjectionExec: expr=[ClientIP@0 as ClientIP, hits.ClientIP - Int64(1)@1 as hits.ClientIP - Int64(1), hits.ClientIP - Int64(2)@2 as hits.ClientIP - Int64(2), hits.ClientIP - Int64(3)@3 as hits.ClientIP - Int64(3), count(Int64(1))@4 as c]
          │     AggregateExec: mode=FinalPartitioned, gby=[ClientIP@0 as ClientIP, hits.ClientIP - Int64(1)@1 as hits.ClientIP - Int64(1), hits.ClientIP - Int64(2)@2 as hits.ClientIP - Int64(2), hits.ClientIP - Int64(3)@3 as hits.ClientIP - Int64(3)], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
            │ RepartitionExec: partitioning=Hash([ClientIP@0, hits.ClientIP - Int64(1)@1, hits.ClientIP - Int64(2)@2, hits.ClientIP - Int64(3)@3], 12), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[ClientIP@1 as ClientIP, __common_expr_1@0 - 1 as hits.ClientIP - Int64(1), __common_expr_1@0 - 2 as hits.ClientIP - Int64(2), __common_expr_1@0 - 3 as hits.ClientIP - Int64(3)], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   SortExec: TopK(fetch=10), expr=[pageviews@1 DESC], preserve_partitioning=[true]
        │     ProjectionExec: expr=[URL@0 as URL, count(Int64(1))@1 as pageviews]
        │       AggregateExec: mode=FinalPartitioned, gby=[URL@0 as URL], aggr=[count(Int64(1))]
        │         [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([URL@0], 3), input_partitions=2
          │   AggregateExec: mode=Partial, gby=[URL@0 as URL], aggr=[count(Int64(1))]
          │     FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND DontCountHits@4 = 0 AND IsRefresh@3 = 0 AND URL@2 != , projection=[URL@2]
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │   SortExec: TopK(fetch=10), expr=[pageviews@1 DESC], preserve_partitioning=[true]
        │     ProjectionExec: expr=[Title@0 as Title, count(Int64(1))@1 as pageviews]
        │       AggregateExec: mode=FinalPartitioned, gby=[Title@0 as Title], aggr=[count(Int64(1))]
        │         [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([Title@0], 3), input_partitions=2
          │   AggregateExec: mode=Partial, gby=[Title@0 as Title], aggr=[count(Int64(1))]
          │     FilterExec: CounterID@2 = 62 AND CAST(EventDate@1 AS Utf8) >= 2013-07-01 AND CAST(EventDate@1 AS Utf8) <= 2013-07-31 AND DontCountHits@4 = 0 AND IsRefresh@3 = 0 AND Title@0 != , projection=[Title@0]
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │     SortExec: TopK(fetch=1010), expr=[pageviews@1 DESC], preserve_partitioning=[true]
        │       ProjectionExec: expr=[URL@0 as URL, count(Int64(1))@1 as pageviews]
        │         AggregateExec: mode=FinalPartitioned, gby=[URL@0 as URL], aggr=[count(Int64(1))]
        │           [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([URL@0], 3), input_partitions=2
          │   AggregateExec: mode=Partial, gby=[URL@0 as URL], aggr=[count(Int64(1))]
          │     FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@3 = 0 AND IsLink@4 != 0 AND IsDownload@5 = 0, projection=[URL@2]
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │     SortExec: TopK(fetch=1010), expr=[pageviews@5 DESC], preserve_partitioning=[true]
        │       ProjectionExec: expr=[TraficSourceID@0 as TraficSourceID, SearchEngineID@1 as SearchEngineID, AdvEngineID@2 as AdvEngineID, CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END@3 as src, URL@4 as dst, count(Int64(1))@5 as pageviews]
        │         AggregateExec: mode=FinalPartitioned, gby=[TraficSourceID@0 as TraficSourceID, SearchEngineID@1 as SearchEngineID, AdvEngineID@2 as AdvEngineID, CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END@3 as CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END, URL@4 as URL], aggr=[count(Int64(1))]
        │           [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([TraficSourceID@0, SearchEngineID@1, AdvEngineID@2, CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END@3, URL@4], 3), input_partitions=2
          │   AggregateExec: mode=Partial, gby=[TraficSourceID@2 as TraficSourceID, SearchEngineID@3 as SearchEngineID, AdvEngineID@4 as AdvEngineID, CASE WHEN SearchEngineID@3 = 0 AND AdvEngineID@4 = 0 THEN Referer@1 ELSE  END as CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END, URL@0 as URL], aggr=[count(Int64(1))]
          │     FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@4 = 0, projection=[URL@2, Referer@3, TraficSourceID@5, SearchEngineID@6, AdvEngineID@7]
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │     SortExec: TopK(fetch=110), expr=[pageviews@2 DESC], preserve_partitioning=[true]
        │       ProjectionExec: expr=[URLHash@0 as URLHash, EventDate@1 as EventDate, count(Int64(1))@2 as pageviews]
        │         AggregateExec: mode=FinalPartitioned, gby=[URLHash@0 as URLHash, EventDate@1 as EventDate], aggr=[count(Int64(1))]
        │           [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([URLHash@0, EventDate@1], 3), input_partitions=2
          │   AggregateExec: mode=Partial, gby=[URLHash@1 as URLHash, EventDate@0 as EventDate], aggr=[count(Int64(1))]
          │     FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@2 = 0 AND (TraficSourceID@3 = -1 OR TraficSourceID@3 = 6) AND RefererHash@4 = 3594120000172545465, projection=[EventDate@0, URLHash@5]
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
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
        │     SortExec: TopK(fetch=10010), expr=[pageviews@2 DESC], preserve_partitioning=[true]
        │       ProjectionExec: expr=[WindowClientWidth@0 as WindowClientWidth, WindowClientHeight@1 as WindowClientHeight, count(Int64(1))@2 as pageviews]
        │         AggregateExec: mode=FinalPartitioned, gby=[WindowClientWidth@0 as WindowClientWidth, WindowClientHeight@1 as WindowClientHeight], aggr=[count(Int64(1))]
        │           [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([WindowClientWidth@0, WindowClientHeight@1], 3), input_partitions=2
          │   AggregateExec: mode=Partial, gby=[WindowClientWidth@0 as WindowClientWidth, WindowClientHeight@1 as WindowClientHeight], aggr=[count(Int64(1))]
          │     FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@2 = 0 AND DontCountHits@5 = 0 AND URLHash@6 = 2868770270353813622, projection=[WindowClientWidth@3, WindowClientHeight@4]
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,__,__] t2:[__,__,__,p0,__] t3:[__,__,__,__,p0]
          │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, IsRefresh, WindowClientWidth, WindowClientHeight, DontCountHits, URLHash], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND IsRefresh@15 = 0 AND DontCountHits@61 = 0 AND URLHash@103 = 2868770270353813622, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5 AND DontCountHits_null_count@9 != row_count@3 AND DontCountHits_min@7 <= 0 AND 0 <= DontCountHits_max@8 AND URLHash_null_count@12 != row_count@3 AND URLHash_min@10 <= 2868770270353813622 AND 2868770270353813622 <= URLHash_max@11, required_guarantees=[CounterID in (62), DontCountHits in (0), IsRefresh in (0), URLHash in (2868770270353813622)]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_42() -> Result<()> {
        let display = test_clickbench_query("q42").await?;
        assert_snapshot!(display, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ GlobalLimitExec: skip=1000, fetch=10
        │   SortPreservingMergeExec: [date_trunc(minute, m@0) ASC NULLS LAST], fetch=1010
        │     SortExec: TopK(fetch=1010), expr=[date_trunc(minute, m@0) ASC NULLS LAST], preserve_partitioning=[true]
        │       ProjectionExec: expr=[date_trunc(Utf8("minute"),to_timestamp_seconds(hits.EventTime))@0 as m, count(Int64(1))@1 as pageviews]
        │         AggregateExec: mode=FinalPartitioned, gby=[date_trunc(Utf8("minute"),to_timestamp_seconds(hits.EventTime))@0 as date_trunc(Utf8("minute"),to_timestamp_seconds(hits.EventTime))], aggr=[count(Int64(1))]
        │           [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] 
          │ RepartitionExec: partitioning=Hash([date_trunc(Utf8("minute"),to_timestamp_seconds(hits.EventTime))@0], 3), input_partitions=2
          │   AggregateExec: mode=Partial, gby=[date_trunc(minute, to_timestamp_seconds(EventTime@0)) as date_trunc(Utf8("minute"),to_timestamp_seconds(hits.EventTime))], aggr=[count(Int64(1))]
          │     FilterExec: CounterID@2 = 62 AND CAST(EventDate@1 AS Utf8) >= 2013-07-14 AND CAST(EventDate@1 AS Utf8) <= 2013-07-15 AND IsRefresh@3 = 0 AND DontCountHits@4 = 0, projection=[EventTime@0]
          │       PartitionIsolatorExec: t0:[p0,p1,__,__,__] t1:[__,__,p0,p1,__] t2:[__,__,__,__,p0]
          │         DataSourceExec: file_groups={5 groups: [[/testdata/clickbench/plans_range0-3/hits/0.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/1.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>], [/testdata/clickbench/plans_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventTime, EventDate, CounterID, IsRefresh, DontCountHits], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-14 AND CAST(EventDate@5 AS Utf8) <= 2013-07-15 AND IsRefresh@15 = 0 AND DontCountHits@61 = 0, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5 AND DontCountHits_null_count@9 != row_count@3 AND DontCountHits_min@7 <= 0 AND 0 <= DontCountHits_max@8, required_guarantees=[CounterID in (62), DontCountHits in (0), IsRefresh in (0)]
          └──────────────────────────────────────────────────
        "#);
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

        let (d_ctx, _guard, _) = start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
        let d_ctx = d_ctx
            .with_distributed_bytes_processed_per_partition(BYTES_PROCESSED_PER_PARTITION)?
            .with_distributed_broadcast_joins(true)?;

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
