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
            .with_distributed_files_per_task(FILES_PER_TASK)?
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?
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
