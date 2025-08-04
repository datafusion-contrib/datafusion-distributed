#[allow(dead_code)]
mod common;

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::common::localhost::{start_localhost_context, NoopSessionBuilder};
    use crate::common::parquet::register_parquet_tables;
    use crate::common::plan::distribute_aggregate;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion_distributed::assign_stages;
    use futures::TryStreamExt;
    use std::error::Error;

    #[tokio::test]
    async fn distributed_aggregation() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) =
            start_localhost_context([50050, 50051, 50052], NoopSessionBuilder).await;
        register_parquet_tables(&ctx).await?;

        let df = ctx
            .sql(r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#)
            .await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = displayable(physical.as_ref()).indent(true).to_string();

        let physical_distributed = distribute_aggregate(physical.clone())?;
        let physical_distributed = assign_stages(physical_distributed, &ctx)?;

        let physical_distributed_str = displayable(physical_distributed.as_ref())
            .indent(true)
            .to_string();

        assert_snapshot!(physical_str,
            @r"
        ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
          SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
            SortExec: expr=[count(Int64(1))@2 ASC NULLS LAST], preserve_partitioning=[true]
              ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
                AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  CoalesceBatchesExec: target_batch_size=8192
                    RepartitionExec: partitioning=Hash([RainToday@0], CPUs), input_partitions=CPUs
                      RepartitionExec: partitioning=RoundRobinBatch(CPUs), input_partitions=1
                        AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                          DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[RainToday], file_type=parquet
        ",
        );

        assert_snapshot!(physical_distributed_str,
            @r"
        ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
          SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
            SortExec: expr=[count(Int64(1))@2 ASC NULLS LAST], preserve_partitioning=[true]
              ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
                AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  ArrowFlightReadExec: input_tasks=8 hash_expr=[] stage_id=UUID input_stage_id=UUID input_hosts=[http://localhost:50050/, http://localhost:50051/, http://localhost:50052/, http://localhost:50050/, http://localhost:50051/, http://localhost:50052/, http://localhost:50050/, http://localhost:50051/]
                    CoalesceBatchesExec: target_batch_size=8192
                      RepartitionExec: partitioning=Hash([RainToday@0], CPUs), input_partitions=CPUs
                        RepartitionExec: partitioning=RoundRobinBatch(CPUs), input_partitions=1
                          AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                            ArrowFlightReadExec: input_tasks=1 hash_expr=[RainToday@0] stage_id=UUID input_stage_id=UUID input_hosts=[http://localhost:50052/]
                              DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[RainToday], file_type=parquet
        ",
        );

        let batches = pretty_format_batches(
            &execute_stream(physical, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        assert_snapshot!(batches, @r"
        +----------+-----------+
        | count(*) | RainToday |
        +----------+-----------+
        | 66       | Yes       |
        | 300      | No        |
        +----------+-----------+
        ");

        let batches_distributed = pretty_format_batches(
            &execute_stream(physical_distributed, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;
        assert_snapshot!(batches_distributed, @r"
        +----------+-----------+
        | count(*) | RainToday |
        +----------+-----------+
        | 66       | Yes       |
        | 300      | No        |
        +----------+-----------+
        ");

        Ok(())
    }
}
