#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        assert_snapshot, ArrowFlightReadExec, DefaultSessionBuilder,
        DistributedPhysicalOptimizerRule,
    };
    use futures::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn highly_distributed_query() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) = start_localhost_context(9, DefaultSessionBuilder).await;
        register_parquet_tables(&ctx).await?;

        let df = ctx.sql(r#"SELECT * FROM flights_1m"#).await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = displayable(physical.as_ref()).indent(true).to_string();

        let mut physical_distributed = physical.clone();
        for size in [1, 10, 5] {
            physical_distributed = Arc::new(ArrowFlightReadExec::new_pending(Arc::new(
                RepartitionExec::try_new(
                    physical_distributed,
                    Partitioning::RoundRobinBatch(size),
                )?,
            )));
        }

        let physical_distributed =
            DistributedPhysicalOptimizerRule::default().distribute_plan(physical_distributed)?;
        let physical_distributed = Arc::new(physical_distributed);
        let physical_distributed_str = displayable(physical_distributed.as_ref())
            .indent(true)
            .to_string();

        assert_snapshot!(physical_str,
            @"DataSourceExec: file_groups={1 group: [[/testdata/flights-1m.parquet]]}, projection=[FL_DATE, DEP_DELAY, ARR_DELAY, AIR_TIME, DISTANCE, DEP_TIME, ARR_TIME], file_type=parquet",
        );

        assert_snapshot!(physical_distributed_str,
            @r"
        ┌───── Stage 4   Task: partitions: 0..4,unassigned]
        │partitions [out:5            ] ArrowFlightReadExec: Stage 3  
        └──────────────────────────────────────────────────
          ┌───── Stage 3   Task: partitions: 0..4,unassigned]
          │partitions [out:5  <-- in:10 ] RepartitionExec: partitioning=RoundRobinBatch(5), input_partitions=10
          │partitions [out:10           ]   ArrowFlightReadExec: Stage 2  
          └──────────────────────────────────────────────────
            ┌───── Stage 2   Task: partitions: 0..9,unassigned]
            │partitions [out:10 <-- in:1  ] RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
            │partitions [out:1            ]   ArrowFlightReadExec: Stage 1  
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Task: partitions: 0,unassigned]
              │partitions [out:1  <-- in:1  ] RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=1
              │partitions [out:1            ]   DataSourceExec: file_groups={1 group: [[/testdata/flights-1m.parquet]]}, projection=[FL_DATE, DEP_DELAY, ARR_DELAY, AIR_TIME, DISTANCE, DEP_TIME, ARR_TIME], file_type=parquet
              └──────────────────────────────────────────────────
        ",
        );

        let time = std::time::Instant::now();
        let batches = execute_stream(physical, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        println!("time: {:?}", time.elapsed());

        let time = std::time::Instant::now();
        let batches_distributed = execute_stream(physical_distributed, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        println!("time: {:?}", time.elapsed());

        assert_eq!(
            batches.iter().map(|v| v.num_rows()).sum::<usize>(),
            batches_distributed
                .iter()
                .map(|v| v.num_rows())
                .sum::<usize>(),
        );

        Ok(())
    }
}
