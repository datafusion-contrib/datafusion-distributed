mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::physical_plan::collect;
    use datafusion_distributed::{
        DistributedExt, display_plan_ascii, rewrite_distributed_plan_with_dynamic_filters,
    };
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg_test_tools::IcebergTestHarness;
    use datafusion_distributed_iceberg_test_tools::create_taxi_table;
    use std::sync::Arc;

    #[tokio::test]
    async fn lazy_planning_join_fills_in_the_filter_the_scan_reports() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .build()
            .await?;

        let plan = harness
            .physical_plan(
                "SELECT COUNT(*) AS trips FROM taxi \
                 JOIN (VALUES (DATE '2024-01-10')) AS days(day) ON taxi.pickup_date = days.day \
                 WHERE taxi.trip_distance >= 2.0",
            )
            .await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;

        insta::assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ProjectionExec: expr=[count(Int64(1))@0 as trips]
          AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
            CoalescePartitionsExec
              AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
                HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(day@0, pickup_date@0)], projection=[]
                  ProjectionExec: expr=[column1@0 as day]
                    DataSourceExec: partitions=1, partition_sizes=[1]
                  FilterExec: trip_distance@0 >= 2, projection=[pickup_date@1]
                    DataSourceExec: format=iceberg, table=default.taxi, projection=[trip_distance, pickup_date], predicate=trip_distance >= 2, dynamic_filters=DynamicFilter [ pickup_date@1 >= 2024-01-10 AND pickup_date@1 <= 2024-01-10 AND pickup_date@1 IN (SET) ([2024-01-10]) ]
        ");
        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +-------+
        | trips |
        +-------+
        | 11714 |
        +-------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn eager_planning_join_fills_in_the_filter_the_scan_reports() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .build()
            .await?;

        let plan = harness
            .physical_plan(
                "SELECT COUNT(*) AS trips FROM taxi \
                 JOIN (VALUES (DATE '2024-01-10')) AS days(day) ON taxi.pickup_date = days.day \
                 WHERE taxi.trip_distance >= 2.0",
            )
            .await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;

        insta::assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ProjectionExec: expr=[count(Int64(1))@0 as trips]
          AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
            CoalescePartitionsExec
              AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
                HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(day@0, pickup_date@0)], projection=[]
                  ProjectionExec: expr=[column1@0 as day]
                    DataSourceExec: partitions=1, partition_sizes=[1]
                  FilterExec: trip_distance@0 >= 2, projection=[pickup_date@1]
                    DataSourceExec: format=iceberg, planned_files=7, dynamic_filters=DynamicFilter [ pickup_date@1 >= 2024-01-10 AND pickup_date@1 <= 2024-01-10 AND pickup_date@1 IN (SET) ([2024-01-10]) ]
        ");
        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +-------+
        | trips |
        +-------+
        | 11714 |
        +-------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn join_leaves_its_filter_empty_when_the_scan_does_not_report_it() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", false)
            .build()
            .await?;

        let plan = harness
            .physical_plan(
                "SELECT COUNT(*) AS trips FROM taxi \
                 JOIN (VALUES (DATE '2024-01-10')) AS days(day) ON taxi.pickup_date = days.day \
                 WHERE taxi.trip_distance >= 2.0",
            )
            .await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;

        insta::assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ProjectionExec: expr=[count(Int64(1))@0 as trips]
          AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
            CoalescePartitionsExec
              AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
                HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(day@0, pickup_date@0)], projection=[]
                  ProjectionExec: expr=[column1@0 as day]
                    DataSourceExec: partitions=1, partition_sizes=[1]
                  FilterExec: trip_distance@0 >= 2, projection=[pickup_date@1]
                    DataSourceExec: format=iceberg, table=default.taxi, projection=[trip_distance, pickup_date], predicate=trip_distance >= 2, dynamic_filters=DynamicFilter [ empty ]
        ");
        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +-------+
        | trips |
        +-------+
        | 11714 |
        +-------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn eager_planning_workers_fill_in_the_filter_their_scans_report() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .with_workers(2)
            .configure_session(|state| {
                state
                    .with_distributed_file_scan_config_bytes_per_partition(1)?
                    .with_distributed_dynamic_filter_collection(true)
            })?
            .build()
            .await?;

        let plan = harness
            .physical_plan(
                "SELECT COUNT(*) AS trips FROM taxi \
                 JOIN (VALUES (DATE '2024-01-10')) AS days(day) ON taxi.pickup_date = days.day \
                 WHERE taxi.trip_distance >= 2.0",
            )
            .await?;
        let task_ctx = harness.state().task_ctx();
        let batches = collect(Arc::clone(&plan), Arc::clone(&task_ctx)).await?;
        let plan = rewrite_distributed_plan_with_dynamic_filters(plan, &task_ctx).await?;

        insta::assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as trips]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=8
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(day@0, pickup_date@0)], projection=[]
          │     CoalescePartitionsExec
          │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=1, stage_partitions=2, input_tasks=1
          │     FilterExec: trip_distance@0 >= 2, projection=[pickup_date@1]
          │       DistributedLeafExec:
          │         t0: DataSourceExec: format=iceberg, planned_files=3, dynamic_filters=DynamicFilter [ pickup_date@1 >= 2024-01-10 AND pickup_date@1 <= 2024-01-10 AND pickup_date@1 IN (SET) ([2024-01-10]) ]
          │         t1: DataSourceExec: format=iceberg, planned_files=4, dynamic_filters=DynamicFilter [ pickup_date@1 >= 2024-01-10 AND pickup_date@1 <= 2024-01-10 AND pickup_date@1 IN (SET) ([2024-01-10]) ]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=1, partitions=2
            │ BroadcastExec: input_partitions=1, consumer_tasks=2, output_partitions=2
            │   ProjectionExec: expr=[column1@0 as day]
            │     DataSourceExec: partitions=1, partition_sizes=[1]
            └──────────────────────────────────────────────────
        ");
        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +-------+
        | trips |
        +-------+
        | 11714 |
        +-------+
        ");
        Ok(())
    }
}
