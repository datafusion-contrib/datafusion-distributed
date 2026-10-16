//! Reproduces a left-deep, five-stage join with two Iceberg scans and a Parquet probe.
//!
//! Taxi fixture columns stand in for graph keys: vendor_id = org_id, pickup_at =
//! instance key, dropoff_location_id = destination/target key, pickup_location_id =
//! relationship source key. trip_distance is the selective instance attribute.
//! Stage task counts are 1 / 17 / 8 / 6 / 8, with 16 partitions per join task.
//!
//! Run the known-failing case explicitly:
//! `cargo test -p datafusion-distributed-iceberg --features integration
//! --test hash_join_buffering unbuffered_probe_exhaustion -- --ignored --nocapture`
#[cfg(all(test, feature = "integration"))]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::common::config::{ConfigNonZeroUsize, ConfigOptions};
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::common::{Result, exec_datafusion_err};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::ParquetReadOptions;
    use datafusion_distributed::{
        DesiredTaskCountEvent, DesiredTaskCountEventResponse, DistributedExt,
        DistributedMetricsFormat, display_plan_ascii, rewrite_distributed_plan_with_metrics,
    };
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    const QUERY: &str = "
        SELECT instances.pickup_at AS instance_key,
               edges.dropoff_location_id AS role_key,
               relationships.dropoff_location_id AS target_key
        FROM taxi edges
        JOIN taxi instances
          ON edges.vendor_id = instances.vendor_id
         AND edges.pickup_at = instances.pickup_at
        JOIN relationships
          ON edges.vendor_id = relationships.vendor_id
         AND edges.dropoff_location_id = relationships.pickup_location_id
        WHERE (edges.vendor_id = 1 OR edges.vendor_id = 2)
          AND instances.vendor_id = 2
          AND instances.trip_distance = 6.69
          AND instances.dropoff_at = TIMESTAMP '2024-01-12 06:43:35'
          AND relationships.passenger_count IN (1, 2, 3)
        LIMIT 111";

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_node_control() -> Result<()> {
        reproduce(0, false, 64 * 1024 * 1024).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn buffered_control() -> Result<()> {
        reproduce(1, true, 64 * 1024 * 1024).await
    }

    // Kept manual until fixed: the progress assertion should fail on a stalled execution.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "reproduces a shared connection budget deadlock with unpolled probe partitions"]
    async fn unbuffered_probe_exhaustion() -> Result<()> {
        reproduce(0, true, 4096).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "a one-byte probe buffer also stalls under the constrained network budget"]
    async fn buffered_probe_exhaustion() -> Result<()> {
        reproduce(1, true, 4096).await
    }

    async fn reproduce(capacity: usize, distributed: bool, network_budget: usize) -> Result<()> {
        let mut builder = IcebergTestHarness::builder()
            .configure_session(|state| configure(state, capacity, network_budget))?;
        if distributed {
            builder = builder.with_localhost_workers(8);
        }
        let harness = builder.build().await?;
        let path = format!(
            "{}/../testdata/iceberg/taxi/data/",
            env!("CARGO_MANIFEST_DIR")
        );
        harness
            .context()
            .register_parquet("relationships", &path, ParquetReadOptions::default())
            .await?;
        let plan = harness.physical_plan(QUERY).await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        eprintln!(
            "buffering={capacity}, distributed={distributed}, network_budget={network_budget}\n{display}"
        );
        assert_eq!(display.matches("HashJoinExec: mode=Partitioned").count(), 2);
        assert_eq!(display.matches("format=iceberg").count(), 2);
        assert_eq!(
            display.matches("=> NetworkShuffleExec").count(),
            if distributed { 4 } else { 0 }
        );
        assert_eq!(
            display.matches("BufferExec:").count(),
            if capacity == 0 { 0 } else { 2 }
        );
        if distributed {
            for (stage, tasks, partitions) in [
                (1, 1, 128),
                (2, 17, 128),
                (3, 8, 128),
                (4, 6, 128),
                (5, 8, 16),
            ] {
                assert!(
                    display.contains(&format!(
                        "Stage {stage} ── tasks={tasks}, partitions={partitions}"
                    )),
                    "{display}"
                );
            }
        }
        // Check both composite keys and build/probe orientation, not just the number of joins.
        assert!(display.contains("on=[(vendor_id@0, vendor_id@0), (pickup_at@1, pickup_at@1)]"));
        assert!(display.contains(
            "on=[(vendor_id@0, vendor_id@0), (dropoff_location_id@1, pickup_location_id@1)]"
        ));
        let results = tokio::time::timeout(
            Duration::from_secs(30),
            collect(plan.clone(), harness.context().task_ctx()),
        )
        .await
        .map_err(|_| {
            exec_datafusion_err!(
                "Join stalled with buffering={capacity}:\n{}",
                display_plan_ascii(plan.as_ref(), true)
            )
        })??;
        assert_eq!(
            results.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1 // Below LIMIT 111: successful execution must exhaust the probe.
        );
        let plan = tokio::time::timeout(
            Duration::from_secs(10),
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated),
        )
        .await
        .map_err(|_| exec_datafusion_err!("Timed out collecting worker metrics"))??;
        eprintln!(
            "Completed buffering={capacity}, distributed={distributed}, network_budget={network_budget}\n{}",
            display_plan_ascii(plan.as_ref(), true)
        );
        Ok(())
    }

    fn configure(
        mut state: SessionStateBuilder,
        capacity: usize,
        network_budget: usize,
    ) -> Result<SessionStateBuilder> {
        let options = state.config().get_or_insert_default().options_mut();
        options.execution.target_partitions = 16;
        options.execution.batch_size = ConfigNonZeroUsize::try_new(256)?;
        options.execution.hash_join_buffering_capacity = capacity;
        // The reported plan has no join dynamic filter on its Parquet probe.
        options.optimizer.enable_join_dynamic_filter_pushdown = false;
        // Preserve the reported build/probe orientation, independent of fixture statistics.
        let defaults = SessionStateBuilder::new().with_default_features().build();
        let rules = defaults
            .physical_optimizers()
            .iter()
            .map(|rule| {
                if rule.name() == "join_selection" {
                    Arc::new(PreserveJoinOrder) as Arc<dyn PhysicalOptimizerRule + Send + Sync>
                } else {
                    rule.clone()
                }
            })
            .collect();
        state
            .with_physical_optimizer_rules(rules)
            .with_distributed_max_tasks_per_stage(17)?
            // Preserve the direct shuffle shape instead of the newer two-phase default.
            .with_distributed_two_step_shuffle_fanout_threshold(usize::MAX)?
            // Force backpressure with the small checked-in fixture. This is not a
            // claim that the production query stalls at its default 64 MiB budget.
            .with_distributed_worker_connection_buffer_budget_bytes(network_budget)?
            .with_distributed_desired_task_count_handler(|ev: DesiredTaskCountEvent<'_>| {
                let response = if ev.plan.is::<HashJoinExec>() {
                    DesiredTaskCountEventResponse::maximum(8)
                } else if ev.plan.is::<DataSourceExec>() {
                    let schema = ev.plan.schema();
                    let tasks = if schema.index_of("trip_distance").is_ok() {
                        17
                    } else if schema.index_of("passenger_count").is_ok() {
                        6
                    } else {
                        1
                    };
                    DesiredTaskCountEventResponse::desired(tasks)
                } else {
                    return None;
                };
                Some(Ok(response))
            })
            .with_distributed_broadcast_joins(false)
    }

    #[derive(Debug)]
    struct PreserveJoinOrder;

    impl PhysicalOptimizerRule for PreserveJoinOrder {
        fn optimize(
            &self,
            plan: Arc<dyn ExecutionPlan>,
            _: &ConfigOptions,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(plan
                .transform_up(|plan| {
                    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
                        Ok(Transformed::yes(Arc::new(
                            join.builder()
                                .with_partition_mode(PartitionMode::Partitioned)
                                .build()?,
                        )
                            as Arc<dyn ExecutionPlan>))
                    } else {
                        Ok(Transformed::no(plan))
                    }
                })?
                .data)
        }
        fn name(&self) -> &str {
            "preserve_join_order"
        }
        fn schema_check(&self) -> bool {
            true
        }
    }
}
