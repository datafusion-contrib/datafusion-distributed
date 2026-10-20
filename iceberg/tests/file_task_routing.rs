#[cfg(all(test, feature = "integration"))]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use datafusion::common::Result;
    use datafusion_distributed::{
        AffinityRouteTaskHandler, DistributedExt, RouteTaskEvent, RouteTaskEventResponse,
        RouteTaskHandler,
    };
    use datafusion_distributed_iceberg::{IcebergConfig, test_utils::IcebergTestHarness};

    #[tokio::test]
    async fn repeated_queries_keep_file_assignments_and_worker_homes() -> Result<()> {
        let routes = Routes::default();
        let harness = distributed(&routes, true).await?;
        let sql =
            "SELECT pickup_date, COUNT(*) FROM taxi GROUP BY pickup_date ORDER BY pickup_date";
        let (_, expected) = IcebergTestHarness::new().await?.query(sql).await?;
        let (_, actual) = harness.query(sql).await?;
        assert_eq!(actual, expected);
        let first = routes.take();
        assert_eq!(first.len(), 3);
        let mut files: Vec<_> = first.iter().flat_map(|r| r.files.clone()).collect();
        assert_eq!(files.len(), 7);
        files.sort();
        files.dedup();
        assert_eq!(files.len(), 7, "each file must be assigned exactly once");
        let (_, again) = harness.query(sql).await?;
        assert_eq!(again, expected);
        assert_eq!(first, routes.take(), "query IDs must not change homes");
        Ok(())
    }

    #[tokio::test]
    async fn isolated_union_scans_of_the_same_file_choose_the_same_worker() -> Result<()> {
        let routes = Routes::default();
        let harness = distributed(&routes, true).await?;
        let sql = "WITH a AS (
                SELECT 'a' AS kind, pickup_date FROM taxi
                WHERE pickup_date = DATE '2024-01-08' AND trip_distance > 1
            ), b AS (
                SELECT 'b' AS kind, pickup_date FROM taxi
                WHERE pickup_date = DATE '2024-01-08' AND trip_distance > 2
            )
            SELECT kind, pickup_date, COUNT(*) FROM (
                SELECT * FROM a UNION ALL SELECT * FROM b
            ) GROUP BY kind, pickup_date ORDER BY kind, pickup_date";
        let (_, expected) = IcebergTestHarness::new().await?.query(sql).await?;
        let (plan, actual) = harness.query_with_metrics(sql).await?;
        assert_eq!(actual, expected);
        let records = routes.take();
        assert_eq!(records.len(), 2, "{plan}");
        assert_eq!(records[0].files.len(), 1);
        assert_eq!(records[0].files, records[1].files);
        assert_eq!(records[0].worker, records[1].worker);
        assert!(plan.contains("work_unit_affinity_routed_tasks=2"), "{plan}");
        assert!(
            plan.contains("work_unit_affinity_preferred_placements=2"),
            "{plan}"
        );
        let scans: Vec<_> = plan
            .lines()
            .filter(|line| line.contains("format=iceberg"))
            .collect();
        assert_eq!(scans.len(), 2, "{plan}");
        for scan in scans {
            assert!(scan.contains("output_rows="), "{scan}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn affinity_also_works_with_round_robin_and_is_absent_when_disabled() -> Result<()> {
        let routes = Routes::default();
        let harness = distributed(&routes, false).await?;
        // A column-dependent aggregate prevents exact-row-count scan elimination.
        harness
            .query("SELECT pickup_date, COUNT(*) FROM taxi GROUP BY pickup_date")
            .await?;
        assert_eq!(
            routes.take().iter().map(|r| r.files.len()).sum::<usize>(),
            7
        );
        let harness = IcebergTestHarness::builder()
            .with_workers(2)
            .configure_session(|state| {
                Ok(state.with_distributed_route_task_handler(routes.clone()))
            })?
            .build()
            .await?;
        let (plan, _) = harness
            .query("SELECT pickup_date, COUNT(*) FROM taxi GROUP BY pickup_date")
            .await?;
        assert!(plan.contains("format=iceberg"), "{plan}");
        assert!(routes.take().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn routing_options_require_planning_time_file_discovery() -> Result<()> {
        for (greedy_file_assignment, file_task_affinity) in [(true, false), (false, true)] {
            let harness = IcebergTestHarness::builder()
                .configure_session(|state| {
                    let mut config = IcebergConfig::default();
                    config.plan_files = false;
                    config.greedy_file_assignment = greedy_file_assignment;
                    config.file_task_affinity = file_task_affinity;
                    Ok(state.with_distributed_option_extension(config))
                })?
                .build()
                .await?;
            let error = harness
                .physical_plan("SELECT * FROM taxi")
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("requires iceberg.plan_files=true"),
                "{error}"
            );
        }
        Ok(())
    }

    async fn distributed(routes: &Routes, greedy: bool) -> Result<IcebergTestHarness> {
        IcebergTestHarness::builder()
            .with_workers(3)
            .configure_session(|state| {
                let mut config = IcebergConfig::default();
                config.plan_files = true;
                config.greedy_file_assignment = greedy;
                config.file_task_affinity = true;
                state
                    .with_distributed_option_extension(config)
                    .with_distributed_route_task_handler(routes.clone())
                    .with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct Route {
        task: usize,
        files: Vec<String>,
        worker: String,
    }

    #[derive(Clone, Default)]
    struct Routes(Arc<Mutex<Vec<Route>>>);

    impl Routes {
        fn take(&self) -> Vec<Route> {
            let mut records = std::mem::take(&mut *self.0.lock().unwrap());
            records.sort();
            records
        }
    }

    #[async_trait]
    impl RouteTaskHandler for Routes {
        async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
            let hints = ev.work_unit_affinity();
            let result = AffinityRouteTaskHandler.handle(ev).await?;
            if let Ok(response) = &result {
                let mut files: Vec<_> = hints.iter().map(|h| h.key.clone()).collect();
                files.sort();
                self.0.lock().unwrap().push(Route {
                    task: ev.task_key.task_number,
                    files,
                    worker: response.url.to_string(),
                });
            }
            Some(result)
        }
    }
}
