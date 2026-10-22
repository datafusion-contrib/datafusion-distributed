#[cfg(all(test, feature = "integration"))]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use datafusion::common::Result;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_distributed::{
        DistributedExt, DistributedTaskContext, RouteTaskEvent, RouteTaskEventResponse,
        RouteTaskHandler,
    };
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;
    use datafusion_distributed_iceberg::{IcebergConfig, IcebergDataSource};

    #[tokio::test]
    async fn routing_can_inspect_native_files_without_consuming_them() -> Result<()> {
        let sql =
            "SELECT pickup_date, COUNT(*) FROM taxi GROUP BY pickup_date ORDER BY pickup_date";
        let (_, expected) = IcebergTestHarness::new().await?.query(sql).await?;
        for (eager, greedy) in [(false, false), (true, false), (true, true)] {
            let routes = Routes::default();
            let harness = distributed(&routes, eager, greedy).await?;
            for _ in 0..2 {
                let (_, actual) = harness.query(sql).await?;
                assert_eq!(actual, expected);
                let records = routes.take();
                assert_eq!(records.len(), 3);
                if eager {
                    let mut files: Vec<_> =
                        records.into_iter().flat_map(|r| r.files.unwrap()).collect();
                    assert_eq!(files.len(), 7);
                    files.sort();
                    files.dedup();
                    assert_eq!(files.len(), 7);
                } else {
                    assert!(records.iter().all(|r| r.files.is_none()));
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn routing_visits_only_assigned_union_branches_with_local_task_context() -> Result<()> {
        let routes = Routes::default();
        let harness = distributed(&routes, true, true).await?;
        let sql = "WITH a AS (
            SELECT 'a' AS kind, pickup_date FROM taxi
            WHERE pickup_date = DATE '2024-01-08' AND trip_distance > 1
        ), b AS (
            SELECT 'b' AS kind, pickup_date FROM taxi
            WHERE pickup_date = DATE '2024-01-08' AND trip_distance > 2
        ) SELECT kind, pickup_date, COUNT(*) FROM (
            SELECT * FROM a UNION ALL SELECT * FROM b
        ) GROUP BY kind, pickup_date ORDER BY kind, pickup_date";
        let (_, expected) = IcebergTestHarness::new().await?.query(sql).await?;
        let (_, actual) = harness.query(sql).await?;
        assert_eq!(actual, expected);
        let records = routes.take();
        assert_eq!(records.len(), 2);
        for record in &records {
            assert_eq!(
                record.task,
                DistributedTaskContext {
                    task_index: 0,
                    task_count: 1
                }
            );
            assert_eq!(record.files.as_ref().unwrap().len(), 1);
        }
        assert_eq!(records[0].files, records[1].files);
        Ok(())
    }

    async fn distributed(routes: &Routes, eager: bool, greedy: bool) -> Result<IcebergTestHarness> {
        IcebergTestHarness::builder()
            .with_workers(3)
            .configure_session(|state| {
                let mut config = IcebergConfig::default();
                config.eager_file_planning = eager;
                config.greedy_file_assignment = greedy;
                state
                    .with_distributed_option_extension(config)
                    .with_distributed_route_task_handler(routes.clone())
                    .with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await
    }

    struct Record {
        task: DistributedTaskContext,
        files: Option<Vec<String>>,
    }

    #[derive(Clone, Default)]
    struct Routes(Arc<Mutex<Vec<Record>>>);

    impl Routes {
        fn take(&self) -> Vec<Record> {
            std::mem::take(&mut *self.0.lock().unwrap())
        }

        async fn inspect(&self, ev: RouteTaskEvent<'_>) -> Result<()> {
            // The existing routing view still describes what the worker receives.
            ev.task_specialized_plan.apply(|plan| {
                if let Some(source) = iceberg_source(plan) {
                    assert!(source.feed().inner().is_none());
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            let mut scans = Vec::new();
            ev.visit_local_task_plan(|plan, task| {
                if iceberg_source(plan).is_some() {
                    scans.push((Arc::clone(plan), task));
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            for (plan, task) in scans {
                let source = iceberg_source(&plan).unwrap();
                assert!(source.feed().inner().is_some());
                let files = source
                    .assigned_files(task, ev.task_ctx)
                    .await?
                    .map(|files| files.map(|file| file.data_file_path().to_owned()).collect());
                self.0.lock().unwrap().push(Record { task, files });
            }
            Ok(())
        }
    }

    #[async_trait]
    impl RouteTaskHandler for Routes {
        async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
            if let Err(error) = self.inspect(ev).await {
                return Some(Err(error));
            }
            // Let the built-in router execute the same task after inspection.
            None
        }
    }

    fn iceberg_source(plan: &Arc<dyn ExecutionPlan>) -> Option<&IcebergDataSource> {
        plan.downcast_ref::<DataSourceExec>()?
            .data_source()
            .downcast_ref::<IcebergDataSource>()
    }
}
