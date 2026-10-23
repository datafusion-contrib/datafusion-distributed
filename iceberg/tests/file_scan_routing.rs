#[cfg(all(test, feature = "integration"))]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use datafusion::common::Result;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion_distributed::DistributedCodec;
    use datafusion_distributed::{
        DistributedExt, RouteTaskEvent, RouteTaskEventResponse, RouteTaskHandler,
    };
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;
    use datafusion_distributed_iceberg::{
        IcebergConfig, IcebergDataSource, IcebergFileSource, IcebergFileTask,
    };
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf::PhysicalPlanNode;

    #[tokio::test]
    async fn routing_inspects_assigned_files_in_the_worker_facing_plan() -> Result<()> {
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
    async fn routing_sees_only_the_assigned_union_branch() -> Result<()> {
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
            assert_eq!(record.files.as_ref().unwrap().len(), 1);
        }
        assert_eq!(records[0].files, records[1].files);
        Ok(())
    }

    #[tokio::test]
    async fn file_scan_and_decoded_scan_can_be_reset_and_read_again() -> Result<()> {
        let harness = distributed(&Routes::default(), true, true).await?;
        let scan = harness.scan().await?;
        let decoded = harness.roundtrip_plan(Arc::clone(&scan))?;
        let mut expected = None;
        for plan in [scan, decoded] {
            assert!(file_scan(&plan).is_some());
            for _ in 0..2 {
                let batches = collect(
                    Arc::clone(&plan).reset_state()?,
                    Arc::new(TaskContext::default()),
                )
                .await?;
                let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
                assert!(rows > 0);
                assert_eq!(rows, *expected.get_or_insert(rows));
            }
        }
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
        files: Option<Vec<String>>,
    }

    #[derive(Clone, Default)]
    struct Routes(Arc<Mutex<Vec<Record>>>);

    impl Routes {
        fn take(&self) -> Vec<Record> {
            std::mem::take(&mut *self.0.lock().unwrap())
        }

        fn inspect(&self, ev: RouteTaskEvent<'_>) -> Result<bool> {
            let mut found = false;
            ev.task_specialized_plan.apply(|plan| {
                let files = if let Some(scan) = file_scan(plan) {
                    let codec =
                        DistributedCodec::new_combined_with_user(ev.task_ctx.session_config());
                    let proto = PhysicalPlanNode::try_from_physical_plan(Arc::clone(plan), &codec)?;
                    let decoded = proto.try_into_physical_plan(ev.task_ctx, &codec)?;
                    let restored = file_scan(&decoded).unwrap();
                    assert_eq!(scan.file_groups.len(), restored.file_groups.len());
                    for (expected, actual) in scan.file_groups.iter().zip(&restored.file_groups) {
                        let tasks = |group: &datafusion::datasource::physical_plan::FileGroup| {
                            group
                                .iter()
                                .map(|file| {
                                    let task = file.extension::<IcebergFileTask>().unwrap().task();
                                    rmp_serde::to_vec_named(task).unwrap()
                                })
                                .collect::<Vec<_>>()
                        };
                        assert_eq!(tasks(expected), tasks(actual));
                    }
                    Some(
                        scan.file_groups
                            .iter()
                            .flat_map(|group| group.iter())
                            .map(|file| {
                                file.extension::<IcebergFileTask>()
                                    .unwrap()
                                    .task()
                                    .data_file_path()
                                    .to_owned()
                            })
                            .collect(),
                    )
                } else if let Some(source) = iceberg_source(plan) {
                    assert!(source.feed().inner().is_none());
                    None
                } else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                self.0.lock().unwrap().push(Record { files });
                found = true;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(found)
        }
    }

    #[async_trait]
    impl RouteTaskHandler for Routes {
        async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
            match self.inspect(ev) {
                Err(error) => return Some(Err(error)),
                Ok(false) => return None,
                Ok(true) => {}
            }
            let urls = match ev.worker_resolver.get_urls() {
                Ok(urls) if !urls.is_empty() => urls,
                Ok(_) => return None,
                Err(error) => return Some(Err(error)),
            };
            // Exercise the unchanged dialer with the inspected worker-facing scan.
            Some(
                ev.dialer
                    .dial(urls[ev.task_key.task_number % urls.len()].clone())
                    .await,
            )
        }
    }

    fn file_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&FileScanConfig> {
        let scan = plan
            .downcast_ref::<DataSourceExec>()?
            .data_source()
            .downcast_ref::<FileScanConfig>()?;
        scan.file_source.downcast_ref::<IcebergFileSource>()?;
        Some(scan)
    }

    fn iceberg_source(plan: &Arc<dyn ExecutionPlan>) -> Option<&IcebergDataSource> {
        plan.downcast_ref::<DataSourceExec>()?
            .data_source()
            .downcast_ref::<IcebergDataSource>()
    }
}
