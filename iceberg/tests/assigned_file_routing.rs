mod common;

mod tests {
    use std::sync::{Arc, Mutex};

    use crate::common::{IcebergTestHarness, data_file_name};
    use async_trait::async_trait;
    use datafusion::common::Result;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion_distributed::{
        DistributedExt, RouteTaskEvent, RouteTaskEventResponse, RouteTaskHandler, ok_or_some_err,
    };
    use datafusion_distributed_iceberg::EagerIcebergDataSource;

    #[tokio::test]
    async fn routing_uses_the_files_assigned_to_each_task() -> Result<()> {
        let routes = FileRoutes::default();
        let harness = IcebergTestHarness::builder()
            .with_taxi_dataset()
            .with_workers(3)
            .with_eager_file_planning()
            .configure_session(|state| {
                state
                    .with_distributed_route_task_handler(routes.clone())
                    .with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let (plan, rows) = harness
            .query(
                "SELECT pickup_date, COUNT(*) FROM taxi GROUP BY pickup_date ORDER BY pickup_date",
            )
            .await?;
        insta::assert_snapshot!(format!("{plan}\n{}\n{rows}", routes.take()));
        Ok(())
    }

    #[tokio::test]
    async fn pruned_files_choose_the_same_worker_across_union_branches_and_queries() -> Result<()> {
        let routes = FileRoutes::default();
        let harness = IcebergTestHarness::builder()
            .with_taxi_dataset()
            .with_workers(3)
            .with_eager_file_planning()
            .configure_session(|state| {
                state
                    .with_distributed_route_task_handler(routes.clone())
                    .with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let mut output = String::new();
        for day in ["2024-01-08", "2024-01-09", "2024-01-08"] {
            let (_, rows) = harness.query(&format!(
                "SELECT kind, COUNT(*) FROM (
                    SELECT 'a' AS kind FROM taxi WHERE pickup_date = DATE '{day}' AND trip_distance > 1
                    UNION ALL
                    SELECT 'b' AS kind FROM taxi WHERE pickup_date = DATE '{day}' AND trip_distance > 2
                ) GROUP BY kind ORDER BY kind"
            )).await?;
            output.push_str(&format!("Date: {day}\n{}\n{rows}\n", routes.take()));
        }
        insta::assert_snapshot!(output);
        Ok(())
    }

    /// Example affinity policy: route tasks by their first file, named relative to the
    /// table so that routing is the same on every run.
    /// Records placement for snapshots; actual worker execution exercises the codec.
    #[derive(Clone, Default)]
    struct FileRoutes(Arc<Mutex<Vec<String>>>);

    impl FileRoutes {
        fn take(&self) -> String {
            let mut records = std::mem::take(&mut *self.0.lock().unwrap());
            records.sort();
            records.join("\n")
        }
    }

    #[async_trait]
    impl RouteTaskHandler for FileRoutes {
        async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
            let mut files = Vec::new();
            ok_or_some_err!(ev.task_specialized_plan.apply(|plan| {
                if let Some(source) = plan
                    .downcast_ref::<DataSourceExec>()
                    .and_then(|exec| exec.data_source().downcast_ref::<EagerIcebergDataSource>())
                {
                    files.extend(
                        source
                            .planned_files()
                            .map(|file| data_file_name(file.data_file_path()).to_owned()),
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            }));
            files.sort();
            let first = files.first()?;
            let urls = ok_or_some_err!(ev.worker_resolver.get_urls());
            if urls.is_empty() {
                return None;
            }
            let index = first
                .bytes()
                .fold(0usize, |sum, byte| sum.wrapping_add(usize::from(byte)))
                % urls.len();
            let response = ok_or_some_err!(ev.dialer.dial(urls[index].clone()).await);
            self.0.lock().unwrap().push(format!(
                "Task {} -> {}\n  {}",
                ev.task_key.task_number,
                response.url,
                files.join("\n  ")
            ));
            Some(Ok(response))
        }
    }
}
