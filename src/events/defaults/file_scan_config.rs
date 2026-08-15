use crate::DistributedConfig;
use crate::events::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, ScaleUpLeafNodeEvent,
    ScaleUpLeafNodeEventResponse,
};
use crate::execution_plans::DistributedLeafExec;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileGroup, FileGroupPartitioner, FileScanConfig};
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlanProperties;
use std::sync::Arc;

pub(crate) fn file_scan_config_desired_task_count(
    ev: DesiredTaskCountEvent,
) -> Option<Result<DesiredTaskCountEventResponse>> {
    let cfg = ev.session_config;
    let dse: &DataSourceExec = ev.plan.downcast_ref()?;
    let file_scan: &FileScanConfig = dse.data_source().downcast_ref()?;

    let d_cfg = DistributedConfig::from_session_config(cfg).ok()?;

    let mut total_bytes = 0;
    for file_group in &file_scan.file_groups {
        for file in file_group.files() {
            total_bytes += file.effective_size() as usize
        }
    }

    let task_count = total_bytes
        .div_ceil(d_cfg.file_scan_config_bytes_per_partition)
        .div_ceil(cfg.target_partitions());

    Some(Ok(DesiredTaskCountEventResponse::desired(task_count)))
}

pub(crate) fn file_scan_config_scale_up_leaf_node(
    ev: ScaleUpLeafNodeEvent,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let dse = ev.plan.downcast_ref::<DataSourceExec>()?;
    let file_scan = dse.data_source().downcast_ref::<FileScanConfig>()?;
    let partition_count = ev.plan.output_partitioning().partition_count();

    let file_scans = if file_scan.output_partitioning.is_some() {
        // File groups are the declared hash/range partitions. Moving a file to a
        // different group would invalidate that mapping — DataFusion's
        // FileScanConfig::repartitioned also refuses to do so. Keep group i as
        // partition i on every task, and assign each group to a single task so
        // co-partitioned joins and single-partitioned aggregates stay correct.
        scale_declared_partition_file_groups(&file_scan.file_groups, ev.task_count)
            .into_iter()
            .map(|file_groups| {
                let mut cfg = file_scan.clone();
                cfg.file_groups = file_groups;
                cfg
            })
            .collect::<Vec<_>>()
    } else {
        let rebalanced = FileGroupPartitioner::new()
            .with_target_partitions(partition_count * ev.task_count)
            .with_repartition_file_min_size(0)
            .with_preserve_order_within_groups(!file_scan.output_ordering.is_empty())
            .repartition_file_groups(&file_scan.file_groups)
            .unwrap_or_else(|| file_scan.file_groups.clone())
            .into_iter()
            .collect::<Vec<_>>();

        let mut file_scan_template = file_scan.clone();
        file_scan_template.file_groups.clear();
        let mut file_scans = vec![file_scan_template; ev.task_count];
        for (i, file_group) in rebalanced.into_iter().enumerate() {
            file_scans[i % ev.task_count].file_groups.push(file_group);
        }
        file_scans
    };

    let distributed_leaf_result = DistributedLeafExec::try_new(
        Arc::clone(ev.plan),
        file_scans
            .into_iter()
            .map(|file_scan| DataSourceExec::from_data_source(file_scan) as _),
    );
    let distributed_leaf = match distributed_leaf_result {
        Ok(distributed_leaf) => distributed_leaf,
        Err(e) => return Some(Err(e)),
    };

    Some(Ok(ScaleUpLeafNodeEventResponse::new(Arc::new(
        distributed_leaf,
    ))))
}

fn scale_declared_partition_file_groups(
    file_groups: &[FileGroup],
    task_count: usize,
) -> Vec<Vec<FileGroup>> {
    let mut per_task = vec![vec![FileGroup::new(vec![]); file_groups.len()]; task_count];
    for (part_idx, group) in file_groups.iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        per_task[part_idx % task_count][part_idx] = group.clone();
    }
    per_task
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DistributedExt;
    use crate::events::DesiredTaskCountHandlers;
    use crate::test_utils::parquet::register_parquet_tables;
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};

    #[tokio::test]
    async fn test_first_desired_task_count_handler_wins() -> Result<(), DataFusionError> {
        let cfg = SessionConfig::new()
            .with_distributed_desired_task_count_handler(desired_ten)
            .with_distributed_desired_task_count_handler(desired_twenty);

        let plan = make_data_source_exec().await?;
        let response = DesiredTaskCountHandlers::handle(DesiredTaskCountEvent {
            plan: &plan,
            session_config: &cfg,
        })
        .await
        .expect("a handler should respond")?;
        assert_eq!(response.task_count.as_usize(), 10);
        Ok(())
    }

    #[tokio::test]
    async fn test_desired_task_count_handlers_continue_until_some() -> Result<(), DataFusionError> {
        let cfg = SessionConfig::new()
            .with_distributed_desired_task_count_handler(no_desired_task_count)
            .with_distributed_desired_task_count_handler(desired_thirty);

        let plan = make_data_source_exec().await?;
        let response = DesiredTaskCountHandlers::handle(DesiredTaskCountEvent {
            plan: &plan,
            session_config: &cfg,
        })
        .await
        .expect("a handler should respond")?;
        assert_eq!(response.task_count.as_usize(), 30);
        Ok(())
    }

    #[tokio::test]
    async fn test_file_scan_config_desired_task_count_handler() -> Result<(), DataFusionError> {
        let plan = make_data_source_exec().await?;
        let bytes_per_partition = total_scan_bytes(&plan).div_ceil(3);
        let mut cfg = SessionConfig::new();
        cfg.options_mut().execution.target_partitions = 1;
        cfg.set_distributed_option_extension(DistributedConfig::default());
        cfg.set_distributed_file_scan_config_bytes_per_partition(bytes_per_partition)?;

        let response = file_scan_config_desired_task_count(DesiredTaskCountEvent {
            plan: &plan,
            session_config: &cfg,
        })
        .expect("a file scan should be recognized")?;
        assert_eq!(response.task_count.as_usize(), 3);
        Ok(())
    }

    #[test]
    fn test_scale_up_preserves_range_file_group_slots() -> Result<(), DataFusionError> {
        let plan = make_range_data_source_exec(4)?;
        let cfg = SessionConfig::new();
        let response = file_scan_config_scale_up_leaf_node(ScaleUpLeafNodeEvent {
            plan: &plan,
            task_count: 2,
            session_config: &cfg,
        })
        .expect("a file scan should be recognized")?;
        let leaf = response
            .plan
            .downcast_ref::<DistributedLeafExec>()
            .expect("scale-up should return DistributedLeafExec");

        assert_eq!(leaf.variants().len(), 2);
        let slots = variant_file_slots(leaf);
        // Each original range partition stays in its own slot and is assigned
        // to exactly one task. Flattening files and rebalancing would put
        // part-2 in slot 1 of task 0.
        assert_eq!(
            slots,
            vec![
                vec![vec!["part-0"], vec![], vec!["part-2"], vec![]],
                vec![vec![], vec!["part-1"], vec![], vec!["part-3"]],
            ]
        );
        assert!(matches!(
            leaf.variants()[0].output_partitioning(),
            datafusion::physical_expr::Partitioning::Range(range)
                if range.partition_count() == 4
        ));
        Ok(())
    }

    #[test]
    fn test_scale_up_keeps_all_files_of_a_range_partition_on_one_task()
    -> Result<(), DataFusionError> {
        let plan = make_range_data_source_exec_with_files(&[
            &["p0-a", "p0-b"],
            &["p1-a"],
            &["p2-a", "p2-b", "p2-c"],
        ])?;
        let cfg = SessionConfig::new();
        let response = file_scan_config_scale_up_leaf_node(ScaleUpLeafNodeEvent {
            plan: &plan,
            task_count: 2,
            session_config: &cfg,
        })
        .expect("a file scan should be recognized")?;
        let leaf = response
            .plan
            .downcast_ref::<DistributedLeafExec>()
            .expect("scale-up should return DistributedLeafExec");

        assert_eq!(
            variant_file_slots(leaf),
            vec![
                vec![vec!["p0-a", "p0-b"], vec![], vec!["p2-a", "p2-b", "p2-c"]],
                vec![vec![], vec!["p1-a"], vec![]],
            ]
        );
        Ok(())
    }

    fn rebalance_round_robin<T>(items: Vec<T>, target_groups: usize) -> Vec<Vec<T>> {
        let mut groups = (0..target_groups)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<T>>>();
        for (idx, item) in items.into_iter().enumerate() {
            groups[idx % target_groups].push(item);
        }
        groups
    }

    #[test]
    fn test_rebalance_round_robin_fixes_group_boundary_skew() {
        let groups = rebalance_round_robin((0..8).collect(), 5);
        assert_eq!(
            groups.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![2, 2, 2, 1, 1]
        );
    }

    #[test]
    fn test_rebalance_round_robin_pads_with_empty_groups() {
        let groups = rebalance_round_robin(vec![10, 20, 30], 5);
        assert_eq!(
            groups.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1, 1, 1, 0, 0]
        );
    }

    fn total_scan_bytes(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let dse = plan.downcast_ref::<DataSourceExec>().unwrap();
        let file_scan = dse.data_source().downcast_ref::<FileScanConfig>().unwrap();
        file_scan
            .file_groups
            .iter()
            .flat_map(|file_group| file_group.files())
            .map(|file| file.effective_size() as usize)
            .sum()
    }

    fn variant_file_slots(leaf: &DistributedLeafExec) -> Vec<Vec<Vec<String>>> {
        leaf.variants()
            .iter()
            .map(|variant| {
                let dse = variant.downcast_ref::<DataSourceExec>().unwrap();
                let file_scan = dse.data_source().downcast_ref::<FileScanConfig>().unwrap();
                file_scan
                    .file_groups
                    .iter()
                    .map(|group| {
                        group
                            .files()
                            .iter()
                            .map(|file| {
                                file.object_meta
                                    .location
                                    .filename()
                                    .unwrap_or(file.object_meta.location.as_ref())
                                    .to_string()
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect()
    }

    fn make_range_data_source_exec(
        range_partitions: usize,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let files = (0..range_partitions)
            .map(|i| vec![format!("part-{i}")])
            .collect::<Vec<_>>();
        let file_refs = files
            .iter()
            .map(|files| files.iter().map(|s| s.as_str()).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let file_refs = file_refs.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
        make_range_data_source_exec_with_files(&file_refs)
    }

    fn make_range_data_source_exec_with_files(
        files_per_partition: &[&[&str]],
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        use datafusion::common::{ScalarValue, SplitPoint};
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::physical_expr::{PhysicalSortExpr, RangePartitioning, expressions::col};
        use datafusion::physical_expr_common::sort_expr::LexOrdering;

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int32, false),
        ]));
        let file_groups = files_per_partition
            .iter()
            .map(|files| {
                FileGroup::new(
                    files
                        .iter()
                        .map(|name| PartitionedFile::new(name.to_string(), 1024))
                        .collect(),
                )
            })
            .collect::<Vec<_>>();
        let split_points = (1..files_per_partition.len())
            .map(|i| SplitPoint::new(vec![ScalarValue::Int32(Some(i as i32 * 10))]))
            .collect::<Vec<_>>();
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(col("k", schema.as_ref())?)]);
        let Some(ordering) = ordering else {
            return Err(DataFusionError::Internal(
                "range ordering must not be empty".to_string(),
            ));
        };
        let range = RangePartitioning::try_new(ordering, split_points)?;
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(Arc::clone(&schema))),
        )
        .with_file_groups(file_groups)
        .with_output_partitioning(Some(datafusion::physical_expr::Partitioning::Range(range)))
        .build();
        Ok(DataSourceExec::from_data_source(config))
    }

    async fn make_data_source_exec() -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let ctx = SessionContext::new();
        register_parquet_tables(&ctx).await?;
        let mut plan = ctx
            .sql("SELECT * FROM weather")
            .await?
            .create_physical_plan()
            .await?;
        while !plan.children().is_empty() {
            plan = Arc::clone(plan.children()[0]);
        }
        Ok(plan)
    }

    fn desired_ten(_: DesiredTaskCountEvent) -> Option<Result<DesiredTaskCountEventResponse>> {
        Some(Ok(DesiredTaskCountEventResponse::desired(10)))
    }

    fn desired_twenty(_: DesiredTaskCountEvent) -> Option<Result<DesiredTaskCountEventResponse>> {
        Some(Ok(DesiredTaskCountEventResponse::desired(20)))
    }

    fn no_desired_task_count(
        _: DesiredTaskCountEvent,
    ) -> Option<Result<DesiredTaskCountEventResponse>> {
        None
    }

    fn desired_thirty(_: DesiredTaskCountEvent) -> Option<Result<DesiredTaskCountEventResponse>> {
        Some(Ok(DesiredTaskCountEventResponse::desired(30)))
    }
}
