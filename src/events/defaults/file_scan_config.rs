use crate::events::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, ScaleUpLeafNodeEvent,
    ScaleUpLeafNodeEventResponse,
};
use crate::execution_plans::DistributedLeafExec;
use crate::{DistributedConfig, ok_or_some_err};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileGroup, FileGroupPartitioner, FileScanConfig};
use datafusion::error::Result;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlanProperties;
use std::sync::Arc;

pub(crate) fn file_scan_config_desired_task_count(
    ev: DesiredTaskCountEvent,
) -> Option<Result<DesiredTaskCountEventResponse>> {
    let cfg = ev.session_config;
    let dse: &DataSourceExec = ev.plan.downcast_ref()?;
    let file_scan: &FileScanConfig = dse.data_source().downcast_ref()?;

    if let Some(Partitioning::Range(range)) = &file_scan.output_partitioning {
        return Some(Ok(DesiredTaskCountEventResponse::desired(
            range.partition_count(),
        )));
    }

    let d_cfg = DistributedConfig::from_session_config(cfg).ok()?;

    let mut total_bytes = 0;
    for file_group in &file_scan.file_groups {
        for file in file_group.files() {
            total_bytes += file.effective_size() as usize
        }
    }

    let bytes_per_partition = d_cfg.file_scan_config_bytes_per_partition.max(1) as f64;
    let target_partitions = cfg.target_partitions().max(1) as f64;
    let task_count = total_bytes as f64 / bytes_per_partition / target_partitions;

    Some(Ok(DesiredTaskCountEventResponse::desired(task_count)))
}

pub(crate) fn file_scan_config_scale_up_leaf_node(
    ev: ScaleUpLeafNodeEvent,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let dse = ev.plan.downcast_ref::<DataSourceExec>()?;
    let file_scan = dse.data_source().downcast_ref::<FileScanConfig>()?;
    let partition_count = ev.plan.output_partitioning().partition_count();

    let rebalanced = if matches!(file_scan.output_partitioning, Some(Partitioning::Range(_))) {
        rebalance_contiguous(&file_scan.file_groups, ev.task_count)
    } else if file_scan.output_partitioning.is_some() {
        let all_partitioned_files = file_scan
            .file_groups
            .iter()
            .flat_map(|file_group| file_group.iter().cloned())
            .collect::<Vec<_>>();
        let round_robin =
            rebalance_round_robin(all_partitioned_files, partition_count * ev.task_count)
                .into_iter()
                .map(FileGroup::new)
                .collect::<Vec<_>>();
        let mut grouped = vec![vec![]; ev.task_count];
        for (i, fg) in round_robin.into_iter().enumerate() {
            grouped[i % ev.task_count].push(fg);
        }
        grouped
    } else {
        let partitioned = FileGroupPartitioner::new()
            .with_target_partitions(partition_count * ev.task_count)
            .with_repartition_file_min_size(0)
            .with_preserve_order_within_groups(!file_scan.output_ordering.is_empty())
            .repartition_file_groups(&file_scan.file_groups)
            .unwrap_or_else(|| file_scan.file_groups.clone())
            .into_iter()
            .collect::<Vec<_>>();
        let mut grouped = vec![vec![]; ev.task_count];
        for (i, fg) in partitioned.into_iter().enumerate() {
            grouped[i % ev.task_count].push(fg);
        }
        grouped
    };

    let mut file_scans = Vec::with_capacity(ev.task_count);
    for file_groups in rebalanced {
        let mut template = file_scan.clone();
        // When leaf tasks are assigned a subset of range-partitioned file groups, the local scan's
        // partition count no longer matches the full range partitioning, so advertise UnknownPartitioning
        // for the task-local scan.
        if let Some(Partitioning::Range(range)) = &file_scan.output_partitioning
            && file_groups.len() != range.partition_count()
        {
            template.output_partitioning =
                Some(Partitioning::UnknownPartitioning(file_groups.len()));
        }
        template.file_groups = file_groups;
        file_scans.push(template);
    }

    let distributed_leaf_result = DistributedLeafExec::try_new(
        Arc::clone(ev.plan),
        file_scans
            .into_iter()
            .map(|file_scan| DataSourceExec::from_data_source(file_scan) as _),
    );
    let distributed_leaf = ok_or_some_err!(distributed_leaf_result);

    Some(Ok(ScaleUpLeafNodeEventResponse::new(Arc::new(
        distributed_leaf,
    ))))
}

fn rebalance_contiguous<T: Clone>(items: &[T], target_tasks: usize) -> Vec<Vec<T>> {
    if target_tasks == 0 {
        return vec![];
    }
    let n = items.len();
    let mut result = Vec::with_capacity(target_tasks);
    for task_idx in 0..target_tasks {
        let start = (task_idx * n) / target_tasks;
        let end = ((task_idx + 1) * n) / target_tasks;
        result.push(items[start..end].to_vec());
    }
    result
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

    #[test]
    fn test_rebalance_contiguous_even_distribution() {
        let groups = rebalance_contiguous(&[0, 1, 2, 3], 2);
        assert_eq!(groups, vec![vec![0, 1], vec![2, 3]]);
    }

    #[test]
    fn test_rebalance_contiguous_one_to_one() {
        let groups = rebalance_contiguous(&[0, 1, 2, 3], 4);
        assert_eq!(groups, vec![vec![0], vec![1], vec![2], vec![3]]);
    }

    #[test]
    fn test_rebalance_contiguous_uneven_distribution() {
        let groups = rebalance_contiguous(&[0, 1, 2], 2);
        assert_eq!(groups, vec![vec![0], vec![1, 2]]);
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

    #[tokio::test]
    async fn test_scale_up_leaf_node_range_partitioning_downgrades_to_unknown()
    -> Result<(), DataFusionError> {
        use datafusion::common::ScalarValue;
        use datafusion::physical_expr::{
            LexOrdering, PhysicalSortExpr, RangePartitioning, SplitPoint,
        };

        let plan = make_data_source_exec().await?;
        let dse = plan.downcast_ref::<DataSourceExec>().unwrap();
        let file_scan = dse.data_source().downcast_ref::<FileScanConfig>().unwrap();

        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(datafusion::physical_expr::expressions::Column::new(
                "date", 0,
            )),
            Default::default(),
        )])
        .unwrap();
        let splits = vec![
            SplitPoint::new(vec![ScalarValue::Utf8(Some("2022-01-01".to_string()))]),
            SplitPoint::new(vec![ScalarValue::Utf8(Some("2022-06-01".to_string()))]),
            SplitPoint::new(vec![ScalarValue::Utf8(Some("2022-12-01".to_string()))]),
        ];
        let range = RangePartitioning::try_new_with_samples(ordering, splits, 4).unwrap();

        let mut range_file_scan = file_scan.clone();
        range_file_scan.file_groups = vec![
            FileGroup::new(vec![]),
            FileGroup::new(vec![]),
            FileGroup::new(vec![]),
            FileGroup::new(vec![]),
        ];
        range_file_scan.output_partitioning = Some(Partitioning::Range(range));
        let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(range_file_scan);

        let cfg = SessionConfig::new();
        let response = file_scan_config_scale_up_leaf_node(ScaleUpLeafNodeEvent {
            plan: &plan,
            task_count: 2,
            session_config: &cfg,
        })
        .expect("should handle file scan")?;

        let leaf = response
            .plan
            .downcast_ref::<DistributedLeafExec>()
            .expect("should be DistributedLeafExec");
        assert_eq!(leaf.variants().len(), 2);
        for input in leaf.variants() {
            let input_dse = input.downcast_ref::<DataSourceExec>().unwrap();
            let input_scan = input_dse
                .data_source()
                .downcast_ref::<FileScanConfig>()
                .unwrap();
            assert_eq!(input_scan.file_groups.len(), 2);
            assert!(
                matches!(
                    input_scan.output_partitioning,
                    Some(Partitioning::UnknownPartitioning(2))
                ),
                "expected UnknownPartitioning(2), got {:?}",
                input_scan.output_partitioning
            );
        }
        Ok(())
    }
}
