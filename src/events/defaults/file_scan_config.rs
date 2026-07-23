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
) -> Option<DesiredTaskCountEventResponse> {
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

    Some(DesiredTaskCountEventResponse::desired(task_count))
}

pub(crate) fn file_scan_config_scale_up_leaf_node(
    ev: ScaleUpLeafNodeEvent,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let dse = ev.plan.downcast_ref::<DataSourceExec>()?;
    let file_scan = dse.data_source().downcast_ref::<FileScanConfig>()?;
    let partition_count = ev.plan.output_partitioning().partition_count();

    let rebalanced = if file_scan.partitioned_by_file_group {
        let all_partitioned_files = file_scan
            .file_groups
            .iter()
            .flat_map(|file_group| file_group.iter().cloned())
            .collect::<Vec<_>>();
        rebalance_round_robin(all_partitioned_files, partition_count * ev.task_count)
            .into_iter()
            .map(FileGroup::new)
            .collect::<Vec<_>>()
    } else {
        FileGroupPartitioner::new()
            .with_target_partitions(partition_count * ev.task_count)
            .with_repartition_file_min_size(0)
            .with_preserve_order_within_groups(!file_scan.output_ordering.is_empty())
            .repartition_file_groups(&file_scan.file_groups)
            .unwrap_or_else(|| file_scan.file_groups.clone())
            .into_iter()
            .collect()
    };

    let mut file_scan_template = file_scan.clone();
    file_scan_template.file_groups.clear();
    let mut file_scans = vec![file_scan_template; ev.task_count];
    for (i, file_group) in rebalanced.into_iter().enumerate() {
        file_scans[i % ev.task_count].file_groups.push(file_group);
    }

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
    async fn test_most_recent_desired_task_count_handler_wins() -> Result<(), DataFusionError> {
        let cfg = SessionConfig::new()
            .with_distributed_desired_task_count_handler(desired_ten)
            .with_distributed_desired_task_count_handler(desired_twenty);

        let plan = make_data_source_exec().await?;
        let response = DesiredTaskCountHandlers::handle(DesiredTaskCountEvent {
            plan: &plan,
            session_config: &cfg,
        })
        .expect("a handler should respond");
        assert_eq!(response.task_count.as_usize(), 20);
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
        .expect("a handler should respond");
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
        .expect("a file scan should be recognized");
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

    fn desired_ten(_: DesiredTaskCountEvent) -> Option<DesiredTaskCountEventResponse> {
        Some(DesiredTaskCountEventResponse::desired(10))
    }

    fn desired_twenty(_: DesiredTaskCountEvent) -> Option<DesiredTaskCountEventResponse> {
        Some(DesiredTaskCountEventResponse::desired(20))
    }

    fn no_desired_task_count(_: DesiredTaskCountEvent) -> Option<DesiredTaskCountEventResponse> {
        None
    }

    fn desired_thirty(_: DesiredTaskCountEvent) -> Option<DesiredTaskCountEventResponse> {
        Some(DesiredTaskCountEventResponse::desired(30))
    }
}
