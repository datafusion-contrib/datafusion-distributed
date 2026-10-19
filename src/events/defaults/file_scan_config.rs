use crate::events::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, ScaleUpLeafNodeEvent,
    ScaleUpLeafNodeEventResponse,
};
use crate::execution_plans::DistributedLeafExec;
use crate::{DistributedConfig, ok_or_some_err};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::plan_err;
use datafusion::datasource::physical_plan::{FileGroup, FileGroupPartitioner, FileScanConfig};
use datafusion::datasource::source::DataSource;
use datafusion::error::Result;
use datafusion::physical_expr::{Partitioning, RangePartitioning};
use datafusion::physical_plan::ExecutionPlanProperties;
use std::sync::Arc;

pub(crate) fn file_scan_config_desired_task_count(
    ev: DesiredTaskCountEvent,
) -> Option<Result<DesiredTaskCountEventResponse>> {
    let cfg = ev.session_config;
    let dse: &DataSourceExec = ev.plan.downcast_ref()?;
    let file_scan: &FileScanConfig = dse.data_source().downcast_ref()?;

    let target_partitions = cfg.target_partitions().max(1) as f64;
    if is_pre_partitioned(file_scan) {
        let task_count = ev.plan.output_partitioning().partition_count() as f64 / target_partitions;
        return Some(Ok(DesiredTaskCountEventResponse::desired(task_count)));
    }

    let d_cfg = DistributedConfig::from_session_config(cfg).ok()?;

    let mut total_bytes = 0;
    for file_group in &file_scan.file_groups {
        for file in file_group.files() {
            total_bytes += file.effective_size() as usize
        }
    }

    let bytes_per_partition = d_cfg.file_scan_config_bytes_per_partition.max(1) as f64;
    let task_count = total_bytes as f64 / bytes_per_partition / target_partitions;

    Some(Ok(DesiredTaskCountEventResponse::desired(task_count)))
}

pub(crate) fn file_scan_config_scale_up_leaf_node(
    ev: ScaleUpLeafNodeEvent,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let dse = ev.plan.downcast_ref::<DataSourceExec>()?;
    let file_scan = dse.data_source().downcast_ref::<FileScanConfig>()?;

    let partition_count = ev.plan.output_partitioning().partition_count();

    let is_sorted = file_scan.eq_properties().output_ordering().is_some();
    let is_pre_partitioned = is_pre_partitioned(file_scan);
    let supports_byte_range_partitioning = !file_scan.file_compression_type.is_compressed()
        && file_scan.file_source.supports_repartitioning();
    let supports_repartitioning = supports_byte_range_partitioning && !is_pre_partitioned;

    let mut file_scan_template = file_scan.clone();
    file_scan_template.file_groups.clear();
    if !is_sorted {
        // The original scan may declare per-file ordering that cannot be guaranteed
        // across every file group, so its output exposes no ordering.
        //
        // After repartitioning, some scans may regain that ordering because
        // their new groups happen to be ordered, but we need all scans to maintain
        // the same ordering, so we clear it here.
        file_scan_template.output_ordering.clear();
    }
    let mut file_scans = vec![file_scan_template; ev.task_count];

    match (is_sorted, supports_repartitioning) {
        (_, false) if is_pre_partitioned => {
            // Assign contiguous partition ranges to tasks. Do not split, merge, or re-arrange
            // partitions. If a user configured their data source to be partitioned, then respect
            // that configuration.
            let partitions_per_task = partition_count / ev.task_count;
            let extra_partitions = partition_count % ev.task_count;
            let mut start = 0;
            for (task, scan) in file_scans.iter_mut().enumerate() {
                let end = start + partitions_per_task + usize::from(task < extra_partitions);
                let (offset, count) =
                    if let Some(Partitioning::Range(range)) = &file_scan.output_partitioning {
                        // Sometimes a split point must be re-used.
                        //
                        // For example, imagine we have 13 partitions distributed across
                        // 4 tasks with 4 partitiosn each. The partitions are A–M,
                        // with boundaries 10, 20, …, 120.
                        //
                        // Task 0: [ A |10| B |20| C |30| D ]     splits: [10, 20, 30]
                        // Task 1: [ E |50| F |60| G |70| - ]     splits: [50, 60, 70]
                        // Task 2: [ H |80| I |90| J |100| - ]    splits: [80, 90, 100]
                        // Task 3: [ - |100| K |110| L |120| M ]  splits: [100, 110, 120]
                        //
                        // The split point 100 is re-used.
                        let count = partition_count.div_ceil(ev.task_count);
                        let offset = start.min(partition_count - count);
                        scan.output_partitioning = Some(Partitioning::Range(ok_or_some_err!(
                            RangePartitioning::try_new(
                                range.ordering().clone(),
                                range.split_points()[offset..offset + count - 1].to_vec(),
                            )
                        )));
                        (offset, count)
                    } else {
                        // Hash partition IDs must retain the original modulus.
                        (0, partition_count)
                    };
                scan.file_groups.resize(count, FileGroup::default());
                scan.file_groups[start - offset..end - offset]
                    .clone_from_slice(&file_scan.file_groups[start..end]);
                start = end;
            }
        }
        // does not support repartitioning
        (_, false) => {
            let all_partitioned_files = file_scan
                .file_groups
                .iter()
                .flat_map(|file_group| file_group.iter().cloned())
                .collect::<Vec<_>>();
            let rebalanced =
                rebalance_round_robin(all_partitioned_files, partition_count * ev.task_count)
                    .into_iter()
                    .map(FileGroup::new)
                    .collect::<Vec<_>>();
            for (i, file_group) in rebalanced.into_iter().enumerate() {
                file_scans[i % ev.task_count].file_groups.push(file_group);
            }
        }
        // sorted and supports repartitioning
        (true, true) => {
            // Preserve each input group's order while splitting its files and byte ranges
            // across every task. Each task receives one fragment of every original group.
            for file_group in &file_scan.file_groups {
                let mut group_config = file_scan.clone();
                group_config.file_groups = vec![file_group.clone()];
                // The source decides whether byte-range splitting is supported (e.g.
                // CSV with embedded newlines must remain intact). With one input group,
                // the default splitter keeps the sequence of files and ranges intact.
                let repartitioned = ok_or_some_err!(file_scan.file_source.repartitioned(
                    ev.task_count,
                    0,
                    None,
                    &group_config,
                ));
                let mut fragments = repartitioned
                    .map(|config| config.file_groups)
                    .unwrap_or_else(|| vec![file_group.clone()]);
                if fragments.len() > ev.task_count {
                    return Some(plan_err!(
                        "File source returned {} groups for {} requested partitions",
                        fragments.len(),
                        ev.task_count
                    ));
                }
                fragments.resize(ev.task_count, FileGroup::default());
                for (file_scan, fragment) in file_scans.iter_mut().zip(fragments) {
                    file_scan.file_groups.push(fragment);
                }
            }
        }
        // not sorted and supports repartitioning
        (false, true) => {
            // The source may refuse byte splitting, e.g. CSV with embedded newlines.
            let rebalanced = FileGroupPartitioner::new()
                .with_target_partitions(partition_count * ev.task_count)
                .with_repartition_file_min_size(0)
                .repartition_file_groups(&file_scan.file_groups)
                .unwrap_or_else(|| file_scan.file_groups.clone());

            for (i, file_group) in rebalanced.into_iter().enumerate() {
                file_scans[i % ev.task_count].file_groups.push(file_group);
            }
        }
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

fn is_pre_partitioned(file_scan: &FileScanConfig) -> bool {
    matches!(
        file_scan.output_partitioning(),
        Partitioning::Hash(_, _) | Partitioning::Range(_)
    )
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
}
