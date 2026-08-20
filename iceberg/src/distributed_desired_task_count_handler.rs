use std::num::NonZeroUsize;

use datafusion::common::{Result, config_datafusion_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion_distributed::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, DistributedConfig,
};

use crate::IcebergDataSource;

/// Estimates scan parallelism from the selected Iceberg snapshot's total file size.
pub fn iceberg_desired_task_count(
    ev: DesiredTaskCountEvent,
) -> Option<Result<DesiredTaskCountEventResponse>> {
    let node = ev
        .plan
        .downcast_ref::<DataSourceExec>()?
        .data_source()
        .downcast_ref::<IcebergDataSource>()?;
    let feed = node.feed().inner()?;
    let metadata = feed.iceberg_table.metadata();
    let snapshot = match feed.snapshot_id {
        Some(id) => Some(metadata.snapshot_by_id(id)?),
        None => metadata.current_snapshot(),
    };
    let total_bytes = match snapshot {
        Some(snapshot) => snapshot
            .summary()
            .additional_properties
            .get("total-files-size")?
            .parse()
            .ok()?,
        None => 0,
    };
    let config = DistributedConfig::from_session_config(ev.session_config).ok()?;

    Some(
        calculate_task_count(
            total_bytes,
            config.file_scan_config_bytes_per_partition,
            ev.session_config.target_partitions(),
        )
        .map(DesiredTaskCountEventResponse::desired),
    )
}

fn calculate_task_count(
    total_bytes: usize,
    bytes_per_partition: usize,
    target_partitions: usize,
) -> Result<usize> {
    let bytes_per_partition = non_zero_divisor(bytes_per_partition, "bytes per partition")?;
    let target_partitions = non_zero_divisor(target_partitions, "target partitions")?;

    Ok(total_bytes
        .div_ceil(bytes_per_partition.get())
        .div_ceil(target_partitions.get()))
}

fn non_zero_divisor(value: usize, name: &str) -> Result<NonZeroUsize> {
    NonZeroUsize::new(value)
        .ok_or_else(|| config_datafusion_err!("Iceberg {name} must be greater than zero"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionConfig;
    use datafusion_distributed::DistributedExt;

    use super::*;
    use crate::test_utils::IcebergTestHarness;

    #[test]
    fn calculates_exact_rounded_and_boundary_task_counts() {
        assert_eq!(task_count(24, 4, 3), 2);
        assert_eq!(task_count(25, 4, 3), 3);
        assert_eq!(task_count(usize::MAX, usize::MAX, 2), 1);
        assert_eq!(task_count(usize::MAX, 1, 1), usize::MAX);
    }

    #[test]
    fn zero_bytes_require_zero_tasks() {
        assert_eq!(task_count(0, 1, 1), 0);
    }

    #[test]
    fn rejects_zero_divisors() {
        assert!(calculate_task_count(1, 0, 1).is_err());
        assert!(calculate_task_count(1, 1, 0).is_err());
    }

    #[tokio::test]
    async fn estimates_desired_task_count_from_the_iceberg_plan() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = iceberg_plan(&harness.physical_plan("SELECT * FROM taxi").await?)
            .expect("fixture plan should contain an Iceberg scan");
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = 2;
        config.set_distributed_option_extension(DistributedConfig::default());
        config.set_distributed_file_scan_config_bytes_per_partition(1_000_000)?;

        let response = iceberg_desired_task_count(DesiredTaskCountEvent {
            plan: &plan,
            session_config: &config,
        })
        .expect("Iceberg handler should respond")?;

        assert_eq!(response.task_count.as_usize(), 3);
        Ok(())
    }

    fn task_count(
        total_bytes: usize,
        bytes_per_partition: usize,
        target_partitions: usize,
    ) -> usize {
        calculate_task_count(total_bytes, bytes_per_partition, target_partitions)
            .expect("test task count should be valid")
    }

    fn iceberg_plan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
            && exec
                .data_source()
                .downcast_ref::<IcebergDataSource>()
                .is_some()
        {
            return Some(Arc::clone(plan));
        }
        plan.children().into_iter().find_map(iceberg_plan)
    }
}
