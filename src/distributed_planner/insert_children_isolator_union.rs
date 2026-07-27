use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;

use crate::execution_plans::ChildrenIsolatorUnionExec;

use super::DistributedConfig;

/// Replaces every [`UnionExec`] with a single-node-correct [`ChildrenIsolatorUnionExec`].
///
/// The placeholder maps every child to one default task. Network-boundary injection later
/// replaces that mapping with the final per-child task allocation.
pub(super) fn insert_children_isolator_unions(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    if !d_cfg.children_isolator_unions {
        return Ok(plan);
    }

    plan.transform_down(|node| {
        if !node.is::<UnionExec>() {
            return Ok(Transformed::no(node));
        }
        Ok(Transformed::yes(Arc::new(
            ChildrenIsolatorUnionExec::new_single_task(node.children().into_iter().cloned())?,
        )))
    })
    .map(|transformed| transformed.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_ext::DistributedExt;
    use datafusion::arrow::array::{ArrayRef, Int32Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    #[tokio::test]
    async fn inserted_union_is_single_node_correct() {
        let original = union_plan();
        let expected_schema = original.schema();
        let expected_partition_count = original.output_partitioning().partition_count();
        let expected_rows = collect_rows(Arc::clone(&original)).await;

        let plan = insert_children_isolator_unions(original, config(true).options())
            .expect("insert children isolator union");

        assert!(plan.is::<ChildrenIsolatorUnionExec>());
        assert_eq!(plan.schema(), expected_schema);
        assert_eq!(
            plan.output_partitioning().partition_count(),
            expected_partition_count
        );
        assert_eq!(collect_rows(plan).await, expected_rows);
    }

    #[test]
    fn replaces_nested_unions() {
        let inner = union_plan();
        let plan =
            UnionExec::try_new(vec![inner, memory_plan(&[5, 6])]).expect("create outer union");

        let plan = insert_children_isolator_unions(plan, config(true).options())
            .expect("insert children isolator unions");

        assert!(plan.is::<ChildrenIsolatorUnionExec>());
        assert!(plan.children()[0].is::<ChildrenIsolatorUnionExec>());
    }

    #[test]
    fn leaves_union_when_disabled() {
        let plan = insert_children_isolator_unions(union_plan(), config(false).options())
            .expect("skip children isolator union");

        assert!(plan.is::<UnionExec>());
    }

    fn union_plan() -> Arc<dyn ExecutionPlan> {
        UnionExec::try_new(vec![memory_plan(&[1, 2]), memory_plan(&[3, 4])]).expect("create union")
    }

    fn memory_plan(values: &[i32]) -> Arc<dyn ExecutionPlan> {
        let values: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        let batch = RecordBatch::try_from_iter([("value", values)]).expect("create record batch");
        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)
            .expect("create memory plan")
    }

    async fn collect_rows(plan: Arc<dyn ExecutionPlan>) -> Vec<i32> {
        let context = SessionContext::new().task_ctx();
        let mut rows = Vec::new();
        for partition in 0..plan.output_partitioning().partition_count() {
            let stream = plan
                .execute(partition, Arc::clone(&context))
                .expect("execute partition");
            let batches = collect(stream).await.expect("collect partition");
            for batch in batches {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int32 column");
                for row in 0..values.len() {
                    rows.push(values.value(row));
                }
            }
        }
        rows
    }

    fn config(children_isolator_unions: bool) -> SessionConfig {
        let mut config = SessionConfig::new();
        config.set_distributed_option_extension(DistributedConfig {
            children_isolator_unions,
            ..Default::default()
        });
        config
    }
}
