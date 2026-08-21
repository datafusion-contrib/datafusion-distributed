use crate::{NetworkBoundaryExt, TaskKey};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PlannedHashJoinMode {
    CollectLeft,
    Partitioned,
}

#[derive(Default)]
pub(super) struct PlannedDynamicFilter {
    pub(super) mode: Option<PlannedHashJoinMode>,
    pub(super) producer_schema: Option<SchemaRef>,
    pub(super) producer_tasks: HashSet<TaskKey>,
    pub(super) consumer_tasks: HashSet<TaskKey>,
    pub(super) producer_stages: HashSet<usize>,
    pub(super) disabled: bool,
}

#[derive(Default)]
pub(super) struct DynamicFilterRegistryState {
    pub(super) filters: HashMap<u64, PlannedDynamicFilter>,
    pub(super) sealed_stages: HashSet<usize>,
}

/// Query-scoped runtime topology for distributed hash-join dynamic filters.
///
/// This is intentionally independent from the completed-consumer reports used to render plans.
#[derive(Default)]
pub(crate) struct DynamicFilterRegistry {
    pub(super) state: Mutex<DynamicFilterRegistryState>,
}

impl DynamicFilterRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register_task(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_key: TaskKey,
    ) -> Result<()> {
        let mut producers = vec![];
        let mut consumers = HashSet::new();

        plan.apply(|node| {
            if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                let mode = match join.partition_mode() {
                    PartitionMode::CollectLeft => Some(PlannedHashJoinMode::CollectLeft),
                    PartitionMode::Partitioned => Some(PlannedHashJoinMode::Partitioned),
                    PartitionMode::Auto => None,
                };
                if let Some(mode) = mode {
                    for expression in node.dynamic_expressions_produced() {
                        if expression
                            .downcast_ref::<DynamicFilterPhysicalExpr>()
                            .is_some()
                        {
                            producers.push((
                                expression.expression_id().expect(
                                    "DynamicFilterPhysicalExpr always has an expression ID",
                                ),
                                mode,
                                join.right().schema(),
                            ));
                        }
                    }
                }
            }

            // Network-boundary expressions are visibility anchors, not destinations.
            if node.is_network_boundary() {
                return Ok(TreeNodeRecursion::Continue);
            }
            let produced = node.dynamic_expressions_produced();
            node.apply_expressions(&mut |root| {
                root.apply(|expression| {
                    if expression
                        .downcast_ref::<DynamicFilterPhysicalExpr>()
                        .is_some()
                        && !produced
                            .iter()
                            .any(|produced| Arc::ptr_eq(produced, expression))
                    {
                        consumers.insert(
                            expression
                                .expression_id()
                                .expect("DynamicFilterPhysicalExpr always has an expression ID"),
                        );
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        for (id, mode, producer_schema) in producers {
            let filter = state.filters.entry(id).or_default();
            if filter.mode.is_some_and(|existing| existing != mode)
                || filter
                    .producer_schema
                    .as_ref()
                    .is_some_and(|existing| existing.as_ref() != producer_schema.as_ref())
            {
                filter.disabled = true;
                continue;
            }
            filter.mode = Some(mode);
            filter.producer_schema = Some(producer_schema);
            filter.producer_tasks.insert(task_key);
            filter.producer_stages.insert(task_key.stage_id);
        }
        for id in consumers {
            state
                .filters
                .entry(id)
                .or_default()
                .consumer_tasks
                .insert(task_key);
        }
        Ok(())
    }

    pub(crate) fn seal_stage(&self, stage_id: usize) {
        self.state
            .lock()
            .expect("dynamic filter registry poisoned")
            .sealed_stages
            .insert(stage_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use uuid::Uuid;

    #[test]
    fn registers_task_roles_and_seals_stage() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let left = Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let probe = Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        let id = dynamic_filter.expression_id().unwrap();
        let right =
            Arc::new(FilterExec::try_new(dynamic_filter.clone(), probe)?) as Arc<dyn ExecutionPlan>;
        let join = HashJoinExec::try_new(
            left,
            right,
            vec![(Arc::new(Column::new("a", 0)), Arc::new(Column::new("a", 0)))],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?
        .with_dynamic_filter_expr(dynamic_filter)?;
        let plan = Arc::new(join) as Arc<dyn ExecutionPlan>;
        let task_key = TaskKey {
            query_id: Uuid::nil(),
            stage_id: 3,
            task_number: 1,
        };

        let registry = DynamicFilterRegistry::new();
        registry.register_task(&plan, task_key)?;
        registry.seal_stage(3);

        let state = registry.state.lock().unwrap();
        let filter = state.filters.get(&id).unwrap();
        assert_eq!(filter.mode, Some(PlannedHashJoinMode::Partitioned));
        assert_eq!(filter.producer_tasks, HashSet::from([task_key]));
        assert_eq!(filter.consumer_tasks, HashSet::from([task_key]));
        assert!(state.sealed_stages.contains(&3));
        Ok(())
    }
}
