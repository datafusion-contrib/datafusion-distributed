use crate::{NetworkBoundaryExt, ProducedDynamicFilter, TaskKey};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_proto::protobuf::PhysicalExprNode;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
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
    /// Latest observed producer state for each task, including incomplete states.
    pub(super) reports: HashMap<TaskKey, PhysicalExprNode>,
    /// Number of producer tasks whose latest state is complete.
    pub(super) completed_producer_count: usize,
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

    pub(crate) fn add_report(&self, task_key: TaskKey, report: ProducedDynamicFilter) {
        if report.expression.expr_id != Some(report.expression_id) {
            return;
        }
        let Some((generation, is_complete)) = report_state(&report.expression) else {
            return;
        };

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        let Some(filter) = state.filters.get_mut(&report.expression_id) else {
            return;
        };
        if filter.disabled || !filter.producer_tasks.contains(&task_key) {
            return;
        }

        let Some(previous) = filter.reports.get(&task_key) else {
            filter.completed_producer_count += usize::from(is_complete);
            filter.reports.insert(task_key, report.expression);
            return;
        };
        let Some((previous_generation, previous_is_complete)) = report_state(previous) else {
            // Only validated reports are inserted above.
            unreachable!("stored producer report must be a dynamic filter")
        };

        // A complete report is terminal. Otherwise, keep only a newer generation, except that
        // completion is allowed to replace an incomplete report at the same generation because
        // DataFusion's mark_complete() does not increment the generation.
        let advances_state = !previous_is_complete
            && (generation > previous_generation
                || (generation == previous_generation && is_complete));
        if !advances_state {
            return;
        }

        if is_complete {
            filter.completed_producer_count += 1;
        }
        filter.reports.insert(task_key, report.expression);
    }
}

fn report_state(expression: &PhysicalExprNode) -> Option<(u64, bool)> {
    let Some(ExprType::DynamicFilter(filter)) = expression.expr_type.as_ref() else {
        return None;
    };
    Some((filter.generation, filter.is_complete))
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
    use datafusion_proto::protobuf::PhysicalDynamicFilterNode;
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

    #[test]
    fn stores_latest_report_and_counts_completion_once() {
        let expression_id = 42;
        let task_key = task_key(0);
        let registry = registry_with_producer(expression_id, task_key);

        registry.add_report(task_key, report(expression_id, 1, false));
        registry.add_report(task_key, report(expression_id, 0, false));
        registry.add_report(task_key, report(expression_id, 2, false));
        {
            let state = registry.state.lock().unwrap();
            let filter = &state.filters[&expression_id];
            assert_eq!(report_state(&filter.reports[&task_key]), Some((2, false)));
            assert_eq!(filter.completed_producer_count, 0);
        }

        // Completion is a state change even though it has the same generation as the last update.
        registry.add_report(task_key, report(expression_id, 2, true));
        registry.add_report(task_key, report(expression_id, 2, true));
        registry.add_report(task_key, report(expression_id, 3, false));
        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
        assert_eq!(report_state(&filter.reports[&task_key]), Some((2, true)));
        assert_eq!(filter.completed_producer_count, 1);
    }

    #[test]
    fn ignores_reports_from_unknown_tasks_and_expression_ids() {
        let expression_id = 42;
        let producer_task = task_key(0);
        let registry = registry_with_producer(expression_id, producer_task);

        registry.add_report(task_key(1), report(expression_id, 1, true));
        registry.add_report(producer_task, report(expression_id + 1, 1, true));
        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
        assert!(filter.reports.is_empty());
        assert_eq!(filter.completed_producer_count, 0);
    }

    fn registry_with_producer(expression_id: u64, task_key: TaskKey) -> DynamicFilterRegistry {
        let registry = DynamicFilterRegistry::new();
        registry.state.lock().unwrap().filters.insert(
            expression_id,
            PlannedDynamicFilter {
                producer_tasks: HashSet::from([task_key]),
                ..Default::default()
            },
        );
        registry
    }

    fn report(expression_id: u64, generation: u64, is_complete: bool) -> ProducedDynamicFilter {
        ProducedDynamicFilter {
            expression_id,
            expression: PhysicalExprNode {
                expr_id: Some(expression_id),
                expr_type: Some(ExprType::DynamicFilter(Box::new(
                    PhysicalDynamicFilterNode {
                        generation,
                        is_complete,
                        ..Default::default()
                    },
                ))),
            },
        }
    }

    fn task_key(task_number: usize) -> TaskKey {
        TaskKey {
            query_id: Uuid::nil(),
            stage_id: 3,
            task_number,
        }
    }
}
