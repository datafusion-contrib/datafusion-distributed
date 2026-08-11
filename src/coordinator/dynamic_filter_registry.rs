use crate::{NetworkBoundaryExt, ProducedDynamicFilter, TaskKey};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, internal_err};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use datafusion_proto::protobuf::{PhysicalBinaryExprNode, PhysicalExprNode};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DynamicFilterMergeMode {
    /// Wait for every planned producer task to report a complete filter, then merge them.
    AllProducersComplete,
    /// Use the first complete producer report.
    FirstProducerComplete,
}

#[derive(Default)]
pub(super) struct PlannedDynamicFilter {
    pub(super) merge_mode: Option<DynamicFilterMergeMode>,
    pub(super) producer_tasks: HashSet<TaskKey>,
    pub(super) consumer_tasks: HashSet<TaskKey>,
    /// Latest observed producer state for each task, including incomplete states.
    pub(super) reports: HashMap<TaskKey, PhysicalExprNode>,
    /// Number of producer tasks whose latest state is complete.
    pub(super) completed_producer_count: usize,
    pub(super) merged: Option<PhysicalExprNode>,
    pub(super) disabled: bool,
}

#[derive(Default)]
pub(super) struct DynamicFilterRegistryState {
    pub(super) filters: HashMap<u64, PlannedDynamicFilter>,
    pub(super) sealed_stages: HashSet<usize>,
}

/// Query-scoped runtime topology and producer state for distributed dynamic filters.
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
            // A CollectLeft hash join broadcasts an equivalent build side to every producer task,
            // so one complete replica is sufficient. This is deliberately the only producer-type
            // optimization; all other current and future producers use the conservative mode.
            let merge_mode = if node
                .downcast_ref::<HashJoinExec>()
                .is_some_and(|join| matches!(join.partition_mode(), PartitionMode::CollectLeft))
            {
                DynamicFilterMergeMode::FirstProducerComplete
            } else {
                DynamicFilterMergeMode::AllProducersComplete
            };
            let produced_ids: HashSet<_> = node
                .dynamic_expressions_produced()
                .into_iter()
                .filter_map(|expression| {
                    expression
                        .downcast_ref::<DynamicFilterPhysicalExpr>()
                        .map(|_| expression.expression_id())
                })
                .map(|id| match id {
                    Some(id) => Ok(id),
                    None => {
                        internal_err!("DynamicFilterPhysicalExpr did not have an expression ID")
                    }
                })
                .collect::<Result<_>>()?;
            producers.extend(produced_ids.iter().map(|id| (*id, merge_mode)));

            // Network-boundary expressions preserve visibility across stages. They are anchors,
            // not consumers which can receive a dynamic-filter update.
            if !node.is_network_boundary() {
                node.apply_expressions(&mut |root| {
                    root.apply(|expression| {
                        if expression
                            .downcast_ref::<DynamicFilterPhysicalExpr>()
                            .is_some()
                        {
                            let Some(id) = expression.expression_id() else {
                                return internal_err!(
                                    "DynamicFilterPhysicalExpr did not have an expression ID"
                                );
                            };
                            if !produced_ids.contains(&id) {
                                consumers.insert(id);
                            }
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })
                })?;
            }

            Ok(TreeNodeRecursion::Continue)
        })?;

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        for (id, merge_mode) in producers {
            let filter = state.filters.entry(id).or_default();
            filter.merge_mode = Some(match filter.merge_mode {
                Some(existing) if existing != merge_mode => {
                    DynamicFilterMergeMode::AllProducersComplete
                }
                Some(existing) => existing,
                None => merge_mode,
            });
            filter.producer_tasks.insert(task_key);
        }
        for id in consumers {
            state
                .filters
                .entry(id)
                .or_default()
                .consumer_tasks
                .insert(task_key);
        }
        drop(state);
        self.try_merge_all();
        Ok(())
    }

    pub(crate) fn seal_stage(&self, stage_id: usize) {
        self.state
            .lock()
            .expect("dynamic filter registry poisoned")
            .sealed_stages
            .insert(stage_id);
        self.try_merge_all();
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
        if filter.disabled || filter.merged.is_some() || !filter.producer_tasks.contains(&task_key)
        {
            return;
        }

        let advances_state = match filter.reports.get(&task_key) {
            None => true,
            Some(previous) => {
                let Some((previous_generation, previous_is_complete)) = report_state(previous)
                else {
                    // Only validated reports are inserted below.
                    unreachable!("stored producer report must be a dynamic filter")
                };
                // A complete report is terminal. Otherwise, keep only a newer generation, except
                // that completion may replace an incomplete report at the same generation because
                // DataFusion's mark_complete() does not increment the generation.
                !previous_is_complete
                    && (generation > previous_generation
                        || (generation == previous_generation && is_complete))
            }
        };
        if !advances_state {
            return;
        }

        if is_complete {
            filter.completed_producer_count += 1;
        }
        filter.reports.insert(task_key, report.expression);
        drop(state);
        self.try_merge(report.expression_id);
    }

    fn try_merge_all(&self) {
        let ids: Vec<_> = self
            .state
            .lock()
            .expect("dynamic filter registry poisoned")
            .filters
            .keys()
            .copied()
            .collect();
        for id in ids {
            self.try_merge(id);
        }
    }

    fn try_merge(&self, id: u64) {
        if self.try_merge_inner(id).is_err()
            && let Some(filter) = self
                .state
                .lock()
                .expect("dynamic filter registry poisoned")
                .filters
                .get_mut(&id)
        {
            // Dynamic filtering is an optimization. A malformed or unsupported report must not
            // fail the query.
            filter.disabled = true;
        }
    }

    fn try_merge_inner(&self, id: u64) -> Result<()> {
        let merged = {
            let state = self.state.lock().expect("dynamic filter registry poisoned");
            let Some(filter) = state.filters.get(&id) else {
                return Ok(());
            };
            if filter.disabled || filter.merged.is_some() {
                return Ok(());
            }
            let Some(merge_mode) = filter.merge_mode else {
                return Ok(());
            };

            let mut task_keys: Vec<_> = filter.producer_tasks.iter().copied().collect();
            task_keys.sort_unstable_by_key(|key| (key.stage_id, key.task_number));
            let ready_keys = match merge_mode {
                DynamicFilterMergeMode::FirstProducerComplete => task_keys
                    .into_iter()
                    .find(|task_key| {
                        filter
                            .reports
                            .get(task_key)
                            .and_then(report_state)
                            .is_some_and(|(_, is_complete)| is_complete)
                    })
                    .into_iter()
                    .collect::<Vec<_>>(),
                DynamicFilterMergeMode::AllProducersComplete => {
                    let stages_sealed = filter
                        .producer_tasks
                        .iter()
                        .all(|task| state.sealed_stages.contains(&task.stage_id));
                    if !stages_sealed
                        || task_keys.is_empty()
                        || filter.completed_producer_count != task_keys.len()
                    {
                        return Ok(());
                    }
                    task_keys
                }
            };
            let predicates = ready_keys
                .into_iter()
                .map(|task_key| completed_inner_expr(&filter.reports[&task_key]))
                .collect::<Result<Vec<_>>>()?;
            merge_predicates(predicates)?
        };

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        if let Some(filter) = state.filters.get_mut(&id)
            && !filter.disabled
        {
            filter.merged.get_or_insert(merged);
        }
        Ok(())
    }
}

fn report_state(expression: &PhysicalExprNode) -> Option<(u64, bool)> {
    let Some(ExprType::DynamicFilter(filter)) = expression.expr_type.as_ref() else {
        return None;
    };
    Some((filter.generation, filter.is_complete))
}

fn completed_inner_expr(expression: &PhysicalExprNode) -> Result<PhysicalExprNode> {
    let Some(ExprType::DynamicFilter(filter)) = expression.expr_type.as_ref() else {
        return internal_err!("producer report was not a DynamicFilterPhysicalExpr");
    };
    if !filter.is_complete {
        return internal_err!("attempted to merge an incomplete dynamic filter");
    }
    let Some(inner_expr) = filter.inner_expr.as_deref() else {
        return internal_err!("completed dynamic filter did not contain an inner expression");
    };
    Ok(inner_expr.clone())
}

fn merge_predicates(mut predicates: Vec<PhysicalExprNode>) -> Result<PhysicalExprNode> {
    match predicates.len() {
        0 => internal_err!("attempted to merge no dynamic filters"),
        1 => predicates
            .pop()
            .ok_or_else(|| datafusion::common::internal_datafusion_err!("missing predicate")),
        _ => Ok(PhysicalExprNode {
            expr_id: None,
            expr_type: Some(ExprType::BinaryExpr(Box::new(PhysicalBinaryExprNode {
                l: None,
                r: None,
                op: "Or".to_owned(),
                operands: predicates,
            }))),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::NetworkShuffleExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, lit};
    use datafusion::physical_expr::{Partitioning, PhysicalExpr};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::HashJoinExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, apply_expression_roots,
    };
    use datafusion_proto::protobuf::PhysicalDynamicFilterNode;
    use std::fmt::Formatter;
    use uuid::Uuid;

    #[test]
    fn registers_generic_task_roles_by_expression_id() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        )) as Arc<dyn PhysicalExpr>;
        let id = dynamic_filter.expression_id().unwrap();

        // with_new_children creates a distinct wrapper which keeps the same logical expression ID.
        let producer_occurrence =
            Arc::clone(&dynamic_filter).with_new_children(vec![Arc::new(Column::new("a", 0))])?;

        let input = Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let repartition = Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 1),
        )?) as Arc<dyn ExecutionPlan>;
        let boundary = Arc::new(
            NetworkShuffleExec::try_new(repartition, 1)?
                .with_dynamic_filter_anchors(vec![Arc::clone(&dynamic_filter)]),
        ) as Arc<dyn ExecutionPlan>;
        let producer = Arc::new(ExpressionExec::new(
            boundary,
            producer_occurrence,
            Some(Arc::clone(&dynamic_filter)),
        )) as Arc<dyn ExecutionPlan>;
        let producer_task = task_key(0);

        let consumer = Arc::new(ExpressionExec::new(
            Arc::new(EmptyExec::new(schema)),
            dynamic_filter,
            None,
        )) as Arc<dyn ExecutionPlan>;
        let consumer_task = task_key(1);

        let registry = DynamicFilterRegistry::new();
        registry.register_task(&producer, producer_task)?;
        registry.register_task(&consumer, consumer_task)?;

        let state = registry.state.lock().unwrap();
        let filter = state.filters.get(&id).unwrap();
        assert_eq!(
            filter.merge_mode,
            Some(DynamicFilterMergeMode::AllProducersComplete)
        );
        assert_eq!(filter.producer_tasks, HashSet::from([producer_task]));
        assert_eq!(filter.consumer_tasks, HashSet::from([consumer_task]));
        Ok(())
    }

    #[test]
    fn collect_left_uses_first_complete_mode() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        let id = dynamic_filter.expression_id().unwrap();
        let probe = Arc::new(FilterExec::try_new(dynamic_filter.clone(), empty(&schema))?)
            as Arc<dyn ExecutionPlan>;
        let join = HashJoinExec::try_new(
            empty(&schema),
            probe,
            vec![(Arc::new(Column::new("a", 0)), Arc::new(Column::new("a", 0)))],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?
        .with_dynamic_filter_expr(dynamic_filter)?;

        let registry = DynamicFilterRegistry::new();
        registry.register_task(&(Arc::new(join) as Arc<dyn ExecutionPlan>), task_key(0))?;

        assert_eq!(
            registry.state.lock().unwrap().filters[&id].merge_mode,
            Some(DynamicFilterMergeMode::FirstProducerComplete)
        );
        Ok(())
    }

    #[test]
    fn stores_latest_report_and_counts_completion_once() {
        let expression_id = 42;
        let producer_task = task_key(0);
        let registry = registry_with_producers(
            expression_id,
            DynamicFilterMergeMode::AllProducersComplete,
            [producer_task],
        );

        registry.add_report(producer_task, report(expression_id, 1, false, predicate(1)));
        registry.add_report(producer_task, report(expression_id, 0, false, predicate(0)));
        registry.add_report(producer_task, report(expression_id, 2, false, predicate(2)));
        {
            let state = registry.state.lock().unwrap();
            let filter = &state.filters[&expression_id];
            assert_eq!(
                report_state(&filter.reports[&producer_task]),
                Some((2, false))
            );
            assert_eq!(filter.completed_producer_count, 0);
            assert!(filter.merged.is_none());
        }

        // Completion is a state change even though it has the same generation as the last update.
        registry.add_report(producer_task, report(expression_id, 2, true, predicate(2)));
        registry.add_report(producer_task, report(expression_id, 2, true, predicate(2)));
        registry.add_report(producer_task, report(expression_id, 3, false, predicate(3)));
        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
        assert_eq!(
            report_state(&filter.reports[&producer_task]),
            Some((2, true))
        );
        assert_eq!(filter.completed_producer_count, 1);
        assert!(filter.merged.is_none());
    }

    #[test]
    fn all_producers_complete_merges_flat_or_in_task_order() {
        let expression_id = 43;
        let first = task_key(0);
        let second = task_key(1);
        let registry = registry_with_producers(
            expression_id,
            DynamicFilterMergeMode::AllProducersComplete,
            [first, second],
        );

        registry.add_report(second, report(expression_id, 1, true, predicate(2)));
        registry.seal_stage(3);
        assert!(
            registry.state.lock().unwrap().filters[&expression_id]
                .merged
                .is_none()
        );

        registry.add_report(first, report(expression_id, 1, true, predicate(1)));
        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
        assert_eq!(filter.completed_producer_count, 2);
        let ExprType::BinaryExpr(binary) =
            filter.merged.as_ref().unwrap().expr_type.as_ref().unwrap()
        else {
            panic!("expected a binary OR expression");
        };
        assert_eq!(binary.op, "Or");
        assert!(binary.l.is_none());
        assert!(binary.r.is_none());
        assert_eq!(binary.operands, vec![predicate(1), predicate(2)]);
    }

    #[test]
    fn first_producer_complete_merges_without_waiting_for_stage() {
        let expression_id = 44;
        let first = task_key(0);
        let second = task_key(1);
        let registry = registry_with_producers(
            expression_id,
            DynamicFilterMergeMode::FirstProducerComplete,
            [first, second],
        );

        registry.add_report(second, report(expression_id, 1, true, predicate(2)));

        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
        assert_eq!(filter.completed_producer_count, 1);
        assert_eq!(filter.merged.as_ref(), Some(&predicate(2)));
    }

    #[test]
    fn ignores_reports_from_unknown_tasks_and_expression_ids() {
        let expression_id = 45;
        let producer_task = task_key(0);
        let registry = registry_with_producers(
            expression_id,
            DynamicFilterMergeMode::AllProducersComplete,
            [producer_task],
        );

        registry.add_report(task_key(1), report(expression_id, 1, true, predicate(1)));
        registry.add_report(
            producer_task,
            report(expression_id + 1, 1, true, predicate(1)),
        );
        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
        assert!(filter.reports.is_empty());
        assert_eq!(filter.completed_producer_count, 0);
    }

    fn registry_with_producers<const N: usize>(
        expression_id: u64,
        merge_mode: DynamicFilterMergeMode,
        producer_tasks: [TaskKey; N],
    ) -> DynamicFilterRegistry {
        let registry = DynamicFilterRegistry::new();
        registry.state.lock().unwrap().filters.insert(
            expression_id,
            PlannedDynamicFilter {
                merge_mode: Some(merge_mode),
                producer_tasks: HashSet::from(producer_tasks),
                ..Default::default()
            },
        );
        registry
    }

    fn report(
        expression_id: u64,
        generation: u64,
        is_complete: bool,
        inner_expr: PhysicalExprNode,
    ) -> ProducedDynamicFilter {
        ProducedDynamicFilter {
            expression_id,
            expression: PhysicalExprNode {
                expr_id: Some(expression_id),
                expr_type: Some(ExprType::DynamicFilter(Box::new(
                    PhysicalDynamicFilterNode {
                        generation,
                        inner_expr: Some(Box::new(inner_expr)),
                        is_complete,
                        ..Default::default()
                    },
                ))),
            },
        }
    }

    fn predicate(id: u64) -> PhysicalExprNode {
        PhysicalExprNode {
            expr_id: Some(id),
            expr_type: None,
        }
    }

    fn empty(schema: &Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::clone(schema)))
    }

    fn task_key(task_number: usize) -> TaskKey {
        TaskKey {
            query_id: Uuid::nil(),
            stage_id: 3,
            task_number,
        }
    }

    #[derive(Debug)]
    struct ExpressionExec {
        input: Arc<dyn ExecutionPlan>,
        expression: Arc<dyn PhysicalExpr>,
        produced_expression: Option<Arc<dyn PhysicalExpr>>,
    }

    impl ExpressionExec {
        fn new(
            input: Arc<dyn ExecutionPlan>,
            expression: Arc<dyn PhysicalExpr>,
            produced_expression: Option<Arc<dyn PhysicalExpr>>,
        ) -> Self {
            Self {
                input,
                expression,
                produced_expression,
            }
        }
    }

    impl DisplayAs for ExpressionExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "ExpressionExec")
        }
    }

    impl ExecutionPlan for ExpressionExec {
        fn name(&self) -> &str {
            "ExpressionExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }

        fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
            self.produced_expression.iter().cloned().collect()
        }

        fn apply_expressions(
            &self,
            f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            apply_expression_roots([&self.expression], f)
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self::new(
                children.remove(0),
                Arc::clone(&self.expression),
                self.produced_expression.clone(),
            )))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.input.execute(partition, context)
        }
    }
}
