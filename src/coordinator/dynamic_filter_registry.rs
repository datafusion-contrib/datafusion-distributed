use crate::{ProducedDynamicFilter, TaskKey};
use crate::dynamic_filtering::discover_dynamic_filter_consumers;
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
    /// Use the first complete producer report and forward it.
    FirstProducerComplete,
    // TODO: Merge and forward updates for filters that do not complete, such as filters produced
    // by aggregates and sorts. See https://github.com/datafusion-contrib/datafusion-distributed/issues/665.
}

#[derive(Default)]
pub(super) struct PlannedDynamicFilter {
    pub(super) merge_mode: Option<DynamicFilterMergeMode>,
    // Producer and consumer tasks for a dynamic filter. Note that it is not guaranteed
    // that every task within a stage produces / consumes dynamic filters so we specifically store
    // task keys rather than stage ids. For example, a distributed union may prevent a dynamic filter
    // consumer from appearing in all tasks.
    pub(super) producer_tasks: HashSet<TaskKey>,
    pub(super) consumer_tasks: HashSet<TaskKey>,
    /// Complete inner predicate reported by each producer task.
    pub(super) completed_predicates: HashMap<TaskKey, PhysicalExprNode>,
    /// The result of merging all the filters from the producer tasks.
    pub(super) merged: Option<PhysicalExprNode>,
}

#[derive(Default)]
pub(super) struct DynamicFilterRegistryState {
    pub(super) filters: HashMap<u64, PlannedDynamicFilter>,
    /// Track which stages have registered all of their tasks.
    ///
    /// Dynamic filter updates need to be forwarded to consumers when all
    /// producer tasks have sent updates. However, the coorindator needs to
    /// indicate that it will not register any more tasks by sealing
    /// particular stage ids.
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
            Ok(TreeNodeRecursion::Continue)
        })?;
        let consumers = discover_dynamic_filter_consumers(plan)?.consumers;

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
        for consumer in consumers {
            state
                .filters
                .entry(consumer.id)
                .or_default()
                .consumer_tasks
                .insert(task_key);
        }
        Ok(())
    }

    /// Mark that a stage has registered all of its tasks.
    pub(crate) fn seal_stage(&self, stage_id: usize) {
        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        state.sealed_stages.insert(stage_id);
        let ids = state.filters.keys().copied().collect::<Vec<_>>();
        for id in ids {
            Self::try_merge(&mut state, id);
        }
    }

    /// Tracks a partial dynamic filter update in hte registry.
    pub(crate) fn update(&self, task_key: TaskKey, report: ProducedDynamicFilter) {
        if report.expression.expr_id != Some(report.expression_id) {
            return;
        }
        let Some(ExprType::DynamicFilter(dynamic_filter)) = report.expression.expr_type else {
            return;
        };
        if !dynamic_filter.is_complete {
            return;
        }
        let Some(predicate) = dynamic_filter.inner_expr.map(|predicate| *predicate) else {
            return;
        };

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        let Some(filter) = state.filters.get_mut(&report.expression_id) else {
            return;
        };
        if filter.merged.is_some()
            || !filter.producer_tasks.contains(&task_key)
            || filter.completed_predicates.contains_key(&task_key)
        {
            return;
        }
        filter.completed_predicates.insert(task_key, predicate);
        Self::try_merge(&mut state, report.expression_id);
    }

    /// Merges partial dynamic filters together for the provided dynamic filter
    /// id only if there are enough updates present.
    fn try_merge(state: &mut DynamicFilterRegistryState, id: u64) {
        let merged = {
            let Some(filter) = state.filters.get(&id) else {
                return;
            };
            if filter.merged.is_some() {
                return;
            }
            let Some(merge_mode) = filter.merge_mode else {
                return;
            };

            let mut task_keys: Vec<_> = filter.producer_tasks.iter().copied().collect();
            task_keys.sort_unstable_by_key(|key| (key.stage_id, key.task_number));
            let predicates = match merge_mode {
                DynamicFilterMergeMode::FirstProducerComplete => {
                    let Some(predicate) = task_keys
                        .iter()
                        .find_map(|task_key| filter.completed_predicates.get(task_key))
                    else {
                        return;
                    };
                    vec![predicate.clone()]
                }
                DynamicFilterMergeMode::AllProducersComplete => {
                    let stages_sealed = filter
                        .producer_tasks
                        .iter()
                        .all(|task| state.sealed_stages.contains(&task.stage_id));
                    if !stages_sealed
                        || task_keys.is_empty()
                        || filter.completed_predicates.len() != task_keys.len()
                    {
                        return;
                    }
                    task_keys
                        .iter()
                        .filter_map(|task_key| filter.completed_predicates.get(task_key).cloned())
                        .collect()
                }
            };
            let Some(merged) = merge_predicates(predicates) else {
                return;
            };
            merged
        };

        if let Some(filter) = state.filters.get_mut(&id) {
            filter.merged.get_or_insert(merged);
        }
    }
}

/// Merges [`PhysicalExprNode`] together by ORing them.
fn merge_predicates(mut predicates: Vec<PhysicalExprNode>) -> Option<PhysicalExprNode> {
    match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(PhysicalExprNode {
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
    use datafusion::common::tree_node::TreeNodeRecursion;
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
    fn all_producers_complete_merges_flat_or_in_task_order() {
        let expression_id = 43;
        let first = task_key(0);
        let second = task_key(1);
        let registry = registry_with_producers(
            expression_id,
            DynamicFilterMergeMode::AllProducersComplete,
            [first, second],
        );

        registry.update(second, report(expression_id, true, predicate(2)));
        registry.seal_stage(3);
        assert!(
            registry.state.lock().unwrap().filters[&expression_id]
                .merged
                .is_none()
        );

        registry.update(first, report(expression_id, true, predicate(1)));
        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
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

        registry.update(second, report(expression_id, true, predicate(2)));

        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
        assert_eq!(filter.merged.as_ref(), Some(&predicate(2)));
    }

    #[test]
    fn ignores_incomplete_and_unknown_reports() {
        let expression_id = 45;
        let producer_task = task_key(0);
        let registry = registry_with_producers(
            expression_id,
            DynamicFilterMergeMode::AllProducersComplete,
            [producer_task],
        );

        registry.update(producer_task, report(expression_id, false, predicate(1)));
        registry.update(task_key(1), report(expression_id, true, predicate(1)));
        registry.update(producer_task, report(expression_id + 1, true, predicate(1)));
        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
        assert!(filter.completed_predicates.is_empty());
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
        is_complete: bool,
        inner_expr: PhysicalExprNode,
    ) -> ProducedDynamicFilter {
        ProducedDynamicFilter {
            expression_id,
            expression: PhysicalExprNode {
                expr_id: Some(expression_id),
                expr_type: Some(ExprType::DynamicFilter(Box::new(
                    PhysicalDynamicFilterNode {
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
