use crate::{NetworkBoundaryExt, TaskKey};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, internal_err};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(super) struct PlannedDynamicFilter {
    // Producer and consumer tasks for a dynamic filter. Note that it is not guaranteed
    // that every task within a stage produces / consumes dynamic filters so we specifically store
    // task keys rather than stage ids. For example, a distributed union may prevent a dynamic filter
    // consumer from appearing in all tasks.
    pub(super) producer_tasks: HashSet<TaskKey>,
    pub(super) consumer_tasks: HashSet<TaskKey>,
}

#[derive(Default)]
pub(super) struct DynamicFilterRegistryState {
    pub(super) filters: HashMap<u64, PlannedDynamicFilter>,
}

/// Query-scoped runtime topology for distributed dynamic filters.
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
        let mut producers = HashSet::new();
        let mut consumers = HashSet::new();

        plan.apply(|node| {
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
            producers.extend(produced_ids.iter().copied());

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
        for id in producers {
            state
                .filters
                .entry(id)
                .or_default()
                .producer_tasks
                .insert(task_key);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::NetworkShuffleExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, lit};
    use datafusion::physical_expr::{Partitioning, PhysicalExpr};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, apply_expression_roots,
    };
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

        // Exercise ID-based matching with a distinct expression wrapper for the same logical
        // dynamic filter.
        let producer_occurrence =
            Arc::clone(&dynamic_filter).with_new_children(vec![Arc::new(Column::new("a", 0))])?;
        assert!(!Arc::ptr_eq(&dynamic_filter, &producer_occurrence));

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
        assert_eq!(filter.producer_tasks, HashSet::from([producer_task]));
        assert_eq!(filter.consumer_tasks, HashSet::from([consumer_task]));
        Ok(())
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
