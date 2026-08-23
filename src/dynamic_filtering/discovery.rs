use crate::NetworkBoundaryExt;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, internal_err};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// A dynamic filter produced by an execution plan.
#[derive(Clone)]
pub(crate) struct DiscoveredDynamicFilterProducer {
    pub(crate) id: u64,
}

/// A dynamic-filter consumer discovered in an execution plan along with the schema it is evaluated
/// against.
#[derive(Clone)]
pub(crate) struct DiscoveredDynamicFilter {
    pub(crate) id: u64,
    pub(crate) expression: Arc<DynamicFilterPhysicalExpr>,
    pub(crate) input_schema: SchemaRef,
}

#[derive(Clone)]
pub(crate) struct DiscoveredDynamicFilterAnchor {
    pub(crate) id: u64,
    pub(crate) expression: Arc<dyn PhysicalExpr>,
}

pub(crate) struct DiscoveredDynamicFilterConsumers {
    /// Dynamic filters that are evaluated by an execution-plan node.
    pub(crate) consumers: Vec<DiscoveredDynamicFilter>,
    /// Metadata-only references carried by network boundaries to trick producers
    /// into thinking the dynamic filter is used.
    pub(crate) anchors: Vec<DiscoveredDynamicFilterAnchor>,
}

/// Finds dynamic-filter consumers and network-boundary anchors in `plan`, deduplicated by
/// expression ID within each category.
pub(crate) fn discover_dynamic_filter_consumers(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<DiscoveredDynamicFilterConsumers> {
    let mut consumers = HashMap::new();
    let mut anchors = HashMap::new();

    plan.apply(|node| {
        let produced_ids: HashSet<_> = node
            .dynamic_expressions_produced()
            .into_iter()
            .map(|produced| {
                let Some(id) = produced.expression_id() else {
                    return internal_err!(
                        "{}::dynamic_expressions_produced returned an expression without an expression ID",
                        node.name()
                    );
                };
                Ok(id)
            })
            .collect::<Result<_>>()?;
        let input_schema = node
            .children()
            .first()
            .map(|child| child.schema())
            .unwrap_or_else(|| node.schema());
        let is_network_boundary = node.is_network_boundary();

        node.apply_expressions(&mut |root| {
            root.apply(|expression| {
                let expression = Arc::clone(expression);
                let Ok(expression) = Arc::downcast::<DynamicFilterPhysicalExpr>(expression) else {
                    return Ok(TreeNodeRecursion::Continue);
                };

                let Some(id) = expression.expression_id() else {
                    return internal_err!(
                        "DynamicFilterPhysicalExpr did not have an expression ID"
                    );
                };
                if is_network_boundary {
                    // Network-boundary expressions are metadata-only dependencies, not expressions
                    // evaluated by the node.
                    anchors
                        .entry(id)
                        .or_insert_with(|| DiscoveredDynamicFilterAnchor {
                            id,
                            expression: Arc::clone(expression),
                        });
                } else if !produced_ids.contains(&id) {
                    consumers
                        .entry(id)
                        .or_insert_with(|| DiscoveredDynamicFilter {
                            id,
                            expression,
                            input_schema: Arc::clone(&input_schema),
                        });
                }

                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut consumers: Vec<_> = consumers.into_values().collect();
    consumers.sort_unstable_by_key(|consumer| consumer.id);
    let mut anchors: Vec<_> = anchors.into_values().collect();
    anchors.sort_unstable_by_key(|anchor| anchor.id);
    Ok(DiscoveredDynamicFilterConsumers { consumers, anchors })
}

/// Returns whether `plan` contains only the consumer side of a dynamic filter
/// relationship.
pub(crate) fn has_nonlocal_dynamic_filter_relationships(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<bool> {
    let consumer_ids: HashSet<_> = discover_dynamic_filter_consumers(plan)?
        .consumers
        .into_iter()
        .map(|consumer| consumer.id)
        .collect();

    let mut producer_ids = HashSet::new();
    plan.apply(|node| {
        for produced in node.dynamic_expressions_produced() {
            let Some(id) = produced.expression_id() else {
                return internal_err!(
                    "{}::dynamic_expressions_produced returned an expression without an expression ID",
                    node.name()
                );
            };
            producer_ids.insert(id);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(consumer_ids != producer_ids)
}

/// Finds dynamic-filter producers in `plan`, deduplicated by expression ID.
///
/// Producer type is intentionally unrestricted: hash joins, aggregates, sorts, and future
/// producers are all discovered through [`ExecutionPlan::dynamic_expressions_produced`].
pub(crate) fn discover_dynamic_filter_producers(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<DiscoveredDynamicFilterProducer>> {
    let mut producers = HashMap::new();
    plan.apply(|node| {
        for expression in node.dynamic_expressions_produced() {
            if expression
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .is_none()
            {
                continue;
            }
            let Some(id) = expression.expression_id() else {
                return internal_err!("DynamicFilterPhysicalExpr did not have an expression ID");
            };
            producers
                .entry(id)
                .or_insert(DiscoveredDynamicFilterProducer { id });
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut producers: Vec<_> = producers.into_values().collect();
    producers.sort_unstable_by_key(|producer| producer.id);
    Ok(producers)
}

/// Finds consumers whose producer does not occur in `plan`.
///
/// This includes both consumers evaluated in `plan` and metadata-only anchors propagated through
/// network boundaries.
///
/// These consumers become orphaned from their producer when `plan` is moved behind a remote
/// network boundary, so their expressions must remain discoverable on that boundary.
pub(crate) fn orphan_dynamic_filter_consumers(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let produced_here: HashSet<_> = discover_dynamic_filter_producers(plan)?
        .into_iter()
        .map(|producer| producer.id)
        .collect();
    let discovered = discover_dynamic_filter_consumers(plan)?;
    let orphaned: HashMap<_, _> = discovered
        .consumers
        .into_iter()
        .map(|consumer| (consumer.id, consumer.expression))
        .chain(
            discovered
                .anchors
                .into_iter()
                .map(|anchor| (anchor.id, anchor.expression)),
        )
        .filter(|(id, _)| !produced_here.contains(id))
        .collect();
    let mut orphaned: Vec<_> = orphaned.into_iter().collect();
    orphaned.sort_unstable_by_key(|(id, _)| *id);
    Ok(orphaned
        .into_iter()
        .map(|(_, expression)| expression)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NetworkShuffleExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, apply_expression_roots,
    };
    use std::fmt::Formatter;

    #[tokio::test]
    async fn discovers_nested_consumer_but_not_its_producer_occurrence() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input = Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let column = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&column)],
            lit(true),
        )) as Arc<dyn PhysicalExpr>;
        let nested = Arc::new(BinaryExpr::new(
            Arc::clone(&dynamic_filter),
            Operator::And,
            lit(true),
        )) as Arc<dyn PhysicalExpr>;

        let consumer =
            Arc::new(ExpressionExec::new(input, nested, false)) as Arc<dyn ExecutionPlan>;
        let plan = Arc::new(ExpressionExec::new(
            consumer,
            Arc::clone(&dynamic_filter),
            true,
        )) as Arc<dyn ExecutionPlan>;

        let discovered = discover_dynamic_filter_consumers(&plan)?;
        assert_eq!(discovered.consumers.len(), 1);
        assert!(discovered.anchors.is_empty());
        assert_eq!(
            discovered.consumers[0].id,
            dynamic_filter.expression_id().unwrap()
        );
        assert!(!has_nonlocal_dynamic_filter_relationships(&plan)?);
        assert!(orphan_dynamic_filter_consumers(&plan)?.is_empty());

        dynamic_filter
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap()
            .update(Arc::new(BinaryExpr::new(column, Operator::Gt, lit(10_i32))))?;
        dynamic_filter
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap()
            .mark_complete();

        let current = discovered.consumers[0].expression.current()?;
        assert_eq!(current.to_string(), "a@0 > 10");
        Ok(())
    }

    #[test]
    fn deduplicates_consumers_with_the_same_expression_id() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        )) as Arc<dyn PhysicalExpr>;
        let consumers = (0..2)
            .map(|_| {
                Arc::new(ExpressionExec::new(
                    Arc::new(EmptyExec::new(Arc::clone(&schema))),
                    Arc::clone(&dynamic_filter),
                    false,
                )) as Arc<dyn ExecutionPlan>
            })
            .collect();
        let plan = UnionExec::try_new(consumers)?;

        let discovered = discover_dynamic_filter_consumers(&plan)?;

        assert_eq!(discovered.consumers.len(), 1);
        assert!(discovered.anchors.is_empty());
        assert_eq!(
            discovered.consumers[0].id,
            dynamic_filter.expression_id().unwrap()
        );
        assert!(has_nonlocal_dynamic_filter_relationships(&plan)?);
        Ok(())
    }

    #[test]
    fn identifies_a_producer_without_a_local_consumer() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        )) as Arc<dyn PhysicalExpr>;
        let plan = Arc::new(ExpressionExec::new(
            Arc::new(EmptyExec::new(schema)),
            dynamic_filter,
            true,
        )) as Arc<dyn ExecutionPlan>;

        assert!(has_nonlocal_dynamic_filter_relationships(&plan)?);
        Ok(())
    }

    #[test]
    fn distinguishes_consumers_from_network_boundary_anchors() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let column = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&column)],
            lit(true),
        )) as Arc<dyn PhysicalExpr>;
        let id = dynamic_filter.expression_id().unwrap();

        let input = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;
        let repartition = Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::Hash(vec![column], 1),
        )?) as Arc<dyn ExecutionPlan>;
        let boundary = Arc::new(
            NetworkShuffleExec::try_new(repartition, 1)?
                .with_dynamic_filter_anchors(vec![Arc::clone(&dynamic_filter)]),
        ) as Arc<dyn ExecutionPlan>;
        let plan = Arc::new(ExpressionExec::new(
            boundary,
            Arc::clone(&dynamic_filter),
            false,
        )) as Arc<dyn ExecutionPlan>;

        let discovered = discover_dynamic_filter_consumers(&plan)?;
        assert_eq!(
            discovered
                .consumers
                .iter()
                .map(|consumer| consumer.id)
                .collect::<Vec<_>>(),
            vec![id]
        );
        assert_eq!(
            discovered
                .anchors
                .iter()
                .map(|anchor| anchor.id)
                .collect::<Vec<_>>(),
            vec![id]
        );
        assert_eq!(orphan_dynamic_filter_consumers(&plan)?.len(), 1);
        Ok(())
    }

    #[derive(Debug)]
    struct ExpressionExec {
        input: Arc<dyn ExecutionPlan>,
        expression: Arc<dyn PhysicalExpr>,
        produces_expression: bool,
    }

    impl ExpressionExec {
        fn new(
            input: Arc<dyn ExecutionPlan>,
            expression: Arc<dyn PhysicalExpr>,
            produces_expression: bool,
        ) -> Self {
            Self {
                input,
                expression,
                produces_expression,
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
            self.produces_expression
                .then(|| Arc::clone(&self.expression))
                .into_iter()
                .collect()
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
                self.produces_expression,
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
