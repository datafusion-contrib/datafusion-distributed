use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// A dynamic-filter consumer discovered in an execution plan.
#[derive(Clone)]
pub(crate) struct DiscoveredDynamicFilter {
    pub(crate) id: u64,
    pub(crate) expression: Arc<dyn PhysicalExpr>,
    pub(crate) input_schema: SchemaRef,
}

/// Finds dynamic-filter consumers in `plan`, optionally restricting the result to `allowed_ids`.
///
/// Producer and consumer occurrences intentionally share expression IDs. Producer occurrences are
/// therefore removed only from the node that reports them through
/// [`ExecutionPlan::dynamic_expressions_produced`], rather than subtracting producer IDs from the
/// whole plan.
pub(crate) fn discover_dynamic_filter_consumers(
    plan: &Arc<dyn ExecutionPlan>,
    allowed_ids: Option<&HashSet<u64>>,
) -> Result<Vec<DiscoveredDynamicFilter>> {
    let mut consumers = HashMap::new();

    plan.apply(|node| {
        let produced = node.dynamic_expressions_produced();
        let input_schema = node
            .children()
            .first()
            .map(|child| child.schema())
            .unwrap_or_else(|| node.schema());

        node.apply_expressions(&mut |root| {
            root.apply(|expression| {
                let Some(dynamic_filter) = expression.downcast_ref::<DynamicFilterPhysicalExpr>()
                else {
                    return Ok(TreeNodeRecursion::Continue);
                };

                let id = dynamic_filter.inner().expression_id;
                let is_producer_occurrence = produced
                    .iter()
                    .any(|produced| Arc::ptr_eq(produced, expression));
                let is_allowed = allowed_ids.is_none_or(|ids| ids.contains(&id));
                if !is_producer_occurrence && is_allowed {
                    consumers
                        .entry(id)
                        .or_insert_with(|| DiscoveredDynamicFilter {
                            id,
                            expression: Arc::clone(expression),
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
    Ok(consumers)
}

pub(crate) fn dynamic_filter_consumer_ids(plan: &Arc<dyn ExecutionPlan>) -> Result<HashSet<u64>> {
    Ok(discover_dynamic_filter_consumers(plan, None)?
        .into_iter()
        .map(|consumer| consumer.id)
        .collect())
}

/// Returns selected consumers that have reached their final state.
///
/// This is called after task execution has stopped, when an incomplete filter can no longer make
/// progress. Skipping incomplete filters prevents a cancelled or short-circuited task from
/// indefinitely delaying query finalization.
pub(crate) fn completed_dynamic_filters(
    plan: &Arc<dyn ExecutionPlan>,
    allowed_ids: &HashSet<u64>,
) -> Result<Vec<(u64, Arc<dyn PhysicalExpr>)>> {
    let consumers = discover_dynamic_filter_consumers(plan, Some(allowed_ids))?;
    Ok(consumers
        .into_iter()
        .filter_map(|consumer| {
            let inner = consumer
                .expression
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .expect("dynamic filter was checked during discovery")
                .inner();
            inner.is_complete.then_some((consumer.id, inner.expr))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};
    use datafusion::physical_plan::empty::EmptyExec;
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

        let discovered = discover_dynamic_filter_consumers(&plan, None)?;
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].id, dynamic_filter.expression_id().unwrap());

        dynamic_filter
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap()
            .update(Arc::new(BinaryExpr::new(column, Operator::Gt, lit(10_i32))))?;
        dynamic_filter
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap()
            .mark_complete();

        let allowed = HashSet::from_iter([dynamic_filter.expression_id().unwrap()]);
        let completed = completed_dynamic_filters(&plan, &allowed)?;
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].1.to_string(), "a@0 > 10");
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
