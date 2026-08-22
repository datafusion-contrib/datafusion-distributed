use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, internal_err};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// A dynamic-filter consumer discovered in an execution plan along with the schema its evaluated
/// against.
#[derive(Clone)]
pub(crate) struct DiscoveredDynamicFilter {
    pub(crate) id: u64,
    pub(crate) expression: Arc<dyn PhysicalExpr>,
    pub(crate) input_schema: SchemaRef,
}

/// Finds dynamic-filter consumers in `plan`, deduplicated by expression ID.
pub(crate) fn discover_dynamic_filter_consumers(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<DiscoveredDynamicFilter>> {
    let mut consumers = HashMap::new();

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

        node.apply_expressions(&mut |root| {
            root.apply(|expression| {
                let Some(_) = expression.downcast_ref::<DynamicFilterPhysicalExpr>() else {
                    return Ok(TreeNodeRecursion::Continue);
                };

                let Some(id) = expression.expression_id() else {
                    return internal_err!(
                        "DynamicFilterPhysicalExpr did not have an expression ID"
                    );
                };
                let is_producer_occurrence = produced_ids.contains(&id);
                if !is_producer_occurrence {
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};
    use datafusion::physical_plan::empty::EmptyExec;
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

        let current = discovered[0]
            .expression
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap()
            .current()?;
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

        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].id, dynamic_filter.expression_id().unwrap());
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
