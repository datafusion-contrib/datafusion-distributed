use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, internal_err};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

use crate::NetworkBoundaryExt;

/// A dynamic filter produced by an execution plan.
#[derive(Clone)]
pub(crate) struct DiscoveredDynamicFilterProducer {
    pub(crate) id: u64,
    pub(crate) expression: Arc<dyn PhysicalExpr>,
}

/// A dynamic-filter consumer discovered in an execution plan.
#[derive(Clone)]
pub(crate) struct DiscoveredDynamicFilter {
    pub(crate) id: u64,
    pub(crate) expression: Arc<dyn PhysicalExpr>,
    /// Schema used to decode a producer-coordinate predicate before this consumer remaps it.
    pub(crate) input_schema: SchemaRef,
    /// Schema used by this concrete consumer occurrence after expression remapping. File scan
    /// predicates, for example, are evaluated against the unprojected table schema.
    pub(crate) expression_schema: SchemaRef,
}

fn expression_schema(node: &Arc<dyn ExecutionPlan>, fallback: &SchemaRef) -> SchemaRef {
    node.downcast_ref::<DataSourceExec>()
        .and_then(|exec| exec.data_source().downcast_ref::<FileScanConfig>())
        .map(|scan| {
            let mut fields = scan.file_schema().fields().to_vec();
            fields.extend(scan.table_partition_cols().iter().cloned());
            Arc::new(Schema::new_with_metadata(
                fields,
                scan.file_schema().metadata().clone(),
            ))
        })
        .unwrap_or_else(|| Arc::clone(fallback))
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
        let expression_schema = expression_schema(node, &input_schema);

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
                            expression_schema: Arc::clone(&expression_schema),
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

/// Finds every runtime dynamic-filter consumer in a worker task plan, grouped by expression ID.
///
/// Expressions attached to network operators are coordinator visibility anchors. They preserve
/// topology through distributed planning but do not evaluate rows, so only real plan-node
/// consumers are returned here.
pub(crate) fn discover_runtime_dynamic_filter_consumers(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<HashMap<u64, Vec<DiscoveredDynamicFilter>>> {
    let mut consumers: HashMap<u64, Vec<DiscoveredDynamicFilter>> = HashMap::new();

    plan.apply(|node| {
        if node.is_network_boundary() {
            return Ok(TreeNodeRecursion::Continue);
        }
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
        let expression_schema = expression_schema(node, &input_schema);

        node.apply_expressions(&mut |root| {
            root.apply(|expression| {
                if expression
                    .downcast_ref::<DynamicFilterPhysicalExpr>()
                    .is_none()
                {
                    return Ok(TreeNodeRecursion::Continue);
                }

                let Some(id) = expression.expression_id() else {
                    return internal_err!(
                        "DynamicFilterPhysicalExpr did not have an expression ID"
                    );
                };
                if produced_ids.contains(&id) {
                    return Ok(TreeNodeRecursion::Continue);
                }
                consumers
                    .entry(id)
                    .or_default()
                    .push(DiscoveredDynamicFilter {
                        id,
                        expression: Arc::clone(expression),
                        input_schema: Arc::clone(&input_schema),
                        expression_schema: Arc::clone(&expression_schema),
                    });
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(consumers)
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
                .or_insert_with(|| DiscoveredDynamicFilterProducer { id, expression });
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut producers: Vec<_> = producers.into_values().collect();
    producers.sort_unstable_by_key(|producer| producer.id);
    Ok(producers)
}

/// Finds consumer expressions that must remain discoverable after `plan` is moved behind
/// a remote network boundary.
pub(crate) fn crossing_dynamic_filter_consumers(
    plan: &Arc<dyn ExecutionPlan>,
    dynamic_filter_ids: &HashSet<u64>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let produced_here: HashSet<_> = discover_dynamic_filter_producers(plan)?
        .into_iter()
        .map(|producer| producer.id)
        .collect();
    Ok(discover_dynamic_filter_consumers(plan)?
        .into_iter()
        .filter(|consumer| dynamic_filter_ids.contains(&consumer.id))
        .filter(|consumer| !produced_here.contains(&consumer.id))
        .map(|consumer| consumer.expression)
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

    #[test]
    fn discovers_producers_without_restricting_execution_plan_type() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;
        let first = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        )) as Arc<dyn PhysicalExpr>;
        let second = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        )) as Arc<dyn PhysicalExpr>;

        let plan = Arc::new(ExpressionExec::new(
            Arc::new(ExpressionExec::new(
                Arc::new(ExpressionExec::new(input, Arc::clone(&first), true)),
                Arc::clone(&second),
                true,
            )),
            Arc::clone(&first),
            true,
        )) as Arc<dyn ExecutionPlan>;

        let discovered = discover_dynamic_filter_producers(&plan)?;
        assert_eq!(
            discovered
                .iter()
                .map(|producer| producer.id)
                .collect::<Vec<_>>(),
            vec![
                first.expression_id().unwrap(),
                second.expression_id().unwrap()
            ]
        );
        Ok(())
    }

    #[test]
    fn runtime_discovery_keeps_every_consumer_occurrence() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        )) as Arc<dyn PhysicalExpr>;
        let first = Arc::new(ExpressionExec::new(
            input,
            Arc::clone(&dynamic_filter),
            false,
        )) as Arc<dyn ExecutionPlan>;
        let plan = Arc::new(ExpressionExec::new(
            first,
            Arc::clone(&dynamic_filter),
            false,
        )) as Arc<dyn ExecutionPlan>;

        let consumers = discover_runtime_dynamic_filter_consumers(&plan)?;
        assert_eq!(consumers[&dynamic_filter.expression_id().unwrap()].len(), 2);
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
