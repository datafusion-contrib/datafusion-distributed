use crate::NetworkBoundaryExt;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, internal_err};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// A dynamic filter produced by an [`ExecutionPlan`].
#[derive(Clone)]
pub struct DiscoveredDynamicFilterProducer {
    pub id: u64,
}

/// A dynamic-filter consumer discovered in an execution plan along with the schema it is evaluated
/// against.
#[derive(Clone)]
pub struct DiscoveredDynamicFilter {
    pub id: u64,
    pub expression: Arc<DynamicFilterPhysicalExpr>,
    pub input_schema: SchemaRef,
}

/// An anchor is an artificial dynamic filter consumer injected into network boundaries
/// to keep consumer references alive when they are moved across network boundaries.
///
/// TODO(#697): remove anchors in df-56.
#[derive(Clone)]
pub struct DiscoveredDynamicFilterAnchor {
    pub id: u64,
    pub expression: Arc<dyn PhysicalExpr>,
}

pub struct DiscoveredDynamicFilterConsumers {
    // Real consumers, ordered by expression id.
    pub consumers: Vec<DiscoveredDynamicFilter>,
    // Artificial consumers. Dynamic filters in network boundaries. Also ordered by expression id.
    pub anchors: Vec<DiscoveredDynamicFilterAnchor>,
}

/// Finds dynamic-filter consumers and network-boundary anchors in `plan`, deduplicated by
/// expression ID within each category.
pub fn discover_dynamic_filter_consumers(
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
                            expression: expression.clone(),
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

/// Finds dynamic-filter producers in `plan`, deduplicated and ordered by expression ID.
pub fn discover_dynamic_filter_producers(
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

/// Finds consumers whose producer does not occur in `plan`. These consumers become orphaned
/// from their producer when the producer is moved behind a remote network boundary. These
/// orphans become network boundary anchors, artificially keeping the producers alive.
///
/// TODO(697): remove anchors in df-56
pub(crate) fn orphan_dynamic_filter_consumers(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let produced_here: HashSet<_> = discover_dynamic_filter_producers(plan)?
        .into_iter()
        .map(|producer| producer.id)
        .collect();
    let discovered = discover_dynamic_filter_consumers(plan)?;
    // Include anchors here because we want anchors to work recursively. For example,
    // if a producer is in stage 4 and its consumer is in stage 1, an
    // anchor should exist in stage 4. The easiest way to guarantee that is to ensure
    // the anchor exists in stages 2, 3, and 4 recursively via this function.
    let orphaned: HashMap<_, _> = discovered
        .consumers
        .into_iter()
        .map(|consumer| (consumer.id, consumer.expression as Arc<dyn PhysicalExpr>))
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
