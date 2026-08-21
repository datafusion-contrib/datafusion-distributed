use crate::codec::decode_physical_expr;
use crate::common::discover_dynamic_filter_consumers;
use crate::execution_plans::DistributedLeafExec;
use crate::{DistributedCodec, TaskCompletedDynamicFilters, TaskKey};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{DeduplicatingProtoConverter, PhysicalPlanNodeExt};
use datafusion_proto::protobuf::PhysicalPlanNode;
use std::sync::Arc;

/// Replaces the variants in the visualization plan with independent per-task copies.
pub(super) fn isolate_distributed_leaf_variants_for_display(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<TaskContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
    let converter = DeduplicatingProtoConverter::default();
    plan.transform_up(|node| {
        let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() else {
            return Ok(Transformed::no(node));
        };

        let variants = leaf
            .variants()
            .iter()
            .map(|variant| {
                let proto = PhysicalPlanNode::try_from_physical_plan_with_converter(
                    Arc::clone(variant),
                    &codec,
                    &converter,
                )?;
                proto.try_into_physical_plan_with_converter(task_ctx, &codec, &converter)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Transformed::yes(Arc::new(DistributedLeafExec::try_new(
            Arc::clone(leaf.original()),
            variants,
        )?) as Arc<dyn ExecutionPlan>))
    })
    .map(|transformed| transformed.data)
}

/// Applies successful worker reports only to the matching task-local visualization variants.
pub(super) fn apply_reports_to_distributed_leaves(
    plan: &Arc<dyn ExecutionPlan>,
    reports: &HashMap<TaskKey, TaskCompletedDynamicFilters>,
    task_ctx: &Arc<TaskContext>,
) {
    let _ = plan.apply(|node| {
        let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() else {
            return Ok(TreeNodeRecursion::Continue);
        };

        for (task_key, report) in reports {
            let Some(variant) = leaf.variants().get(task_key.task_number) else {
                continue;
            };
            let updates: HashMap<_, _> = report
                .filters
                .iter()
                .map(|filter| (filter.expression_id, &filter.expression))
                .collect();
            let Ok(consumers) = discover_dynamic_filter_consumers(variant) else {
                continue;
            };
            for consumer in consumers {
                let Some(proto) = updates.get(&consumer.id).copied() else {
                    continue;
                };
                let Ok(reported_expression) =
                    decode_physical_expr(proto, consumer.input_schema.as_ref(), task_ctx)
                else {
                    continue;
                };
                let Some(reported_dynamic_filter) =
                    reported_expression.downcast_ref::<DynamicFilterPhysicalExpr>()
                else {
                    continue;
                };
                let Ok(expression) = reported_dynamic_filter.current() else {
                    continue;
                };
                let Some(dynamic_filter) = consumer
                    .expression
                    .downcast_ref::<DynamicFilterPhysicalExpr>()
                else {
                    continue;
                };
                if dynamic_filter.update(expression).is_ok() {
                    dynamic_filter.mark_complete();
                }
            }
        }

        Ok(TreeNodeRecursion::Continue)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, lit,
    };
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::prelude::SessionContext;
    use uuid::Uuid;

    #[test]
    fn visualization_variants_do_not_share_dynamic_filter_state() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let column = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&column)],
            lit(true),
        )) as Arc<dyn PhysicalExpr>;
        let input = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;
        let variant = Arc::new(FilterExec::try_new(
            Arc::clone(&dynamic_filter),
            Arc::clone(&input),
        )?) as Arc<dyn ExecutionPlan>;
        let leaf = Arc::new(DistributedLeafExec::try_new(
            Arc::clone(&variant),
            [Arc::clone(&variant), variant],
        )?) as Arc<dyn ExecutionPlan>;

        let task_ctx = SessionContext::new().task_ctx();
        let isolated = isolate_distributed_leaf_variants_for_display(leaf, &task_ctx)?;
        let expression =
            Arc::new(BinaryExpr::new(column, Operator::Gt, lit(10_i32))) as Arc<dyn PhysicalExpr>;
        dynamic_filter
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap()
            .update(expression)?;
        dynamic_filter
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap()
            .mark_complete();
        let report = TaskCompletedDynamicFilters {
            filters: vec![crate::TaskDynamicFilter {
                expression_id: dynamic_filter.expression_id().unwrap(),
                expression: crate::codec::encode_physical_expr(&dynamic_filter, &task_ctx)?,
            }],
        };
        let reports = HashMap::from_iter([(
            TaskKey {
                query_id: Uuid::nil(),
                stage_id: 1,
                task_number: 0,
            },
            report,
        )]);

        apply_reports_to_distributed_leaves(&isolated, &reports, &task_ctx);
        let leaf = isolated.downcast_ref::<DistributedLeafExec>().unwrap();
        let task_0 = displayable(leaf.variants()[0].as_ref())
            .one_line()
            .to_string();
        let task_1 = displayable(leaf.variants()[1].as_ref())
            .one_line()
            .to_string();
        assert!(task_0.contains("DynamicFilter [ a@0 > 10 ]"));
        assert!(task_1.contains("DynamicFilter [ empty ]"));
        Ok(())
    }
}
