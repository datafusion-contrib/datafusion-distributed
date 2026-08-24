use crate::codec::{decode_physical_expr, dynamic_filter_update_target, roundtrip_pb};
use crate::coordinator::DistributedExec;
use crate::dynamic_filtering::discover_dynamic_filter_consumers;
use crate::execution_plans::DistributedLeafExec;
use crate::{TaskCompletedDynamicFilters, TaskKey};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
};
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use std::sync::Arc;

/// Rewrites an executed distributed plan with the dynamic filters reported by its completed
/// worker tasks.
///
/// When composing this with [`crate::rewrite_distributed_plan_with_metrics`], dynamic filters must
/// be rewritten first.
/// `task_ctx` must have the same session configuration and codecs as the context used to execute
/// the plan.
pub async fn rewrite_distributed_plan_with_dynamic_filters(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<TaskContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(distributed_exec) = plan.downcast_ref::<DistributedExec>() else {
        return Ok(plan);
    };

    if distributed_exec.completed_dynamic_filter_store.is_none() {
        return Ok(plan);
    }
    let plan_for_viz = distributed_exec.plan_for_viz()?;
    let Some(reports) = distributed_exec.wait_for_dynamic_filters().await else {
        return internal_err!("dynamic filters were enabled but the execution was not prepared");
    };
    // Avoids mutating the `plan_for_viz` of the incoming DistributedExec.
    let plan_for_viz =
        sever_dynamic_filter_relationships_in_plan_for_display(plan_for_viz, task_ctx)?;
    apply_reports_to_distributed_leaves(&plan_for_viz, &reports, task_ctx);
    let plan = distributed_exec.with_plan_for_viz(Arc::clone(&plan_for_viz))?;
    plan.replace_children(
        vec![plan_for_viz],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

/// Severs dynamic filter connections so we can update filter values for
/// display purposes without having an update in one node propagate to another.
///
/// For example, in this plan, we would like to be able to [`update()`] every variant independently
/// without mutating the producer or other variants
///
/// ```text
///  RepartitionExec:
///    AggregateExec: mode=Partial
///      HashJoinExec: mode=Partitioned
///        DistributedLeafExec:
///          t0: DataSourceExec: ...
///          t1: DataSourceExec: ...
///        DistributedLeafExec:
///          t0: DataSourceExec:  predicate=DynamicFilter [ f_dkey@2 >= A AND f_dkey@2 <= A AND f_dkey@2 IN (SET) ([<values>]) ] <- unique filter
///          t1: DataSourceExec:  predicate=DynamicFilter [ f_dkey@2 >= B AND f_dkey@2 <= B AND f_dkey@2 IN (SET) ([<values>]) ] <- unique filter
/// ```
///
/// This is done by deep-copying every leaf variant so we don't have to
/// worry about any shared state.
///
/// [`update()`]: DynamicFilterPhysicalExpr::update()
pub(crate) fn sever_dynamic_filter_relationships_in_plan_for_display(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<TaskContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() else {
            return Ok(Transformed::no(node));
        };

        let variants = leaf
            .variants()
            .iter()
            .map(|variant| {
                let variant = roundtrip_pb(Arc::clone(variant), task_ctx)?;
                // The variant can have a SortExec
                isolate_sort_dynamic_filters_for_display(variant, task_ctx)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Transformed::yes(Arc::new(DistributedLeafExec::try_new(
            Arc::clone(leaf.original()),
            variants,
        )?) as Arc<dyn ExecutionPlan>))
    })
    // Handle SortExec nodes not inside variants.
    .and_then(|transformed| isolate_sort_dynamic_filters_for_display(transformed.data, task_ctx))
}

/// Deep-copies dynamic-filter-producing [`SortExec`]s by doing a proto roundtrip. Some producers
/// like [`SortExec`] display their dynamic filters, so we need to explicitly handle
/// displaying different dynamic filters for each task containing a [`SortExec`]. For now, we
/// just clear the dynamic filter and don't display it.
///
/// To avoid serializing an entire subtree, we swap in an [`EmptyExec`]:
///
/// ```text
/// SortExec           SortExec          SortExec
///   ...children  ->    EmptyExec  ->     ...children
/// ```
///
/// TODO(#677): display producer dynamic filters
fn isolate_sort_dynamic_filters_for_display(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<TaskContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(sort) = node.downcast_ref::<SortExec>() else {
            return Ok(Transformed::no(node));
        };
        if node.dynamic_expressions_produced().is_empty() {
            return Ok(Transformed::no(node));
        }

        let input = Arc::clone(sort.input());
        let placeholder = Arc::new(
            EmptyExec::new(input.schema())
                .with_partitions(input.output_partitioning().partition_count()),
        ) as Arc<dyn ExecutionPlan>;
        let recompute = ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute);
        let sort_with_placeholder = node.replace_children(vec![placeholder], recompute)?;

        let isolated = roundtrip_pb(sort_with_placeholder, task_ctx)?;
        let isolated = isolated.replace_children(vec![input], recompute)?;

        Ok(Transformed::yes(isolated))
    })
    .map(|transformed| transformed.data)
}

/// Applies successful worker reports only to the matching task-local visualization variants.
fn apply_reports_to_distributed_leaves(
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
            let Ok(discovered) = discover_dynamic_filter_consumers(variant) else {
                continue;
            };
            for consumer in discovered.consumers {
                let Some(expression) = updates.get(&consumer.id).copied() else {
                    continue;
                };
                let Ok(proto) = expression.to_proto(task_ctx) else {
                    continue;
                };
                let Some(ExprType::DynamicFilter(dynamic_filter_proto)) = proto.expr_type.as_ref()
                else {
                    continue;
                };
                if dynamic_filter_proto.generation <= 1 {
                    continue;
                }
                let Some(predicate) = dynamic_filter_proto.inner_expr.as_deref() else {
                    continue;
                };
                let Ok(predicate) =
                    decode_physical_expr(predicate, consumer.input_schema.as_ref(), task_ctx)
                else {
                    continue;
                };
                let Ok(dynamic_filter) = dynamic_filter_update_target(
                    &consumer.expression,
                    consumer.input_schema.as_ref(),
                    task_ctx,
                ) else {
                    continue;
                };
                let _ = dynamic_filter.update(predicate);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::mock_exec::MockExec;
    use crate::{MaybeEncoded, TaskDynamicFilter};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, lit,
    };
    use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;
    use insta::assert_snapshot;
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
        let isolated = sever_dynamic_filter_relationships_in_plan_for_display(leaf, &task_ctx)?;
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
            filters: vec![TaskDynamicFilter {
                expression_id: dynamic_filter.expression_id().unwrap(),
                expression: MaybeEncoded::Decoded(Arc::clone(&dynamic_filter)),
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
        assert_snapshot!(task_0, @"FilterExec: DynamicFilter [ a@0 > 10 ]");
        assert_snapshot!(task_1, @"FilterExec: DynamicFilter [ empty ]");
        Ok(())
    }

    #[test]
    fn visualization_isolates_sort_producer_filter_without_mutating_original() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        // MockExec has no protobuf representation. A successful isolation therefore proves the
        // real child was replaced before serializing the SortExec.
        let input = Arc::new(MockExec::new_partitioned(vec![vec![], vec![]], schema))
            as Arc<dyn ExecutionPlan>;
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new("a", 0)))])
                .unwrap();
        let sort = Arc::new(
            SortExec::new(ordering, Arc::clone(&input))
                .with_fetch(Some(10))
                .with_preserve_partitioning(true),
        ) as Arc<dyn ExecutionPlan>;
        let produced = sort.dynamic_expressions_produced();
        let dynamic_filter = produced[0]
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap();
        let expression_id = dynamic_filter.expression_id();
        dynamic_filter.update(Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            lit(10_i32),
        )))?;

        let task_ctx = SessionContext::new().task_ctx();
        let isolated = isolate_sort_dynamic_filters_for_display(Arc::clone(&sort), &task_ctx)?;
        let isolated_sort = isolated.downcast_ref::<SortExec>().unwrap();
        assert!(Arc::ptr_eq(isolated_sort.input(), &input));
        assert_eq!(isolated_sort.fetch(), Some(10));
        assert!(isolated_sort.preserve_partitioning());
        assert_eq!(isolated.schema(), sort.schema());
        assert_eq!(
            isolated.output_partitioning().partition_count(),
            sort.output_partitioning().partition_count()
        );
        assert_eq!(
            isolated.dynamic_expressions_produced()[0].expression_id(),
            expression_id
        );
        assert_snapshot!(
            displayable(isolated.as_ref()).one_line().to_string(),
            @"SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[true], filter=[a@0 > 10]"
        );

        dynamic_filter.update(Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            lit(20_i32),
        )))?;
        assert_snapshot!(
            displayable(sort.as_ref()).one_line().to_string(),
            @"SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[true], filter=[a@0 > 20]"
        );
        assert_snapshot!(
            displayable(isolated.as_ref()).one_line().to_string(),
            @"SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[true], filter=[a@0 > 10]"
        );
        Ok(())
    }
}
