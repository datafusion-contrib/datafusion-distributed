use crate::codec::roundtrip_pb;
use crate::common::TreeNodeExt;
use crate::coordinator::DistributedExec;
use crate::dynamic_filtering::{
    apply_dynamic_filter_update, discover_dynamic_filter_consumers,
    discover_dynamic_filter_producers,
};
use crate::execution_plans::DistributedLeafExec;
use crate::stage::{Stage, find_all_stages};
use crate::{DistributedTaskContext, MaybeEncoded, TaskDynamicFilter, TaskKey};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
};
use std::sync::Arc;

/// Rewrites an executed distributed plan with dynamic filters from the coordinator and its
/// completed worker tasks.
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

    // Collect producer schemas so we can decode collected dynamic filters.
    //
    // Note that we display dynamic filters on consumers but decode them with respect
    // to the producer schemas. When updating the consumers, they automatically remap
    // the expressions to their own schema.
    let mut producers = discover_dynamic_filter_producers(&plan_for_viz)?;
    plan_for_viz.apply(|node| {
        if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() {
            producers.extend(discover_dynamic_filter_producers(leaf.original())?);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let producer_schemas = producers
        .into_iter()
        .map(|producer| (producer.id, producer.input_schema))
        .collect();

    // Avoids mutating the `plan_for_viz` of the incoming DistributedExec.
    let plan_for_viz =
        sever_dynamic_filter_relationships_in_plan_for_display(plan_for_viz, task_ctx)?;

    // The head stage executes on the coordinator, so its filters have no worker report.
    // Copy their live values into the isolated display variants before restoring worker stages.
    let head_filters: Vec<_> = discover_dynamic_filter_consumers(&distributed_exec.head_stage()?)?
        .consumers
        .into_iter()
        .filter(|consumer| consumer.expression.snapshot_generation() > 1)
        .map(|consumer| TaskDynamicFilter {
            expression_id: consumer.id,
            expression: MaybeEncoded::Decoded(consumer.expression),
        })
        .collect();
    apply_filters_to_distributed_leaves(
        &plan_for_viz,
        DistributedTaskContext {
            task_index: 0,
            task_count: 1,
        },
        &head_filters,
        &producer_schemas,
        task_ctx,
    )?;

    for stage in find_all_stages(&plan_for_viz) {
        let Stage::Local(stage) = stage else {
            return internal_err!("expected local stages in the visualization plan");
        };
        for task_number in 0..stage.tasks {
            let task_key = TaskKey {
                query_id: stage.query_id,
                stage_id: stage.num,
                task_number,
            };
            let Some(report) = reports.get(&task_key) else {
                continue;
            };
            apply_filters_to_distributed_leaves(
                &stage.plan,
                DistributedTaskContext {
                    task_index: task_number,
                    task_count: stage.tasks,
                },
                &report.filters,
                &producer_schemas,
                task_ctx,
            )?;
        }
    }
    let plan = distributed_exec.with_plan_for_viz(Arc::clone(&plan_for_viz))?;
    plan.replace_children(
        vec![plan_for_viz],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

/// Severs dynamic filter connections so we can update filter values for
/// display purposes without having an update in one node propagate to another.
///
/// Unlike [`maybe_roundtrip_plan_to_sever_in_memory_dynamic_filter_relationships()`], this
/// isolates every dynamic filter found in the plan.
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
///          t0: DataSourceExec:  predicate=DynamicFilter [ f_dkey@2 > A ] <- unique filter
///          t1: DataSourceExec:  predicate=DynamicFilter [ f_dkey@2 < B ] <- unique filter
/// ```
///
/// This is done by deep-copying every leaf variant so we don't have to
/// worry about any shared state.
///
/// [`update()`]: DynamicFilterPhysicalExpr::update()
/// [`maybe_roundtrip_plan_to_sever_in_memory_dynamic_filter_relationships()`]: super::maybe_roundtrip_plan_to_sever_in_memory_dynamic_filter_relationships()
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

/// Applies one task's filters without crossing network boundaries into other stages.
fn apply_filters_to_distributed_leaves(
    plan: &Arc<dyn ExecutionPlan>,
    d_ctx: DistributedTaskContext,
    filters: &[TaskDynamicFilter],
    producer_schemas: &HashMap<u64, SchemaRef>,
    task_ctx: &Arc<TaskContext>,
) -> Result<()> {
    let updates: HashMap<_, _> = filters
        .iter()
        .map(|filter| (filter.expression_id, &filter.expression))
        .collect();
    // Union children can use different task indices from their enclosing stage.
    plan.apply_with_dt_ctx(d_ctx, |node, d_ctx| {
        let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let Some(variant) = leaf.variants().get(d_ctx.task_index) else {
            return internal_err!("missing leaf variant {} for {d_ctx:?}", d_ctx.task_index);
        };
        let discovered = discover_dynamic_filter_consumers(variant)?;
        for consumer in discovered.consumers {
            let Some(expression) = updates.get(&consumer.id).copied() else {
                continue;
            };
            let Some(producer_schema) = producer_schemas.get(&consumer.id) else {
                return internal_err!("missing producer schema for dynamic filter {}", consumer.id);
            };
            apply_dynamic_filter_update(
                &consumer.expression,
                expression,
                producer_schema.as_ref(),
                task_ctx,
            )?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}
