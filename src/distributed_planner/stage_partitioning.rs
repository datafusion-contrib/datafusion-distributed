use crate::{PartitionIsolatorExec, StagePartitioning};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, JoinType, plan_datafusion_err};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use std::sync::Arc;

pub(super) fn stage_partitioning_for_plan(
    plan: &Arc<dyn ExecutionPlan>,
    children: &[StagePartitioning],
) -> Result<StagePartitioning, DataFusionError> {
    if children.is_empty() {
        return Ok(StagePartitioning::Unspecified);
    }

    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let child = children
            .first()
            .cloned()
            .unwrap_or(StagePartitioning::Unspecified);
        return Ok(map_or_unspecified(child, |keys| {
            map_projection_keys(keys, projection)
        }));
    }

    if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() {
        let child = children
            .first()
            .cloned()
            .unwrap_or(StagePartitioning::Unspecified);
        return Ok(map_or_unspecified(child, |keys| {
            map_aggregate_keys(keys, aggregate)
        }));
    }

    if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        // Hash joins can preserve partitioning from the allowed side(s)
        return stage_partitioning_for_hash_join(hash_join, children);
    }

    if is_passthrough_partitioning(plan.as_ref()) {
        // Operators that don't change key semantics keep the child's partitioning
        return Ok(children
            .first()
            .cloned()
            .unwrap_or(StagePartitioning::Unspecified));
    }

    Ok(StagePartitioning::Unspecified)
}

pub(super) fn stage_partitioning_covers_hash(
    stage_partitioning: &StagePartitioning,
    partitioning: &Partitioning,
    eq_properties: &EquivalenceProperties,
    allow_subset: bool,
) -> bool {
    let required_keys = match partitioning {
        Partitioning::Hash(keys, _) => keys,
        _ => return false,
    };

    match stage_partitioning {
        StagePartitioning::Unspecified => false,
        // Single task stages are trivially disjoint, so any hash requirement is satisfied.
        StagePartitioning::Single => true,
        StagePartitioning::Hash(stage_keys) => {
            if stage_keys.is_empty() || required_keys.is_empty() {
                return false;
            }

            // Avoid DF's "partition_count == 1 satisfies any hash" shortcut here:
            // stage-scoped partitioning is about task-level disjointness, not local partitions.
            let eq_group = eq_properties.eq_group();
            let normalize = |keys: &[Arc<dyn PhysicalExpr>]| -> Vec<Arc<dyn PhysicalExpr>> {
                if eq_group.is_empty() {
                    return keys.to_vec();
                }
                keys.iter()
                    .map(|expr| eq_group.normalize_expr(Arc::clone(expr)))
                    .collect()
            };

            let normalized_stage = normalize(stage_keys);
            let normalized_required = normalize(required_keys);

            if physical_exprs_equal(&normalized_stage, &normalized_required) {
                return true;
            }

            allow_subset && is_subset_partitioning(&normalized_stage, &normalized_required)
        }
    }
}

fn is_passthrough_partitioning(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().is::<FilterExec>()
        || plan.as_any().is::<SortExec>()
        || plan.as_any().is::<LocalLimitExec>()
        || plan.as_any().is::<GlobalLimitExec>()
        || plan.as_any().is::<CoalesceBatchesExec>()
        || plan.as_any().is::<RepartitionExec>()
        || plan.as_any().is::<PartitionIsolatorExec>()
}

fn stage_partitioning_for_hash_join(
    join: &HashJoinExec,
    children: &[StagePartitioning],
) -> Result<StagePartitioning, DataFusionError> {
    if children.len() < 2 {
        return Ok(StagePartitioning::Unspecified);
    }

    let join_type = *join.join_type();
    let mut allow_left = join_allows_side(join_type, true);
    let allow_right = join_allows_side(join_type, false);

    match join.mode {
        PartitionMode::Partitioned => {}
        PartitionMode::CollectLeft => allow_left = false,
        // Auto mode may re-plan and change distribution
        PartitionMode::Auto => return Ok(StagePartitioning::Unspecified),
    }

    let left_keys = match children.first() {
        Some(StagePartitioning::Hash(keys)) => Some(keys),
        _ => None,
    };
    let right_keys = match children.get(1) {
        Some(StagePartitioning::Hash(keys)) => Some(keys),
        _ => None,
    };

    let left_len = join.left().schema().fields().len();
    let right_only = matches!(
        join_type,
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark
    );
    let output_left_offset = 0;
    let output_right_offset = if right_only { 0 } else { left_len };

    let left_mapped = if allow_left {
        // Left keys need offsetting for the join output schema and projection
        match left_keys {
            Some(keys) => map_join_keys(keys, output_left_offset, join)?,
            None => None,
        }
    } else {
        None
    };
    let right_mapped = if allow_right {
        // Right keys need offsetting for the join output schema and projection
        match right_keys {
            Some(keys) => map_join_keys(keys, output_right_offset, join)?,
            None => None,
        }
    } else {
        None
    };

    Ok(match (left_mapped, right_mapped) {
        (Some(left), Some(right)) => {
            if keys_equal(&left, &right) {
                StagePartitioning::Hash(left)
            } else {
                StagePartitioning::Unspecified
            }
        }
        (Some(left), None) => StagePartitioning::Hash(left),
        (None, Some(right)) => StagePartitioning::Hash(right),
        (None, None) => StagePartitioning::Unspecified,
    })
}

fn join_allows_side(join_type: JoinType, left: bool) -> bool {
    match join_type {
        JoinType::Inner => true,
        JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => left,
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => !left,
        JoinType::Full => false,
    }
}

fn map_projection_keys(
    keys: &[Arc<dyn PhysicalExpr>],
    projection: &ProjectionExec,
) -> Option<Vec<Arc<dyn PhysicalExpr>>> {
    let exprs = projection.expr();
    let output_schema = projection.schema();
    let mut column_map = std::collections::HashMap::new();
    // Map input column indices to projected output indices
    for (idx, proj_expr) in exprs.iter().enumerate() {
        if let Some(col) = proj_expr.expr.as_any().downcast_ref::<Column>() {
            column_map.insert(col.index(), idx);
        }
    }

    let mut mapped = Vec::with_capacity(keys.len());
    for key in keys {
        let mut direct_match = None;
        for (idx, proj_expr) in exprs.iter().enumerate() {
            if physical_expr_eq(&proj_expr.expr, key) {
                direct_match = Some(idx);
                break;
            }
        }
        if let Some(idx) = direct_match {
            let field = output_schema.field(idx);
            mapped.push(Arc::new(Column::new(field.name(), idx)) as _);
            continue;
        }

        // Remap any column references inside the expression to the new indices
        let mut missing = false;
        let remapped = Arc::clone(key)
            .transform_down(|node| {
                if let Some(column) = node.as_any().downcast_ref::<Column>() {
                    let Some(new_index) = column_map.get(&column.index()) else {
                        missing = true;
                        return Ok(Transformed::no(node));
                    };
                    let field = output_schema.fields().get(*new_index).ok_or_else(|| {
                        plan_datafusion_err!(
                            "Column index {new_index} out of bounds for projection schema"
                        )
                    })?;
                    return Ok(Transformed::yes(Arc::new(Column::new(
                        field.name(),
                        *new_index,
                    ))));
                }
                Ok(Transformed::no(node))
            })
            .data()
            .ok()?;

        if missing {
            return None;
        }
        mapped.push(remapped);
    }
    Some(mapped)
}

fn map_aggregate_keys(
    keys: &[Arc<dyn PhysicalExpr>],
    aggregate: &AggregateExec,
) -> Option<Vec<Arc<dyn PhysicalExpr>>> {
    let group_by = aggregate.group_expr();
    let input_exprs = group_by.input_exprs();
    let output_exprs = group_by.output_exprs();

    // Aggregate outputs group keys in output_exprs, match by input expr identity
    let mut mapped = Vec::with_capacity(keys.len());
    for key in keys {
        let mut found = None;
        for (idx, expr) in input_exprs.iter().enumerate() {
            if physical_expr_eq(expr, key) {
                found = Some(Arc::clone(&output_exprs[idx]));
                break;
            }
        }
        mapped.push(found?);
    }
    Some(mapped)
}

fn map_join_keys(
    keys: &[Arc<dyn PhysicalExpr>],
    offset: usize,
    join: &HashJoinExec,
) -> Result<Option<Vec<Arc<dyn PhysicalExpr>>>, DataFusionError> {
    let output_schema = join.schema();
    let join_schema = join.join_schema();

    if let Some(projection) = &join.projection {
        // Join projection compacts/reorders the output, build inverse mapping
        let mut inverse = vec![None; join_schema.fields().len()];
        for (out_idx, in_idx) in projection.iter().enumerate() {
            if *in_idx < inverse.len() {
                inverse[*in_idx] = Some(out_idx);
            }
        }

        let mut mapped = Vec::with_capacity(keys.len());
        for key in keys {
            let mut missing = false;
            let transformed = Arc::clone(key)
                .transform_down(|node| {
                    if let Some(column) = node.as_any().downcast_ref::<Column>() {
                        let pre_index = column.index() + offset;
                        let Some(new_index) = inverse.get(pre_index).and_then(|v| *v) else {
                            missing = true;
                            return Ok(Transformed::no(node));
                        };
                        let field = output_schema.fields().get(new_index).ok_or_else(|| {
                            plan_datafusion_err!(
                                "Column index {new_index} out of bounds for join schema"
                            )
                        })?;
                        return Ok(Transformed::yes(Arc::new(Column::new(
                            field.name(),
                            new_index,
                        ))));
                    }
                    Ok(Transformed::no(node))
                })
                .data()?;
            if missing {
                return Ok(None);
            }
            mapped.push(transformed);
        }
        return Ok(Some(mapped));
    }

    Ok(Some(
        keys.iter()
            .map(|key| shift_expr_columns(Arc::clone(key), offset, &output_schema))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn shift_expr_columns(
    expr: Arc<dyn PhysicalExpr>,
    offset: usize,
    output_schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    // Offset column indices to account for join output layout
    let schema = Arc::clone(output_schema);
    expr.transform_down(|node| {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            let new_index = column.index() + offset;
            let field = schema.fields().get(new_index).ok_or_else(|| {
                plan_datafusion_err!("Column index {new_index} out of bounds for join schema")
            })?;
            return Ok(Transformed::yes(Arc::new(Column::new(
                field.name(),
                new_index,
            ))));
        }
        Ok(Transformed::no(node))
    })
    .data()
}

fn keys_equal(left: &[Arc<dyn PhysicalExpr>], right: &[Arc<dyn PhysicalExpr>]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .all(|(l, r)| physical_expr_eq(l, r))
}

fn physical_exprs_equal(left: &[Arc<dyn PhysicalExpr>], right: &[Arc<dyn PhysicalExpr>]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .all(|(l, r)| physical_expr_eq(l, r))
}

fn is_subset_partitioning(
    subset: &[Arc<dyn PhysicalExpr>],
    superset: &[Arc<dyn PhysicalExpr>],
) -> bool {
    if subset.is_empty() || subset.len() >= superset.len() {
        return false;
    }

    subset.iter().all(|subset_expr| {
        superset
            .iter()
            .any(|superset_expr| physical_expr_eq(subset_expr, superset_expr))
    })
}

fn physical_expr_eq(left: &Arc<dyn PhysicalExpr>, right: &Arc<dyn PhysicalExpr>) -> bool {
    left.as_ref().eq(right.as_ref())
}

fn map_or_unspecified(
    stage: StagePartitioning,
    map_fn: impl FnOnce(&[Arc<dyn PhysicalExpr>]) -> Option<Vec<Arc<dyn PhysicalExpr>>>,
) -> StagePartitioning {
    match stage {
        StagePartitioning::Hash(keys) => map_fn(&keys)
            .map(StagePartitioning::Hash)
            .unwrap_or(StagePartitioning::Unspecified),
        other => other,
    }
}
