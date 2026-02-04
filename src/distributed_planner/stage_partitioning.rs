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
use std::borrow::Cow;
use std::sync::Arc;
/// Stage partitioning is the same core concept as partitioning in single-node DataFusion: some
/// subset of data that is distributed in a defined fashion.
///
/// Stage partitioning comes in three variants:
///     1. Unspecified: The distirbution is unknown.
///     2. Single: There is one task in the stage, thus it is partitioned by all the keys in the
///        data.
///     3. Hash(Vec<Arc<dyn PhysicalExpr>>): Each task in this stage contains a disjoint set of the
///        provides keys.
///
/// Stage partitioning enables several optimizations by eliminating unnecessary network shuffles
/// shuffles when data is already partitioned correctly across tasks.
///
/// ## 1. Aggregation with Pre-Partitioned Data
///
/// When a DataSource exposes hash partitioning and an aggregate operation requires the same or
/// a superset of those keys, the shuffle can be avoided. The example at the bottom of this doc
/// demonstrates this: DataSource partitioned by `[a]`, aggregation groups by `[a]`. Since all
/// rows with the same `a` value are in the same task, the RepartitionExec does not need to
/// insert a shuffle.
///
/// This also works with subset matching: if data is partitioned by `[a]` and aggregation
/// groups by `[a, b]`, the shuffle can still be skipped since all rows with the same
/// `a` are already co-located in the same task.
///
/// ## 2. Multiple Sequential Aggregations
///
/// When performing multiple aggregations on the same keys, stage partitioning prevents redundant
/// shuffles. After the first aggregation by `[a]` establishes `Hash([a])` stage
/// partitioning, a second aggregation by `[a]` sees the data is already partitioned
/// correctly and skips the shuffle entirely.
///
/// ## 3. Join Optimizations
///
/// ### Inner Join
/// When both join inputs are already partitioned by their join keys (e.g., left has
/// `Hash([a])` and right has `Hash([a])`), an inner join on `a` maintains
/// the partitioning. Both sides are already co-located, so the output maintains `Hash([a])`
/// and subsequent operations on `a` can skip shuffles.
///
/// ### Semi/Anti Joins
/// For semi and anti joins, only the filtered side's partitioning matters. A left semi join
/// preserves the left side's partitioning, and a right semi join preserves the right side's
/// partitioning. This is useful when one side acts as a filter and doesn't need full co-location.
///
/// ### Broadcast Joins (CollectLeft)
/// When the left side is broadcast to all tasks, the right side maintains its partitioning and
/// the output inherits the right side's stage partitioning. This is useful for dimension table
/// joins where the large fact table stays partitioned.
///
/// ### Full Outer Joins
/// Full outer joins cannot preserve partitioning because unmatched rows from either side are
/// NULL-padded, breaking the hash partitioning guarantee. NULL values don't belong to any
/// specific hash partition.
///
/// ## 4. Projection and Filter Passthrough
///
/// Operators that don't change row-to-task assignment preserve stage partitioning:
/// - **Projections**: If partition keys remain in the output (with possibly remapped column
///   indices), partitioning is preserved. If keys are dropped, partitioning becomes Unspecified.
/// - **Filters**: Row filtering doesn't change which task rows belong to.
/// - **Sorts**: Sorting within each task doesn't affect cross-task distribution.
/// - **Limits**: Local and global limits preserve partitioning guarantees.
///
/// ## 5. Aggregate Subset Matching vs Partitioned Hash Joins
///
/// Stage partitioning allows subset matching for aggregates but requires exact matching for
/// partitioned hash joins:
/// - **Aggregates**: Stage partitioned by `Hash([a])` satisfies requirement `Hash([a, b])`
///   because all rows with the same `a` are co-located, making `(a, b)` grouping safe.
/// - **Partitioned Hash Joins**: Require exact matching on all join keys because both sides
///   must align precisely to ensure correct co-location and join semantics.
///
/// ## 6. Expression-Based Partitioning
///
/// Stage partitioning tracks complex expressions, not just column references. If a DataSource
/// is partitioned by `[a + b]` and an aggregate groups by `[a + b]`, the
/// shuffle can be elided if the expressions match exactly (after normalization via equivalence
/// properties).
///
/// ```text
///                ┌───────────────────────────┐
///                │      DistributedExec      │
///                │┌─────────────────────────┐│
///                ││                         ││
///                ││   CoalescePartitions    ││
///                ││                         ││
///                │└────────────▲────────────┘│
///                │             │             │
///                │┌────────────┴────────────┐│
///                ││                         ││
///                ││     NetworkCoalesce     ││
///                ││                         ││
///                │└────────▲───────▲────────┘│
///                └─────────┼───────┼─────────┘
///                          │       │
///               ┌──────────┘       └───────────┐
/// ┌─────────────┼──────────────────────────────┼─────────────┐
/// │             │           Stage 1            │             │
/// │             │  stage_partitoning=Hash(a)   │             │
/// │                                                          │
/// │         Worker 0                       Worker 1          │
/// │                                                          │
/// │┌─────────────────────────┐    ┌─────────────────────────┐│
/// ││                         │    │                         ││
/// ││       Projection        │    │       Projection        ││
/// ││                         │    │                         ││
/// │└────────────▲────────────┘    └────────────▲────────────┘│
/// │             │                              │             │
/// │┌────────────┴────────────┐    ┌────────────┴────────────┐│
/// ││   Aggregation (Final)   │    │   Aggregation (Final)   ││
/// ││          gby=a          │    │          gby=a          ││
/// ││                         │    │                         ││
/// │└─────▲──────▲──────▲─────┘    └─────▲──────▲──────▲─────┘│
/// │      │      │      │                │      │      │      │
/// │┌─────┴──────┴──────┴─────┐    ┌─────┴──────┴──────┴─────┐│
/// ││       Repartition       │    │       Repartition       ││
/// ││   partitoning=Hash(a)   │    │   partitoning=Hash(a)   ││
/// ││                         │    │                         ││
/// │└─────▲──────▲──────▲─────┘    └─────▲──────▲──────▲─────┘│
/// │      │      │      │                │      │      │      │
/// │┌─────┴──────┴──────┴─────┐    ┌─────┴──────┴──────┴─────┐│
/// ││  Aggregation (Partial)  │    │  Aggregation (Partial)  ││
/// ││          gby=a          │    │          gby=a          ││
/// ││                         │    │                         ││
/// │└─────▲──────▲──────▲─────┘    └─────▲──────▲──────▲─────┘│
/// │      │      │      │                │      │      │      │
/// │┌─────┴──────┴──────┴─────┐    ┌─────┴──────┴──────┴─────┐│
/// ││       DataSource        │    │       DataSource        ││
/// ││ouput_partitoning=Hash(a)│    │ouput_partitoning=Hash(a)││
/// ││                         │    │                         ││
/// │└─────────────────────────┘    └─────────────────────────┘│
/// └──────────────────────────────────────────────────────────┘
/// ```
///
pub(super) fn stage_partitioning_for_plan(
    plan: &Arc<dyn ExecutionPlan>,
    children: &[StagePartitioning],
) -> Result<StagePartitioning, DataFusionError> {
    if children.is_empty() {
        return Ok(StagePartitioning::Unspecified);
    }
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        // Projection can preserve partitioning if keys are still present/referenced
        let child = children.first().unwrap_or(&StagePartitioning::Unspecified);
        return Ok(map_or_unspecified(child, |keys| {
            map_projection_keys(keys, projection)
        }));
    }

    if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() {
        // Aggregates preserve partitioning by group keys (if they can be mapped)
        let child = children.first().unwrap_or(&StagePartitioning::Unspecified);
        return Ok(map_or_unspecified(child, |keys| {
            map_aggregate_keys(keys, aggregate)
        }));
    }

    if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        // Hash joins can preserve partitioning only from output-projected sides
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
        // Single task stages are trivially disjoint, so any hash requirement is satisfied
        StagePartitioning::Single => true,
        StagePartitioning::Hash(stage_keys) => {
            if stage_keys.is_empty() || required_keys.is_empty() {
                return false;
            }

            // Avoid DF's "partition_count == 1 satisfies any hash" shortcut here:
            // stage-scoped partitioning is about task-level disjointness, not local partitions
            let normalized_stage = normalize_keys(eq_properties, stage_keys);
            let normalized_required = normalize_keys(eq_properties, required_keys);

            if physical_exprs_equal(normalized_stage.as_ref(), normalized_required.as_ref()) {
                return true;
            }

            // Some operators can accept subset satisfaction (e.g. aggregate on a superset key)
            allow_subset
                && is_subset_partitioning(normalized_stage.as_ref(), normalized_required.as_ref())
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
    let (mut allow_left, allow_right) = join_allowed_sides(join_type);

    match join.mode {
        PartitionMode::Partitioned => {}
        PartitionMode::CollectLeft => allow_left = false,
        // Auto mode may re-plan and change distribution
        PartitionMode::Auto => return Ok(StagePartitioning::Unspecified),
    }

    let left_len = join.left().schema().fields().len();
    let output_right_offset = match join_type {
        // Right-only outputs don't include left columns, so no offset
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => 0,
        // All other joins include left columns first.
        _ => left_len,
    };

    let left_keys = match children.first() {
        // Only hash partitioning guarantees disjointness by key
        Some(StagePartitioning::Hash(keys)) => Some(keys),
        _ => None,
    };
    let right_keys = match children.get(1) {
        // Only hash partitioning guarantees disjointness by key
        Some(StagePartitioning::Hash(keys)) => Some(keys),
        _ => None,
    };

    let left_mapped = map_join_side(left_keys, allow_left, 0, join)?;
    let right_mapped = map_join_side(right_keys, allow_right, output_right_offset, join)?;

    Ok(match (left_mapped, right_mapped) {
        (Some(left), Some(right)) => {
            // Only keep hash partitioning if both sides align on the same keys
            if physical_exprs_equal(&left, &right) {
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

fn join_allowed_sides(join_type: JoinType) -> (bool, bool) {
    match join_type {
        // Output includes both sidesk
        JoinType::Inner => (true, true),
        // Output includes only the left side
        JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            (true, false)
        }
        // Output includes only the right side
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            (false, true)
        }
        // Output includes both sides but nulls can be introduced on either side
        JoinType::Full => (false, false),
    }
}

fn map_join_side(
    keys: Option<&Vec<Arc<dyn PhysicalExpr>>>,
    allow: bool,
    offset: usize,
    join: &HashJoinExec,
) -> Result<Option<Vec<Arc<dyn PhysicalExpr>>>, DataFusionError> {
    if !allow {
        // If the join output doesn't include this side, its partitioning cannot survive
        return Ok(None);
    }
    match keys {
        // Remap input key expressions into the join's output schema
        Some(keys) => map_join_keys(keys, offset, join),
        None => Ok(None),
    }
}

fn normalize_keys<'a>(
    eq_properties: &'a EquivalenceProperties,
    keys: &'a [Arc<dyn PhysicalExpr>],
) -> Cow<'a, [Arc<dyn PhysicalExpr>]> {
    let eq_group = eq_properties.eq_group();
    if eq_group.is_empty() {
        return Cow::Borrowed(keys);
    }
    Cow::Owned(
        keys.iter()
            .map(|expr| eq_group.normalize_expr(Arc::clone(expr)))
            .collect(),
    )
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
            if proj_expr.expr.as_ref().eq(key.as_ref()) {
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

        // If any column reference is missing after projection, the key is not preserved
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
            if expr.as_ref().eq(key.as_ref()) {
                found = Some(Arc::clone(&output_exprs[idx]));
                break;
            }
        }
        // If any key isn't part of the grouping, partitioning can't be preserved
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

fn physical_exprs_equal(left: &[Arc<dyn PhysicalExpr>], right: &[Arc<dyn PhysicalExpr>]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .all(|(l, r)| l.as_ref().eq(r.as_ref()))
}

fn is_subset_partitioning(
    subset: &[Arc<dyn PhysicalExpr>],
    superset: &[Arc<dyn PhysicalExpr>],
) -> bool {
    if subset.is_empty() || subset.len() >= superset.len() {
        return false;
    }

    // Subset check is order-insensitive: every subset key must exist in superset
    subset.iter().all(|subset_expr| {
        superset
            .iter()
            .any(|superset_expr| subset_expr.as_ref().eq(superset_expr.as_ref()))
    })
}

fn map_or_unspecified(
    stage: &StagePartitioning,
    map_fn: impl FnOnce(&[Arc<dyn PhysicalExpr>]) -> Option<Vec<Arc<dyn PhysicalExpr>>>,
) -> StagePartitioning {
    match stage {
        StagePartitioning::Hash(keys) => map_fn(keys)
            .map(StagePartitioning::Hash)
            .unwrap_or(StagePartitioning::Unspecified),
        StagePartitioning::Single => StagePartitioning::Single,
        StagePartitioning::Unspecified => StagePartitioning::Unspecified,
    }
}
