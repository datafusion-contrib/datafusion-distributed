use datafusion::common::Result;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::PlanProperties;
use std::sync::Arc;

pub(super) fn scale_partitioning_props(
    props: &Arc<PlanProperties>,
    f: impl FnOnce(usize) -> usize,
) -> Result<Arc<PlanProperties>> {
    Ok(Arc::new(PlanProperties::new(
        props.eq_properties.clone(),
        scale_partitioning(&props.partitioning, f)?,
        props.emission_type,
        props.boundedness,
    )))
}

pub(super) fn scale_partitioning(
    partitioning: &Partitioning,
    f: impl FnOnce(usize) -> usize,
) -> Result<Partitioning> {
    match &partitioning {
        Partitioning::RoundRobinBatch(p) => Ok(Partitioning::RoundRobinBatch(f(*p))),
        Partitioning::Hash(hash, p) => Ok(Partitioning::Hash(hash.clone(), f(*p))),
        Partitioning::UnknownPartitioning(p) => Ok(Partitioning::UnknownPartitioning(f(*p))),
        // A task-scaled range layout has no representation: the consumer side of a
        // coalesce boundary sees every input task's ranges repeated, and
        // `RangePartitioning` cannot express repeated split points. Cloning the layout
        // unscaled would make the consumer request a partition set the producers never
        // serve, so drop to an unknown layout with the scaled count; the boundary math
        // and the consumer run on counts alone.
        // TODO(#68): carry the range property across the boundary so consumer-side
        // joins can stay co-partitioned.
        Partitioning::Range(range) => Ok(Partitioning::UnknownPartitioning(f(
            range.partition_count()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, RangePartitioning, SplitPoint};

    #[test]
    fn range_partitioning_scales_to_an_unknown_layout() {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            Default::default(),
        )])
        .unwrap();
        let splits = vec![
            SplitPoint::new(vec![ScalarValue::Int64(Some(10))]),
            SplitPoint::new(vec![ScalarValue::Int64(Some(20))]),
        ];
        let range = RangePartitioning::try_new(ordering, splits).unwrap();
        let scaled = scale_partitioning(&Partitioning::Range(range), |p| p * 4);
        assert!(matches!(scaled, Ok(Partitioning::UnknownPartitioning(12))));
    }
}
