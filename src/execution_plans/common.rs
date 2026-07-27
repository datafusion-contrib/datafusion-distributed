use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::PlanProperties;
use std::sync::Arc;

pub(super) fn scale_partitioning_props(
    props: &Arc<PlanProperties>,
    f: impl FnOnce(usize) -> usize,
) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        props.eq_properties.clone(),
        scale_partitioning(&props.partitioning, f),
        props.emission_type,
        props.boundedness,
    ))
}

pub(super) fn scale_partitioning(
    partitioning: &Partitioning,
    f: impl FnOnce(usize) -> usize,
) -> Partitioning {
    match &partitioning {
        Partitioning::RoundRobinBatch(p) => Partitioning::RoundRobinBatch(f(*p)),
        Partitioning::Hash(hash, p) => Partitioning::Hash(hash.clone(), f(*p)),
        Partitioning::UnknownPartitioning(p) => Partitioning::UnknownPartitioning(f(*p)),
        // A range shuffle never reaches a network boundary, so its count is never
        // scaled. If that changes, the caller would misroute against an unscaled
        // count, so fail loud in debug rather than clone silently.
        Partitioning::Range(_) => {
            debug_assert!(
                false,
                "scale_partitioning: range partitioning is not scaled at a boundary"
            );
            partitioning.clone()
        }
    }
}
