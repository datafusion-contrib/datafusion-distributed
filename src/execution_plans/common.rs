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
    }
}
