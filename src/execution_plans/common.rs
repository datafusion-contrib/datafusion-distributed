use datafusion::common::{Result, not_impl_err};
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
        Partitioning::Range(_) => not_impl_err!(
            "scaling up range partitioned data is not supported. See https://github.com/datafusion-contrib/datafusion-distributed/issues/628"
        ),
    }
}
