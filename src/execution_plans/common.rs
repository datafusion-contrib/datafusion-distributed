use datafusion::common::{DataFusionError, plan_err};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use std::borrow::Borrow;
use std::sync::Arc;

pub(super) fn require_one_child<L, T>(
    children: L,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>
where
    L: AsRef<[T]>,
    T: Borrow<Arc<dyn ExecutionPlan>>,
{
    let children = children.as_ref();
    if children.len() != 1 {
        return plan_err!("Expected exactly 1 children, got {}", children.len());
    }
    Ok(children[0].borrow().clone())
}

pub(super) fn scale_partitioning_props(
    props: &PlanProperties,
    f: impl FnOnce(usize) -> usize,
) -> PlanProperties {
    PlanProperties::new(
        props.eq_properties.clone(),
        scale_partitioning(&props.partitioning, f),
        props.emission_type,
        props.boundedness,
    )
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
