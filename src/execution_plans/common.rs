use crate::DistributedConfig;
use datafusion::common::{DataFusionError, plan_err};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use http::HeaderMap;
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

/// Manual propagation of the [DistributedConfig] fields relevant for execution. Can be removed
/// after https://github.com/datafusion-contrib/datafusion-distributed/issues/247 is fixed, as this will become automatic.
pub(super) fn manually_propagate_distributed_config(
    mut headers: HeaderMap,
    d_cfg: &DistributedConfig,
) -> HeaderMap {
    headers.insert(
        "distributed.collect_metrics",
        d_cfg.collect_metrics.to_string().parse().unwrap(),
    );
    headers
}
