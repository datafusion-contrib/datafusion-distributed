use datafusion::common::{Result, ScalarValue, not_impl_err};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
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

/// Like [scale_partitioning] but appends `Literal(salt)` to `Hash` expressions. Without a salt,
/// consumers computing `hash(row) % N` collide when N and M share a common factor. The salt makes
/// each consumer's partition assignments independent.
pub(super) fn salted_partitioning(
    partitioning: &Partitioning,
    salt: u64,
    new_count: usize,
) -> Result<Partitioning> {
    match partitioning {
        Partitioning::Hash(exprs, _) => {
            let salt_expr: Arc<dyn PhysicalExpr> =
                Arc::new(Literal::new(ScalarValue::UInt64(Some(salt))));
            let mut salted = exprs.clone();
            salted.push(salt_expr);
            Ok(Partitioning::Hash(salted, new_count))
        }
        other => scale_partitioning(other, |_| new_count),
    }
}
