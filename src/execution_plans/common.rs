use datafusion::common::{Result, ScalarValue, not_impl_err};
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::expressions::Literal;
use std::sync::Arc;

pub(super) fn scale_partitioning_props(
    props: &Arc<PlanProperties>,
    f: impl FnOnce(usize) -> usize,
) -> Result<Arc<PlanProperties>> {
    if let Partitioning::Range(range) = &props.partitioning {
        // Declare UnknownPartitioning when coalescing range-partitioned data.
        //
        // Consider the partitions A-H with split points 10, 20, 30, 40 etc.
        //
        //   A |10| B |20| C |30| D |40| E |50| F |60| G |70| H
        //
        // After splitting into two tasks, we may have the following:
        //
        //    Task 0 Range(4): [A |10| B |20| C |30| D]
        //    Task 1 Range(4): [E |50| F |60| G |70| H]
        //
        // The task-local metadata no longer contains split point 40, so we cannot
        // reconstruct Range(8) from it. For now, declare UnknownPartitioning.
        return Ok(Arc::new(props.as_ref().clone().with_partitioning(
            Partitioning::UnknownPartitioning(f(range.partition_count())),
        )));
    }

    Ok(Arc::new(PlanProperties::new(
        props.eq_properties.clone(),
        scale_partitioning(&props.partitioning, f)?,
        props.emission_type,
        props.boundedness,
    )))
}

/// Returns a new Hash partitioning with `salt` appended to the expressions and the partition
/// count set to `consumer_task_count`. The salt breaks hash correlation that would skew
/// distribution when `consumer_task_count` and the producer's partition count share a common factor.
pub(super) fn salted_partitioning(
    partitioning: &Partitioning,
    salt: u64,
    consumer_task_count: usize,
) -> Result<Partitioning> {
    match partitioning {
        Partitioning::Hash(exprs, _) => {
            let salt_lit: Arc<dyn PhysicalExpr> =
                Arc::new(Literal::new(ScalarValue::UInt64(Some(salt))));
            let mut salted_exprs = exprs.clone();
            salted_exprs.push(salt_lit);
            Ok(Partitioning::Hash(salted_exprs, consumer_task_count))
        }
        _ => not_impl_err!("salted_partitioning only supports Hash partitioning"),
    }
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
