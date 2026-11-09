use datafusion::common::{Result, ScalarValue, internal_err, not_impl_err, plan_err};
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::expressions::Literal;
use std::sync::Arc;

/// Validate the shared properties before using the first task as a representative.
pub(super) fn validate_input_properties(
    properties: &[Arc<PlanProperties>],
    tasks: usize,
) -> Result<&Arc<PlanProperties>> {
    if properties.len() != tasks {
        return internal_err!(
            "Expected properties for {tasks} input tasks, got {}",
            properties.len()
        );
    }
    let Some((first, rest)) = properties.split_first() else {
        return plan_err!("A network boundary must have at least one input task");
    };
    for (task, props) in rest.iter().enumerate() {
        for (property, matches) in [
            (
                "partition count",
                props.partitioning.partition_count() == first.partitioning.partition_count(),
            ),
            (
                "schema",
                props.eq_properties.schema() == first.eq_properties.schema(),
            ),
            (
                "ordering",
                props.output_ordering() == first.output_ordering(),
            ),
            ("emission type", props.emission_type == first.emission_type),
            ("boundedness", props.boundedness == first.boundedness),
        ] {
            if !matches {
                return plan_err!(
                    "Input task {} has a different {property} from task 0",
                    task + 1
                );
            }
        }
    }
    Ok(first)
}

pub(super) fn scale_partitioning_props(
    props: &Arc<PlanProperties>,
    f: impl FnOnce(usize) -> usize,
) -> Result<Arc<PlanProperties>> {
    let partitions = f(props.partitioning.partition_count());
    let partitioning = if partitions == props.partitioning.partition_count() {
        props.partitioning.clone()
    } else {
        // Concatenating task partitions does not rehash rows or create new range boundaries.
        Partitioning::UnknownPartitioning(partitions)
    };
    Ok(Arc::new(PlanProperties::new(
        props.eq_properties.clone(),
        partitioning,
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
