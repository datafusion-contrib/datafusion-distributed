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
        Partitioning::Range(range) => {
            // Range partition count is defined by the split points. Changing the
            // count (for example concatenating several range-partitioned tasks in
            // NetworkCoalesceExec) does not produce a valid Range of the new size,
            // so keep Range only when the count is unchanged.
            let new_count = f(range.partition_count());
            if new_count == range.partition_count() {
                Partitioning::Range(range.clone())
            } else {
                Partitioning::UnknownPartitioning(new_count)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{ScalarValue, SplitPoint};
    use datafusion::physical_expr::{PhysicalSortExpr, RangePartitioning, expressions::col};
    use datafusion::physical_expr_common::sort_expr::LexOrdering;

    fn range_partitioning(split_count: usize) -> Partitioning {
        let schema = Schema::new(vec![Field::new("k", DataType::Int32, false)]);
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(col("k", &schema).unwrap())]).unwrap();
        let split_points = (1..=split_count)
            .map(|i| SplitPoint::new(vec![ScalarValue::Int32(Some(i as i32 * 10))]))
            .collect();
        Partitioning::Range(RangePartitioning::try_new(ordering, split_points).unwrap())
    }

    #[test]
    fn scale_partitioning_preserves_range_when_count_is_unchanged() {
        let range = range_partitioning(2);
        let scaled = scale_partitioning(&range, |p| p);
        assert_eq!(scaled, range);
        assert_eq!(scaled.partition_count(), 3);
    }

    #[test]
    fn scale_partitioning_degrades_range_when_count_changes() {
        let range = range_partitioning(2);
        let scaled = scale_partitioning(&range, |p| p * 2);
        assert!(matches!(scaled, Partitioning::UnknownPartitioning(6)));
    }

    #[test]
    fn scale_partitioning_still_scales_hash() {
        let schema = Schema::new(vec![Field::new("k", DataType::Int32, false)]);
        let hash = Partitioning::Hash(vec![col("k", &schema).unwrap()], 4);
        let scaled = scale_partitioning(&hash, |p| p * 2);
        assert!(matches!(scaled, Partitioning::Hash(_, 8)));
    }
}
