use std::sync::Arc;

use datafusion::common::stats::Precision;
use datafusion::common::{JoinSide, JoinType, Result, internal_err};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::{HashJoinExec, utils::build_join_schema};
use datafusion::physical_plan::operator_statistics::{
    ExtendedStatistics, StatisticsProvider, StatisticsResult,
};
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs};

/// Propagates available column byte estimates without changing the operators'
/// row-count, distinct-count or value-statistics models.
///
/// Direct grouping keys and inner hash-join columns retain their average input
/// width, scaled to the estimated output row count. Missing sizes stay unknown.
/// Their totals can be recovered when every output column has a size. Grouping sets,
/// computed group keys and aggregate result columns are left to DataFusion.
///
/// Register through [`StatisticsRegistry::with_providers`] and enable
/// `datafusion.optimizer.use_statistics_registry`. A registry containing only
/// this provider uses the operators' normal cardinality estimates rather than
/// the registry's alternative built-in join estimator.
///
/// This is a uniform-width heuristic, not a memory measurement: in particular,
/// compressed Iceberg column bytes remain compressed-byte estimates.
///
/// [`StatisticsRegistry::with_providers`]: datafusion::physical_plan::operator_statistics::StatisticsRegistry::with_providers
#[derive(Debug, Default)]
pub struct ColumnByteStatisticsProvider;

impl StatisticsProvider for ColumnByteStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        children: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        let inputs: Vec<_> = children
            .iter()
            .map(|stats| Arc::clone(stats.base_arc()))
            .collect();
        let mut stats =
            Arc::unwrap_or_clone(plan.statistics_from_inputs(&inputs, &StatisticsArgs::new())?);
        let mut repaired_columns = false;
        if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
            let groups = aggregate.group_expr();
            if groups.groups().len() == 1 && groups.groups()[0].iter().all(|is_null| !is_null) {
                for (output, (expr, _)) in groups.expr().iter().enumerate() {
                    if let Some(column) = expr.downcast_ref::<Column>() {
                        stats.column_statistics[output].byte_size = scale_bytes(
                            inputs[0].column_statistics[column.index()].byte_size,
                            inputs[0].num_rows,
                            stats.num_rows,
                        );
                        repaired_columns = true;
                    }
                }
            }
        }
        if let Some(join) = plan.downcast_ref::<HashJoinExec>()
            && *join.join_type() == JoinType::Inner
        {
            let (_, columns) = build_join_schema(
                join.left().schema().as_ref(),
                join.right().schema().as_ref(),
                join.join_type(),
            );
            for (output, column_stats) in stats.column_statistics.iter_mut().enumerate() {
                let index = join
                    .projection
                    .as_ref()
                    .map_or(output, |projection| projection[output]);
                let column = &columns[index];
                let input = match column.side {
                    JoinSide::Left => &inputs[0],
                    JoinSide::Right => &inputs[1],
                    JoinSide::None => {
                        return internal_err!("Unexpected synthetic inner-join column");
                    }
                };
                column_stats.byte_size = scale_bytes(
                    input.column_statistics[column.index].byte_size,
                    input.num_rows,
                    stats.num_rows,
                );
            }
            repaired_columns = true;
        }
        if repaired_columns
            && let Some(bytes) = stats
                .column_statistics
                .iter()
                .try_fold(0_usize, |total, column| {
                    total.checked_add(*column.byte_size.get_value()?)
                })
        {
            stats.total_byte_size = Precision::Inexact(bytes);
        }
        Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)))
    }
}

fn scale_bytes(
    bytes: Precision<usize>,
    input_rows: Precision<usize>,
    output_rows: Precision<usize>,
) -> Precision<usize> {
    let Some(output) = output_rows.get_value() else {
        return Precision::Absent;
    };
    if *output == 0 {
        return Precision::Inexact(0);
    }
    let (Some(bytes), Some(input)) = (bytes.get_value(), input_rows.get_value()) else {
        return Precision::Absent;
    };
    if *input == 0 {
        return Precision::Absent;
    }
    let scaled = ((*bytes as u128) * (*output as u128)).div_ceil(*input as u128);
    usize::try_from(scaled)
        .map(Precision::Inexact)
        .unwrap_or(Precision::Absent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{NullEquality, Statistics};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_optimizer::{PhysicalOptimizerRule, join_selection::JoinSelection};
    use datafusion::physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::operator_statistics::StatisticsRegistry;
    use datafusion::physical_plan::statistics::StatisticsContext;
    use datafusion::physical_plan::test::exec::StatisticsExec;
    use datafusion::prelude::SessionConfig;

    #[test]
    fn group_keys_keep_bytes_without_retaining_payload_cost() -> Result<()> {
        let aggregate = distinct_keys(source(10_000, 32, true))?;
        let ordinary =
            StatisticsContext::new().compute(aggregate.as_ref(), &StatisticsArgs::new())?;
        let stats = registry().compute_base(aggregate.as_ref())?;
        assert_eq!(stats.num_rows, ordinary.num_rows);
        assert_eq!(
            stats.column_statistics[0].distinct_count,
            ordinary.column_statistics[0].distinct_count
        );
        assert_eq!(ordinary.column_statistics[0].byte_size, Precision::Absent);
        assert_eq!(
            stats.column_statistics[0].byte_size,
            Precision::Inexact(160_000)
        );
        assert_eq!(stats.total_byte_size, Precision::Inexact(160_000));
        Ok(())
    }

    #[test]
    fn join_selection_can_build_derived_keys_instead_of_fewer_wide_rows() -> Result<()> {
        let keys = inner_join(
            distinct_keys(source(10_000, 32, true))?,
            source(5_000, 32, true),
        )?;
        let join = Arc::new(HashJoinExec::try_new(
            source(1_000, 32, true),
            keys,
            vec![(key(), key())],
            None,
            &JoinType::LeftSemi,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        for (enabled, expected) in [(false, JoinType::LeftSemi), (true, JoinType::RightSemi)] {
            let mut config = SessionConfig::new();
            config.options_mut().optimizer.use_statistics_registry = enabled;
            let state = SessionStateBuilder::new()
                .with_default_features()
                .with_config(config)
                .with_statistics_registry(registry())
                .build();
            let optimized =
                JoinSelection::new().optimize_with_context(Arc::clone(&join), &state)?;
            assert_eq!(
                optimized
                    .downcast_ref::<HashJoinExec>()
                    .unwrap()
                    .join_type(),
                &expected
            );
            assert_eq!(optimized.schema(), join.schema());
        }
        Ok(())
    }

    #[test]
    fn missing_sizes_are_not_invented_and_known_source_totals_are_preserved() -> Result<()> {
        let input = source(10_000, 32, false);
        let original = StatisticsContext::new().compute(input.as_ref(), &StatisticsArgs::new())?;
        assert_eq!(
            registry().compute_base(input.as_ref())?.total_byte_size,
            original.total_byte_size
        );
        let keys = inner_join(distinct_keys(input)?, source(5_000, 32, true))?;
        let stats = registry().compute_base(keys.as_ref())?;
        assert_eq!(stats.column_statistics[0].byte_size, Precision::Absent);
        assert_eq!(stats.total_byte_size, Precision::Absent);
        Ok(())
    }

    #[test]
    fn outer_joins_do_not_get_totals_from_unscaled_input_columns() -> Result<()> {
        for join_type in [JoinType::Left, JoinType::Right, JoinType::Full] {
            let join = HashJoinExec::try_new(
                source(100, 32, true),
                source(200, 32, true),
                vec![(key(), key())],
                None,
                &join_type,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )?;
            assert_eq!(
                registry().compute_base(&join)?.total_byte_size,
                Precision::Absent
            );
        }
        Ok(())
    }

    #[test]
    fn scaling_handles_expansion_empty_inputs_unknowns_and_overflow() {
        assert_eq!(
            scale_bytes(
                Precision::Exact(10),
                Precision::Exact(3),
                Precision::Inexact(10)
            ),
            Precision::Inexact(34)
        );
        assert_eq!(
            scale_bytes(Precision::Absent, Precision::Exact(3), Precision::Exact(0)),
            Precision::Inexact(0)
        );
        assert_eq!(
            scale_bytes(
                Precision::Exact(10),
                Precision::Exact(0),
                Precision::Inexact(10)
            ),
            Precision::Absent
        );
        assert_eq!(
            scale_bytes(
                Precision::Exact(usize::MAX),
                Precision::Exact(1),
                Precision::Inexact(2)
            ),
            Precision::Absent
        );
    }

    fn registry() -> StatisticsRegistry {
        StatisticsRegistry::with_providers(vec![Arc::new(ColumnByteStatisticsProvider)])
    }

    fn source(rows: usize, key_width: usize, known: bool) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, true),
        ]);
        let mut stats = Statistics::new_unknown(&schema);
        stats.num_rows = Precision::Inexact(rows);
        stats.total_byte_size = Precision::Inexact(rows * (key_width + 1024));
        stats.column_statistics[0].distinct_count = Precision::Inexact(rows.min(5_000));
        if known {
            stats.column_statistics[0].byte_size = Precision::Inexact(rows * key_width);
            stats.column_statistics[1].byte_size = Precision::Inexact(rows * 1024);
        }
        Arc::new(StatisticsExec::new(stats, schema))
    }

    fn key() -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new("key", 0))
    }

    fn distinct_keys(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(key(), "key".to_owned())]),
            vec![],
            vec![],
            Arc::clone(&input),
            input.schema(),
        )?))
    }

    fn inner_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HashJoinExec::try_new(
            left,
            right,
            vec![(key(), key())],
            None,
            &JoinType::Inner,
            Some(vec![0]),
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?))
    }
}
