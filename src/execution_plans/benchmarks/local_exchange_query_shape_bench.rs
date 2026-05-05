use crate::LocalExchangeSplitExec;
use arrow::array::Int64Array;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::common::exec_err;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::collect_partitioned;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum QueryBenchShape {
    LowCardinalityFinalAgg,
    HighCardinalityFinalAgg,
    BalancedPartitionedJoin,
    SkewedPartitionedJoin,
    JoinThenRegroupTopK,
    DistinctRegroup,
}

impl Display for QueryBenchShape {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LowCardinalityFinalAgg => write!(f, "low_cardinality_final_agg"),
            Self::HighCardinalityFinalAgg => write!(f, "high_cardinality_final_agg"),
            Self::BalancedPartitionedJoin => write!(f, "balanced_partitioned_join"),
            Self::SkewedPartitionedJoin => write!(f, "skewed_partitioned_join"),
            Self::JoinThenRegroupTopK => write!(f, "join_then_regroup_topk"),
            Self::DistinctRegroup => write!(f, "distinct_regroup"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryBenchProfile {
    Baseline,
    Split,
    Repartition,
}

impl Display for QueryBenchProfile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Baseline => write!(f, "baseline"),
            Self::Split => write!(f, "split"),
            Self::Repartition => write!(f, "repartition"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalExchangeQueryBench {
    pub shape: QueryBenchShape,
    pub profile: QueryBenchProfile,
    pub source_partitions: usize,
    pub base_partitions: usize,
    pub local_partitions: usize,
    pub total_rows: usize,
    pub key_domain: usize,
    pub batch_rows: usize,
}

impl LocalExchangeQueryBench {
    pub fn new(shape: QueryBenchShape, profile: QueryBenchProfile) -> Self {
        Self {
            shape,
            profile,
            source_partitions: 4,
            base_partitions: 4,
            local_partitions: 2,
            total_rows: 250_000,
            key_domain: 16_384,
            batch_rows: 4_096,
        }
    }

    pub fn label(&self) -> String {
        format!(
            "shape={},profile={},source_partitions={},base_partitions={},local_partitions={},total_rows={},key_domain={},batch_rows={}",
            self.shape,
            self.profile,
            self.source_partitions,
            self.base_partitions,
            self.local_partitions,
            self.total_rows,
            self.key_domain,
            self.batch_rows,
        )
    }

    pub fn prepare(&self) -> Result<LocalExchangeQueryFixture> {
        Ok(LocalExchangeQueryFixture {
            bench: self.clone(),
            sources: self.prepare_sources()?,
            task_ctx: SessionStateBuilder::new()
                .with_default_features()
                .build()
                .task_ctx(),
        })
    }

    fn prepare_sources(&self) -> Result<QueryBenchSources> {
        match self.shape {
            QueryBenchShape::LowCardinalityFinalAgg => {
                self.single_source("value", 8, KeyDistribution::Uniform, 0)
            }
            QueryBenchShape::HighCardinalityFinalAgg | QueryBenchShape::DistinctRegroup => {
                self.single_source("value", 64, KeyDistribution::Uniform, 0)
            }
            QueryBenchShape::BalancedPartitionedJoin | QueryBenchShape::JoinThenRegroupTopK => {
                self.join_sources(KeyDistribution::Uniform, self.total_rows, self.key_domain)
            }
            QueryBenchShape::SkewedPartitionedJoin => {
                self.join_sources(KeyDistribution::HotKey, self.total_rows, self.key_domain)
            }
        }
    }

    fn single_source(
        &self,
        value_name: &str,
        grp_domain: usize,
        key_distribution: KeyDistribution,
        value_offset: i64,
    ) -> Result<QueryBenchSources> {
        Ok(QueryBenchSources::Single {
            input: make_fact_source(
                value_name,
                FactSourceSpec {
                    total_rows: self.total_rows,
                    batch_rows: self.batch_rows,
                    source_partitions: self.source_partitions,
                    key_domain: self.key_domain,
                    grp_domain,
                    key_distribution,
                    value_offset,
                },
            )?,
        })
    }

    fn join_sources(
        &self,
        left_distribution: KeyDistribution,
        right_total_rows: usize,
        right_key_domain: usize,
    ) -> Result<QueryBenchSources> {
        Ok(QueryBenchSources::Join {
            left: make_fact_source(
                "left_value",
                FactSourceSpec {
                    total_rows: self.total_rows,
                    batch_rows: self.batch_rows,
                    source_partitions: self.source_partitions,
                    key_domain: self.key_domain,
                    grp_domain: 16,
                    key_distribution: left_distribution,
                    value_offset: 0,
                },
            )?,
            right: make_fact_source(
                "right_value",
                FactSourceSpec {
                    total_rows: right_total_rows,
                    batch_rows: self.batch_rows,
                    source_partitions: self.source_partitions,
                    key_domain: right_key_domain,
                    grp_domain: 16,
                    key_distribution: KeyDistribution::Uniform,
                    value_offset: 17,
                },
            )?,
        })
    }

    fn build_plan(&self, sources: &QueryBenchSources) -> Result<Arc<dyn ExecutionPlan>> {
        match self.shape {
            QueryBenchShape::LowCardinalityFinalAgg => {
                self.build_low_cardinality_final_agg(sources.single()?.exec()?)
            }
            QueryBenchShape::HighCardinalityFinalAgg => {
                self.build_high_cardinality_final_agg(sources.single()?.exec()?)
            }
            QueryBenchShape::BalancedPartitionedJoin | QueryBenchShape::SkewedPartitionedJoin => {
                let (left, right) = sources.join()?;
                self.build_partitioned_join(left.exec()?, right.exec()?)
            }
            QueryBenchShape::JoinThenRegroupTopK => {
                let (left, right) = sources.join()?;
                self.build_join_then_regroup_topk(left.exec()?, right.exec()?)
            }
            QueryBenchShape::DistinctRegroup => {
                self.build_distinct_regroup(sources.single()?.exec()?)
            }
        }
    }

    pub fn with_base_partitions(mut self, base_partitions: usize) -> Self {
        self.base_partitions = base_partitions;
        self
    }

    pub fn with_local_partitions(mut self, local_partitions: usize) -> Self {
        self.local_partitions = local_partitions;
        self
    }

    pub fn with_total_rows(mut self, total_rows: usize) -> Self {
        self.total_rows = total_rows;
        self
    }

    pub fn with_key_domain(mut self, key_domain: usize) -> Self {
        self.key_domain = key_domain;
        self
    }

    fn build_low_cardinality_final_agg(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        build_two_stage_sum_pipeline(
            input,
            &[("grp", 1)],
            ("value", 2),
            self.base_partitions,
            self.local_partitions,
            self.profile,
            None,
        )
    }

    fn build_high_cardinality_final_agg(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        build_two_stage_sum_pipeline(
            input,
            &[("key", 0)],
            ("value", 2),
            self.base_partitions,
            self.local_partitions,
            self.profile,
            None,
        )
    }

    fn build_partitioned_join(
        &self,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        build_partitioned_join_pipeline(
            left,
            right,
            self.base_partitions,
            self.local_partitions,
            self.profile,
        )
    }

    fn build_join_then_regroup_topk(
        &self,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let join = build_partitioned_join_pipeline(
            left,
            right,
            self.base_partitions,
            self.local_partitions,
            QueryBenchProfile::Baseline,
        )?;
        build_two_stage_sum_pipeline(
            join,
            &[("key", 0)],
            ("left_value", 2),
            self.base_partitions,
            self.local_partitions,
            self.profile,
            Some(100),
        )
    }

    fn build_distinct_regroup(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        build_distinct_pipeline(
            input,
            &[("key", 0), ("grp", 1)],
            self.base_partitions,
            self.local_partitions,
            self.profile,
        )
    }
}

impl Display for LocalExchangeQueryBench {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

pub struct LocalExchangeQueryFixture {
    bench: LocalExchangeQueryBench,
    sources: QueryBenchSources,
    task_ctx: Arc<datafusion::execution::TaskContext>,
}

impl LocalExchangeQueryFixture {
    pub async fn run(&self) -> Result<()> {
        let plan = self.bench.build_plan(&self.sources)?;
        let output = collect_partitioned(plan, Arc::clone(&self.task_ctx)).await?;
        let _row_count: usize = output
            .iter()
            .flat_map(|batches| batches.iter())
            .map(|batch| batch.num_rows())
            .sum();
        Ok(())
    }
}

struct PreparedFactSource {
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
}

impl PreparedFactSource {
    fn exec(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            &self.partitions,
            Arc::clone(&self.schema),
            None,
        )?)
    }
}

enum QueryBenchSources {
    Single {
        input: PreparedFactSource,
    },
    Join {
        left: PreparedFactSource,
        right: PreparedFactSource,
    },
}

impl QueryBenchSources {
    fn single(&self) -> Result<&PreparedFactSource> {
        match self {
            Self::Single { input } => Ok(input),
            Self::Join { .. } => exec_err!("benchmark shape expected one input source"),
        }
    }

    fn join(&self) -> Result<(&PreparedFactSource, &PreparedFactSource)> {
        match self {
            Self::Join { left, right } => Ok((left, right)),
            Self::Single { .. } => exec_err!("benchmark shape expected two input sources"),
        }
    }
}

fn build_partitioned_join_pipeline(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    base_partitions: usize,
    local_partitions: usize,
    profile: QueryBenchProfile,
) -> Result<Arc<dyn ExecutionPlan>> {
    let key_exprs = vec![Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>];
    let left = Arc::new(RepartitionExec::try_new(
        left,
        Partitioning::Hash(key_exprs.clone(), base_partitions),
    )?);
    let right = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::Hash(key_exprs.clone(), base_partitions),
    )?);
    let left = apply_local_profile(
        left,
        profile,
        key_exprs.clone(),
        base_partitions,
        local_partitions,
    )?;
    let right = apply_local_profile(
        right,
        profile,
        key_exprs.clone(),
        base_partitions,
        local_partitions,
    )?;
    let on = vec![(
        Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
        Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
    )];
    Ok(Arc::new(HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Partitioned,
        datafusion::common::NullEquality::NullEqualsNothing,
        false,
    )?))
}

fn build_two_stage_sum_pipeline(
    input: Arc<dyn ExecutionPlan>,
    group_cols: &[(&str, usize)],
    value_col: (&str, usize),
    base_partitions: usize,
    local_partitions: usize,
    profile: QueryBenchProfile,
    top_k: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    let partial_group_by = PhysicalGroupBy::new_single(group_cols_to_exprs(group_cols));
    let sum_alias = format!("sum_{}", value_col.0);
    let aggr_expr = vec![Arc::new(
        AggregateExprBuilder::new(
            sum_udaf(),
            vec![Arc::new(Column::new(value_col.0, value_col.1)) as Arc<dyn PhysicalExpr>],
        )
        .schema(Arc::clone(&input_schema))
        .alias(sum_alias.clone())
        .build()?,
    )];
    let partial = Arc::new(AggregateExec::try_new(
        AggregateMode::Partial,
        partial_group_by,
        aggr_expr.clone(),
        vec![None],
        input,
        Arc::clone(&input_schema),
    )?);

    let repartition_exprs = partial_group_columns(group_cols);
    let repartition = Arc::new(RepartitionExec::try_new(
        Arc::clone(&partial) as Arc<dyn ExecutionPlan>,
        Partitioning::Hash(repartition_exprs.clone(), base_partitions),
    )?);
    let fanout = apply_local_profile(
        repartition,
        profile,
        repartition_exprs,
        base_partitions,
        local_partitions,
    )?;

    let final_group_by = PhysicalGroupBy::new_single(group_cols_to_final_exprs(group_cols));
    let final_agg: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
        AggregateMode::FinalPartitioned,
        final_group_by,
        aggr_expr,
        vec![None],
        fanout,
        partial.schema(),
    )?);

    match top_k {
        Some(fetch) => build_topk(
            final_agg,
            group_cols[0].0,
            group_cols.len(),
            &sum_alias,
            fetch,
        ),
        None => Ok(final_agg),
    }
}

fn build_distinct_pipeline(
    input: Arc<dyn ExecutionPlan>,
    group_cols: &[(&str, usize)],
    base_partitions: usize,
    local_partitions: usize,
    profile: QueryBenchProfile,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    let partial_group_by = PhysicalGroupBy::new_single(group_cols_to_exprs(group_cols));
    let partial = Arc::new(AggregateExec::try_new(
        AggregateMode::Partial,
        partial_group_by,
        vec![],
        vec![],
        input,
        Arc::clone(&input_schema),
    )?);

    let repartition_exprs = partial_group_columns(group_cols);
    let repartition = Arc::new(RepartitionExec::try_new(
        Arc::clone(&partial) as Arc<dyn ExecutionPlan>,
        Partitioning::Hash(repartition_exprs.clone(), base_partitions),
    )?);
    let fanout = apply_local_profile(
        repartition,
        profile,
        repartition_exprs,
        base_partitions,
        local_partitions,
    )?;

    Ok(Arc::new(AggregateExec::try_new(
        AggregateMode::FinalPartitioned,
        PhysicalGroupBy::new_single(group_cols_to_final_exprs(group_cols)),
        vec![],
        vec![],
        fanout,
        partial.schema(),
    )?))
}

fn build_topk(
    input: Arc<dyn ExecutionPlan>,
    tie_break_name: &str,
    metric_index: usize,
    metric_name: &str,
    fetch: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(input));
    let Some(ordering) = LexOrdering::new(vec![
        PhysicalSortExpr::new(
            Arc::new(Column::new(metric_name, metric_index)) as Arc<dyn PhysicalExpr>,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
        PhysicalSortExpr::new(
            Arc::new(Column::new(tie_break_name, 0)) as Arc<dyn PhysicalExpr>,
            SortOptions::default(),
        ),
    ]) else {
        return exec_err!("invalid top-k ordering");
    };

    Ok(Arc::new(
        SortExec::new(ordering, coalesced).with_fetch(Some(fetch)),
    ))
}

fn apply_local_profile(
    input: Arc<dyn ExecutionPlan>,
    profile: QueryBenchProfile,
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    base_partitions: usize,
    local_partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    match profile {
        QueryBenchProfile::Baseline => Ok(input),
        QueryBenchProfile::Split => Ok(Arc::new(LocalExchangeSplitExec::try_new(
            input,
            hash_exprs,
            base_partitions,
            local_partitions,
        )?)),
        QueryBenchProfile::Repartition => Ok(Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::Hash(
                hash_exprs,
                base_partitions
                    .checked_mul(local_partitions)
                    .unwrap_or(base_partitions),
            ),
        )?)),
    }
}

fn group_cols_to_exprs(group_cols: &[(&str, usize)]) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    group_cols
        .iter()
        .map(|(name, index)| {
            (
                Arc::new(Column::new(name, *index)) as Arc<dyn PhysicalExpr>,
                name.to_string(),
            )
        })
        .collect()
}

fn group_cols_to_final_exprs(group_cols: &[(&str, usize)]) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    group_cols
        .iter()
        .enumerate()
        .map(|(index, (name, _))| {
            (
                Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                name.to_string(),
            )
        })
        .collect()
}

fn partial_group_columns(group_cols: &[(&str, usize)]) -> Vec<Arc<dyn PhysicalExpr>> {
    group_cols
        .iter()
        .enumerate()
        .map(|(index, (name, _))| Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>)
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum KeyDistribution {
    Uniform,
    HotKey,
}

#[derive(Clone, Copy)]
struct FactSourceSpec {
    total_rows: usize,
    batch_rows: usize,
    source_partitions: usize,
    key_domain: usize,
    grp_domain: usize,
    key_distribution: KeyDistribution,
    value_offset: i64,
}

fn make_fact_source(value_name: &str, spec: FactSourceSpec) -> Result<PreparedFactSource> {
    let schema = fact_schema(value_name);
    let partitions = make_fact_batches(Arc::clone(&schema), spec)?;
    Ok(PreparedFactSource { schema, partitions })
}

fn fact_schema(value_name: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("grp", DataType::Int64, false),
        Field::new(value_name, DataType::Int64, false),
    ]))
}

fn make_fact_batches(schema: SchemaRef, spec: FactSourceSpec) -> Result<Vec<Vec<RecordBatch>>> {
    let FactSourceSpec {
        total_rows,
        batch_rows,
        source_partitions,
        key_domain,
        grp_domain,
        key_distribution,
        value_offset,
    } = spec;

    if batch_rows == 0 {
        return exec_err!("benchmark batch_rows must be greater than zero");
    }

    let source_partitions = source_partitions.max(1);
    let key_domain = key_domain.max(1);
    let grp_domain = grp_domain.max(1);
    let mut partitions = vec![Vec::new(); source_partitions];
    let mut remaining = total_rows.max(batch_rows);
    let mut global_row = 0usize;
    let mut batch_index = 0usize;

    while remaining > 0 {
        let rows = remaining.min(batch_rows);
        let mut keys = Vec::with_capacity(rows);
        let mut grps = Vec::with_capacity(rows);
        let mut values = Vec::with_capacity(rows);
        for row_offset in 0..rows {
            let row = global_row + row_offset;
            let key = match key_distribution {
                KeyDistribution::Uniform => (row % key_domain) as i64,
                KeyDistribution::HotKey => {
                    if row % 10 < 8 {
                        0
                    } else {
                        ((row / 10) % key_domain) as i64
                    }
                }
            };
            keys.push(key);
            grps.push((row % grp_domain) as i64);
            values.push(((row % 1_024) as i64) + 1 + value_offset);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(grps)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        partitions[batch_index % source_partitions].push(batch);

        batch_index += 1;
        global_row += rows;
        remaining = remaining.saturating_sub(rows);
    }

    Ok(partitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn smoke_aggregate() -> Result<()> {
        let fixture = LocalExchangeQueryBench::new(
            QueryBenchShape::LowCardinalityFinalAgg,
            QueryBenchProfile::Split,
        )
        .prepare()?;
        fixture.run().await
    }

    #[tokio::test]
    async fn smoke_join() -> Result<()> {
        let fixture = LocalExchangeQueryBench::new(
            QueryBenchShape::BalancedPartitionedJoin,
            QueryBenchProfile::Repartition,
        )
        .prepare()?;
        fixture.run().await
    }
}
