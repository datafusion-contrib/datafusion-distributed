use super::fixture::benchmark_schema;
use arrow::array::Int64Array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::collect_partitioned;
use datafusion::physical_plan::repartition::RepartitionExec;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum LocalFanoutStrategy {
    Split,
    Repartition,
}

impl Display for LocalFanoutStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Split => write!(f, "split"),
            Self::Repartition => write!(f, "repartition"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum LocalExchangeIdMode {
    Uniform,
    HotKey,
}

impl Display for LocalExchangeIdMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uniform => write!(f, "uniform"),
            Self::HotKey => write!(f, "hot_key"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalExchangeSplitBench {
    pub scenario_name: &'static str,
    pub strategy: LocalFanoutStrategy,
    pub input_partitions: usize,
    pub base_partitions: usize,
    pub local_partitions: usize,
    pub total_rows: usize,
    pub batch_size: usize,
    pub id_mode: LocalExchangeIdMode,
}

impl LocalExchangeSplitBench {
    pub fn one_owned_baseline() -> Self {
        Self {
            scenario_name: "one_owned_baseline",
            strategy: LocalFanoutStrategy::Split,
            input_partitions: 1,
            base_partitions: 8,
            local_partitions: 2,
            total_rows: 1_000_000,
            batch_size: 1024,
            id_mode: LocalExchangeIdMode::Uniform,
        }
    }

    pub fn many_owned(input_partitions: usize, local_partitions: usize) -> Self {
        Self {
            scenario_name: "many_owned",
            strategy: LocalFanoutStrategy::Split,
            input_partitions,
            base_partitions: 8,
            local_partitions,
            total_rows: 1_000_000,
            batch_size: 1024,
            id_mode: LocalExchangeIdMode::Uniform,
        }
    }

    pub fn with_total_rows(mut self, total_rows: usize) -> Self {
        self.total_rows = total_rows;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_id_mode(mut self, id_mode: LocalExchangeIdMode) -> Self {
        self.id_mode = id_mode;
        self
    }

    pub fn with_local_partitions(mut self, local_partitions: usize) -> Self {
        self.local_partitions = local_partitions;
        self
    }

    pub fn with_strategy(mut self, strategy: LocalFanoutStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn label(&self) -> String {
        format!(
            "scenario={},strategy={},input_partitions={},base_partitions={},local_partitions={},total_rows={},batch_size={},id_mode={}",
            self.scenario_name,
            self.strategy,
            self.input_partitions,
            self.base_partitions,
            self.local_partitions,
            self.total_rows,
            self.batch_size,
            self.id_mode,
        )
    }

    pub fn prepare(&self) -> Result<LocalExchangeSplitFixture> {
        let schema = benchmark_schema();
        let input_partitions = make_local_exchange_input_partitions(
            Arc::clone(&schema),
            self.total_rows,
            self.batch_size,
            self.input_partitions,
            self.id_mode,
        )?;

        Ok(LocalExchangeSplitFixture {
            bench: self.clone(),
            schema,
            input_partitions,
            task_ctx: SessionStateBuilder::new()
                .with_default_features()
                .build()
                .task_ctx(),
        })
    }
}

impl Display for LocalExchangeSplitBench {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

pub struct LocalExchangeSplitFixture {
    bench: LocalExchangeSplitBench,
    schema: Arc<Schema>,
    input_partitions: Vec<Vec<RecordBatch>>,
    task_ctx: Arc<datafusion::execution::TaskContext>,
}

impl LocalExchangeSplitFixture {
    pub async fn run(&self) -> Result<()> {
        let input = MemorySourceConfig::try_new_exec(
            &self.input_partitions,
            Arc::clone(&self.schema),
            None,
        )?;
        let plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> = match self.bench.strategy {
            LocalFanoutStrategy::Split => Arc::new(crate::LocalExchangeSplitExec::try_new(
                input,
                vec![Arc::new(Column::new("id", 0))],
                self.bench.base_partitions,
                self.bench.local_partitions,
            )?),
            LocalFanoutStrategy::Repartition => Arc::new(RepartitionExec::try_new(
                input,
                Partitioning::Hash(
                    vec![Arc::new(Column::new("id", 0))],
                    self.bench
                        .input_partitions
                        .checked_mul(self.bench.local_partitions)
                        .unwrap_or(self.bench.local_partitions),
                ),
            )?),
        };

        let output = collect_partitioned(plan, Arc::clone(&self.task_ctx)).await?;
        let _row_count: usize = output
            .iter()
            .flat_map(|batches| batches.iter())
            .map(|batch| batch.num_rows())
            .sum();
        Ok(())
    }
}

fn make_local_exchange_input_partitions(
    schema: Arc<Schema>,
    total_rows: usize,
    batch_size: usize,
    partition_count: usize,
    id_mode: LocalExchangeIdMode,
) -> Result<Vec<Vec<RecordBatch>>> {
    use arrow::util::data_gen::create_random_batch;

    let partition_count = partition_count.max(1);
    let batch_size = batch_size.max(1);

    let mut partitions = vec![Vec::new(); partition_count];
    let mut remaining = total_rows.max(batch_size);
    let mut global_row_offset = 0i64;
    let mut batch_index = 0usize;

    while remaining > 0 {
        let rows = remaining.min(batch_size);
        let batch = create_random_batch(Arc::clone(&schema), rows, 0.1, 0.5)?;
        let ids = match id_mode {
            LocalExchangeIdMode::Uniform => Int64Array::from(
                (global_row_offset..global_row_offset + rows as i64).collect::<Vec<_>>(),
            ),
            LocalExchangeIdMode::HotKey => Int64Array::from(vec![0; rows]),
        };

        let mut columns = batch.columns().to_vec();
        columns[0] = Arc::new(ids);
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        partitions[batch_index % partition_count].push(batch);

        batch_index += 1;
        global_row_offset += rows as i64;
        remaining = remaining.saturating_sub(rows);
    }

    Ok(partitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn smoke() -> Result<()> {
        let fixture = LocalExchangeSplitBench::one_owned_baseline()
            .with_total_rows(128)
            .with_batch_size(64)
            .prepare()?;
        fixture.run().await
    }
}
