use super::fixture::{benchmark_schema, make_input_partitions};
use arrow::datatypes::Schema;
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
pub enum LocalRepartitionMode {
    Hash,
    RoundRobin,
}

impl Display for LocalRepartitionMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hash => write!(f, "hash"),
            Self::RoundRobin => write!(f, "round_robin"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalRepartitionBench {
    pub mode: LocalRepartitionMode,
    pub scenario_name: &'static str,
    pub input_partitions: usize,
    pub output_partitions: usize,
    pub total_rows: usize,
    pub batch_size: usize,
}

impl LocalRepartitionBench {
    pub fn one_to_one_baseline(mode: LocalRepartitionMode) -> Self {
        Self {
            mode,
            scenario_name: "one_to_one_baseline",
            input_partitions: 1,
            output_partitions: 1,
            total_rows: 1_000_000,
            batch_size: 1024,
        }
    }

    pub fn many_to_one_baseline(mode: LocalRepartitionMode, input_partitions: usize) -> Self {
        Self {
            mode,
            scenario_name: "many_to_one_baseline",
            input_partitions,
            output_partitions: 1,
            total_rows: 1_000_000,
            batch_size: 1024,
        }
    }

    pub fn one_to_many_baseline(mode: LocalRepartitionMode, output_partitions: usize) -> Self {
        Self {
            mode,
            scenario_name: "one_to_many_baseline",
            input_partitions: 1,
            output_partitions,
            total_rows: 1_000_000,
            batch_size: 1024,
        }
    }

    pub fn many_to_many_baseline(mode: LocalRepartitionMode, partitions: usize) -> Self {
        Self {
            mode,
            scenario_name: "many_to_many_baseline",
            input_partitions: partitions,
            output_partitions: partitions,
            total_rows: 1_000_000,
            batch_size: 1024,
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

    pub fn required_total_rows(&self) -> usize {
        self.input_partitions
            .max(1)
            .saturating_mul(self.batch_size.max(1))
    }

    pub fn normalized(&self) -> Self {
        let mut normalized = self.clone();
        normalized.total_rows = normalized.total_rows.max(normalized.required_total_rows());
        normalized
    }

    pub fn label(&self) -> String {
        format!(
            "scenario={},mode={},input_partitions={},output_partitions={},total_rows={},batch_size={}",
            self.scenario_name,
            self.mode,
            self.input_partitions,
            self.output_partitions,
            self.total_rows,
            self.batch_size,
        )
    }

    pub async fn run(&self) -> Result<()> {
        self.prepare()?.run().await
    }

    pub fn prepare(&self) -> Result<LocalRepartitionFixture> {
        let bench = self.normalized();
        let schema = benchmark_schema();
        let input_partitions = make_input_partitions(
            Arc::clone(&schema),
            bench.total_rows,
            bench.batch_size,
            bench.input_partitions,
        )?;

        Ok(LocalRepartitionFixture {
            bench,
            schema,
            input_partitions,
            task_ctx: SessionStateBuilder::new()
                .with_default_features()
                .build()
                .task_ctx(),
        })
    }
}

impl Display for LocalRepartitionBench {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

pub struct LocalRepartitionFixture {
    bench: LocalRepartitionBench,
    schema: Arc<Schema>,
    input_partitions: Vec<Vec<datafusion::arrow::record_batch::RecordBatch>>,
    task_ctx: Arc<datafusion::execution::TaskContext>,
}

impl LocalRepartitionFixture {
    pub async fn run(&self) -> Result<()> {
        let input = MemorySourceConfig::try_new_exec(
            &self.input_partitions,
            Arc::clone(&self.schema),
            None,
        )?;
        let repartition = Arc::new(RepartitionExec::try_new(
            input,
            match self.bench.mode {
                LocalRepartitionMode::Hash => Partitioning::Hash(
                    vec![Arc::new(Column::new("id", 0))],
                    self.bench.output_partitions,
                ),
                LocalRepartitionMode::RoundRobin => {
                    Partitioning::RoundRobinBatch(self.bench.output_partitions)
                }
            },
        )?);

        let output = collect_partitioned(repartition, Arc::clone(&self.task_ctx)).await?;
        let _row_count: usize = output
            .iter()
            .flat_map(|batches| batches.iter())
            .map(|batch| batch.num_rows())
            .sum();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn smoke() -> Result<()> {
        let fixture = LocalRepartitionBench::one_to_one_baseline(LocalRepartitionMode::Hash)
            .with_total_rows(128)
            .with_batch_size(64)
            .prepare()?;
        fixture.run().await
    }
}
