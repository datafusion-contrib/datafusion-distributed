use dashmap::{DashMap, Entry};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use std::sync::Arc;
use uuid::Uuid;
// TODO: find some way of cleaning up abandoned partitioners

/// Keeps track of all the [StreamPartitioner] currently running in the program, identifying them
/// by stage id.
#[derive(Default)]
pub struct StreamPartitionerRegistry {
    map: DashMap<(Uuid, usize), Arc<RepartitionExec>>,
}

impl StreamPartitionerRegistry {
    /// Builds a new [StreamPartitioner] if there was not one for this specific stage id.
    /// If there was already one, return a reference to it.
    pub fn get_or_create_stream_partitioner(
        &self,
        id: Uuid,
        actor_idx: usize,
        plan: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Result<Arc<RepartitionExec>, DataFusionError> {
        match self.map.entry((id, actor_idx)) {
            Entry::Occupied(entry) => Ok(Arc::clone(entry.get())),
            Entry::Vacant(entry) => Ok(Arc::clone(
                &entry.insert(Arc::new(RepartitionExec::try_new(plan, partitioning)?)),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::mock_exec::MockExec;
    use datafusion::arrow::array::{RecordBatch, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::expressions::col;
    use futures::StreamExt;

    #[tokio::test]
    async fn round_robin_1() -> Result<(), Box<dyn std::error::Error>> {
        const PARTITIONS: usize = 10;

        let registry = StreamPartitionerRegistry::default();
        let partitioner = registry.get_or_create_stream_partitioner(
            Uuid::new_v4(),
            0,
            mock_exec(15, 10),
            Partitioning::RoundRobinBatch(PARTITIONS),
        )?;

        let rows_per_partition = gather_rows_per_partition(&partitioner).await;

        assert_eq!(
            rows_per_partition,
            vec![20, 20, 20, 20, 20, 10, 10, 10, 10, 10]
        );
        Ok(())
    }

    #[tokio::test]
    async fn round_robin_2() -> Result<(), Box<dyn std::error::Error>> {
        const PARTITIONS: usize = 10;

        let registry = StreamPartitionerRegistry::default();
        let partitioner = registry.get_or_create_stream_partitioner(
            Uuid::new_v4(),
            0,
            mock_exec(5, 10),
            Partitioning::RoundRobinBatch(PARTITIONS),
        )?;

        let rows_per_partition = gather_rows_per_partition(&partitioner).await;

        assert_eq!(rows_per_partition, vec![10, 10, 10, 10, 10, 0, 0, 0, 0, 0]);
        Ok(())
    }

    #[tokio::test]
    async fn hash_1() -> Result<(), Box<dyn std::error::Error>> {
        const PARTITIONS: usize = 10;

        let registry = StreamPartitionerRegistry::default();
        let partitioner = registry.get_or_create_stream_partitioner(
            Uuid::new_v4(),
            0,
            mock_exec(15, 10),
            Partitioning::Hash(vec![col("c0", &test_schema())?], PARTITIONS),
        )?;

        let rows_per_partition = gather_rows_per_partition(&partitioner).await;

        assert_eq!(
            rows_per_partition,
            vec![30, 15, 0, 45, 0, 15, 0, 15, 15, 15]
        );
        Ok(())
    }

    #[tokio::test]
    async fn hash_2() -> Result<(), Box<dyn std::error::Error>> {
        const PARTITIONS: usize = 10;

        let registry = StreamPartitionerRegistry::default();
        let partitioner = registry.get_or_create_stream_partitioner(
            Uuid::new_v4(),
            0,
            mock_exec(5, 10),
            Partitioning::Hash(vec![col("c0", &test_schema())?], PARTITIONS),
        )?;

        let rows_per_partition = gather_rows_per_partition(&partitioner).await;

        assert_eq!(rows_per_partition, vec![10, 5, 0, 15, 0, 5, 0, 5, 5, 5]);
        Ok(())
    }

    async fn gather_rows_per_partition(partitioner: &RepartitionExec) -> Vec<usize> {
        let mut data = vec![];
        let n_partitions = partitioner.partitioning().partition_count();
        let ctx = Arc::new(TaskContext::default());
        for i in 0..n_partitions {
            let mut stream = partitioner.execute(i, ctx.clone()).unwrap();
            data.push(0);
            while let Some(msg) = stream.next().await {
                data[i] += msg.unwrap().num_rows();
            }
        }
        data
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    fn mock_exec(n_batches: usize, n_rows: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(MockExec::new(
            create_vec_batches(n_batches, n_rows),
            test_schema(),
        ))
    }

    /// Create vector batches
    fn create_vec_batches(
        n_batches: usize,
        n_rows: usize,
    ) -> Vec<Result<RecordBatch, DataFusionError>> {
        let batch = create_batch(n_rows);
        (0..n_batches).map(|_| Ok(batch.clone())).collect()
    }

    /// Create batch
    fn create_batch(n_rows: usize) -> RecordBatch {
        let schema = test_schema();
        let mut data = vec![];
        for i in 0..n_rows {
            data.push(i as u32)
        }
        RecordBatch::try_new(schema, vec![Arc::new(UInt32Array::from(data))]).unwrap()
    }
}
