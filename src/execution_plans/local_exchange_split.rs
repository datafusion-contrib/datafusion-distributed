use crate::common::require_one_child;
use datafusion::arrow::array::{ArrayRef, PrimitiveArray};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{SchemaRef, UInt32Type};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet, Time,
};
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::StreamExt;
use std::any::Any;
use std::borrow::Cow;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_stream::wrappers::ReceiverStream;

type SharedError = Arc<datafusion::error::DataFusionError>;
type SplitReceiver = Receiver<Result<RecordBatch, SharedError>>;

const SPLIT_CHANNEL_CAPACITY: usize = 2;

/// Refines input partitions into more local output partitions.
///
/// [LocalExchangeSplitExec] is a local fanout operator. For each input partition, it evaluates
/// `hash_exprs`, re-hashes the rows, and emits `local_partition_count` output partitions on the
/// same task.
///
/// The operator does not change distributed routing. It never sends rows to another task; it only
/// subdivides partitions that the current task is already reading.
///
/// ```text
///    ┌────────┐     ┌────────┐
///    │  P1.1  │     │  P1.L  │
///    │[r1, r4]│     │[r0, r3]│
///    └────────┘ ... └────────┘
///          ▲           ▲
///          │           │
///         ┌─┐         ┌─┐
///         │1│         │L│
/// ┌───────┴─┴─────────┴─┴───────┐
/// │                             │
/// │   LocalExchangeSplitExec    │
/// │          (Task M)           │
/// │                             │
/// └─────────────┬─┬─────────────┘
///               │1│
///               └▲┘
///                │
///                │
/// ┌─────────────────────────────┐
/// │      Partition 1 (P1)       │
/// │  [r0, r1, r2, r3, r4, ...]  │
/// │                             │
/// └─────────────────────────────┘
/// ```
///
/// The first `execute()` call for a given input partition initializes a background splitter task
/// and one bounded channel per local output partition via [`OnceLock`]. Subsequent calls for
/// sibling local partitions receive their channel endpoint from the shared state.
///
/// # Contract
///
/// All `local_partition_count` output partitions for a given input partition must be consumed.
/// If a sibling is never polled, its bounded channel fills and the splitter stalls, preventing
/// other consumers from making progress. This is safe in practice since the planning pass only
/// inserts this operator above [AggregateExec(FinalPartitioned)] and [HashJoinExec(Partitioned)]
/// which consume every input partition.
#[derive(Debug)]
pub struct LocalExchangeSplitExec {
    input: Arc<dyn ExecutionPlan>,
    /// Expressions to hash — must match the upstream shuffle keys.
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Total logical hash partitions from the upstream `RepartitionExec` (`N` in `Hash([exprs], N)`).
    /// Used in the sub-partition formula to keep same-key rows on the same local partition.
    base_partition_count: usize,
    /// Number of local output partitions each input partition fans out into.
    local_partition_count: usize,
    /// `input.output_partitioning().partition_count()`. In practice always 1: the distributed
    /// planner normalizes the upstream shuffle to one logical partition per consumer task.
    input_partition_count: usize,
    /// `input_partition_count * local_partition_count`
    output_partition_count: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    /// One entry per input partition, initialized on the first `execute()` call for that partition.
    split_states: Vec<OnceLock<Result<SplitState, SharedError>>>,
}

#[derive(Debug)]
struct SplitState {
    receivers: Vec<Mutex<Option<SplitReceiver>>>,
    task: Arc<SpawnedTask<()>>,
}

#[derive(Clone)]
struct SplitMetrics {
    input_rows: Count,
    input_batches: Count,
    output_batches: Count,
    non_empty_output_partitions: Count,
    hash_eval_time: Time,
    batch_take_time: Time,
}

impl SplitMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        let input_rows = MetricBuilder::new(metrics).counter("split_input_rows", 0);
        let input_batches = MetricBuilder::new(metrics).counter("split_input_batches", 0);
        let output_batches = MetricBuilder::new(metrics).counter("split_output_batches", 0);
        let non_empty_output_partitions =
            MetricBuilder::new(metrics).counter("split_non_empty_output_partitions", 0);

        let hash_eval_time = Time::new();
        MetricBuilder::new(metrics).build(MetricValue::Time {
            name: Cow::Borrowed("split_hash_eval_time"),
            time: hash_eval_time.clone(),
        });

        let batch_take_time = Time::new();
        MetricBuilder::new(metrics).build(MetricValue::Time {
            name: Cow::Borrowed("split_batch_take_time"),
            time: batch_take_time.clone(),
        });

        Self {
            input_rows,
            input_batches,
            output_batches,
            non_empty_output_partitions,
            hash_eval_time,
            batch_take_time,
        }
    }
}

impl LocalExchangeSplitExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
        base_partition_count: usize,
        local_partition_count: usize,
    ) -> Result<Self> {
        if base_partition_count == 0 {
            return plan_err!("LocalExchangeSplitExec requires base_partition_count > 0");
        }
        if local_partition_count == 0 {
            return plan_err!("LocalExchangeSplitExec requires local_partition_count > 0");
        }

        let input_partition_count = input.output_partitioning().partition_count();
        let output_partition_count = input_partition_count
            .checked_mul(local_partition_count)
            .ok_or_else(|| {
                datafusion::common::plan_datafusion_err!(
                    "LocalExchangeSplitExec partition count overflow: input_partition_count={} local_partition_count={}",
                    input_partition_count,
                    local_partition_count
                )
            })?;

        let properties =
            <PlanProperties as Clone>::clone(input.properties().as_ref()).with_partitioning(
                Partitioning::Hash(hash_exprs.clone(), output_partition_count),
            );

        Ok(Self {
            input,
            hash_exprs,
            base_partition_count,
            local_partition_count,
            input_partition_count,
            output_partition_count,
            properties: Arc::new(properties),
            metrics: ExecutionPlanMetricsSet::new(),
            split_states: (0..input_partition_count)
                .map(|_| OnceLock::new())
                .collect(),
        })
    }

    pub fn base_partition_count(&self) -> usize {
        self.base_partition_count
    }

    pub fn local_partition_count(&self) -> usize {
        self.local_partition_count
    }

    fn initialize_split(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SplitState> {
        let mut senders = Vec::with_capacity(self.local_partition_count);
        let mut receivers = Vec::with_capacity(self.local_partition_count);

        for _ in 0..self.local_partition_count {
            let (tx, rx) = channel(SPLIT_CHANNEL_CAPACITY);
            senders.push(tx);
            receivers.push(Mutex::new(Some(rx)));
        }

        let mut input = self.input.execute(input_partition, context)?;
        let split_metrics = SplitMetrics::new(&self.metrics);
        let task = Arc::new(SpawnedTask::spawn({
            let mut senders: Vec<Option<Sender<Result<RecordBatch, SharedError>>>> =
                senders.into_iter().map(Some).collect();
            let local_partition_count = self.local_partition_count;
            let base_partition_count = self.base_partition_count;
            let hash_exprs = self.hash_exprs.clone();
            let schema = self.schema();
            let split_metrics = split_metrics.clone();

            async move {
                let hash_expr_count = hash_exprs.len();
                let mut splitter = Splitter {
                    hash_exprs,
                    base_partition_count,
                    local_partition_count,
                    schema,
                    metrics: split_metrics,
                    evaluated: Vec::with_capacity(hash_expr_count),
                    hashes: Vec::new(),
                    indices: (0..local_partition_count).map(|_| Vec::new()).collect(),
                    output_batches: Vec::with_capacity(local_partition_count),
                };
                while let Some(next_batch) = input.next().await {
                    match next_batch {
                        Ok(batch) => match splitter.split_batch(batch) {
                            Ok(mut split_batches) => {
                                for (partition, batch) in split_batches.drain(..) {
                                    let Some(sender) = &senders[partition] else {
                                        continue;
                                    };
                                    if sender.send(Ok(batch)).await.is_err() {
                                        senders[partition] = None;
                                    }
                                }
                                splitter.reuse_output_buffer(split_batches);
                                if senders.iter().all(|sender| sender.is_none()) {
                                    return;
                                }
                            }
                            Err(err) => {
                                let err = Arc::new(err);
                                for sender in senders.iter_mut() {
                                    let Some(sender) = sender else {
                                        continue;
                                    };
                                    let _ = sender.send(Err(Arc::clone(&err))).await;
                                }
                                return;
                            }
                        },
                        Err(err) => {
                            let err = Arc::new(err);
                            for sender in senders.iter_mut() {
                                let Some(sender) = sender else {
                                    continue;
                                };
                                let _ = sender.send(Err(Arc::clone(&err))).await;
                            }
                            return;
                        }
                    }
                }
            }
        }));

        Ok(SplitState { receivers, task })
    }
}

/// Per-input-partition batch router for [LocalExchangeSplitExec].
///
/// `Splitter` runs as a background task. It reads one input stream and routes each row to a local
/// output partition using the same hash seed as DataFusion's `RepartitionExec`
/// (`REPARTITION_RANDOM_STATE`). This ensures that rows with the same key always land on the same
/// local partition.
///
/// # Routing Formula
///
/// The routing formula is `(hash / base_partition_count) % local_partition_count`. The upstream
/// `RepartitionExec` used `hash % base_partition_count` to assign rows to logical partitions, so
/// dividing by `base_partition_count` eliminates that selector and gives us the higher-order bits.
/// Using those with `local_partition_count` consistently sub-partitions each logical partition
/// while preserving that same-key rows share a partition.
struct Splitter {
    /// Same expressions and seed used by the upstream `RepartitionExec`.
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// `N` from the upstream `Hash([exprs], N)`, the logical partition count.
    base_partition_count: usize,
    /// Number of local output partitions to fan out into.
    local_partition_count: usize,
    schema: SchemaRef,
    metrics: SplitMetrics,
    /// Reused buffer for evaluated hash expression columns.
    evaluated: Vec<ArrayRef>,
    /// Reused buffer for per-row hash values.
    hashes: Vec<u64>,
    /// Reused per-partition row index lists, one inner `Vec` per local output partition.
    indices: Vec<Vec<u32>>,
    /// Reused output batch list returned from `split_batch`.
    output_batches: Vec<(usize, RecordBatch)>,
}

impl Splitter {
    fn split_batch(&mut self, batch: RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        self.metrics.input_batches.add(1);
        self.metrics.input_rows.add(batch.num_rows());

        let hash_start = Instant::now();
        self.evaluated.clear();
        for expr in &self.hash_exprs {
            self.evaluated
                .push(expr.evaluate(&batch)?.into_array(batch.num_rows())?);
        }

        self.hashes.clear();
        self.hashes.resize(batch.num_rows(), 0);
        create_hashes(
            &self.evaluated,
            &REPARTITION_RANDOM_STATE.random_state(),
            &mut self.hashes,
        )?;
        self.metrics.hash_eval_time.add_elapsed(hash_start);

        for rows in &mut self.indices {
            rows.clear();
        }

        let base_partition_count = self.base_partition_count as u64;
        let local_partition_count = self.local_partition_count as u64;
        for (row_idx, hash) in self.hashes.iter().copied().enumerate() {
            let local_partition = ((hash / base_partition_count) % local_partition_count) as usize;
            self.indices[local_partition].push(row_idx as u32);
        }

        let take_start = Instant::now();
        let mut non_empty_partitions = 0usize;
        let mut output_batches = 0usize;

        self.output_batches.clear();

        for (partition, rows) in self.indices.iter_mut().enumerate() {
            if rows.is_empty() {
                continue;
            }
            non_empty_partitions += 1;
            output_batches += 1;

            let output = if rows.len() == batch.num_rows() {
                batch.clone()
            } else {
                let taken_rows = std::mem::take(rows);
                let rows_array: PrimitiveArray<UInt32Type> = taken_rows.into();
                let columns = take_arrays(batch.columns(), &rows_array, None)?;
                let output = RecordBatch::try_new_with_options(
                    Arc::clone(&self.schema),
                    columns,
                    &RecordBatchOptions::new().with_row_count(Some(rows_array.len())),
                )?;

                let (_, buffer, _) = rows_array.into_parts();
                let mut reusable_rows = buffer.into_inner().into_vec::<u32>().map_err(|err| {
                    datafusion::common::internal_datafusion_err!(
                        "could not recover LocalExchangeSplitExec row indices: {err:?}"
                    )
                })?;
                reusable_rows.clear();
                *rows = reusable_rows;

                output
            };
            self.output_batches.push((partition, output));
        }

        self.metrics
            .non_empty_output_partitions
            .add(non_empty_partitions);
        self.metrics.output_batches.add(output_batches);
        self.metrics.batch_take_time.add_elapsed(take_start);

        Ok(std::mem::take(&mut self.output_batches))
    }

    fn reuse_output_buffer(&mut self, mut output_batches: Vec<(usize, RecordBatch)>) {
        output_batches.clear();
        self.output_batches = output_batches;
    }
}

impl DisplayAs for LocalExchangeSplitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "LocalExchangeSplitExec: input_partitions={}, base_partitions={}, local_partitions={}, exprs=[{}]",
            self.input_partition_count,
            self.base_partition_count,
            self.local_partition_count,
            self.hash_exprs
                .iter()
                .map(|expr| expr.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl ExecutionPlan for LocalExchangeSplitExec {
    fn name(&self) -> &str {
        "LocalExchangeSplitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Each output partition is a stable subsequence of exactly one input partition.
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            require_one_child(children)?,
            self.hash_exprs.clone(),
            self.base_partition_count,
            self.local_partition_count,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.output_partition_count {
            return exec_err!(
                "LocalExchangeSplitExec partition {} out of range for {} output partitions",
                partition,
                self.output_partition_count
            );
        }

        let input_partition = partition / self.local_partition_count;
        let local_partition = partition % self.local_partition_count;

        let state = self.split_states[input_partition].get_or_init(|| {
            self.initialize_split(input_partition, context)
                .map_err(Arc::new)
        });
        let state = match state {
            Ok(state) => state,
            Err(err) => return Err(datafusion::error::DataFusionError::Shared(Arc::clone(err))),
        };
        let task = Arc::clone(&state.task);

        let receiver = state.receivers[local_partition]
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(format!(
                    "LocalExchangeSplitExec partition {partition} already consumed"
                ))
            })?;

        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let schema = self.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            ReceiverStream::new(receiver)
                .map(move |result| match result {
                    Ok(batch) => {
                        output_rows.add(batch.num_rows());
                        Ok(batch)
                    }
                    Err(err) => Err(datafusion::error::DataFusionError::Shared(err)),
                })
                // Keeps the SpawnedTask alive for the stream's lifetime. Without this the task
                // could be dropped and cancelled before the stream is fully drained.
                .inspect(move |_| {
                    let _ = &task;
                }),
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::mock_exec::MockExec;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::test::TestMemoryExec;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn col(name: &str, idx: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, idx))
    }

    fn int32_batch(schema: SchemaRef, values: &[i32]) -> Result<RecordBatch> {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
            .map_err(Into::into)
    }

    fn batch_values(batch: &RecordBatch) -> Vec<i32> {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        (0..values.len()).map(|idx| values.value(idx)).collect()
    }

    fn expected_local_values(
        batches: &[RecordBatch],
        hash_exprs: &[Arc<dyn PhysicalExpr>],
        base_partition_count: usize,
        local_partition_count: usize,
        local_partition: usize,
    ) -> Result<Vec<i32>> {
        let mut result = Vec::new();
        for batch in batches {
            let evaluated = hash_exprs
                .iter()
                .map(|expr| expr.evaluate(batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            let mut hashes = vec![0; batch.num_rows()];
            create_hashes(
                &evaluated,
                &REPARTITION_RANDOM_STATE.random_state(),
                &mut hashes,
            )?;

            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int32 column");
            for (row_idx, hash) in hashes.iter().enumerate() {
                let partition =
                    ((hash / base_partition_count as u64) % local_partition_count as u64) as usize;
                if partition == local_partition {
                    result.push(values.value(row_idx));
                }
            }
        }
        Ok(result)
    }

    #[tokio::test]
    async fn splits_rows_once() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let input_batch = int32_batch(Arc::clone(&schema), &[0, 1, 2, 3, 4, 5, 6, 7])?;
        let execute_counts = Arc::new(vec![AtomicUsize::new(0)]);
        let input = Arc::new(
            MockExec::new_partitioned(vec![vec![Ok(input_batch.clone())]], Arc::clone(&schema))
                .with_execute_counts(Arc::clone(&execute_counts)),
        );
        let hash_exprs = vec![col("k", 0)];
        let split = Arc::new(LocalExchangeSplitExec::try_new(
            input,
            hash_exprs.clone(),
            2,
            2,
        )?);

        assert_eq!(
            split.properties().output_partitioning().partition_count(),
            2
        );
        match split.properties().output_partitioning() {
            Partitioning::Hash(_, n) => assert_eq!(*n, 2),
            other => panic!("expected hash partitioning, got {other:?}"),
        }

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let batches0 = collect(split.execute(0, task_ctx.clone())?).await?;
        let batches1 = collect(split.execute(1, task_ctx)?).await?;

        let actual0 = batches0.iter().flat_map(batch_values).collect::<Vec<_>>();
        let actual1 = batches1.iter().flat_map(batch_values).collect::<Vec<_>>();
        let expected0 = expected_local_values(&[input_batch.clone()], &hash_exprs, 2, 2, 0)?;
        let expected1 = expected_local_values(&[input_batch], &hash_exprs, 2, 2, 1)?;

        assert_eq!(actual0, expected0);
        assert_eq!(actual1, expected1);
        assert_eq!(execute_counts[0].load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[tokio::test]
    async fn handles_dropped_sibling() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let input_batches = vec![
            int32_batch(Arc::clone(&schema), &[0, 1, 2, 3])?,
            int32_batch(Arc::clone(&schema), &[4, 5, 6, 7])?,
            int32_batch(Arc::clone(&schema), &[8, 9, 10, 11])?,
        ];
        let execute_counts = Arc::new(vec![AtomicUsize::new(0)]);
        let input = Arc::new(
            MockExec::new_partitioned(
                vec![input_batches.iter().cloned().map(Ok).collect::<Vec<_>>()],
                Arc::clone(&schema),
            )
            .with_execute_counts(Arc::clone(&execute_counts)),
        );
        let hash_exprs = vec![col("k", 0)];
        let split = Arc::new(LocalExchangeSplitExec::try_new(
            input,
            hash_exprs.clone(),
            2,
            2,
        )?);

        let expected0 = expected_local_values(&input_batches, &hash_exprs, 2, 2, 0)?;
        let expected1 = expected_local_values(&input_batches, &hash_exprs, 2, 2, 1)?;
        let (survivor_partition, expected_survivor) = if !expected0.is_empty() {
            (0usize, expected0)
        } else {
            (1usize, expected1)
        };
        let dropped_partition = 1 - survivor_partition;

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let dropped_stream = split.execute(dropped_partition, task_ctx.clone())?;
        let survivor_stream = split.execute(survivor_partition, task_ctx)?;
        drop(dropped_stream);

        let survivor_batches = collect(survivor_stream).await?;
        let actual = survivor_batches
            .iter()
            .flat_map(batch_values)
            .collect::<Vec<_>>();

        assert_eq!(actual, expected_survivor);
        assert_eq!(execute_counts[0].load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[tokio::test]
    async fn rejects_duplicate_consumer() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let input = Arc::new(MockExec::new_partitioned(
            vec![vec![Ok(int32_batch(Arc::clone(&schema), &[1, 2, 3])?)]],
            Arc::clone(&schema),
        ));
        let split = Arc::new(LocalExchangeSplitExec::try_new(
            input,
            vec![col("k", 0)],
            2,
            2,
        )?);

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let _stream = split.execute(0, task_ctx.clone())?;
        let err = match split.execute(0, task_ctx) {
            Ok(_) => panic!("expected duplicate execute to fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("LocalExchangeSplitExec partition 0 already consumed")
        );

        Ok(())
    }

    #[tokio::test]
    async fn preserves_output_ordering() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let input_batches = vec![
            int32_batch(Arc::clone(&schema), &[0, 1, 2, 3])?,
            int32_batch(Arc::clone(&schema), &[4, 5, 6, 7])?,
        ];
        let input =
            TestMemoryExec::try_new_exec(&[input_batches.clone()], Arc::clone(&schema), None)?;
        let input = Arc::unwrap_or_clone(input).try_with_sort_information(vec![
            [datafusion::physical_expr::PhysicalSortExpr::new_default(
                Arc::new(Column::new("k", 0)),
            )]
            .into(),
        ])?;
        let input = Arc::new(TestMemoryExec::update_cache(&Arc::new(input)));
        let hash_exprs = vec![col("k", 0)];
        let split = Arc::new(LocalExchangeSplitExec::try_new(
            input,
            hash_exprs.clone(),
            2,
            2,
        )?);

        assert_eq!(split.maintains_input_order(), vec![true]);
        assert_eq!(
            split
                .properties()
                .output_ordering()
                .map(|ordering| ordering.to_string()),
            Some("k@0 ASC".to_string())
        );

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let batches0 = collect(split.execute(0, task_ctx.clone())?).await?;
        let batches1 = collect(split.execute(1, task_ctx)?).await?;

        let actual0 = batches0.iter().flat_map(batch_values).collect::<Vec<_>>();
        let actual1 = batches1.iter().flat_map(batch_values).collect::<Vec<_>>();
        let expected0 = expected_local_values(&input_batches, &hash_exprs, 2, 2, 0)?;
        let expected1 = expected_local_values(&input_batches, &hash_exprs, 2, 2, 1)?;

        assert_eq!(actual0, expected0);
        assert_eq!(actual1, expected1);
        assert!(actual0.windows(2).all(|window| window[0] <= window[1]));
        assert!(actual1.windows(2).all(|window| window[0] <= window[1]));

        Ok(())
    }
}
