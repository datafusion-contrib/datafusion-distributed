use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::common::compute_record_batch_statistics;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, stream};
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::sleep;
// Copied from https://github.com/apache/datafusion/blob/4b9a468cc1949062cf3cd8685ba8ced377fd212e/datafusion/physical-plan/src/test/exec.rs#L121

/// A Mock ExecutionPlan that can be used for writing tests of other
/// ExecutionPlans
#[derive(Debug)]
pub struct MockExec {
    /// the results to send back
    data: Vec<Vec<datafusion::common::Result<RecordBatch>>>,
    schema: SchemaRef,
    partitions: usize,
    /// if true (the default), sends data using a separate task to ensure the
    /// batches are not available without this stream yielding first
    use_task: bool,
    delay: Option<Duration>,
    start_notify: Option<Arc<Notify>>,
    permit_open: Option<Arc<AtomicBool>>,
    permit_notify: Option<Arc<Notify>>,
    execute_counts: Option<Arc<Vec<AtomicUsize>>>,
    cache: PlanProperties,
}

impl MockExec {
    /// Create a new `MockExec` with a single partition that returns
    /// the specified `Results`s.
    ///
    /// By default, the batches are not produced immediately (the
    /// caller has to actually yield and another task must run) to
    /// ensure any poll loops are correct. This behavior can be
    /// changed with `with_use_task`
    pub fn new(data: Vec<datafusion::common::Result<RecordBatch>>, schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(Arc::clone(&schema), 1);
        Self {
            data: vec![data],
            schema,
            partitions: 1,
            use_task: true,
            delay: None,
            start_notify: None,
            permit_open: None,
            permit_notify: None,
            execute_counts: None,
            cache,
        }
    }

    /// Create a new `MockExec` with per-partition data.
    pub fn new_partitioned(
        data: Vec<Vec<datafusion::common::Result<RecordBatch>>>,
        schema: SchemaRef,
    ) -> Self {
        let partitions = data.len().max(1);
        let cache = Self::compute_properties(Arc::clone(&schema), partitions);
        Self {
            data,
            schema,
            partitions,
            use_task: true,
            delay: None,
            start_notify: None,
            permit_open: None,
            permit_notify: None,
            execute_counts: None,
            cache,
        }
    }

    /// If `use_task` is true (the default) then the batches are sent
    /// back using a separate task to ensure the underlying stream is
    /// not immediately ready
    pub fn with_use_task(mut self, use_task: bool) -> Self {
        self.use_task = use_task;
        self
    }

    /// Adds a delay between emitted batches (simulates a slow producer).
    pub fn with_delay_between_batches(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Notify when execute is called (before emitting any batches).
    pub fn with_start_notify(mut self, start_notify: Arc<Notify>) -> Self {
        self.start_notify = Some(start_notify);
        self
    }

    /// Block emission until `permit_open` is true (use with `permit_notify`).
    pub fn with_gate(mut self, permit_open: Arc<AtomicBool>, permit_notify: Arc<Notify>) -> Self {
        self.permit_open = Some(permit_open);
        self.permit_notify = Some(permit_notify);
        self
    }

    /// Track execute calls per partition (for replay/once-only assertions).
    pub fn with_execute_counts(mut self, execute_counts: Arc<Vec<AtomicUsize>>) -> Self {
        self.execute_counts = Some(execute_counts);
        self
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef, partitions: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for MockExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MockExec")
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for MockExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    /// Returns a stream which yields data
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        assert!(partition < self.partitions);

        if let Some(counts) = &self.execute_counts {
            counts[partition].fetch_add(1, Ordering::SeqCst);
        }

        if let Some(start_notify) = &self.start_notify {
            start_notify.notify_waiters();
        }

        // Result doesn't implement clone, so do it ourself
        let data: Vec<_> = self
            .data
            .get(partition)
            .expect("partition data")
            .iter()
            .map(|r| match r {
                Ok(batch) => Ok(batch.clone()),
                Err(e) => Err(clone_error(e)),
            })
            .collect();

        if self.use_task {
            let mut builder = RecordBatchReceiverStream::builder(self.schema(), 2);
            // send data in order but in a separate task (to ensure
            // the batches are not available without the stream
            // yielding).
            let tx = builder.tx();
            let delay = self.delay;
            let permit_open = self.permit_open.clone();
            let permit_notify = self.permit_notify.clone();
            builder.spawn(async move {
                if let Some(open) = permit_open {
                    let notify = permit_notify.expect("permit_notify");
                    while !open.load(Ordering::SeqCst) {
                        notify.notified().await;
                    }
                }
                for batch in data {
                    if let Some(delay) = delay
                        && delay > Duration::ZERO
                    {
                        sleep(delay).await;
                    }
                    // println!("Sending batch via delayed stream");
                    if let Err(e) = tx.send(batch).await {
                        println!("ERROR batch via delayed stream: {e}");
                    }
                }

                Ok(())
            });
            // returned stream simply reads off the rx stream
            Ok(builder.build())
        } else {
            let delay = self.delay;
            let permit_open = self.permit_open.clone();
            let permit_notify = self.permit_notify.clone();
            let stream: Pin<
                Box<dyn Stream<Item = datafusion::common::Result<RecordBatch>> + Send>,
            > = if delay.is_some() || permit_open.is_some() {
                Box::pin(stream::unfold(
                    (data.into_iter(), false),
                    move |(mut iter, mut gate_done)| {
                        let permit_open = permit_open.clone();
                        let permit_notify = permit_notify.clone();
                        async move {
                            if !gate_done {
                                if let Some(open) = permit_open {
                                    let notify = permit_notify.expect("permit_notify");
                                    while !open.load(Ordering::SeqCst) {
                                        notify.notified().await;
                                    }
                                }
                                gate_done = true;
                            }
                            let batch = iter.next()?;
                            if let Some(delay) = delay
                                && delay > Duration::ZERO
                            {
                                sleep(delay).await;
                            }
                            Some((batch, (iter, gate_done)))
                        }
                    },
                ))
            } else {
                Box::pin(stream::iter(data))
            };
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream,
            )))
        }
    }

    // Panics if one of the batches is an error
    fn statistics(&self) -> datafusion::common::Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<Statistics> {
        if partition.is_some() {
            return Ok(Statistics::new_unknown(&self.schema));
        }
        let data: datafusion::common::Result<Vec<Vec<RecordBatch>>> = self
            .data
            .iter()
            .map(|partition_data| {
                partition_data
                    .iter()
                    .map(|r| match r {
                        Ok(batch) => Ok(batch.clone()),
                        Err(e) => Err(clone_error(e)),
                    })
                    .collect()
            })
            .collect();

        let data = data?;

        Ok(compute_record_batch_statistics(&data, &self.schema, None))
    }
}

fn clone_error(e: &DataFusionError) -> DataFusionError {
    use DataFusionError::*;
    match e {
        Execution(msg) => Execution(msg.to_string()),
        _ => unimplemented!(),
    }
}
