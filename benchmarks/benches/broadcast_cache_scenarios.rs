use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::UInt8Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::SessionContext;
use datafusion_distributed::BroadcastExec;
use futures::{StreamExt, stream};
use std::any::Any;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{System, get_current_pid};
use tokio::runtime::Builder as RuntimeBuilder;

#[derive(Clone, Copy)]
enum ConsumerBehavior {
    Fast,
    Slow(Duration),
    CancelAfter(usize),
}

#[derive(Clone)]
struct ConsumerSpec {
    partition: usize,
    behavior: ConsumerBehavior,
}

#[derive(Clone)]
struct Scenario {
    name: &'static str,
    input_partitions: usize,
    consumer_tasks: usize,
    rows_per_batch: usize,
    num_batches: usize,
    consumers: Vec<ConsumerSpec>,
}

struct PeakRssSampler {
    stop: Arc<AtomicBool>,
    peak_kb: Arc<AtomicU64>,
    handle: Option<thread::JoinHandle<()>>,
}

impl PeakRssSampler {
    fn start(interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_kb = Arc::new(AtomicU64::new(0));
        let stop_clone = Arc::clone(&stop);
        let peak_clone = Arc::clone(&peak_kb);
        let handle = thread::spawn(move || {
            let mut sys = System::new();
            let pid = get_current_pid().expect("pid");
            while !stop_clone.load(Ordering::Relaxed) {
                sys.refresh_process(pid);
                if let Some(proc) = sys.process(pid) {
                    let mem_kb = proc.memory();
                    peak_clone.fetch_max(mem_kb, Ordering::Relaxed);
                }
                thread::sleep(interval);
            }
        });
        Self {
            stop,
            peak_kb,
            handle: Some(handle),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.peak_kb.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct SyntheticExec {
    schema: SchemaRef,
    partitions: usize,
    batches: Arc<Vec<Arc<RecordBatch>>>,
    properties: PlanProperties,
}

impl SyntheticExec {
    fn new(
        schema: SchemaRef,
        partitions: usize,
        batches: Arc<Vec<Arc<RecordBatch>>>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            schema,
            partitions,
            batches,
            properties,
        }
    }
}

impl DisplayAs for SyntheticExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(f, "SyntheticExec"),
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for SyntheticExec {
    fn name(&self) -> &str {
        "SyntheticExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert!(partition < self.partitions);
        let schema = Arc::clone(&self.schema);
        let batches = Arc::clone(&self.batches);
        let len = batches.len();

        let stream = stream::iter((0..len).map(move |idx| {
            let batch = &batches[idx];
            Ok(batch.as_ref().clone())
        }));

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

async fn consume_partition(
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
    task_ctx: Arc<TaskContext>,
    behavior: ConsumerBehavior,
) -> Result<()> {
    let mut stream = plan.execute(partition, task_ctx)?;
    let mut seen = 0usize;
    while let Some(batch) = stream.next().await {
        let _ = batch?;
        seen += 1;
        match behavior {
            ConsumerBehavior::Fast => {}
            ConsumerBehavior::Slow(delay) => tokio::time::sleep(delay).await,
            ConsumerBehavior::CancelAfter(limit) => {
                if seen >= limit {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn run_scenario(
    scenario: &Scenario,
    schema: Arc<Schema>,
    batches: Arc<Vec<Arc<RecordBatch>>>,
    task_ctx: Arc<TaskContext>,
    sample_rss: bool,
) -> Result<(Duration, u64)> {
    let input: Arc<dyn ExecutionPlan> = Arc::new(SyntheticExec::new(
        Arc::clone(&schema),
        scenario.input_partitions,
        batches,
    ));

    let broadcast = Arc::new(BroadcastExec::new(
        Arc::clone(&input),
        scenario.consumer_tasks,
    ));

    let sampler = sample_rss.then(|| PeakRssSampler::start(Duration::from_millis(25)));
    let start = Instant::now();

    let mut join_set = tokio::task::JoinSet::new();
    for consumer in &scenario.consumers {
        let plan = Arc::clone(&broadcast) as Arc<dyn ExecutionPlan>;
        let task_ctx = Arc::clone(&task_ctx);
        let behavior = consumer.behavior;
        let partition = consumer.partition;
        join_set.spawn(async move { consume_partition(plan, partition, task_ctx, behavior).await });
    }

    while let Some(res) = join_set.join_next().await {
        let res =
            res.map_err(|err| datafusion::error::DataFusionError::Execution(err.to_string()))?;
        res?;
    }

    let elapsed = start.elapsed();
    let peak_kb = sampler.map(|s| s.stop()).unwrap_or(0);
    Ok((elapsed, peak_kb))
}

fn all_fast_consumers(output_partitions: usize) -> Vec<ConsumerSpec> {
    (0..output_partitions)
        .map(|partition| ConsumerSpec {
            partition,
            behavior: ConsumerBehavior::Fast,
        })
        .collect()
}

fn scenario_matrix() -> Vec<Scenario> {
    let input_partitions = 16;
    let consumer_tasks = 4;
    let output_partitions = input_partitions * consumer_tasks;

    let mut scenarios = vec![
        // All consumers read at full speed. Baseline overhead without cancellations or lag.
        // Example: Broadcast of a small dimension table, everything goes smoothly.
        Scenario {
            name: "all_fast",
            input_partitions,
            consumer_tasks,
            rows_per_batch: 500_000,
            num_batches: 100,
            consumers: all_fast_consumers(output_partitions),
        },
        // One consumer is slow, creating backpressure or queue buildup.
        // Example: Broadcast build side for a join where one probe task is a straggler
        // due to skewed partition.
        Scenario {
            name: "one_slow",
            input_partitions,
            consumer_tasks,
            rows_per_batch: 500_000,
            num_batches: 100,
            consumers: {
                let mut consumers = all_fast_consumers(output_partitions);
                if let Some(first) = consumers.first_mut() {
                    first.behavior = ConsumerBehavior::Slow(Duration::from_millis(2));
                }
                consumers
            },
        },
        // One consumer cancels early (TopK/LIMIT finishes upstream, task killed).
        // Example: SELECT ... FROM store_sales JOIN store ON ... ORDER BY ... LIMIT 100.
        // The broadcast build side (store) is still fully materialized, but the LIMIT
        // above the join can cancel probe-side consumers once enough rows are produced.
        Scenario {
            name: "one_cancel",
            input_partitions,
            consumer_tasks,
            rows_per_batch: 500_000,
            num_batches: 100,
            consumers: {
                let mut consumers = all_fast_consumers(output_partitions);
                if let Some(first) = consumers.first_mut() {
                    first.behavior = ConsumerBehavior::CancelAfter(5);
                }
                consumers
            },
        },
        // One virtual partition is never executed (no consumer spawned).
        // Example: Fault tolerance, a consumer task fails.
        Scenario {
            name: "unused_partition",
            input_partitions,
            consumer_tasks,
            rows_per_batch: 500_000,
            num_batches: 100,
            consumers: {
                let mut consumers = all_fast_consumers(output_partitions);
                consumers.pop();
                consumers
            },
        },
    ];

    let many_consumers = 16usize;
    let many_output = input_partitions * many_consumers;
    scenarios.push(Scenario {
        name: "many_consumers",
        input_partitions,
        consumer_tasks: many_consumers,
        rows_per_batch: 8192,
        num_batches: 100,
        consumers: all_fast_consumers(many_output),
    });

    scenarios
}

fn verbose_enabled() -> bool {
    match std::env::var("BROADCAST_BENCH_VERBOSE") {
        Ok(val) => {
            let val = val.to_ascii_lowercase();
            val == "1" || val == "true" || val == "yes"
        }
        Err(_) => false,
    }
}

fn rss_enabled() -> bool {
    match std::env::var("BROADCAST_BENCH_RSS") {
        Ok(val) => {
            let val = val.to_ascii_lowercase();
            val == "1" || val == "true" || val == "yes"
        }
        Err(_) => false,
    }
}

fn runtime_threads() -> Option<usize> {
    std::env::var("BROADCAST_BENCH_THREADS")
        .ok()
        .and_then(|val| val.parse::<usize>().ok())
        .filter(|threads| *threads > 0)
}

fn bench_broadcast_cache(c: &mut Criterion) {
    let mut rt_builder = RuntimeBuilder::new_multi_thread();
    if let Some(threads) = runtime_threads() {
        rt_builder.worker_threads(threads);
    }
    let rt = rt_builder.enable_all().build().expect("tokio runtime");

    let mut group = c.benchmark_group("broadcast_cache_scenarios");
    group.sample_size(10);
    let verbose = verbose_enabled();
    let sample_rss = rss_enabled();
    let task_ctx = SessionContext::new().task_ctx();

    for scenario in scenario_matrix() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "bytes",
            DataType::UInt8,
            false,
        )]));
        let batches = (0..scenario.num_batches)
            .map(|_| {
                let data = vec![0u8; scenario.rows_per_batch];
                let array = UInt8Array::from(data);
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])
                    .expect("batch");
                Arc::new(batch)
            })
            .collect::<Vec<_>>();
        let batches = Arc::new(batches);

        group.bench_function(BenchmarkId::new("scenario", scenario.name), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                let mut peaks = Vec::with_capacity(iters as usize);
                for i in 0..iters {
                    let (elapsed, peak_kb) = rt
                        .block_on(run_scenario(
                            &scenario,
                            Arc::clone(&schema),
                            Arc::clone(&batches),
                            Arc::clone(&task_ctx),
                            sample_rss,
                        ))
                        .expect("scenario");
                    if verbose || sample_rss {
                        eprintln!(
                            "scenario={} iter={} peak_rss_kb={} elapsed_ms={}",
                            scenario.name,
                            i,
                            peak_kb,
                            elapsed.as_millis()
                        );
                    }
                    peaks.push(peak_kb);
                    total += elapsed;
                }
                if sample_rss && !peaks.is_empty() {
                    peaks.sort_unstable();
                    let min = peaks[0];
                    let max = peaks[peaks.len() - 1];
                    let median = peaks[peaks.len() / 2];
                    eprintln!(
                        "scenario={} peak_rss_kb[min/median/max]={}/{}/{} runs={}",
                        scenario.name,
                        min,
                        median,
                        max,
                        peaks.len()
                    );
                }
                total
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_broadcast_cache);
criterion_main!(benches);
