//! # Task startup overlap
//!
//! ```shell
//! cargo bench -p datafusion-distributed-benchmarks --bench startup_overlap -- --save-baseline main
//! # Run the same benchmark on the candidate revision:
//! cargo bench -p datafusion-distributed-benchmarks --bench startup_overlap -- --baseline main
//! ```
//!
//! This controlled benchmark runs two tasks over localhost gRPC in static and AQE
//! mode. Task 0 performs 100 million dependent integer operations; task 1 returns
//! one row. The existing localhost fixture starts three endpoints so both active
//! tasks can route away from the coordinator's local endpoint. Each query must
//! return task indices `[0, 1]`, and the CPU function must execute remotely once.
//!
//! `delay_light_task` adds 250 ms to task 1's routing setup, allowing task 0's CPU
//! work to overlap that wait. `delay_heavy_task` delays task 0 instead, keeping the
//! wait on the critical path; `no_delay` provides the other control. The delay is
//! only in routing setup, not in the CPU workload. Criterion measures execution
//! through complete output collection; worker startup and query planning are
//! excluded. An untimed preflight also reports first-batch time and whether remote
//! CPU work started and finished before the delayed task returned a ready
//! destination. The witnesses are reported without requiring overlap, so the
//! benchmark also runs before the optimization.
//!
//! Run both revisions on a quiet host and alternate comparisons. Verify a positive
//! overlap witness in the candidate's `delay_light_task` preflight and its absence
//! in the baseline before interpreting timings. This isolates scheduling value
//! under controlled setup skew; it does not replace representative suite results.

use arrow::array::Int64Array;
use arrow::datatypes::DataType;
use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::common::{Result, exec_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{ScalarUDF, Volatility, create_udf};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::test_utils::routing::{
    URLEmitterExtensionCodec, URLEmitterFunction, UrlEmitterRouteTaskHandler,
    url_emitter_desired_task_count, url_emitter_scale_up_leaf_node,
};
use datafusion_distributed::{
    DistributedExt, MappedWorkerSessionBuilderExt, RouteTaskEvent, RouteTaskEventResponse,
    RouteTaskHandler, WorkerQueryContext,
};
use futures::TryStreamExt;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::time::timeout;

const QUERY: &str = "SELECT cpu_work(task_index) FROM url_emitter(2, 2, 'startup')";
const SETUP_DELAY: Duration = Duration::from_millis(250);
const WAIT: Duration = Duration::from_secs(10);

fn startup_overlap(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("startup_overlap");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    for (mode, adaptive) in [("static", false), ("aqe", true)] {
        for (scenario, delayed_task) in [
            ("no_delay", None),
            ("delay_light_task", Some(1)),
            ("delay_heavy_task", Some(0)),
        ] {
            let probe = Arc::new(Probe::default());
            let udf = Arc::new(cpu_work(Some(Arc::clone(&probe))));
            let worker = build_state.map(move |builder| {
                Ok(builder
                    .with_scalar_functions(vec![Arc::clone(&udf)])
                    .build())
            });
            // Routing uses URLs 2 and 1, avoiding URL 0's local-worker shortcut.
            let (mut ctx, _workers, _) = rt.block_on(start_localhost_context(3, worker));
            ctx.set_distributed_desired_task_count_handler(url_emitter_desired_task_count);
            ctx.set_distributed_scale_up_leaf_node_handler(url_emitter_scale_up_leaf_node);
            ctx.set_distributed_user_codec(URLEmitterExtensionCodec);
            ctx.register_udtf("url_emitter", Arc::new(URLEmitterFunction));
            ctx.register_udf(cpu_work(None));
            let ctx = ctx
                .with_distributed_dynamic_task_count(adaptive)
                .unwrap()
                .with_distributed_route_task_handler(SetupDelay {
                    delayed_task,
                    probe: Arc::clone(&probe),
                });
            rt.block_on(ctx.sql("SET datafusion.execution.target_partitions = 2"))
                .unwrap();

            let (first, total) = rt.block_on(run_query(&ctx, &probe)).unwrap();
            println!(
                "preflight {mode}/{scenario}: first={first:?} total={total:?} \
                 heavy_started_before_ready={} heavy_finished_before_ready={}",
                probe.started_before_ready.load(Ordering::SeqCst),
                probe.finished_before_ready.load(Ordering::SeqCst),
            );
            group.bench_function(BenchmarkId::new(mode, scenario), |b| {
                b.iter_custom(|iterations| {
                    rt.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            elapsed += run_query(&ctx, &probe).await.unwrap().1;
                        }
                        elapsed
                    })
                })
            });
        }
    }
    group.finish();
}

async fn run_query(ctx: &SessionContext, probe: &Probe) -> Result<(Duration, Duration)> {
    let plan = ctx.sql(QUERY).await?.create_physical_plan().await?;
    probe.delayed_ready.store(false, Ordering::SeqCst);
    probe.started_before_ready.store(false, Ordering::SeqCst);
    probe.finished_before_ready.store(false, Ordering::SeqCst);
    probe.heavy_executions.store(0, Ordering::SeqCst);
    let start = Instant::now();
    let mut output = execute_stream(plan, ctx.task_ctx())?;
    let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
    let first_elapsed = start.elapsed();
    let mut batches = vec![first];
    batches.extend(
        timeout(WAIT, output.try_collect::<Vec<_>>())
            .await
            .unwrap()?,
    );
    let total_elapsed = start.elapsed();
    let mut values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, vec![0, 1]);
    assert_eq!(probe.heavy_executions.load(Ordering::SeqCst), 1);
    Ok((first_elapsed, total_elapsed))
}

async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState> {
    Ok(ctx
        .builder
        .with_distributed_user_codec(URLEmitterExtensionCodec)
        .build())
}

#[derive(Default)]
struct Probe {
    delayed_ready: AtomicBool,
    started_before_ready: AtomicBool,
    finished_before_ready: AtomicBool,
    heavy_executions: AtomicUsize,
}

struct SetupDelay {
    delayed_task: Option<usize>,
    probe: Arc<Probe>,
}

#[async_trait]
impl RouteTaskHandler for SetupDelay {
    async fn handle(&self, event: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        let delayed = self.delayed_task == Some(event.task_key.task_number);
        if delayed {
            tokio::time::sleep(SETUP_DELAY).await;
        }
        let response = UrlEmitterRouteTaskHandler.handle(event).await?;
        if delayed || self.delayed_task.is_none() {
            self.probe.delayed_ready.store(true, Ordering::SeqCst);
        }
        Some(response)
    }
}

fn cpu_work(probe: Option<Arc<Probe>>) -> ScalarUDF {
    create_udf(
        "cpu_work",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |args| {
            let Some(probe) = &probe else {
                return exec_err!("cpu_work must execute on a remote worker");
            };
            let input = args[0].clone().into_array(1)?;
            let input = input.as_any().downcast_ref::<Int64Array>().unwrap();
            for task in input.values() {
                if *task == 0 {
                    probe.heavy_executions.fetch_add(1, Ordering::SeqCst);
                    probe.started_before_ready.store(
                        !probe.delayed_ready.load(Ordering::SeqCst),
                        Ordering::SeqCst,
                    );
                    let mut value = 1u64;
                    for _ in 0..100_000_000 {
                        value = black_box(value.wrapping_mul(17).wrapping_add(1));
                    }
                    probe.finished_before_ready.store(
                        !probe.delayed_ready.load(Ordering::SeqCst),
                        Ordering::SeqCst,
                    );
                }
            }
            Ok(args[0].clone())
        }),
    )
}

criterion_group!(benches, startup_overlap);
criterion_main!(benches);
