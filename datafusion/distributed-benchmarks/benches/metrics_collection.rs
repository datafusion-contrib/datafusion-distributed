use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::collect;
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::{
    DefaultSessionBuilder, DistributedExec, DistributedExt, DistributedMetricsFormat,
    display_plan_ascii, rewrite_distributed_plan_with_metrics,
};
use datafusion_distributed_benchmarks::datasets::output::TESTDATA_ROOT;
use datafusion_distributed_benchmarks::datasets::parquet_layout::ParquetLayout;
use datafusion_distributed_benchmarks::datasets::tpch::TpchSpec;
use datafusion_distributed_benchmarks::datasets::{register_tables, tpch};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Builder as RuntimeBuilder;

const NUM_WORKERS: usize = 10;
const TARGET_PARTITIONS: usize = 128;

fn metrics_collection(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("planning");
    group.sample_size(10);

    let tpch_spec = TpchSpec {
        location: TESTDATA_ROOT.to_string(),
        scale_factor: 1.0,
        layout: ParquetLayout::default(),
    };

    let (ctx, _join_set, _workers) = rt
        .block_on(async move {
            tpch::generate_data(&tpch_spec).await.unwrap();

            let (ctx, join_set, workers) =
                start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
            let ctx = ctx.with_distributed_metrics_collection(true)?;
            ctx.sql(&format!(
                "SET datafusion.execution.target_partitions = {TARGET_PARTITIONS}"
            ))
            .await?;
            register_tables(&ctx, &PathBuf::from(tpch_spec.dataset())).await?;

            Ok::<_, DataFusionError>((ctx, join_set, workers))
        })
        .unwrap();

    for (i, query) in tpch::get_queries().iter().enumerate() {
        if query == "q15" {
            // q15 is weird because it has views, rather than complicating the code here, let's just
            // skip it.
            continue;
        }
        let ctx = ctx.clone();

        let (plan, plan_with_metrics) = rt
            .block_on(async move {
                let sql = tpch::get_query(query)?;
                let df = ctx.sql(&sql).await?;
                let plan = df.create_physical_plan().await?;
                if !plan.is::<DistributedExec>() {
                    panic!("The plan was not distributed")
                }
                let _ = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
                let plan_with_metrics = rewrite_distributed_plan_with_metrics(
                    Arc::clone(&plan),
                    DistributedMetricsFormat::PerTask,
                )
                .await?;
                Ok::<_, DataFusionError>((plan, plan_with_metrics))
            })
            .unwrap();

        group.bench_function(BenchmarkId::new("rewrite", i), |b| {
            b.iter(|| {
                rt.block_on(rewrite_distributed_plan_with_metrics(
                    Arc::clone(&plan),
                    DistributedMetricsFormat::PerTask,
                ))
            })
        });

        group.bench_function(BenchmarkId::new("display", i), |b| {
            b.iter(|| display_plan_ascii(plan_with_metrics.as_ref(), true))
        });
    }

    group.finish()
}

criterion_group!(benches, metrics_collection);
criterion_main!(benches);
