use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::collect;
use datafusion_datasets::output::TESTDATA_ROOT;
use datafusion_datasets::parquet_layout::ParquetLayout;
use datafusion_datasets::tpch::TpchSpec;
use datafusion_datasets::{register_tables, tpch};
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::{
    DefaultSessionBuilder, DistributedExec, DistributedExt, DistributedMetricsFormat,
    generate_plan_dump, rewrite_distributed_plan_with_metrics,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Builder as RuntimeBuilder;

const NUM_WORKERS: usize = 10;
const TARGET_PARTITIONS: usize = 128;

/// Measures [generate_plan_dump] on executed TPC-H plans with their metrics, which is the part of
/// dumping a plan that runs in an application. Executing the queries and collecting their metrics
/// happens once per query, outside of the measurement.
fn plan_dump(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("plan_dump");

    let tpch_spec = TpchSpec {
        location: TESTDATA_ROOT.to_string(),
        scale_factor: 0.1,
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

        let plan_with_metrics = rt
            .block_on(async move {
                let sql = tpch::get_query(query)?;
                let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
                if !plan.is::<DistributedExec>() {
                    panic!("The plan was not distributed")
                }
                let _ = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
                rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::PerTask).await
            })
            .unwrap();

        group.bench_function(BenchmarkId::new("generate", i), |b| {
            b.iter(|| generate_plan_dump(&plan_with_metrics).unwrap())
        });
    }

    group.finish()
}

criterion_group!(benches, plan_dump);
criterion_main!(benches);
