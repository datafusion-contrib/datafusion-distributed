use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use datafusion_distributed::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
use datafusion_distributed::{DistributedExt, create_distributed_plan};
use datafusion_distributed_benchmarks::datasets::output::TESTDATA_ROOT;
use datafusion_distributed_benchmarks::datasets::parquet_layout::ParquetLayout;
use datafusion_distributed_benchmarks::datasets::tpch::TpchSpec;
use datafusion_distributed_benchmarks::datasets::{register_tables, tpch};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Builder as RuntimeBuilder;

fn planning(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("planning");
    group.sample_size(10);

    let tpch_spec = TpchSpec {
        location: TESTDATA_ROOT.to_string(),
        scale_factor: 0.1,
        layout: ParquetLayout::default(),
    };

    rt.block_on(tpch::generate_data(&tpch_spec)).unwrap();

    let ctx = SessionContext::new();
    let session_state = ctx
        .state()
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(10));
    rt.block_on(register_tables(&ctx, &PathBuf::from(tpch_spec.dataset())))
        .unwrap();

    for (i, query) in tpch::get_queries().iter().enumerate() {
        if query == "q15" {
            // q15 is weird because it has views, rather than complicating the code here, let's just
            // skip it.
            continue;
        }
        let sql = tpch::get_query(query).unwrap();
        let df = rt.block_on(ctx.sql(&sql)).unwrap();
        let plan = rt.block_on(df.create_physical_plan()).unwrap();
        group.bench_function(BenchmarkId::new("query", i.to_string()), |b| {
            b.iter(|| rt.block_on(create_distributed_plan(Arc::clone(&plan), &session_state)))
        });
    }

    group.finish()
}

criterion_group!(benches, planning);
criterion_main!(benches);
