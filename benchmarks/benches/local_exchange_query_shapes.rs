use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_distributed::{LocalExchangeQueryBench, QueryBenchProfile, QueryBenchShape};
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RuntimeBuilder;

fn local_exchange_query_shapes(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("local_exchange_query_shapes");
    group.sample_size(10);

    let mut benches = Vec::new();

    for shape in [
        QueryBenchShape::LowCardinalityFinalAgg,
        QueryBenchShape::HighCardinalityFinalAgg,
        QueryBenchShape::BalancedPartitionedJoin,
        QueryBenchShape::SkewedPartitionedJoin,
        QueryBenchShape::AsymmetricPartitionedJoin,
        QueryBenchShape::JoinThenRegroupTopK,
        QueryBenchShape::DistinctRegroup,
    ] {
        benches.extend(all_profiles(shape));
    }

    benches.extend(
        all_profiles(QueryBenchShape::HighCardinalityFinalAgg).map(|bench| {
            bench
                .with_base_partitions(1)
                .with_local_partitions(32)
                .with_total_rows(1_000_000)
                .with_key_domain(1_000_000)
        }),
    );

    for bench in benches {
        let name = bench.label();
        let prepared = bench
            .prepare()
            .expect("prepare local exchange query fixture");
        group.bench_function(BenchmarkId::new("query", name), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    rt.block_on(prepared.run()).unwrap();
                    total += start.elapsed();
                }
                total
            });
        });
    }

    group.finish();
}

fn all_profiles(shape: QueryBenchShape) -> impl Iterator<Item = LocalExchangeQueryBench> {
    [
        QueryBenchProfile::Baseline,
        QueryBenchProfile::Split,
        QueryBenchProfile::Repartition,
    ]
    .into_iter()
    .map(move |profile| LocalExchangeQueryBench::new(shape, profile))
}

criterion_group!(benches, local_exchange_query_shapes);
criterion_main!(benches);
