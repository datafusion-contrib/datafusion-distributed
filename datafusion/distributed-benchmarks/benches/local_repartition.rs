//! Benchmark isolated upstream (single-node) repartition cost without any network boundary.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_distributed::{LocalRepartitionBench, LocalRepartitionMode};
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RuntimeBuilder;

fn local_repartition(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("local_repartition");
    group.sample_size(10);

    let benches = vec![
        LocalRepartitionBench::one_to_one_baseline(LocalRepartitionMode::Hash),
        LocalRepartitionBench::one_to_many_baseline(LocalRepartitionMode::Hash, 16)
            .with_total_rows(2_000_000),
        LocalRepartitionBench::many_to_one_baseline(LocalRepartitionMode::Hash, 8),
        LocalRepartitionBench::many_to_many_baseline(LocalRepartitionMode::Hash, 8),
        LocalRepartitionBench::one_to_many_baseline(LocalRepartitionMode::RoundRobin, 16)
            .with_total_rows(2_000_000),
        LocalRepartitionBench::many_to_many_baseline(LocalRepartitionMode::RoundRobin, 8),
    ];

    for bench in benches {
        let name = bench.label();
        let prepared = bench.prepare().expect("prepare local_repartition fixture");
        group.bench_function(BenchmarkId::new("stream", name), |b| {
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

criterion_group!(benches, local_repartition);
criterion_main!(benches);
