//! Benchmark the `RepartitionExec -> NetworkShuffleExec` shuffle pipeline end to end.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_distributed::{CompressionType, ShuffleBench};
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RuntimeBuilder;

fn shuffle(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("shuffle");
    group.sample_size(10);

    let benches = vec![
        ShuffleBench::one_to_one_baseline(),
        ShuffleBench::one_to_one_baseline().with_compression(Some(CompressionType::LZ4_FRAME)),
        ShuffleBench::many_to_one_baseline(8),
        ShuffleBench::one_to_many_baseline(8),
        ShuffleBench::one_to_many_baseline(16)
            .with_partitions(16)
            .with_total_rows(2_000_000),
        ShuffleBench::one_to_many_baseline(16)
            .with_partitions(16)
            .with_total_rows(2_000_000)
            .with_compression(Some(CompressionType::LZ4_FRAME)),
        ShuffleBench::many_to_many_baseline(8),
    ];

    for bench in benches {
        let name = bench.label();
        let prepared = rt
            .block_on(bench.prepare())
            .expect("prepare shuffle fixture");
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

criterion_group!(benches, shuffle);
criterion_main!(benches);
