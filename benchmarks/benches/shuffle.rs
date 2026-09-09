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

// Paired modes on identical inputs; the existing single-mode group stays unchanged.
fn shuffle_modes(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("shuffle_modes");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    for (n, m, rows) in [(1, 8, 8_192), (8, 8, 262_144)] {
        for compression in [None, Some(CompressionType::LZ4_FRAME)] {
            let mut bench = ShuffleBench::many_to_many_baseline(n)
                .with_total_rows(rows)
                .with_batch_size(1024)
                .with_partitions(8)
                .with_compression(compression);
            bench.consumer_tasks = m;
            let fixture = rt
                .block_on(bench.prepare())
                .expect("prepare shuffle fixture");
            for (mode, two_level) in [("single", false), ("two-level", true)] {
                group.bench_function(BenchmarkId::new(mode, bench.label()), |b| {
                    b.iter_custom(|iters| {
                        let start = Instant::now();
                        for _ in 0..iters {
                            rt.block_on(async {
                                if two_level {
                                    fixture.run_two_level().await
                                } else {
                                    fixture.run().await
                                }
                            })
                            .unwrap();
                        }
                        start.elapsed()
                    });
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, shuffle, shuffle_modes);
criterion_main!(benches);
