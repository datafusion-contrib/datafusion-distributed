//! Benchmark isolated `NetworkShuffleExec` exchange transport cost with upstream repartition removed.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_distributed::{CompressionType, TransportBench, TransportBenchMode};
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RuntimeBuilder;

fn transport(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("transport");
    group.sample_size(10);

    let benches = vec![
        TransportBench::one_to_one_baseline(TransportBenchMode::InMemory),
        TransportBench::one_to_one_baseline(TransportBenchMode::Tcp),
        TransportBench::many_to_one_baseline(TransportBenchMode::InMemory, 8),
        TransportBench::many_to_one_baseline(TransportBenchMode::Tcp, 8),
        TransportBench::one_to_many_baseline(TransportBenchMode::InMemory, 16)
            .with_partitions(16)
            .with_total_rows(2_000_000),
        TransportBench::one_to_many_baseline(TransportBenchMode::Tcp, 16)
            .with_partitions(16)
            .with_total_rows(2_000_000),
        TransportBench::many_to_many_baseline(TransportBenchMode::InMemory, 8),
        TransportBench::many_to_many_baseline(TransportBenchMode::Tcp, 8),
        TransportBench::one_to_many_baseline(TransportBenchMode::Tcp, 16)
            .with_partitions(16)
            .with_total_rows(2_000_000)
            .with_compression(Some(CompressionType::LZ4_FRAME)),
    ];

    for bench in benches {
        let name = bench.label();
        let prepared = rt
            .block_on(bench.prepare())
            .expect("prepare transport fixture");
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

criterion_group!(benches, transport);
criterion_main!(benches);
