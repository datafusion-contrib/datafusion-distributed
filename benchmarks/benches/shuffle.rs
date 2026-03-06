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
        ShuffleBench {
            producer_tasks: 1,
            consumer_tasks: 1,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        },
        ShuffleBench {
            producer_tasks: 1,
            consumer_tasks: 1,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: Some(CompressionType::LZ4_FRAME),
        },
        ShuffleBench {
            producer_tasks: 8,
            consumer_tasks: 1,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        },
        ShuffleBench {
            producer_tasks: 1,
            consumer_tasks: 8,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        },
        ShuffleBench {
            producer_tasks: 8,
            consumer_tasks: 8,
            partitions: 8,
            total_rows: 1_000_000,
            batch_size: 1024,
            compression: None,
        },
    ];

    for bench in benches {
        let name = bench.to_string();
        group.bench_function(BenchmarkId::new("stream", name), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    rt.block_on(bench.run()).unwrap();
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
