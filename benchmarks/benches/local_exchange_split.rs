use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_distributed::{LocalExchangeIdMode, LocalExchangeSplitBench, LocalFanoutStrategy};
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RuntimeBuilder;

fn local_exchange_split(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("local_exchange_split");
    group.sample_size(10);

    let mut benches = Vec::new();

    benches.extend(split_and_repartition(
        LocalExchangeSplitBench::one_owned_baseline(),
    ));
    benches.extend(split_and_repartition(
        LocalExchangeSplitBench::one_owned_baseline().with_id_mode(LocalExchangeIdMode::HotKey),
    ));

    benches.extend(split_and_repartition(
        LocalExchangeSplitBench::one_owned_baseline()
            .with_local_partitions(32)
            .with_total_rows(1_000_000),
    ));
    // Small batches stress channel overhead relative to hash throughput.
    benches.extend(split_and_repartition(
        LocalExchangeSplitBench::one_owned_baseline()
            .with_local_partitions(32)
            .with_total_rows(1_000_000)
            .with_batch_size(256),
    ));

    for bench in benches {
        let name = bench.label();
        let prepared = bench
            .prepare()
            .expect("prepare local exchange split fixture");
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

fn split_and_repartition(bench: LocalExchangeSplitBench) -> [LocalExchangeSplitBench; 2] {
    [
        bench.clone(),
        bench.with_strategy(LocalFanoutStrategy::Repartition),
    ]
}

criterion_group!(benches, local_exchange_split);
criterion_main!(benches);
