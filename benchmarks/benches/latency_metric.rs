use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_distributed::P50LatencyMetric;
use std::sync::Arc;
use std::time::Duration;

fn bench_concurrent_add_duration(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_add_duration");
    let ops_per_thread = 1_000;

    group.bench_function("P50LatencyMetric", |b| {
        b.iter(|| {
            let m = Arc::new(P50LatencyMetric::default());
            std::thread::scope(|s| {
                for t in 0..16 {
                    let m = Arc::clone(&m);
                    s.spawn(move || {
                        for i in 0..ops_per_thread {
                            m.add_duration(Duration::from_nanos(1000 + (t * 37 + i * 13) % 5000));
                        }
                    });
                }
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_concurrent_add_duration,);
criterion_main!(benches);
