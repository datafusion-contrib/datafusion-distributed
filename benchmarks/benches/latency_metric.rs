use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_distributed::P50LatencyMetric;
use std::sync::Arc;
use std::time::Duration;

fn bench_concurrent_add_duration(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_add_duration");
    let num_tasks = 1_000_u64;

    group.bench_function("P50LatencyMetric (Mutex<DDSketch>)", |b| {
        b.iter(|| {
            rt.block_on(async {
                let m = Arc::new(P50LatencyMetric::default());
                let mut handles = Vec::with_capacity(num_tasks as usize);
                let duration = Duration::from_millis(1);
                for _ in 0..num_tasks {
                    let m = Arc::clone(&m);
                    handles.push(tokio::spawn(async move {
                        for _ in 0..100 {
                            m.add_duration(duration);
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_concurrent_add_duration,);
criterion_main!(benches);
