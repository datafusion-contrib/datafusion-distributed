//! Compare allocations before and after consuming owned protobuf metrics.
//! Run with: cargo run --locked --example metrics_allocations

use datafusion::error::DataFusionError;
use datafusion::physical_plan::metrics::MetricsSet;
pub use datafusion_distributed::{
    AvgLatencyMetric, BytesCounterMetric, FirstLatencyMetric, MaxGaugeMetric, MaxLatencyMetric,
    MinLatencyMetric, P50LatencyMetric, P75LatencyMetric, P95LatencyMetric, P99LatencyMetric,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};

#[allow(dead_code)]
mod generated {
    pub mod worker {
        include!("../src/protocol/grpc/generated/worker.rs");
    }
}

// Include the production converter without exposing an internal library API.
mod converter {
    include!("../src/protocol/grpc/metrics_proto.rs");
}

use generated::worker as pb;
static TRACK: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

struct Counter;

fn record(n: usize) {
    if TRACK.load(Relaxed) {
        CALLS.fetch_add(1, Relaxed);
        BYTES.fetch_add(n, Relaxed);
    }
}

unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        record(l.size());
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        record(l.size());
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        record(n);
        unsafe { System.realloc(p, l, n) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}

#[global_allocator]
static ALLOCATOR: Counter = Counter;

fn measure(f: impl FnOnce() -> MetricsSet) -> (MetricsSet, usize, usize) {
    CALLS.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    TRACK.store(true, Relaxed);
    let value = black_box(f());
    TRACK.store(false, Relaxed);
    (value, CALLS.load(Relaxed), BYTES.load(Relaxed))
}

// The pre-change loop, calling the same unchanged per-metric converter.
// Keeping this small baseline here makes the comparison independent of Git history.
fn decode_before(metrics_set_proto: &pb::MetricsSet) -> Result<MetricsSet, DataFusionError> {
    let mut metrics_set = MetricsSet::new();
    metrics_set_proto.metrics.iter().try_for_each(|metric| {
        let proto = converter::metric_proto_to_df(metric.clone())?;
        metrics_set.push(proto);
        Ok::<(), DataFusionError>(())
    })?;
    Ok(metrics_set)
}

fn fixture(mixed: bool) -> pb::MetricsSet {
    let mut sketch = sketches_ddsketch::DDSketch::new(sketches_ddsketch::Config::default());
    for n in 1..=100 {
        sketch.add(n as f64 * 1000.0);
    }
    let bytes = bincode::serialize(&sketch).unwrap();
    pb::MetricsSet {
        metrics: (0..128)
            .map(|i| pb::Metric {
                partition: Some(i % 4),
                labels: vec![
                    pb::Label {
                        name: "operator".into(),
                        value: "ParquetScanExec".into(),
                    },
                    pb::Label {
                        name: "task_id".into(),
                        value: i.to_string(),
                    },
                ],
                value: Some(if mixed && i % 8 == 0 {
                    pb::metric::Value::CustomP95Latency(pb::PercentileLatency {
                        name: format!("scan_latency_{i}"),
                        sketch_bytes: bytes.clone(),
                    })
                } else {
                    pb::metric::Value::Count(pb::NamedCount {
                        name: format!("scan_rows_{i}"),
                        value: i * 100,
                    })
                }),
            })
            .collect(),
    }
}

fn main() {
    for mixed in [false, true] {
        let fixture = fixture(mixed);
        let mut counts = None;
        for _ in 0..32 {
            // Input construction and cloning are outside the measured region.
            let before_input = fixture.clone();
            let after_input = fixture.clone();
            let (before_metrics, before_calls, before_bytes) =
                measure(|| decode_before(black_box(&before_input)).unwrap());
            let (after_metrics, after_calls, after_bytes) =
                measure(|| converter::metrics_set_proto_to_df(black_box(after_input)).unwrap());
            // Verify complete protobuf values, including labels, order, and sketch bytes.
            let before_roundtrip = converter::df_metrics_set_to_proto(&before_metrics).unwrap();
            let after_roundtrip = converter::df_metrics_set_to_proto(&after_metrics).unwrap();
            assert_eq!(before_roundtrip, fixture);
            assert_eq!(after_roundtrip, fixture);
            let this = (before_calls, before_bytes, after_calls, after_bytes);
            if let Some(previous) = counts {
                assert_eq!(previous, this);
            }
            counts = Some(this);
        }
        let (before_calls, before_bytes, after_calls, after_bytes) = counts.unwrap();
        println!(
            "{}: 32 identical repeated roundtrips, calls {before_calls} -> {after_calls} ({:.1}% reduction), bytes {before_bytes} -> {after_bytes} ({:.1}% reduction)",
            if mixed { "mixed16" } else { "counters" },
            100.0 * (before_calls - after_calls) as f64 / before_calls as f64,
            100.0 * (before_bytes - after_bytes) as f64 / before_bytes as f64
        );
    }
}
