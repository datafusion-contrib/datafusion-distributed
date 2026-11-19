//! # Config propagation
//!
//! ```shell
//! cargo bench -p datafusion-distributed-benchmarks --bench config_propagation -- --save-baseline main
//! cargo bench -p datafusion-distributed-benchmarks --bench config_propagation -- --baseline main
//! ```
//!
//! Every task the coordinator sends to a worker carries the session's config extensions as gRPC
//! headers, which the worker parses back into its own session. This runs a query that fans out to
//! many tasks over localhost workers, each producing a single row, so the time is dominated by
//! dispatching tasks. To stress the headers, the session carries a user config extension next to
//! `DistributedConfig` with large string options, integer, float and boolean options, and options
//! with long names. `defaults` leaves every option at its default; `few_overrides` changes a
//! handful of options of each kind, as a typical session would.
//! An untimed preflight reports the config headers a worker received per task.

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::common::extensions_options;
use datafusion::config::{ConfigExtension, ExtensionOptions};
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::test_utils::routing::{
    URLEmitterExtensionCodec, URLEmitterFunction, url_emitter_desired_task_count,
    url_emitter_scale_up_leaf_node,
};
use datafusion_distributed::{DistributedExt, WorkerQueryContext};
use futures::TryStreamExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Builder as RuntimeBuilder;

const NUM_WORKERS: usize = 32;
const QUERY: &str = "SELECT * FROM url_emitter(128, 128, 'config_propagation')";
const CONFIG_HEADER_PREFIX: &str = "x-datafusion-distributed-config-";
/// Size of each [LargeOptions] string value, both default and overridden. With every option
/// sent, the config headers stay just under the 16 KiB HTTP/2 header list limit that hyper
/// applies by default; beyond it the worker rejects the task.
const STRING_BYTES: usize = 1024;
/// Shared name of the [LargeOptions] options with long keys, followed by their index.
const LONG_KEY: &str = "option_with_a_deliberately_long_name_to_stress_the_header_keys";

/// Scenario name and how many [LargeOptions] options of each kind are changed from their default.
const SCENARIOS: [(&str, usize); 2] = [("defaults", 0), ("few_overrides", 4)];

fn config_propagation(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("config_propagation");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));

    for (scenario, overridden) in SCENARIOS {
        // (headers, bytes) of the config headers received by the last task a worker started.
        let received = Arc::new(Mutex::new((0, 0)));
        let worker_received = Arc::clone(&received);
        let worker = move |ctx: WorkerQueryContext| {
            let received = Arc::clone(&worker_received);
            async move { build_state(ctx, &received) }
        };

        let (mut ctx, _workers, _) = rt.block_on(start_localhost_context(NUM_WORKERS, worker));
        ctx.set_distributed_desired_task_count_handler(url_emitter_desired_task_count);
        ctx.set_distributed_scale_up_leaf_node_handler(url_emitter_scale_up_leaf_node);
        ctx.set_distributed_user_codec(URLEmitterExtensionCodec);
        ctx.register_udtf("url_emitter", Arc::new(URLEmitterFunction));
        let mut options = LargeOptions::default();
        for i in 0..overridden {
            let string = "y".repeat(STRING_BYTES);
            options.set(&format!("string_{i:02}"), &string).unwrap();
            options.set(&format!("integer_{i:02}"), "1").unwrap();
            options.set(&format!("float_{i:02}"), "1.5").unwrap();
            options.set(&format!("boolean_{i:02}"), "true").unwrap();
            options.set(&format!("{LONG_KEY}_{i:02}"), "1").unwrap();
        }
        ctx.set_distributed_option_extension(options);

        let rows = rt.block_on(run_query(&ctx)).unwrap();
        let (headers, bytes) = *received.lock().unwrap();
        println!("preflight {scenario}: rows={rows} config_headers={headers} bytes={bytes}");

        group.bench_function(scenario, |b| {
            b.iter(|| rt.block_on(run_query(&ctx)).unwrap())
        });
    }
    group.finish();
}

/// Plans and executes the query, draining the output without keeping it, and returns its row
/// count.
async fn run_query(ctx: &SessionContext) -> Result<usize> {
    let plan = ctx.sql(QUERY).await?.create_physical_plan().await?;
    execute_stream(plan, ctx.task_ctx())?
        .try_fold(0, |rows, batch| async move { Ok(rows + batch.num_rows()) })
        .await
}

fn build_state(ctx: WorkerQueryContext, received: &Mutex<(usize, usize)>) -> Result<SessionState> {
    let config_headers = ctx
        .headers
        .iter()
        .filter(|(k, _)| k.as_str().starts_with(CONFIG_HEADER_PREFIX));
    *received.lock().unwrap() = config_headers.fold((0, 0), |(headers, bytes), (k, v)| {
        (headers + 1, bytes + k.as_str().len() + v.len())
    });
    Ok(ctx
        .builder
        .with_distributed_user_codec(URLEmitterExtensionCodec)
        .with_distributed_option_extension_from_headers::<LargeOptions>(&ctx.headers)?
        .build())
}

extensions_options! {
    /// A user config extension with many options of every kind, propagated to workers on every task.
    pub struct LargeOptions {
        pub string_00: String, default = "x".repeat(STRING_BYTES)
        pub string_01: String, default = "x".repeat(STRING_BYTES)
        pub string_02: String, default = "x".repeat(STRING_BYTES)
        pub string_03: String, default = "x".repeat(STRING_BYTES)
        pub string_04: String, default = "x".repeat(STRING_BYTES)
        pub string_05: String, default = "x".repeat(STRING_BYTES)
        pub integer_00: usize, default = 0
        pub integer_01: usize, default = 0
        pub integer_02: usize, default = 0
        pub integer_03: usize, default = 0
        pub integer_04: usize, default = 0
        pub integer_05: usize, default = 0
        pub integer_06: usize, default = 0
        pub integer_07: usize, default = 0
        pub integer_08: usize, default = 0
        pub integer_09: usize, default = 0
        pub integer_10: usize, default = 0
        pub integer_11: usize, default = 0
        pub integer_12: usize, default = 0
        pub integer_13: usize, default = 0
        pub integer_14: usize, default = 0
        pub integer_15: usize, default = 0
        pub float_00: f64, default = 0.0
        pub float_01: f64, default = 0.0
        pub float_02: f64, default = 0.0
        pub float_03: f64, default = 0.0
        pub float_04: f64, default = 0.0
        pub float_05: f64, default = 0.0
        pub float_06: f64, default = 0.0
        pub float_07: f64, default = 0.0
        pub float_08: f64, default = 0.0
        pub float_09: f64, default = 0.0
        pub float_10: f64, default = 0.0
        pub float_11: f64, default = 0.0
        pub float_12: f64, default = 0.0
        pub float_13: f64, default = 0.0
        pub float_14: f64, default = 0.0
        pub float_15: f64, default = 0.0
        pub boolean_00: bool, default = false
        pub boolean_01: bool, default = false
        pub boolean_02: bool, default = false
        pub boolean_03: bool, default = false
        pub boolean_04: bool, default = false
        pub boolean_05: bool, default = false
        pub boolean_06: bool, default = false
        pub boolean_07: bool, default = false
        pub boolean_08: bool, default = false
        pub boolean_09: bool, default = false
        pub boolean_10: bool, default = false
        pub boolean_11: bool, default = false
        pub boolean_12: bool, default = false
        pub boolean_13: bool, default = false
        pub boolean_14: bool, default = false
        pub boolean_15: bool, default = false
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_00: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_01: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_02: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_03: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_04: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_05: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_06: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_07: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_08: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_09: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_10: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_11: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_12: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_13: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_14: usize, default = 0
        pub option_with_a_deliberately_long_name_to_stress_the_header_keys_15: usize, default = 0
    }
}

impl ConfigExtension for LargeOptions {
    const PREFIX: &'static str = "large";
}

criterion_group!(benches, config_propagation);
criterion_main!(benches);
