//! # Over-distribution
//!
//! ```shell
//! cargo bench -p datafusion-distributed-benchmarks --bench over_distribution -- --save-baseline main
//! cargo bench -p datafusion-distributed-benchmarks --bench over_distribution -- --baseline main
//! ```
//!
//! Queries over TPC-H SF0.1 spread across 32 localhost workers sharing one machine, so the time is
//! dominated by the cost of distributing. Each scenario is named after the cost it exposes.

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion_datasets::output::TESTDATA_ROOT;
use datafusion_datasets::parquet_layout::ParquetLayout;
use datafusion_datasets::tpch::TpchSpec;
use datafusion_datasets::{register_tables, tpch};
use datafusion_distributed::DefaultSessionBuilder;
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use futures::TryStreamExt;
use std::path::PathBuf;
use std::time::Duration;
use tokio::runtime::Builder as RuntimeBuilder;

const NUM_WORKERS: usize = 32;
const PARTITIONS: usize = 16;

const SCENARIOS: [(&str, &str); 6] = [
    (
        "coordinator_fan_in",
        "SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice,
                l_discount, l_shipdate
         FROM lineitem",
    ),
    (
        "repeated_reshuffles",
        "SELECT n_name,
                sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS profit
         FROM lineitem
         JOIN orders ON l_orderkey = o_orderkey
         JOIN customer ON o_custkey = c_custkey
         JOIN partsupp ON l_partkey = ps_partkey AND l_suppkey = ps_suppkey
         JOIN part ON l_partkey = p_partkey
         JOIN supplier ON l_suppkey = s_suppkey
         JOIN nation ON c_nationkey = n_nationkey
         GROUP BY n_name",
    ),
    (
        "unreduced_shuffle",
        "SELECT count(*) AS groups, sum(quantity) AS quantity
         FROM (
             SELECT l_orderkey, l_suppkey, sum(l_quantity) AS quantity
             FROM lineitem
             GROUP BY l_orderkey, l_suppkey
         )",
    ),
    (
        "coordinator_merge",
        "SELECT l_orderkey, l_linenumber, l_extendedprice
         FROM lineitem
         ORDER BY l_extendedprice, l_orderkey, l_linenumber",
    ),
    (
        "idle_tasks",
        "SELECT l_returnflag, max(row_number) AS rows
         FROM (
             SELECT l_returnflag,
                    row_number() OVER (PARTITION BY l_returnflag ORDER BY l_extendedprice)
                        AS row_number
             FROM lineitem
         )
         GROUP BY l_returnflag",
    ),
    (
        "early_cancellation",
        "SELECT l_orderkey, l_comment
         FROM lineitem
         WHERE l_comment LIKE '%unusual%'
         LIMIT 100",
    ),
];

fn over_distribution(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("over_distribution");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));

    let tpch_spec = TpchSpec {
        location: TESTDATA_ROOT.to_string(),
        scale_factor: 0.1,
        // Small files so every table has enough independently readable files (one row group each)
        // to give every worker real data; with the default size each table is a single file.
        layout: ParquetLayout {
            max_file_size: 512 * 1024,
            ..ParquetLayout::default()
        },
    };

    let (ctx, _workers, _) = rt
        .block_on(async {
            tpch::generate_data(&tpch_spec).await.unwrap();
            let (ctx, workers, handles) =
                start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
            ctx.sql("SET distributed.file_scan_config_bytes_per_partition = 1")
                .await?;
            ctx.sql(&format!(
                "SET datafusion.execution.target_partitions = {PARTITIONS}"
            ))
            .await?;
            register_tables(&ctx, &PathBuf::from(tpch_spec.dataset())).await?;
            Ok::<_, DataFusionError>((ctx, workers, handles))
        })
        .unwrap();

    for (scenario, sql) in SCENARIOS {
        group.bench_function(scenario, |b| {
            b.iter(|| rt.block_on(run_query(&ctx, sql)).unwrap())
        });
    }
    group.finish();
}

/// Plans and executes `sql`, draining the output without keeping it, and returns its row count.
async fn run_query(ctx: &SessionContext, sql: &str) -> Result<usize> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    execute_stream(plan, ctx.task_ctx())?
        .try_fold(0, |rows, batch| async move { Ok(rows + batch.num_rows()) })
        .await
}

criterion_group!(benches, over_distribution);
criterion_main!(benches);
