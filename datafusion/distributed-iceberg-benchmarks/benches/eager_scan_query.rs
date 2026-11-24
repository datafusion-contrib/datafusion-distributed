//! # Distributed queries over wide Iceberg tables with many files
//!
//! ```shell
//! cargo bench -p datafusion-distributed-iceberg-benchmarks --bench eager_scan_query -- --save-baseline main
//! # Run the same benchmark on the candidate revision:
//! cargo bench -p datafusion-distributed-iceberg-benchmarks --bench eager_scan_query -- --baseline main
//! ```
//!
//! This runs queries over eager Iceberg scans on four localhost gRPC workers, so every task plan
//! sent to a remote worker is encoded by the coordinator and decoded by the worker, as in
//! production. Tasks routed to the coordinator's own worker skip both, as they do there too.
//!
//! Each table has 100 columns, a default name mapping, and [`FILES`] one-row data files across
//! 30 identity partitions (see `wide_table`). `one_table` counts the rows of one table.
//! `three_tables` sums per-partition counts from a `UNION ALL` over three tables, whose scans
//! become sibling stages that the coordinator sets up together.
//! The rows are trivial to read, so the time is mostly spent setting up and running the tasks of
//! the scans.
//!
//! Criterion measures execution through complete output collection; worker startup and query
//! planning, including eager file planning, are excluded. Every iteration checks the row count.
//! An untimed preflight prints the stages and tasks of each query and its first-batch time.

#[path = "wide_table/mod.rs"]
mod wide_table;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::common::{Result, exec_datafusion_err};
use datafusion::execution::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::{WorkerQueryContext, display_plan_ascii};
use datafusion_distributed_iceberg::IcebergExt;
use datafusion_distributed_iceberg::iceberg::TableIdent;
use datafusion_distributed_iceberg::iceberg::spec::Transform;
use datafusion_distributed_iceberg_test_tools::{FixtureCatalog, FixtureTable};
use futures::TryStreamExt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::time::timeout;
use wide_table::{
    FILTER_COLUMN, NAME_MAPPING_PROPERTY, PARTITION_COLUMN, iceberg_schema, name_mapping,
};

const WORKERS: usize = 4;
const COLUMNS: usize = 100;
/// Data files in each table.
const FILES: usize = 1_000;
/// Distinct values of the partition column across a table's files.
const PARTITION_VALUES: usize = 30;
const WAIT: Duration = Duration::from_secs(60);

fn eager_scan_query(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let fixtures = rt
        .block_on(FixtureCatalog::try_new())
        .expect("fixture catalog");
    let tables = rt.block_on(create_tables(&fixtures, 3)).expect("tables");
    let (ctx, _workers, _) = rt.block_on(start_localhost_context(WORKERS, worker_state));
    let ctx = rt.block_on(coordinator(ctx, &tables)).expect("coordinator");

    let mut group = c.benchmark_group("eager_scan_query");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(10));

    // Each branch aggregates its own table, so each scan feeds its own shuffle and becomes its
    // own stage.
    let union = (0..3)
        .map(|table| {
            format!(
                "SELECT {PARTITION_COLUMN}, count(*) AS n FROM t{table} \
                 WHERE {FILTER_COLUMN} IS NULL GROUP BY {PARTITION_COLUMN}"
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let queries = [
        (
            "one_table",
            format!("SELECT count(*) FROM t0 WHERE {FILTER_COLUMN} IS NULL"),
            FILES,
        ),
        (
            "three_tables",
            format!("SELECT sum(n) FROM ({union})"),
            3 * FILES,
        ),
    ];
    for (name, sql, rows) in queries {
        let plan = rt
            .block_on(async { ctx.sql(&sql).await?.create_physical_plan().await })
            .expect("plan");
        let display = display_plan_ascii(plan.as_ref(), false);
        let (first, total) = rt.block_on(run_query(&ctx, &sql, rows)).expect("preflight");
        println!("preflight {name}: first={first:?} total={total:?}\n{display}");
        group.bench_function(
            BenchmarkId::new(name, format!("{COLUMNS}_columns_{FILES}_files")),
            |b| {
                b.iter_custom(|iterations| {
                    rt.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            elapsed += run_query(&ctx, &sql, rows).await.unwrap().1;
                        }
                        elapsed
                    })
                })
            },
        );
    }
    group.finish();
}

/// Plans `sql` untimed, then returns the time to its first batch and to its last.
async fn run_query(ctx: &SessionContext, sql: &str, rows: usize) -> Result<(Duration, Duration)> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let start = Instant::now();
    let mut output = execute_stream(plan, ctx.task_ctx())?;
    let first = timeout(WAIT, output.try_next())
        .await
        .map_err(|_| exec_datafusion_err!("query timed out"))??
        .ok_or_else(|| exec_datafusion_err!("query returned no batches"))?;
    let first_elapsed = start.elapsed();
    let mut batches = vec![first];
    batches.extend(
        timeout(WAIT, output.try_collect::<Vec<_>>())
            .await
            .map_err(|_| exec_datafusion_err!("query timed out"))??,
    );
    let total_elapsed = start.elapsed();
    let count = batches
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("the count is an Int64 column")
                .value(0)
        })
        .sum::<i64>();
    assert_eq!(count, rows as i64, "every row must be counted once");
    Ok((first_elapsed, total_elapsed))
}

/// Creates tables `t0`, `t1`, ... with [`FILES`] one-row data files each.
async fn create_tables(fixtures: &FixtureCatalog, count: usize) -> Result<Vec<FixtureTable>> {
    let schema = iceberg_schema(COLUMNS)?;
    let mapping = serde_json::to_string(&name_mapping(&schema))
        .map_err(|error| exec_datafusion_err!("cannot encode name mapping: {error}"))?;
    let mut tables = Vec::with_capacity(count);
    for table in 0..count {
        let ident = TableIdent::from_strs(["bench", &format!("t{table}")])
            .map_err(|error| exec_datafusion_err!("invalid table name: {error}"))?;
        let mut table = fixtures
            .table(ident, schema.clone())
            .partition_by(PARTITION_COLUMN, Transform::Identity)
            .property(NAME_MAPPING_PROPERTY, mapping.clone())
            .create()
            .await?;
        let mut append = table.append();
        for file in 0..FILES {
            let value = Arc::new(Int64Array::from(vec![(file % PARTITION_VALUES) as i64]));
            append = append.file(RecordBatch::try_from_iter([(
                PARTITION_COLUMN,
                value as ArrayRef,
            )])?);
        }
        append.commit().await?;
        tables.push(table);
    }
    Ok(tables)
}

/// Adds the Iceberg integration to the localhost coordinator and registers `tables`.
async fn coordinator(ctx: SessionContext, tables: &[FixtureTable]) -> Result<SessionContext> {
    let state = SessionStateBuilder::new_from_existing(ctx.state())
        .with_iceberg_integration(Default::default())
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.sql("SET iceberg.eager_file_planning = true")
        .await?
        .collect()
        .await?;
    for table in tables {
        let statement = format!(
            "CREATE EXTERNAL TABLE {} STORED AS ICEBERG LOCATION '{}'",
            table.ident().name(),
            table.metadata_location()?
        );
        ctx.sql(&statement).await?.collect().await?;
    }
    Ok(ctx)
}

async fn worker_state(ctx: WorkerQueryContext) -> Result<SessionState> {
    Ok(ctx
        .builder
        .with_iceberg_integration(Default::default())
        .build())
}

criterion_group!(benches, eager_scan_query);
criterion_main!(benches);
