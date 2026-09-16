//! Benchmark distributed range join scenarios and stage boundary pipelines.

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    LexOrdering, Partitioning, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionContext, col};
use datafusion_distributed::DefaultSessionBuilder;
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use parquet::arrow::ArrowWriter;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Builder as RuntimeBuilder;

#[derive(Debug)]
struct RangePartitionedTableWrapper {
    inner: Arc<dyn TableProvider>,
    col_name: String,
    col_idx: usize,
    splits: Vec<SplitPoint>,
}

#[async_trait::async_trait]
impl TableProvider for RangePartitionedTableWrapper {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }
    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        if let Some(dse) = plan.downcast_ref::<DataSourceExec>()
            && let Some(file_scan) = dse.data_source().downcast_ref::<FileScanConfig>()
        {
            let proj_col_idx = match projection {
                Some(proj) => proj.iter().position(|&idx| idx == self.col_idx),
                None => Some(self.col_idx),
            };
            if let Some(proj_idx) = proj_col_idx {
                let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
                    Arc::new(Column::new(&self.col_name, proj_idx)),
                    Default::default(),
                )])
                .unwrap();
                let range = RangePartitioning::try_new(ordering, self.splits.clone()).unwrap();
                let mut new_file_scan = file_scan.clone();
                new_file_scan.output_partitioning = Some(Partitioning::Range(range));
                return Ok(DataSourceExec::from_data_source(new_file_scan));
            }
        }
        Ok(plan)
    }
}

fn split_points() -> Vec<SplitPoint> {
    vec![
        SplitPoint::new(vec![ScalarValue::Utf8(Some("B".to_string()))]),
        SplitPoint::new(vec![ScalarValue::Utf8(Some("C".to_string()))]),
        SplitPoint::new(vec![ScalarValue::Utf8(Some("D".to_string()))]),
    ]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BroadcastMode {
    Disabled,
    Enabled,
}

fn configure_context(
    ctx: &mut SessionContext,
    target_partitions: usize,
    broadcast_mode: BroadcastMode,
) {
    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .optimizer
        .preserve_file_partitions = 1;
    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = target_partitions;
    match broadcast_mode {
        BroadcastMode::Enabled => {
            ctx.state_ref()
                .write()
                .config_mut()
                .options_mut()
                .optimizer
                .hash_join_single_partition_threshold = 1024 * 1024;
            ctx.state_ref()
                .write()
                .config_mut()
                .options_mut()
                .optimizer
                .hash_join_single_partition_threshold_rows = 131_072;
        }
        BroadcastMode::Disabled => {
            ctx.state_ref()
                .write()
                .config_mut()
                .options_mut()
                .optimizer
                .hash_join_single_partition_threshold = 0;
            ctx.state_ref()
                .write()
                .config_mut()
                .options_mut()
                .optimizer
                .hash_join_single_partition_threshold_rows = 0;
        }
    }
}

struct BenchmarkData {
    _dir: TempDir,
    dim_dir: PathBuf,
    fact_dir: PathBuf,
    services_dir: PathBuf,
}

impl BenchmarkData {
    fn generate(rows_per_dim_part: usize, rows_per_fact_part: usize) -> Result<Self> {
        let dir = TempDir::new()?;
        let base = dir.path();
        let dim_dir = base.join("dim");
        let fact_dir = base.join("fact");
        let services_dir = base.join("services");

        let keys = ["A", "B", "C", "D"];

        // Generate dim partitions
        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("env", DataType::Utf8, false),
            Field::new("service", DataType::Utf8, false),
            Field::new("host", DataType::Utf8, false),
        ]));
        for key in &keys {
            let part_dir = dim_dir.join(format!("d_dkey={key}"));
            fs::create_dir_all(&part_dir)?;
            let envs = (0..rows_per_dim_part)
                .map(|i| if i % 2 == 0 { "prod" } else { "dev" })
                .collect::<Vec<_>>();
            let services = (0..rows_per_dim_part)
                .map(|i| if i % 2 == 0 { "log" } else { "trace" })
                .collect::<Vec<_>>();
            let hosts = (0..rows_per_dim_part)
                .map(|i| if i % 2 == 0 { "host-x" } else { "host-y" })
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                Arc::clone(&dim_schema),
                vec![
                    Arc::new(StringArray::from(envs)) as ArrayRef,
                    Arc::new(StringArray::from(services)) as ArrayRef,
                    Arc::new(StringArray::from(hosts)) as ArrayRef,
                ],
            )?;
            let file = File::create(part_dir.join("data0.parquet"))?;
            let mut writer = ArrowWriter::try_new(file, Arc::clone(&dim_schema), None)?;
            writer.write(&batch)?;
            writer.close()?;
        }

        // Generate fact partitions
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]));
        let base_ts = 1_672_563_600_000_000_000_i64; // 2023-01-01T09:00:00Z in ns
        for key in &keys {
            let part_dir = fact_dir.join(format!("f_dkey={key}"));
            fs::create_dir_all(&part_dir)?;
            let timestamps = (0..rows_per_fact_part)
                .map(|i| base_ts + (i as i64 * 10_000_000_000))
                .collect::<Vec<_>>();
            let values = (0..rows_per_fact_part)
                .map(|i| 50.0 + (i as f64 * 0.1))
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                Arc::clone(&fact_schema),
                vec![
                    Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
                    Arc::new(Float64Array::from(values)) as ArrayRef,
                ],
            )?;
            let file = File::create(part_dir.join("data0.parquet"))?;
            let mut writer = ArrowWriter::try_new(file, Arc::clone(&fact_schema), None)?;
            writer.write(&batch)?;
            writer.close()?;
        }

        // Generate services
        fs::create_dir_all(&services_dir)?;
        let services_schema = Arc::new(Schema::new(vec![
            Field::new("service", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
        ]));
        let services_batch = RecordBatch::try_new(
            Arc::clone(&services_schema),
            vec![
                Arc::new(StringArray::from(vec!["log", "trace"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Logging", "Tracing"])) as ArrayRef,
            ],
        )?;
        let file = File::create(services_dir.join("data0.parquet"))?;
        let mut writer = ArrowWriter::try_new(file, services_schema, None)?;
        writer.write(&services_batch)?;
        writer.close()?;

        Ok(Self {
            _dir: dir,
            dim_dir,
            fact_dir,
            services_dir,
        })
    }
}

async fn register_dim(
    ctx: &SessionContext,
    path: &Path,
    as_range: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let opts = ParquetReadOptions::default()
        .table_partition_cols(vec![("d_dkey".to_string(), DataType::Utf8)]);
    ctx.register_parquet("dim", path.to_str().unwrap(), opts)
        .await?;
    if as_range {
        let table = ctx.table_provider("dim").await?;
        ctx.deregister_table("dim")?;
        ctx.register_table(
            "dim",
            Arc::new(RangePartitionedTableWrapper {
                inner: table,
                col_name: "d_dkey".to_string(),
                col_idx: 3,
                splits: split_points(),
            }),
        )?;
    }
    Ok(())
}

async fn register_fact(
    ctx: &SessionContext,
    path: &Path,
    as_range: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let opts = ParquetReadOptions::default()
        .table_partition_cols(vec![("f_dkey".to_string(), DataType::Utf8)])
        .file_sort_order(vec![vec![
            col("f_dkey").sort(true, false),
            col("timestamp").sort(true, false),
        ]]);
    ctx.register_parquet("fact", path.to_str().unwrap(), opts)
        .await?;
    if as_range {
        let table = ctx.table_provider("fact").await?;
        ctx.deregister_table("fact")?;
        ctx.register_table(
            "fact",
            Arc::new(RangePartitionedTableWrapper {
                inner: table,
                col_name: "f_dkey".to_string(),
                col_idx: 2,
                splits: split_points(),
            }),
        )?;
    }
    Ok(())
}

async fn register_services(
    ctx: &SessionContext,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    ctx.register_parquet("services", path.to_str().unwrap(), Default::default())
        .await?;
    Ok(())
}

const TWO_WAY_JOIN_QUERY: &str = r#"
    SELECT 
        f.f_dkey,
        f.timestamp,
        f.value,
        d.env,
        d.service,
        d.host
    FROM dim d
    INNER JOIN fact f ON d.d_dkey = f.f_dkey
    WHERE d.service = 'log'
"#;

const THREE_WAY_JOIN_QUERY: &str = r#"
    SELECT 
        f.f_dkey,
        f.timestamp,
        f.value,
        d.env,
        d.service,
        s.service_name
    FROM dim d
    INNER JOIN fact f ON d.d_dkey = f.f_dkey
    INNER JOIN services s ON d.service = s.service
    WHERE d.service = 'log'
"#;

const THREE_WAY_AGG_QUERY: &str = r#"
    SELECT 
        s.service_name,
        COUNT(*) as row_count,
        SUM(f.value) as total_value
    FROM dim d
    INNER JOIN fact f ON d.d_dkey = f.f_dkey
    INNER JOIN services s ON d.service = s.service
    GROUP BY s.service_name
    ORDER BY s.service_name
"#;

fn range_join_scenarios(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let data = BenchmarkData::generate(1_000, 10_000).expect("generate benchmark datasets");

    let mut group = c.benchmark_group("range_join_scenarios");
    group.sample_size(10);

    // Scenario 1: Co-partitioned range join (zero network shuffle)
    {
        let (ctx, mut join_set) = rt.block_on(async {
            let (mut ctx, join_set, _workers) =
                start_localhost_context(4, DefaultSessionBuilder).await;
            configure_context(&mut ctx, 4, BroadcastMode::Disabled);
            register_dim(&ctx, &data.dim_dir, true).await.unwrap();
            register_fact(&ctx, &data.fact_dir, true).await.unwrap();
            (ctx, join_set)
        });
        group.bench_function(
            BenchmarkId::new("join", "co_partitioned_range_zero_shuffle"),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = Instant::now();
                        rt.block_on(async {
                            let df = ctx.sql(TWO_WAY_JOIN_QUERY).await.unwrap();
                            let _ = df.collect().await.unwrap();
                        });
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
        join_set.abort_all();
    }

    // Baseline: Symmetric hash shuffle join (two network shuffles)
    {
        let (ctx, mut join_set) = rt.block_on(async {
            let (mut ctx, join_set, _workers) =
                start_localhost_context(4, DefaultSessionBuilder).await;
            configure_context(&mut ctx, 4, BroadcastMode::Disabled);
            register_dim(&ctx, &data.dim_dir, false).await.unwrap();
            register_fact(&ctx, &data.fact_dir, false).await.unwrap();
            (ctx, join_set)
        });
        group.bench_function(BenchmarkId::new("join", "hash_shuffle_two_stage"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    rt.block_on(async {
                        let df = ctx.sql(TWO_WAY_JOIN_QUERY).await.unwrap();
                        let _ = df.collect().await.unwrap();
                    });
                    total += start.elapsed();
                }
                total
            });
        });
        join_set.abort_all();
    }

    // Baseline / Comparison: Broadcast join (CollectLeft, one broadcast exchange)
    {
        let (ctx, mut join_set) = rt.block_on(async {
            let (mut ctx, join_set, _workers) =
                start_localhost_context(4, DefaultSessionBuilder).await;
            configure_context(&mut ctx, 4, BroadcastMode::Enabled);
            register_dim(&ctx, &data.dim_dir, false).await.unwrap();
            register_fact(&ctx, &data.fact_dir, true).await.unwrap();
            (ctx, join_set)
        });
        group.bench_function(BenchmarkId::new("join", "broadcast_single_stage"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    rt.block_on(async {
                        let df = ctx.sql(TWO_WAY_JOIN_QUERY).await.unwrap();
                        let _ = df.collect().await.unwrap();
                    });
                    total += start.elapsed();
                }
                total
            });
        });
        join_set.abort_all();
    }

    // Scenario 4: Asymmetric range-adapted shuffle join (fact is range partitioned, dim dynamically range-shuffled)
    {
        let (ctx, mut join_set) = rt.block_on(async {
            let (mut ctx, join_set, _workers) =
                start_localhost_context(4, DefaultSessionBuilder).await;
            configure_context(&mut ctx, 4, BroadcastMode::Disabled);
            register_dim(&ctx, &data.dim_dir, false).await.unwrap();
            register_fact(&ctx, &data.fact_dir, true).await.unwrap();
            (ctx, join_set)
        });
        group.bench_function(
            BenchmarkId::new("join", "range_adapted_shuffle_single_stage"),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = Instant::now();
                        rt.block_on(async {
                            let df = ctx.sql(TWO_WAY_JOIN_QUERY).await.unwrap();
                            let _ = df.collect().await.unwrap();
                        });
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
        join_set.abort_all();
    }

    // Scenario 5: Pathological reference child shuffle (Issue #25302)
    // Dim is range-partitioned (satisfied child), Fact is unpartitioned.
    // DataFusion forces Fact (large table) to range-repartition to match Dim's split points.
    {
        let (ctx, mut join_set) = rt.block_on(async {
            let (mut ctx, join_set, _workers) =
                start_localhost_context(4, DefaultSessionBuilder).await;
            configure_context(&mut ctx, 4, BroadcastMode::Disabled);
            register_dim(&ctx, &data.dim_dir, true).await.unwrap();
            register_fact(&ctx, &data.fact_dir, false).await.unwrap();
            (ctx, join_set)
        });
        group.bench_function(
            BenchmarkId::new("join", "pathological_large_table_range_shuffle"),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = Instant::now();
                        rt.block_on(async {
                            let df = ctx.sql(TWO_WAY_JOIN_QUERY).await.unwrap();
                            let _ = df.collect().await.unwrap();
                        });
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
        join_set.abort_all();
    }

    // Scenario 2: 3-way join with Range Join -> Hash Shuffle -> Downstream Hash Join
    {
        let (ctx, mut join_set) = rt.block_on(async {
            let (mut ctx, join_set, _workers) =
                start_localhost_context(4, DefaultSessionBuilder).await;
            configure_context(&mut ctx, 4, BroadcastMode::Disabled);
            register_dim(&ctx, &data.dim_dir, true).await.unwrap();
            register_fact(&ctx, &data.fact_dir, true).await.unwrap();
            register_services(&ctx, &data.services_dir).await.unwrap();
            (ctx, join_set)
        });
        group.bench_function(
            BenchmarkId::new("pipeline", "three_way_range_to_hash_pipeline"),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = Instant::now();
                        rt.block_on(async {
                            let df = ctx.sql(THREE_WAY_JOIN_QUERY).await.unwrap();
                            let _ = df.collect().await.unwrap();
                        });
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
        join_set.abort_all();
    }

    // Scenario 3: 3-way aggregation over range-partitioned tables (Hybrid broadcast plan)
    // Stage 1: Co-partitioned range join (dim JOIN fact) -> 0 shuffles
    // Stage 2: Broadcast services dimension via NetworkBroadcastExec
    // Stage 3: Partial aggregation -> Hash shuffle -> Final aggregation
    {
        let (ctx, mut join_set) = rt.block_on(async {
            let (mut ctx, join_set, _workers) =
                start_localhost_context(4, DefaultSessionBuilder).await;
            configure_context(&mut ctx, 4, BroadcastMode::Enabled);
            register_dim(&ctx, &data.dim_dir, true).await.unwrap();
            register_fact(&ctx, &data.fact_dir, true).await.unwrap();
            register_services(&ctx, &data.services_dir).await.unwrap();
            (ctx, join_set)
        });
        group.bench_function(
            BenchmarkId::new("pipeline", "three_way_aggregation_broadcast"),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = Instant::now();
                        rt.block_on(async {
                            let df = ctx.sql(THREE_WAY_AGG_QUERY).await.unwrap();
                            let _ = df.collect().await.unwrap();
                        });
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
        join_set.abort_all();
    }

    group.finish();
}

criterion_group!(benches, range_join_scenarios);
criterion_main!(benches);
