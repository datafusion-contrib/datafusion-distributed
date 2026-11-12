//! Controlled Top-K scan workloads. See the benchmark README for interpretation and resource runs.

use arrow::array::{BooleanArray, Int64Array};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashSet, Result, ScalarValue};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::test_utils::localhost::{
    LocalHostWorkerResolver, start_localhost_context,
};
use datafusion_distributed::{
    DefaultSessionBuilder, DistributedExec, DistributedExt, DistributedGetterExt,
    DistributedMetricsFormat, MappedWorkerSessionBuilderExt, TaskFinishedEvent,
    TaskFinishedEventResponse, TaskFinishedHandler, Worker, rewrite_distributed_plan_with_metrics,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::time::timeout;

const ROWS_PER_GROUP: usize = 65_536;
const GROUPS_PER_FILE: usize = 32;
const ROWS_PER_FILE: usize = ROWS_PER_GROUP * GROUPS_PER_FILE;
const HIGH: i64 = 1_000_000_000;
const K: usize = 100;
const QUERY: &str = "SELECT score FROM topk_data WHERE included ORDER BY score DESC LIMIT 100";

fn sibling_topk(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let selected = std::env::var("TOPK_RESOURCE_CASE").ok();
    let resource_queries = std::env::var("TOPK_RESOURCE_QUERIES")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(20);
    assert!(resource_queries > 0);
    let mut selected_found = selected.is_none();
    let mut group = c.benchmark_group("sibling_topk");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    for scenario in ["pruning", "nonprunable"] {
        for tasks in [2, 8] {
            let name = format!("{scenario}/{tasks}");
            if selected.as_ref().is_some_and(|selected| selected != &name) {
                continue;
            }
            selected_found = true;
            let fixture = Fixture::new(scenario, tasks).unwrap();
            let probe = Arc::new(Probe {
                groups: fixture.groups.clone(),
                ..Probe::default()
            });
            let (ctx, _servers, _workers) = rt.block_on(context(tasks, &fixture, &probe)).unwrap();
            let (_, report) = rt.block_on(run_query(&ctx, &fixture, &probe)).unwrap();
            println!(
                "preflight {name}: {report:?} useful={} effective={}",
                report.useful(),
                report.effective()
            );

            if selected.is_some() {
                let unix_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                println!(
                    "resource_begin case={name} queries={resource_queries} unix_ns={unix_ns} configured={}",
                    report.configured
                );
                io::stdout().flush().unwrap();
                let mut elapsed = Duration::ZERO;
                let mut updates = 0;
                let mut bytes = 0;
                let mut queries_with_updates = 0;
                let mut useful_queries = 0;
                let mut effective_queries = 0;
                for _ in 0..resource_queries {
                    let (duration, report) =
                        rt.block_on(run_query(&ctx, &fixture, &probe)).unwrap();
                    elapsed += duration;
                    updates += report.updates;
                    bytes += report.scans.iter().map(|scan| scan.bytes).sum::<usize>();
                    queries_with_updates += usize::from(report.updates > 0);
                    useful_queries += usize::from(report.useful());
                    effective_queries += usize::from(report.effective());
                }
                let unix_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                println!(
                    "resource_end case={name} queries={resource_queries} unix_ns={unix_ns} elapsed={elapsed:?} updates={updates} bytes={bytes} queries_with_updates={queries_with_updates} useful_queries={useful_queries} effective_queries={effective_queries}"
                );
                io::stdout().flush().unwrap();
            } else {
                let mut queries = 0;
                let mut queries_with_updates = 0;
                let mut useful_queries = 0;
                let mut effective_queries = 0;
                group.bench_function(BenchmarkId::new(scenario, tasks), |b| {
                    b.iter_custom(|iterations| {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let (duration, report) =
                                rt.block_on(run_query(&ctx, &fixture, &probe)).unwrap();
                            elapsed += duration;
                            queries += 1;
                            queries_with_updates += usize::from(report.updates > 0);
                            useful_queries += usize::from(report.useful());
                            effective_queries += usize::from(report.effective());
                        }
                        elapsed
                    })
                });
                println!(
                    "observed case={name} queries={queries} queries_with_updates={queries_with_updates} useful_queries={useful_queries} effective_queries={effective_queries}"
                );
            }
        }
    }
    assert!(selected_found, "unknown TOPK_RESOURCE_CASE: {selected:?}");
    group.finish();
}

async fn context(
    tasks: usize,
    fixture: &Fixture,
    probe: &Arc<Probe>,
) -> Result<(SessionContext, JoinSet<()>, Vec<Worker>)> {
    let probe = Arc::clone(probe);
    let builder = DefaultSessionBuilder.map(move |builder| {
        Ok(builder
            .with_distributed_task_finished_handler(
                Arc::clone(&probe) as Arc<dyn TaskFinishedHandler>
            )
            .build())
    });
    let (mut ctx, servers, workers) = start_localhost_context(tasks + 1, builder).await;
    // Exclude the fixture's local-worker endpoint so every scan uses localhost gRPC.
    let urls = ctx
        .state()
        .config()
        .get_distributed_worker_resolver()?
        .get_urls()?;
    ctx.set_distributed_worker_resolver(LocalHostWorkerResolver::new(
        urls.iter().skip(1).map(|url| url.port().unwrap()),
    ));
    let ctx = ctx
        .with_distributed_dynamic_task_count(false)?
        .with_distributed_metrics_collection(true)?;
    ctx.sql("SET datafusion.execution.target_partitions = 2")
        .await?;
    ctx.sql("SET datafusion.execution.parquet.pushdown_filters = false")
        .await?;
    ctx.sql("SET datafusion.execution.parquet.enable_page_index = false")
        .await?;
    ctx.register_parquet(
        "topk_data",
        fixture.directory.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok((ctx, servers, workers))
}

async fn run_query(
    ctx: &SessionContext,
    fixture: &Fixture,
    probe: &Probe,
) -> Result<(Duration, Report)> {
    let plan = ctx.sql(QUERY).await?.create_physical_plan().await?;
    assert!(plan.is::<DistributedExec>());
    let start = Instant::now();
    let batches = timeout(
        Duration::from_secs(60),
        collect(Arc::clone(&plan), ctx.task_ctx()),
    )
    .await
    .unwrap()?;
    let elapsed = start.elapsed();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, fixture.expected);
    let _ =
        rewrite_distributed_plan_with_metrics(Arc::clone(&plan), DistributedMetricsFormat::PerTask)
            .await?;
    // Metric reports are sent after TaskFinishedHandler, so every observation is now available.
    let mut scans = std::mem::take(&mut *probe.scans.lock().unwrap());
    scans.sort_by_key(|scan| scan.task);
    let updates = metric_total(
        &plan.metrics().unwrap_or_default(),
        "dynamic_filter_updates_received",
    );
    let configured = ctx
        .state()
        .config()
        .options()
        .extensions
        .iter()
        .find(|(prefix, _)| *prefix == "distributed")
        .is_some_and(|(_, options)| {
            options.entries().iter().any(|entry| {
                entry.key == "enable_sibling_topk_filter_pushdown"
                    && entry.value.as_deref() == Some("true")
            })
        });
    let report = Report {
        scans,
        updates,
        configured,
    };
    report.check(fixture);
    if fixture.nonprunable {
        let bytes = report
            .scans
            .iter()
            .map(|scan| scan.bytes)
            .collect::<Vec<_>>();
        let mut previous = probe.scan_bytes.lock().unwrap();
        if let Some(previous) = previous.as_ref() {
            assert_eq!(previous, &bytes);
        } else {
            *previous = Some(bytes);
        }
    }
    Ok((elapsed, report))
}

struct Fixture {
    directory: PathBuf,
    tasks: usize,
    nonprunable: bool,
    expected: Vec<i64>,
    groups: Vec<(i64, i64)>,
}

impl Fixture {
    fn new(scenario: &str, tasks: usize) -> Result<Self> {
        let nonprunable = scenario == "nonprunable";
        let directory = std::env::temp_dir().join(format!(
            "dfd-topk-{}-{scenario}-{tasks}",
            std::process::id()
        ));
        fs::create_dir(&directory)?;
        let expected = if nonprunable {
            (0..ROWS_PER_FILE)
                .rev()
                .filter(|row| row % ROWS_PER_GROUP != 0)
                .flat_map(|row| std::iter::repeat_n(row as i64, tasks))
                .take(K)
                .collect()
        } else {
            vec![HIGH; K]
        };
        let mut fixture = Self {
            directory,
            tasks,
            nonprunable,
            expected,
            groups: vec![],
        };
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
            .build();
        {
            let path = fixture.directory.join("00.parquet");
            let mut writer = None;
            for group in 0..GROUPS_PER_FILE {
                let start = group * ROWS_PER_GROUP;
                let scores =
                    Int64Array::from_iter_values((start..start + ROWS_PER_GROUP).map(|row| {
                        if nonprunable && row % ROWS_PER_GROUP == 0 {
                            i64::MAX
                        } else if !nonprunable && group == 0 {
                            HIGH
                        } else {
                            row as i64
                        }
                    }));
                let included = BooleanArray::from_iter(
                    (start..start + ROWS_PER_GROUP)
                        .map(|row| Some(!nonprunable || row % ROWS_PER_GROUP != 0)),
                );
                let batch = RecordBatch::try_from_iter(vec![
                    ("score", Arc::new(scores) as _),
                    ("included", Arc::new(included) as _),
                ])?;
                if writer.is_none() {
                    writer = Some(ArrowWriter::try_new(
                        File::create(&path)?,
                        batch.schema(),
                        Some(properties.clone()),
                    )?);
                }
                writer.as_mut().unwrap().write(&batch)?;
            }
            let metadata = writer.unwrap().close()?;
            fixture.groups = metadata
                .row_groups()
                .iter()
                .map(|group| {
                    let column = group.column(0);
                    let Statistics::Int64(statistics) = column.statistics().unwrap() else {
                        unreachable!()
                    };
                    (column.data_page_offset(), *statistics.max_opt().unwrap())
                })
                .collect::<Vec<_>>();
        }
        for file in 1..tasks {
            fs::copy(
                fixture.directory.join("00.parquet"),
                fixture.directory.join(format!("{file:02}.parquet")),
            )?;
        }
        Ok(fixture)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

#[derive(Debug)]
struct Scan {
    task: usize,
    partitions: usize,
    ranges: Vec<(String, i64, i64)>,
    local_filters: usize,
    remote_filters: usize,
    assigned_groups: usize,
    local_prunable: usize,
    local_rejects_file_maximum: bool,
    remote_useful: bool,
    rows: usize,
    bytes: usize,
    pruned: usize,
    ranges_pruned: usize,
}

#[derive(Debug)]
struct Report {
    scans: Vec<Scan>,
    updates: usize,
    configured: bool,
}

impl Report {
    fn check(&self, fixture: &Fixture) {
        assert_eq!(self.scans.len(), fixture.tasks);
        assert_eq!(
            self.scans
                .iter()
                .map(|scan| scan.task)
                .collect::<HashSet<_>>()
                .len(),
            fixture.tasks
        );
        assert!(
            self.scans.iter().all(|scan| scan.local_filters == 1
                && scan.partitions == 2
                && scan.pruned <= scan.assigned_groups
                && scan.local_prunable <= scan.assigned_groups
                && scan.ranges_pruned <= scan.ranges.len()),
            "unexpected worker scan shape: {self:?}"
        );
        if fixture.nonprunable {
            assert_eq!(self.scans.iter().map(|scan| scan.pruned).sum::<usize>(), 0);
            assert_eq!(
                self.scans
                    .iter()
                    .map(|scan| scan.ranges_pruned)
                    .sum::<usize>(),
                0
            );
            assert_eq!(
                self.scans.iter().map(|scan| scan.rows).sum::<usize>(),
                ROWS_PER_FILE * fixture.tasks
            );
        }
        let remote = self
            .scans
            .iter()
            .map(|scan| scan.remote_filters)
            .sum::<usize>();
        assert_eq!(remote, if self.configured { fixture.tasks } else { 0 });
        assert_eq!(
            self.scans
                .iter()
                .map(|scan| scan.assigned_groups)
                .sum::<usize>(),
            GROUPS_PER_FILE * fixture.tasks
        );
        assert!(self.scans.iter().map(|scan| scan.bytes).sum::<usize>() > 0);
    }

    fn useful(&self) -> bool {
        self.scans.iter().any(|scan| scan.remote_useful)
    }

    fn effective(&self) -> bool {
        self.scans.iter().any(|scan| {
            // Groups surviving the strongest local cutoff require all their rows absent
            // remote pruning. Empty byte fragments contribute no groups to either bound.
            let beyond_local_groups = (scan.pruned > 0 || scan.ranges_pruned > 0)
                && scan.rows < (scan.assigned_groups - scan.local_prunable) * ROWS_PER_GROUP;
            // FilePruner uses full-file statistics. If the local filter still admits that
            // maximum, only a remote filter can stop ranges; discount every RG pruned.
            let remote_range_stop = !scan.local_rejects_file_maximum
                && scan.ranges_pruned > 0
                && scan.rows < (scan.assigned_groups - scan.pruned) * ROWS_PER_GROUP;
            scan.remote_useful && (beyond_local_groups || remote_range_stop)
        })
    }
}

#[derive(Default)]
struct Probe {
    scans: Mutex<Vec<Scan>>,
    groups: Vec<(i64, i64)>,
    scan_bytes: Mutex<Option<Vec<usize>>>,
}

impl TaskFinishedHandler for Probe {
    fn handle(&self, event: TaskFinishedEvent<'_>) -> Result<TaskFinishedEventResponse> {
        let file_maximum = self
            .groups
            .iter()
            .map(|(_, maximum)| *maximum)
            .max()
            .unwrap();
        let file_probe = RecordBatch::try_from_iter(vec![(
            "score",
            Arc::new(Int64Array::from(vec![file_maximum])) as _,
        )])?;
        let mut producers = HashSet::new();
        event.plan.apply(|node| {
            producers.extend(
                node.dynamic_expressions_produced()
                    .iter()
                    .filter_map(|expr| expr.expression_id()),
            );
            Ok(TreeNodeRecursion::Continue)
        })?;
        event.plan.apply(|node| {
            let Some(source) = node.downcast_ref::<DataSourceExec>() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            let Some(config) = source.data_source().downcast_ref::<FileScanConfig>() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            assert!(config.limit.is_none());
            for file in config.file_groups.iter().flat_map(|group| group.iter()) {
                assert_eq!(
                    file.statistics.as_ref().unwrap().column_statistics[0]
                        .max_value
                        .get_value(),
                    Some(&ScalarValue::Int64(Some(file_maximum)))
                );
            }
            let mut scan = Scan {
                task: event.task_key.task_number,
                partitions: config.file_groups.len(),
                ranges: config
                    .file_groups
                    .iter()
                    .flat_map(|group| group.iter())
                    .map(|file| {
                        let range = file.range.as_ref().unwrap();
                        (
                            file.object_meta.location.filename().unwrap().to_owned(),
                            range.start,
                            range.end,
                        )
                    })
                    .collect(),
                local_filters: 0,
                remote_filters: 0,
                assigned_groups: 0,
                local_prunable: 0,
                local_rejects_file_maximum: false,
                remote_useful: false,
                rows: 0,
                bytes: 0,
                pruned: 0,
                ranges_pruned: 0,
            };
            let maxima = scan
                .ranges
                .iter()
                .flat_map(|(_, start, end)| {
                    self.groups
                        .iter()
                        .filter(move |(offset, _)| start <= offset && offset < end)
                        .map(|(_, maximum)| *maximum)
                })
                .collect::<Vec<_>>();
            scan.assigned_groups = maxima.len();
            let batch = RecordBatch::try_from_iter(vec![(
                "score",
                Arc::new(Int64Array::from(maxima)) as _,
            )])?;
            let useful_probe = RecordBatch::try_from_iter(vec![(
                "score",
                Arc::new(Int64Array::from(vec![0])) as _,
            )])?;
            let mut seen = HashSet::new();
            node.apply_expressions(&mut |expr| {
                expr.apply(|expr| {
                    if let Some(filter) = expr.downcast_ref::<DynamicFilterPhysicalExpr>() {
                        let id = expr.expression_id().unwrap();
                        if seen.insert(id) {
                            let current = filter.current()?;
                            if producers.contains(&id) {
                                scan.local_filters += 1;
                                let predicate =
                                    current.evaluate(&batch)?.into_array(batch.num_rows())?;
                                let predicate =
                                    predicate.as_any().downcast_ref::<BooleanArray>().unwrap();
                                scan.local_prunable += predicate
                                    .iter()
                                    .filter(|value| *value == Some(false))
                                    .count();
                                let predicate = current.evaluate(&file_probe)?.into_array(1)?;
                                scan.local_rejects_file_maximum = !predicate
                                    .as_any()
                                    .downcast_ref::<BooleanArray>()
                                    .unwrap()
                                    .value(0);
                            } else {
                                scan.remote_filters += 1;
                                let predicate = current.evaluate(&useful_probe)?.into_array(1)?;
                                scan.remote_useful |= !predicate
                                    .as_any()
                                    .downcast_ref::<BooleanArray>()
                                    .unwrap()
                                    .value(0);
                            }
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            let metrics = node.metrics().unwrap_or_default();
            scan.rows = metrics.output_rows().unwrap_or_default();
            scan.bytes = metric_total(&metrics, "bytes_scanned");
            for metric in metrics.iter() {
                match metric.value() {
                    MetricValue::PruningMetrics {
                        name,
                        pruning_metrics,
                    } if name == "row_groups_pruned_statistics" => {
                        scan.pruned += pruning_metrics.pruned();
                    }
                    MetricValue::PruningMetrics {
                        name,
                        pruning_metrics,
                    } if name == "files_ranges_pruned_statistics" => {
                        scan.ranges_pruned += pruning_metrics.pruned();
                    }
                    _ => {}
                }
            }
            scan.pruned += metric_total(&metrics, "row_groups_pruned_dynamic_filter");
            self.scans.lock().unwrap().push(scan);
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(TaskFinishedEventResponse::new(MetricsSet::new()))
    }
}

fn metric_total(metrics: &MetricsSet, name: &str) -> usize {
    metrics
        .iter()
        .filter(|metric| metric.value().name() == name)
        .map(|metric| metric.value().as_usize())
        .sum()
}

criterion_group!(benches, sibling_topk);
criterion_main!(benches);
