// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::backend::BenchmarkBackend;
use crate::datasets::{clickbench, tpcds, tpch};
use crate::local_workers::LocalWorkers;
use crate::results::{BenchResult, BenchmarkRun, QueryIter, dataset_path};
use crate::stats::stats_estimation_q_error;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::instant::Instant;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{config_err, exec_err, not_impl_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::*;
use datafusion_distributed::test_utils::localhost::LocalHostWorkerResolver;
use datafusion_distributed::test_utils::work_unit_file_scan::{
    WorkUnitFileScanCodec, WorkUnitFileScanConfig, WorkUnitFileScanRule,
    work_unit_file_scan_desired_task_count, work_unit_file_scan_scale_up_leaf_node,
};
use datafusion_distributed::{
    DistributedExt, DistributedMetricsFormat, NetworkBoundaryExt, SessionStateBuilderExt, Worker,
    WorkerQueryContext, display_plan_ascii, rewrite_distributed_plan_with_metrics,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tokio::net::TcpListener;
use tonic::codegen::tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Run the tpch benchmark.
///
/// This benchmarks is derived from the [TPC-H][1] version
/// [2.17.1]. The data and answers are generated using `tpch-gen` from
/// [2].
///
/// [1]: http://www.tpc.org/tpch/
/// [2]: https://github.com/databricks/tpch-dbgen.git
/// [2.17.1]: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number. If not specified, runs all queries
    #[structopt(short, long, alias = "queries", use_delimiter = true)]
    pub query: Vec<String>,

    /// Path to data files
    #[structopt(long)]
    dataset: String,

    /// Number of localhost workers to start in this same process, on the same Tokio runtime as
    /// the benchmark runner. Use 0 for running the benchmarks in single-node mode.
    #[structopt(long, default_value = "0")]
    workers: usize,

    /// Number of bytes each partition is expected to scan from parquet files. Lower values
    /// produce more partitions/tasks. Defaults to the engine default when unset.
    #[structopt(long)]
    file_scan_config_bytes_per_partition: Option<usize>,

    /// Task count scale factor for when nodes in stages change the cardinality of the data
    #[structopt(long)]
    cardinality_task_sf: Option<f64>,

    /// Collects metrics across network boundaries
    #[structopt(long)]
    collect_metrics: bool,

    /// Collects metrics across network boundaries
    #[structopt(long, default_value = "lz4")]
    compression: String,

    /// Sets the limits of tasks for each stage
    #[structopt(long, default_value = "0")]
    max_tasks_per_stage: usize,

    /// Activate dynamic task count
    #[structopt(long)]
    dynamic: bool,

    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "5")]
    iterations: usize,

    /// Number of partitions to process in parallel. Defaults to the number of available cores.
    #[structopt(short = "n", long = "partitions")]
    partitions: Option<usize>,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size")]
    batch_size: Option<usize>,

    /// Activate debug mode to see more details
    #[structopt(short, long)]
    debug: bool,

    /// Replace each `FileScanConfig` data source with a `WorkUnitFileScanConfig`
    /// that streams its `FileGroup`s through the work-unit feed pipeline. Used
    /// to measure the latency overhead introduced by that path.
    #[structopt(long = "work-unit-file-scan")]
    work_unit_file_scan: bool,

    /// Fan-out threshold for choosing between Direct and TwoPhase shuffle modes. In Direct mode
    /// the producer hashes into `consumer_task_count × consumer_partition_count` global buckets;
    /// TwoPhase mode is used when that product meets or exceeds this value, reducing the
    /// producer-side hash to `consumer_task_count` buckets and adding a local RepartitionExec
    /// on the consumer side.
    #[structopt(long = "two-step-shuffle-fanout-threshold", default_value = "128")]
    two_step_shuffle_fanout_threshold: usize,
}

/// Serves a benchmark worker on the provided listener until it fails or `shutdown` resolves.
pub(crate) async fn serve_worker<B: BenchmarkBackend>(
    backend: B,
    listener: TcpListener,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    // Workers need to be able to decode the WorkUnitFileScan plan
    // node when the feature is on. The codec is registered via a
    // session builder so it is installed on every worker session.
    let worker = Worker::from_session_builder(move |ctx: WorkerQueryContext| {
        let builder = backend.configure_session(
            ctx.builder
                .with_distributed_user_codec(WorkUnitFileScanCodec),
        );
        async move { Ok(builder.build()) }
    });
    Server::builder()
        .add_service(worker.into_worker_server())
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}

fn queries_for_dataset(dataset: &str) -> Result<Vec<(String, String)>, DataFusionError> {
    match dataset {
        "tpch" => tpch::get_queries()
            .into_iter()
            .map(|id| Ok((id.clone(), tpch::get_query(&id)?)))
            .collect(),
        "tpcds" => tpcds::get_queries()
            .into_iter()
            .filter(|id| id != "q72") // 72 is terribly slow
            .map(|id| Ok((id.clone(), tpcds::get_query(&id)?)))
            .collect(),
        "clickbench" => clickbench::get_queries()
            .into_iter()
            .map(|id| Ok((id.clone(), clickbench::get_query(&id)?)))
            .collect(),
        _ => not_impl_err!("Unknown benchmark dataset {dataset}"),
    }
}

impl RunOpt {
    fn config(&self) -> Result<SessionConfig> {
        SessionConfig::from_env().map(|mut config| {
            if let Some(batch_size) = self.batch_size {
                config = config.with_batch_size(batch_size);
            }
            config.with_target_partitions(self.partitions.unwrap_or_else(get_available_parallelism))
        })
    }

    pub async fn run<B: BenchmarkBackend + Clone>(self, mut backend: B) -> Result<()> {
        let local_workers = LocalWorkers::start(self.workers, &backend).await?;
        let result = self.run_local(&mut backend, &local_workers.ports).await;
        local_workers.shutdown().await;
        result
    }

    async fn run_local(self, backend: &mut impl BenchmarkBackend, ports: &[u16]) -> Result<()> {
        let mut builder = SessionStateBuilder::new()
            .with_default_features()
            .with_config(self.config()?)
            .with_distributed_worker_resolver(LocalHostWorkerResolver::new(ports.to_vec()))
            .with_distributed_planner()
            .with_distributed_cardinality_effect_task_scale_factor(
                self.cardinality_task_sf.unwrap_or(1.0),
            )?
            .with_distributed_dynamic_task_count(self.dynamic)?
            .with_distributed_compression(match self.compression.as_str() {
                "zstd" => Some(CompressionType::ZSTD),
                "lz4" => Some(CompressionType::LZ4_FRAME),
                "none" => None,
                v => return config_err!("Unknown compression type {v}"),
            })?
            .with_distributed_children_isolator_unions(true)?
            .with_distributed_broadcast_joins(true)?
            .with_distributed_two_step_shuffle_fanout_threshold(
                self.two_step_shuffle_fanout_threshold,
            )?
            .with_distributed_metrics_collection(
                self.collect_metrics || self.debug || self.dynamic,
            )?
            .with_distributed_max_tasks_per_stage(self.max_tasks_per_stage)?
            .with_distributed_user_codec(WorkUnitFileScanCodec)
            .with_distributed_desired_task_count_handler(work_unit_file_scan_desired_task_count)
            .with_distributed_scale_up_leaf_node_handler(work_unit_file_scan_scale_up_leaf_node)
            .with_distributed_work_unit_feed(|dse: &DataSourceExec| {
                dse.data_source()
                    .downcast_ref::<WorkUnitFileScanConfig>()
                    .map(|v| &v.feed)
            });

        if let Some(bytes_per_partition) = self.file_scan_config_bytes_per_partition {
            builder = builder
                .with_distributed_file_scan_config_bytes_per_partition(bytes_per_partition)?;
        }

        if self.work_unit_file_scan {
            builder = builder.with_physical_optimizer_rule(Arc::new(WorkUnitFileScanRule))
        }

        let state = backend.configure_session(builder).build();
        let ctx = SessionContext::new_with_state(state);
        backend.register_tables(&ctx, &self.get_path()?).await?;
        let dataset_suite = if Path::new(&self.dataset).is_absolute() {
            // Absolute paths follow the same <suite>/<variant> convention.
            Path::new(&self.dataset)
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                .unwrap_or("")
        } else {
            self.dataset
                .split_once('/')
                .map_or(self.dataset.as_str(), |(suite, _)| suite)
        };

        println!("Running benchmarks with the following options: {self:?}");
        let mut benchmark_run = BenchmarkRun::new(self.dataset.clone(), ports.len());

        for (id, sql) in queries_for_dataset(dataset_suite)? {
            if !self.query.is_empty() && !self.query.contains(&id.to_string()) {
                continue;
            }
            let query_id = id;
            let query_run = self.benchmark_query(&query_id, &sql, &ctx).await;
            if let Err(e) = &query_run {
                eprintln!("{query_id} failed: {e:?}");
            }
            benchmark_run.results.push(query_run?);
        }

        benchmark_run.compare_with_previous()?;
        benchmark_run.store()?;
        Ok(())
    }

    async fn benchmark_query(
        &self,
        id: &str,
        sql: &str,
        ctx: &SessionContext,
    ) -> Result<BenchResult> {
        let mut bench_query = BenchResult {
            id: id.to_string(),
            dataset: self.dataset.clone(),
            iterations: vec![],
        };

        'outer: for i in 0..self.iterations {
            let start = Instant::now();

            for query in sql.split(";").map(|v| v.trim()) {
                if query.starts_with("create") || query.starts_with("drop") {
                    self.execute_query(ctx, query).await?;
                    continue;
                } else if query.is_empty() {
                    continue;
                }

                match self.execute_query(ctx, query).await {
                    Ok((result, n_tasks, physical_plan)) => {
                        let elapsed = start.elapsed();
                        let ms = elapsed.as_secs_f64() * 1000.0;
                        let row_count = result.iter().map(|b| b.num_rows()).sum();
                        let physical_plan = if self.dynamic || self.debug {
                            rewrite_distributed_plan_with_metrics(
                                physical_plan,
                                DistributedMetricsFormat::PerTask,
                            )
                            .await?
                        } else {
                            physical_plan
                        };
                        let stats_q_error = match self.dynamic {
                            true => stats_estimation_q_error(&physical_plan),
                            false => None,
                        };
                        if let Some(q_error) = stats_q_error {
                            println!(
                                "Query {id} iteration {i} took {ms:.1} ms, stats q-error P50 {:.2}x, P95 {:.2}x and returned {row_count} rows",
                                q_error.p50, q_error.p95
                            );
                        } else {
                            println!(
                                "Query {id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
                            );
                        }
                        if self.debug {
                            println!(
                                "=== Physical plan with metrics ===\n{}\n",
                                display_plan_ascii(physical_plan.as_ref(), true)
                            );
                        }

                        bench_query.iterations.push(QueryIter {
                            elapsed,
                            row_count,
                            n_tasks,
                            stats_q_error_p50: stats_q_error.map(|q_error| q_error.p50),
                            stats_q_error_p95: stats_q_error.map(|q_error| q_error.p95),
                            error: None,
                        });
                    }
                    Err(err) => {
                        println!("Query {id} iteration {i} failed: {err}");
                        bench_query.iterations.push(QueryIter {
                            elapsed: Duration::from_millis(0),
                            row_count: 0,
                            n_tasks: 0,
                            stats_q_error_p50: None,
                            stats_q_error_p95: None,
                            error: Some(err.to_string()),
                        });
                        continue 'outer;
                    }
                }
            }
        }
        println!("Query {id} p50 time: {} ms", bench_query.p50());

        Ok(bench_query)
    }

    async fn execute_query(
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<(Vec<RecordBatch>, usize, Arc<dyn ExecutionPlan>)> {
        let plan = ctx.sql(sql).await?;
        let (state, plan) = plan.into_parts();

        let plan = state.optimize(&plan)?;
        let physical_plan = state.create_physical_plan(&plan).await?;
        let mut n_tasks = 0;
        physical_plan.clone().transform_down(|node| {
            if let Some(node) = node.as_network_boundary() {
                n_tasks += node.input_stage().task_count()
            }
            Ok(Transformed::no(node))
        })?;
        let result = collect(physical_plan.clone(), state.task_ctx()).await?;
        Ok((result, n_tasks, physical_plan))
    }

    fn get_path(&self) -> Result<PathBuf> {
        let data_path = dataset_path(&self.dataset);
        if !data_path.exists() {
            return exec_err!(
                "--dataset {} doesn't exist. Was it generated?",
                self.dataset
            );
        }

        let entries = fs::read_dir(&data_path)?.collect::<Result<Vec<_>, _>>()?;
        if entries.is_empty() {
            return exec_err!("Dataset {} is empty", self.dataset);
        }
        Ok(data_path)
    }
}
