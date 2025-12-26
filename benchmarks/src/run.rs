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

use chrono::{DateTime, Utc};
use datafusion::DATAFUSION_VERSION;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::instant::Instant;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{exec_err, not_impl_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use datafusion_distributed::test_utils::localhost::LocalHostWorkerResolver;
use datafusion_distributed::test_utils::{tpcds, tpch};
use datafusion_distributed::{
    ArrowFlightEndpoint, DistributedExt, DistributedPhysicalOptimizerRule, NetworkBoundaryExt,
};
use log::info;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use structopt::StructOpt;
use tokio::net::TcpListener;
use tonic::codegen::tokio_stream;
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
    #[structopt(short, long)]
    pub query: Option<usize>,

    /// Path to data files
    #[structopt(parse(from_os_str), short = "p", long = "path")]
    path: Option<PathBuf>,

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,

    /// Spawns a worker in the specified port.
    #[structopt(long)]
    spawn: Option<u16>,

    /// The ports of all the workers involved in the query.
    #[structopt(long, use_delimiter = true)]
    workers: Vec<u16>,

    /// Number of physical threads per worker.
    #[structopt(long)]
    threads: Option<usize>,

    /// Number of files per each distributed task.
    #[structopt(long)]
    files_per_task: Option<usize>,

    /// Task count scale factor for when nodes in stages change the cardinality of the data
    #[structopt(long)]
    cardinality_task_sf: Option<f64>,

    /// Collects metrics across network boundaries
    #[structopt(long)]
    collect_metrics: bool,

    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    iterations: usize,

    /// Number of partitions to process in parallel. Defaults to number of available cores.
    /// Should typically be less or equal than --threads.
    #[structopt(short = "n", long = "partitions")]
    partitions: Option<usize>,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size")]
    batch_size: Option<usize>,

    /// Activate debug mode to see more details
    #[structopt(short, long)]
    debug: bool,
}

#[derive(Debug)]
enum Dataset {
    Tpch,
    Tpcds,
}

impl Dataset {
    fn infer_from_data_path(path: PathBuf) -> Result<Self, DataFusionError> {
        if path
            .iter()
            .any(|v| v.to_str().is_some_and(|v| v.contains("tpch")))
        {
            return Ok(Self::Tpch);
        }
        if path
            .iter()
            .any(|v| v.to_str().is_some_and(|v| v.contains("tpcds")))
        {
            return Ok(Self::Tpcds);
        }
        not_impl_err!(
            "Cannot infer benchmark dataset from path {}",
            path.display()
        )
    }

    fn queries(&self) -> Result<Vec<(usize, String)>, DataFusionError> {
        match self {
            Dataset::Tpch => (1..22 + 1)
                .map(|i| Ok((i as usize, tpch::get_test_tpch_query(i)?)))
                .collect(),
            Dataset::Tpcds => (1..72)
                // skip query 72, it's ridiculously slow
                .chain(73..99 + 1)
                .map(|i| Ok((i, tpcds::get_test_tpcds_query(i)?)))
                .collect(),
        }
    }
}

impl RunOpt {
    fn config(&self) -> Result<SessionConfig> {
        SessionConfig::from_env().map(|mut config| {
            if let Some(batch_size) = self.batch_size {
                config = config.with_batch_size(batch_size);
            }
            if let Some(partitions) = self.partitions {
                config = config.with_target_partitions(partitions);
            }
            config
        })
    }

    pub fn run(self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.threads.unwrap_or(get_available_parallelism()))
            .enable_all()
            .build()?;

        if let Some(port) = self.spawn {
            rt.block_on(async move {
                let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
                println!("Listening on {}...", listener.local_addr().unwrap());
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                Ok::<_, Box<dyn Error + Send + Sync>>(
                    Server::builder()
                        .add_service(ArrowFlightEndpoint::default().into_flight_server())
                        .serve_with_incoming(incoming)
                        .await?,
                )
            })?;
        } else {
            rt.block_on(self.run_local())?;
        }
        Ok(())
    }

    async fn run_local(mut self) -> Result<()> {
        let config = self.config()?.with_target_partitions(self.partitions());
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_distributed_worker_resolver(LocalHostWorkerResolver::new(self.workers.clone()))
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_distributed_files_per_task(
                self.files_per_task.unwrap_or(get_available_parallelism()),
            )?
            .with_distributed_cardinality_effect_task_scale_factor(
                self.cardinality_task_sf.unwrap_or(1.0),
            )?
            .with_distributed_metrics_collection(self.collect_metrics)?
            .build();
        let ctx = SessionContext::new_with_state(state);
        let path = self.get_path()?;
        self.register_tables(&ctx, path.clone()).await?;

        println!("Running benchmarks with the following options: {self:?}");
        self.output_path.get_or_insert(path.join("results.json"));
        let mut benchmark_run = BenchmarkRun::new(
            self.workers.len(),
            self.threads.unwrap_or(get_available_parallelism()),
        );

        let dataset = Dataset::infer_from_data_path(path.clone())?;

        for (id, sql) in dataset.queries()? {
            if self.query.is_some_and(|v| v != id) {
                continue;
            }
            let query_id = format!("{dataset:?} {id}");
            benchmark_run.start_new_case(&query_id);
            let query_run = self.benchmark_query(&query_id, &sql, &ctx).await;
            match query_run {
                Ok(query_results) => {
                    for iter in query_results {
                        benchmark_run.write_iter(iter);
                    }
                }
                Err(e) => {
                    benchmark_run.mark_failed();
                    eprintln!("{query_id} failed: {e:?}");
                }
            }
        }
        benchmark_run.maybe_compare_with_previous(self.output_path.as_ref())?;
        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        benchmark_run.maybe_print_failures();
        Ok(())
    }

    async fn benchmark_query(
        &self,
        id: &str,
        sql: &str,
        ctx: &SessionContext,
    ) -> Result<Vec<QueryIter>> {
        let mut millis = vec![];
        // run benchmark
        let mut query_results = vec![];

        let mut n_tasks = 0;
        for i in 0..self.iterations {
            let start = Instant::now();
            let mut result = vec![];

            for query in sql.split(";").map(|v| v.trim()) {
                if query.starts_with("create") || query.starts_with("drop") {
                    self.execute_query(ctx, query).await?;
                } else if !query.is_empty() {
                    (result, n_tasks) = self.execute_query(ctx, query).await?;
                }
            }

            let elapsed = start.elapsed();
            let ms = elapsed.as_secs_f64() * 1000.0;
            millis.push(ms);
            info!("output:\n\n{}\n\n", pretty_format_batches(&result)?);
            let row_count = result.iter().map(|b| b.num_rows()).sum();
            println!("Query {id} iteration {i} took {ms:.1} ms and returned {row_count} rows");

            query_results.push(QueryIter {
                elapsed,
                row_count,
                n_tasks,
            });
        }

        let avg = millis.iter().sum::<f64>() / millis.len() as f64;
        println!("Query {id} avg time: {avg:.2} ms");
        if n_tasks > 0 {
            println!("Query {id} number of tasks: {n_tasks}");
        }

        Ok(query_results)
    }

    async fn register_tables(&self, ctx: &SessionContext, path: PathBuf) -> Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let table_name = path.file_name().unwrap().to_str().unwrap();
                ctx.register_parquet(
                    table_name,
                    path.display().to_string(),
                    ParquetReadOptions::default(),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn execute_query(
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<(Vec<RecordBatch>, usize)> {
        let plan = ctx.sql(sql).await?;
        let (state, plan) = plan.into_parts();

        if self.debug {
            println!("=== Logical plan ===\n{plan}\n");
        }

        let plan = state.optimize(&plan)?;
        if self.debug {
            println!("=== Optimized logical plan ===\n{plan}\n");
        }
        let physical_plan = state.create_physical_plan(&plan).await?;
        if self.debug {
            println!(
                "=== Physical plan ===\n{}\n",
                displayable(physical_plan.as_ref()).indent(true)
            );
        }
        let mut n_tasks = 0;
        physical_plan.clone().transform_down(|node| {
            if let Some(node) = node.as_network_boundary() {
                n_tasks += node.input_stage().tasks.len()
            }
            Ok(Transformed::no(node))
        })?;
        let result = collect(physical_plan.clone(), state.task_ctx()).await?;
        if self.debug {
            println!(
                "=== Physical plan with metrics ===\n{}\n",
                DisplayableExecutionPlan::with_metrics(physical_plan.as_ref()).indent(true)
            );
        }
        Ok((result, n_tasks))
    }

    fn get_path(&self) -> Result<PathBuf> {
        if let Some(path) = &self.path {
            return Ok(path.clone());
        }
        let crate_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let data_path = crate_path.join("data");
        let entries = fs::read_dir(&data_path)?.collect::<Result<Vec<_>, _>>()?;
        if entries.is_empty() {
            exec_err!(
                "No Benchmarking dataset present in '{data_path:?}'. Generate one with ./benchmarks/gen-tpch.sh"
            )
        } else if entries.len() == 1 {
            Ok(entries[0].path())
        } else {
            exec_err!(
                "Multiple Benchmarking datasets present in '{data_path:?}'. One must be selected with --path"
            )
        }
    }

    fn partitions(&self) -> usize {
        if let Some(partitions) = self.partitions {
            return partitions;
        }
        if let Some(threads) = self.threads {
            return threads;
        }
        get_available_parallelism()
    }
}

fn serialize_start_time<S>(start_time: &SystemTime, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    ser.serialize_u64(
        start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("current time is later than UNIX_EPOCH")
            .as_secs(),
    )
}
fn deserialize_start_time<'de, D>(des: D) -> Result<SystemTime, D::Error>
where
    D: Deserializer<'de>,
{
    let secs = u64::deserialize(des)?;
    Ok(SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
}

fn serialize_elapsed<S>(elapsed: &Duration, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let ms = elapsed.as_secs_f64() * 1000.0;
    ser.serialize_f64(ms)
}

fn deserialize_elapsed<'de, D>(des: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let ms = f64::deserialize(des)?;
    Ok(Duration::from_secs_f64(ms / 1000.0))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunContext {
    /// Benchmark crate version
    pub benchmark_version: String,
    /// DataFusion crate version
    pub datafusion_version: String,
    /// Number of CPU cores
    pub num_cpus: usize,
    /// Number of workers involved in a distributed query
    pub workers: usize,
    /// Number of physical threads used per worker
    pub threads: usize,
    /// Start time
    #[serde(
        serialize_with = "serialize_start_time",
        deserialize_with = "deserialize_start_time"
    )]
    pub start_time: SystemTime,
    /// CLI arguments
    pub arguments: Vec<String>,
}

impl RunContext {
    pub fn new(workers: usize, threads: usize) -> Self {
        Self {
            benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
            datafusion_version: DATAFUSION_VERSION.to_owned(),
            num_cpus: get_available_parallelism(),
            workers,
            threads,
            start_time: SystemTime::now(),
            arguments: std::env::args().skip(1).collect::<Vec<String>>(),
        }
    }
}

/// A single iteration of a benchmark query
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryIter {
    #[serde(
        serialize_with = "serialize_elapsed",
        deserialize_with = "deserialize_elapsed"
    )]
    pub elapsed: Duration,
    pub row_count: usize,
    pub n_tasks: usize,
}
/// A single benchmark case
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchQuery {
    query: String,
    iterations: Vec<QueryIter>,
    #[serde(
        serialize_with = "serialize_start_time",
        deserialize_with = "deserialize_start_time"
    )]
    start_time: SystemTime,
    success: bool,
}

/// collects benchmark run data and then serializes it at the end
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkRun {
    context: RunContext,
    queries: Vec<BenchQuery>,
    current_case: Option<usize>,
}

impl BenchmarkRun {
    // create new
    pub fn new(workers: usize, threads: usize) -> Self {
        Self {
            context: RunContext::new(workers, threads),
            queries: vec![],
            current_case: None,
        }
    }
    /// begin a new case. iterations added after this will be included in the new case
    pub fn start_new_case(&mut self, id: &str) {
        self.queries.push(BenchQuery {
            query: id.to_owned(),
            iterations: vec![],
            start_time: SystemTime::now(),
            success: true,
        });
        if let Some(c) = self.current_case.as_mut() {
            *c += 1;
        } else {
            self.current_case = Some(0);
        }
    }
    /// Write a new iteration to the current case
    pub fn write_iter(&mut self, query_iter: QueryIter) {
        if let Some(idx) = self.current_case {
            self.queries[idx].iterations.push(query_iter)
        } else {
            panic!("no cases existed yet");
        }
    }

    /// Print the names of failed queries, if any
    pub fn maybe_print_failures(&self) {
        let failed_queries: Vec<&str> = self
            .queries
            .iter()
            .filter_map(|q| (!q.success).then_some(q.query.as_str()))
            .collect();

        if !failed_queries.is_empty() {
            println!("Failed Queries: {}", failed_queries.join(", "));
        }
    }

    /// Mark current query
    pub fn mark_failed(&mut self) {
        if let Some(idx) = self.current_case {
            self.queries[idx].success = false;
        } else {
            unreachable!("Cannot mark failure: no current case");
        }
    }

    /// Stringify data into formatted json
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(&self).unwrap()
    }

    /// Write data as json into output path if it exists.
    pub fn maybe_write_json(&self, maybe_path: Option<impl AsRef<Path>>) -> Result<()> {
        if let Some(path) = maybe_path {
            fs::write(path, self.to_json())?;
        };
        Ok(())
    }

    pub fn maybe_compare_with_previous(&self, maybe_path: Option<impl AsRef<Path>>) -> Result<()> {
        let Some(path) = maybe_path else {
            return Ok(());
        };
        let Ok(prev) = fs::read(path) else {
            return Ok(());
        };

        let Ok(prev_output) = serde_json::from_slice::<Self>(&prev) else {
            return Ok(());
        };

        let mut header_printed = false;
        for query in self.queries.iter() {
            let Some(prev_query) = prev_output.queries.iter().find(|v| v.query == query.query)
            else {
                continue;
            };
            if prev_query.iterations.is_empty() {
                continue;
            }
            if query.iterations.is_empty() {
                println!("{}: Failed ❌", query.query);
                continue;
            }

            let avg_prev = prev_query.avg();
            let avg = query.avg();
            let (f, tag, emoji) = if avg < avg_prev {
                let f = avg_prev as f64 / avg as f64;
                (f, "faster", if f > 1.2 { "✅" } else { "✔" })
            } else {
                let f = avg as f64 / avg_prev as f64;
                (f, "slower", if f > 1.2 { "❌" } else { "✖" })
            };
            if !header_printed {
                header_printed = true;
                let datetime: DateTime<Utc> = prev_query.start_time.into();
                let header = format!(
                    "==== Comparison with the previous benchmark from {} ====",
                    datetime.format("%Y-%m-%d %H:%M:%S UTC")
                );
                println!("{header}");
                // Print machine information
                println!("os:        {}", std::env::consts::OS);
                println!("arch:      {}", std::env::consts::ARCH);
                println!("cpu cores: {}", get_available_parallelism());
                println!(
                    "threads:   {} -> {}",
                    prev_output.context.threads, self.context.threads
                );
                println!(
                    "workers:   {} -> {}",
                    prev_output.context.workers, self.context.workers
                );
                println!("{}", "=".repeat(header.len()))
            }
            println!(
                "{:>8}: prev={avg_prev:>4} ms, new={avg:>4} ms, diff={f:.2} {tag} {emoji}",
                query.query
            );
        }

        Ok(())
    }
}

impl BenchQuery {
    fn avg(&self) -> u128 {
        self.iterations
            .iter()
            .map(|v| v.elapsed.as_millis())
            .sum::<u128>()
            / self.iterations.len() as u128
    }
}
