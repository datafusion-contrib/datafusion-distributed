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

use crate::results::{BenchResult, BenchmarkRun, QueryIter};
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::instant::Instant;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{config_err, exec_err, not_impl_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use datafusion_distributed::test_utils::benchmarks_common;
use datafusion_distributed::test_utils::localhost::LocalHostWorkerResolver;
use datafusion_distributed::test_utils::{clickbench, tpcds, tpch};
use datafusion_distributed::{
    DistributedExt, DistributedPhysicalOptimizerRule, NetworkBoundaryExt, Worker,
};
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
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
    #[structopt(short, long, use_delimiter = true)]
    pub query: Vec<String>,

    /// Path to data files
    #[structopt(long)]
    dataset: String,

    /// Spawns a worker in the specified port.
    #[structopt(long)]
    spawn: Option<u16>,

    /// The ports of all the workers involved in the query.
    #[structopt(long, use_delimiter = true)]
    workers: Vec<u16>,

    /// Number of physical threads per worker.
    #[structopt(long)]
    threads: Option<usize>,

    /// Number of bytes each partition is expected to process.
    #[structopt(long, default_value = "128 * 1024 * 1024")]
    bytes_processed_per_partition: usize,

    /// Use children isolator UNIONs for distributing UNION operations.
    #[structopt(long)]
    children_isolator_unions: bool,

    /// Turns on broadcast joins.
    #[structopt(long = "broadcast-joins")]
    broadcast_joins: bool,

    /// Collects metrics across network boundaries
    #[structopt(long)]
    collect_metrics: bool,

    /// Collects metrics across network boundaries
    #[structopt(long, default_value = "lz4")]
    compression: String,

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
            config.with_target_partitions(self.partitions())
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
                        .add_service(Worker::default().into_flight_server())
                        .serve_with_incoming(incoming)
                        .await?,
                )
            })?;
        } else {
            rt.block_on(self.run_local())?;
        }
        Ok(())
    }

    async fn run_local(self) -> Result<()> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(self.config()?)
            .with_distributed_worker_resolver(LocalHostWorkerResolver::new(self.workers.clone()))
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_distributed_bytes_processed_per_partition(self.bytes_processed_per_partition)?
            .with_distributed_compression(match self.compression.as_str() {
                "zstd" => Some(CompressionType::ZSTD),
                "lz4" => Some(CompressionType::LZ4_FRAME),
                "none" => None,
                v => return config_err!("Unknown compression type {v}"),
            })?
            .with_distributed_children_isolator_unions(self.children_isolator_unions)?
            .with_distributed_broadcast_joins(self.broadcast_joins)?
            .with_distributed_metrics_collection(self.collect_metrics)?
            .build();
        let ctx = SessionContext::new_with_state(state);
        benchmarks_common::register_tables(&ctx, &self.get_path()?).await?;

        println!("Running benchmarks with the following options: {self:?}");
        let mut benchmark_run = BenchmarkRun::new(
            self.dataset.clone(),
            self.workers.len(),
            self.threads.unwrap_or(get_available_parallelism()),
        );

        let dataset_prefix = self.dataset.split("_").next().unwrap();
        for (id, sql) in queries_for_dataset(dataset_prefix)? {
            if !self.query.is_empty() && !self.query.contains(&id.to_string()) {
                continue;
            }
            let query_id = format!("{} {id}", self.dataset);
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
                    Ok((result, n_tasks)) => {
                        let elapsed = start.elapsed();
                        let ms = elapsed.as_secs_f64() * 1000.0;
                        let row_count = result.iter().map(|b| b.num_rows()).sum();
                        println!(
                            "Query {id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
                        );

                        bench_query.iterations.push(QueryIter {
                            elapsed,
                            row_count,
                            n_tasks,
                            error: None,
                        });
                    }
                    Err(err) => {
                        println!("Query {id} iteration {i} failed: {err}");
                        bench_query.iterations.push(QueryIter {
                            elapsed: Duration::from_millis(0),
                            row_count: 0,
                            n_tasks: 0,
                            error: Some(err.to_string()),
                        });
                        continue 'outer;
                    }
                }
            }
        }
        println!("Query {id} avg time: {:.2} ms", bench_query.avg());

        Ok(bench_query)
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
        let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .join(&self.dataset);
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
