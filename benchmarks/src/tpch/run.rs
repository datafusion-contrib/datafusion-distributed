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

use super::{
    TPCH_QUERY_END_ID, TPCH_QUERY_START_ID, TPCH_TABLES, get_query_sql, get_tbl_tpch_table_schema,
    get_tpch_table_schema,
};
use crate::util::{
    BenchmarkRun, CommonOpt, InMemoryCacheExecCodec, InMemoryDataSourceRule, QueryIter,
    WarmingUpMarker,
};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::instant::Instant;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{DEFAULT_CSV_EXTENSION, DEFAULT_PARQUET_EXTENSION, exec_err};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use datafusion_distributed::test_utils::localhost::{
    LocalHostChannelResolver, spawn_flight_service,
};
use datafusion_distributed::{
    DistributedExt, DistributedPhysicalOptimizerRule, DistributedSessionBuilder,
    DistributedSessionBuilderContext, StageExec,
};
use log::info;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::net::TcpListener;

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

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Path to data files
    #[structopt(parse(from_os_str), short = "p", long = "path")]
    path: Option<PathBuf>,

    /// File format: `csv` or `parquet`
    #[structopt(short = "f", long = "format", default_value = "parquet")]
    file_format: String,

    /// Load the data into a MemTable before executing the query
    #[structopt(short = "m", long = "mem-table")]
    mem_table: bool,

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,

    /// Whether to disable collection of statistics (and cost based optimizations) or not.
    #[structopt(short = "S", long = "disable-statistics")]
    disable_statistics: bool,

    /// Mark the first column of each table as sorted in ascending order.
    /// The tables should have been created with the `--sort` option for this to have any effect.
    #[structopt(short = "t", long = "sorted")]
    sorted: bool,

    /// Upon shuffling data, this defines how many tasks are employed into performing the shuffling.
    /// ```text
    ///  ( task 1 )  ( task 2 ) ( task 3 )
    ///      ▲           ▲          ▲
    ///      └────┬──────┴─────┬────┘
    ///       ( task 1 )  ( task 2 )       N tasks
    /// ```
    /// This parameter defines N
    #[structopt(long)]
    shuffle_tasks: Option<usize>,

    /// Upon merging multiple tasks into one, this defines how many tasks are merged.
    /// ```text
    ///              ( task 1 )
    ///                  ▲
    ///      ┌───────────┴──────────┐
    ///  ( task 1 )  ( task 2 ) ( task 3 )  N tasks
    /// ```
    /// This parameter defines N
    #[structopt(long)]
    coalesce_tasks: Option<usize>,

    /// Spawns a worker in the specified port.
    #[structopt(long)]
    spawn: Option<u16>,

    /// The ports of all the workers involved in the query.
    #[structopt(long, use_delimiter = true)]
    workers: Vec<u16>,

    /// Number of physical threads per worker.
    #[structopt(long)]
    threads: Option<usize>,
}

#[async_trait]
impl DistributedSessionBuilder for RunOpt {
    async fn build_session_state(
        &self,
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError> {
        let mut builder = SessionStateBuilder::new().with_default_features();

        let config = self
            .common
            .config()?
            .with_collect_statistics(!self.disable_statistics)
            .with_distributed_user_codec(InMemoryCacheExecCodec)
            .with_distributed_channel_resolver(LocalHostChannelResolver::new(self.workers.clone()))
            .with_distributed_option_extension_from_headers::<WarmingUpMarker>(&ctx.headers)?
            .with_target_partitions(self.partitions());

        let rt_builder = self.common.runtime_env_builder()?;

        if self.mem_table {
            builder = builder.with_physical_optimizer_rule(Arc::new(InMemoryDataSourceRule));
        }
        if !self.workers.is_empty() {
            let rule = DistributedPhysicalOptimizerRule::new()
                .with_network_coalesce_tasks(self.coalesce_tasks.unwrap_or(self.workers.len()))
                .with_network_shuffle_tasks(self.shuffle_tasks.unwrap_or(self.workers.len()));
            builder = builder.with_physical_optimizer_rule(Arc::new(rule));
        }

        Ok(builder
            .with_config(config)
            .with_runtime_env(rt_builder.build_arc()?)
            .build())
    }
}

impl RunOpt {
    pub fn run(self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.threads.unwrap_or(get_available_parallelism()))
            .enable_all()
            .build()?;

        if let Some(port) = self.spawn {
            rt.block_on(async move {
                let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
                println!("Listening on {}...", listener.local_addr().unwrap());
                spawn_flight_service(self, listener).await
            })?;
        } else {
            rt.block_on(self.run_local())?;
        }
        Ok(())
    }

    async fn run_local(mut self) -> Result<()> {
        let state = self.build_session_state(Default::default()).await?;
        let ctx = SessionContext::new_with_state(state);
        self.register_tables(&ctx).await?;

        println!("Running benchmarks with the following options: {self:?}");
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => TPCH_QUERY_START_ID..=TPCH_QUERY_END_ID,
        };

        self.output_path
            .get_or_insert(self.get_path()?.join("results.json"));
        let mut benchmark_run = BenchmarkRun::new(
            self.workers.len(),
            self.threads.unwrap_or(get_available_parallelism()),
        );

        // Warmup the cache for the in-memory mode.
        if self.mem_table {
            for query_id in query_range.clone() {
                // put the WarmingUpMarker in the context, otherwise, queries will fail as the
                // InMemoryCacheExec node will think they should already be warmed up.
                let ctx = ctx
                    .clone()
                    .with_distributed_option_extension(WarmingUpMarker::warming_up())?;
                for query in get_query_sql(query_id)? {
                    self.execute_query(&ctx, &query).await?;
                }
                println!("Query {query_id} data loaded in memory");
            }
        }

        for query_id in query_range {
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let query_run = self.benchmark_query(query_id, &ctx).await;
            match query_run {
                Ok(query_results) => {
                    for iter in query_results {
                        benchmark_run.write_iter(iter);
                    }
                }
                Err(e) => {
                    benchmark_run.mark_failed();
                    eprintln!("Query {query_id} failed: {e:?}");
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
        query_id: usize,
        ctx: &SessionContext,
    ) -> Result<Vec<QueryIter>> {
        let mut millis = vec![];
        // run benchmark
        let mut query_results = vec![];

        let sql = &get_query_sql(query_id)?;

        let mut n_tasks = 0;
        for i in 0..self.iterations() {
            let start = Instant::now();
            let mut result = vec![];

            // query 15 is special, with 3 statements. the second statement is the one from which we
            // want to capture the results
            let result_stmt = if query_id == 15 { 1 } else { sql.len() - 1 };

            for (i, query) in sql.iter().enumerate() {
                if i == result_stmt {
                    (result, n_tasks) = self.execute_query(ctx, query).await?;
                } else {
                    self.execute_query(ctx, query).await?;
                }
            }

            let elapsed = start.elapsed();
            let ms = elapsed.as_secs_f64() * 1000.0;
            millis.push(ms);
            info!("output:\n\n{}\n\n", pretty_format_batches(&result)?);
            let row_count = result.iter().map(|b| b.num_rows()).sum();
            println!(
                "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
            );

            query_results.push(QueryIter {
                elapsed,
                row_count,
                n_tasks,
            });
        }

        let avg = millis.iter().sum::<f64>() / millis.len() as f64;
        println!("Query {query_id} avg time: {avg:.2} ms");
        if n_tasks > 0 {
            println!("Query {query_id} number of tasks: {n_tasks}");
        }

        Ok(query_results)
    }

    async fn register_tables(&self, ctx: &SessionContext) -> Result<()> {
        for table in TPCH_TABLES {
            ctx.register_table(*table, self.get_table(ctx, table).await?)?;
        }
        Ok(())
    }

    async fn execute_query(
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<(Vec<RecordBatch>, usize)> {
        let debug = self.common.debug;
        let plan = ctx.sql(sql).await?;
        let (state, plan) = plan.into_parts();

        if debug {
            println!("=== Logical plan ===\n{plan}\n");
        }

        let plan = state.optimize(&plan)?;
        if debug {
            println!("=== Optimized logical plan ===\n{plan}\n");
        }
        let physical_plan = state.create_physical_plan(&plan).await?;
        if debug {
            println!(
                "=== Physical plan ===\n{}\n",
                displayable(physical_plan.as_ref()).indent(true)
            );
        }
        let mut n_tasks = 0;
        physical_plan.clone().transform_down(|node| {
            if let Some(node) = node.as_any().downcast_ref::<StageExec>() {
                n_tasks += node.tasks.len()
            }
            Ok(Transformed::no(node))
        })?;
        let result = collect(physical_plan.clone(), state.task_ctx()).await?;
        if debug {
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
                "No TPCH dataset present in '{data_path:?}'. Generate one with ./benchmarks/gen-tpch.sh"
            )
        } else if entries.len() == 1 {
            Ok(entries[0].path())
        } else {
            exec_err!(
                "Multiple TPCH datasets present in '{data_path:?}'. One must be selected with --path"
            )
        }
    }

    async fn get_table(&self, ctx: &SessionContext, table: &str) -> Result<Arc<dyn TableProvider>> {
        let path = self.get_path()?;
        let path = path.to_str().unwrap();
        let table_format = self.file_format.as_str();
        let target_partitions = self.partitions();

        // Obtain a snapshot of the SessionState
        let state = ctx.state();
        let (format, path, extension): (Arc<dyn FileFormat>, String, &'static str) =
            match table_format {
                // dbgen creates .tbl ('|' delimited) files without header
                "tbl" => {
                    let path = format!("{path}/{table}.tbl");

                    let format = CsvFormat::default()
                        .with_delimiter(b'|')
                        .with_has_header(false);

                    (Arc::new(format), path, ".tbl")
                }
                "csv" => {
                    let path = format!("{path}/csv/{table}");
                    let format = CsvFormat::default()
                        .with_delimiter(b',')
                        .with_has_header(true);

                    (Arc::new(format), path, DEFAULT_CSV_EXTENSION)
                }
                "parquet" => {
                    let path = format!("{path}/{table}");
                    let format = ParquetFormat::default()
                        .with_options(ctx.state().table_options().parquet.clone());

                    (Arc::new(format), path, DEFAULT_PARQUET_EXTENSION)
                }
                other => {
                    unimplemented!("Invalid file format '{}'", other);
                }
            };

        let table_path = ListingTableUrl::parse(path)?;
        let options = ListingOptions::new(format)
            .with_file_extension(extension)
            .with_target_partitions(target_partitions)
            .with_collect_stat(state.config().collect_statistics());
        let schema = match table_format {
            "parquet" => options.infer_schema(&state, &table_path).await?,
            "tbl" => Arc::new(get_tbl_tpch_table_schema(table)),
            "csv" => Arc::new(get_tpch_table_schema(table)),
            _ => unreachable!(),
        };
        let options = if self.sorted {
            let key_column_name = schema.fields()[0].name();
            options.with_file_sort_order(vec![vec![col(key_column_name).sort(true, false)]])
        } else {
            options
        };

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(schema);

        Ok(Arc::new(ListingTable::try_new(config)?))
    }

    fn iterations(&self) -> usize {
        self.common.iterations
    }

    fn partitions(&self) -> usize {
        if let Some(partitions) = self.common.partitions {
            return partitions;
        }
        if let Some(threads) = self.threads {
            return threads;
        }
        get_available_parallelism()
    }
}
