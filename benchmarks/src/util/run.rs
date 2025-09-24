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
use datafusion::common::utils::get_available_parallelism;
use datafusion::{DATAFUSION_VERSION, error::Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    path::Path,
    time::{Duration, SystemTime},
};

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
            std::fs::write(path, self.to_json())?;
        };
        Ok(())
    }

    pub fn maybe_compare_with_previous(&self, maybe_path: Option<impl AsRef<Path>>) -> Result<()> {
        let Some(path) = maybe_path else {
            return Ok(());
        };
        let Ok(prev) = std::fs::read(path) else {
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
