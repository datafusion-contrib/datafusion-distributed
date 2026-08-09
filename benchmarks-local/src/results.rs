use crate::{DATA_PATH, RESULTS_DIR};
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{Result, internal_datafusion_err};
use datafusion_distributed_benchmarks::stats::median;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime};

/// A single iteration of a benchmark query
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryIter {
    pub row_count: usize,
    pub n_tasks: usize,
    /// P50 q-error of the byte estimates made at dynamic stage boundaries.
    /// `None` when dynamic planning was disabled or no sampled boundary was present.
    pub stats_q_error_p50: Option<f64>,
    /// P95 q-error of the byte estimates made at dynamic stage boundaries.
    /// `None` when dynamic planning was disabled or no sampled boundary was present.
    pub stats_q_error_p95: Option<f64>,
    #[serde(
        serialize_with = "serialize_elapsed",
        deserialize_with = "deserialize_elapsed"
    )]
    pub elapsed: Duration,
    pub error: Option<String>,
}

/// A single benchmark case
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchResult {
    pub id: String,
    pub dataset: String,
    pub iterations: Vec<QueryIter>,
}

/// collects benchmark run data and then serializes it at the end
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkRun {
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
    pub dataset: String,
    pub branch: String,
    #[serde(serialize_with = "serialize_bench_results")]
    pub results: Vec<BenchResult>,
}

impl BenchmarkRun {
    pub fn new(dataset: String, workers: usize, threads: usize) -> Self {
        Self {
            workers,
            threads,
            dataset,
            branch: get_current_branch(),
            start_time: SystemTime::now(),
            results: vec![],
        }
    }

    pub fn load_previous(dataset: &str) -> Option<Self> {
        let path = PathBuf::from(DATA_PATH).join(dataset).join("previous.json");
        let Ok(prev) = fs::read(path) else {
            return None;
        };

        let Ok(mut prev_output) = serde_json::from_slice::<Self>(&prev) else {
            return None;
        };

        prev_output.results = BenchResult::load_many(&prev_output.dataset, &prev_output.branch);
        Some(prev_output)
    }

    /// Write data as json into output path if it exists.
    pub fn store(&self) -> Result<()> {
        let path = PathBuf::from(DATA_PATH)
            .join(&self.dataset)
            .join("previous.json");
        let json = serde_json::to_string_pretty(&self).unwrap();

        let _ = fs::create_dir_all(path.parent().unwrap());

        fs::write(path, json)?;
        for result in &self.results {
            result.store()?;
        }
        Ok(())
    }

    pub fn compare_with_previous(&self) -> Result<()> {
        let Some(previous) = Self::load_previous(&self.dataset) else {
            return Ok(());
        };

        let header = format!(
            "=== Comparing {} results from branch '{}' [prev] with '{}' [new] ===",
            self.dataset, previous.branch, self.branch
        );
        println!("{header}");
        // Print machine information
        println!("os:        {}", std::env::consts::OS);
        println!("arch:      {}", std::env::consts::ARCH);
        println!("cpu cores: {}", get_available_parallelism());
        println!("threads:   {} -> {}", previous.threads, self.threads);
        println!("workers:   {} -> {}", previous.workers, self.workers);
        println!("{}", "=".repeat(header.len()));
        for query in self.results.iter() {
            let Some(prev_query) = previous.results.iter().find(|v| v.id == query.id) else {
                continue;
            };
            query.compare(prev_query)
        }
        print_comparison_total(&previous.results, &self.results);

        Ok(())
    }
}

fn get_current_branch() -> String {
    let output = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .expect("failed to execute git command");

    let branch_name = String::from_utf8(output.stdout)
        .expect("git output is not valid UTF-8")
        .trim()
        .to_string();

    branch_name.split("/").last().unwrap().to_string()
}

impl BenchResult {
    /// Median (p50) of the successful iteration latencies in milliseconds. The median is robust to
    /// warmup/GC/noise outliers and, unlike a mean or a sum, does not grow with the iteration
    /// count, so runs done with different `-i` values stay comparable.
    pub fn p50(&self) -> u128 {
        let mut xs: Vec<u128> = self
            .iterations
            .iter()
            .filter(|v| v.error.is_none())
            .map(|v| v.elapsed.as_millis())
            .collect();
        if xs.is_empty() {
            return 0;
        }
        xs.sort_unstable();
        let mid = xs.len() / 2;
        if xs.len() % 2 == 1 {
            xs[mid]
        } else {
            (xs[mid - 1] + xs[mid]) / 2
        }
    }

    /// Representative single-run time used to aggregate a suite TOTAL. Returns `None` when any
    /// iteration errored, so the query is dropped from the total. Being a per-query p50 (not a sum
    /// of all iterations), the resulting TOTAL is independent of the iteration count.
    pub fn representative_time(&self) -> Option<u128> {
        if self.iterations.iter().any(|v| v.error.is_some()) {
            return None;
        }
        Some(self.p50())
    }

    pub fn store(&self) -> Result<()> {
        let path = PathBuf::from(DATA_PATH)
            .join(&self.dataset)
            .join(RESULTS_DIR)
            .join(get_current_branch())
            .join(format!("{}.json", self.id));

        let _ = fs::create_dir_all(path.parent().unwrap());

        let result_string =
            serde_json::to_string_pretty(self).map_err(|err| internal_datafusion_err!("{err}"))?;
        fs::write(path, result_string)?;

        Ok(())
    }

    pub fn load_many(dataset: &str, branch: &str) -> Vec<Self> {
        let dir = PathBuf::from(DATA_PATH)
            .join(dataset)
            .join(RESULTS_DIR)
            .join(branch);

        let Ok(dir) = fs::read_dir(dir) else {
            return vec![];
        };

        let mut results = vec![];
        for file in dir {
            let Ok(file) = file else { continue };
            let file_name = file.file_name().to_string_lossy().to_string();
            let id = if file_name.ends_with(".json") {
                file_name.trim_end_matches(".json")
            } else {
                continue;
            };
            let Ok(result) = BenchResult::load(dataset, branch, id) else {
                continue;
            };
            results.push(result);
        }
        results.sort_by(|a, b| {
            let extract_number = |s: &str| -> Option<u32> {
                s.chars()
                    .filter(|c| c.is_ascii_digit())
                    .collect::<String>()
                    .parse::<u32>()
                    .ok()
            };

            match (extract_number(&a.id), extract_number(&b.id)) {
                (Some(num_a), Some(num_b)) => num_a.cmp(&num_b),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.id.cmp(&b.id),
            }
        });
        results
    }

    pub fn load(dataset: &str, branch: &str, id: &str) -> Result<Self> {
        let path = PathBuf::from(DATA_PATH)
            .join(dataset)
            .join(RESULTS_DIR)
            .join(branch)
            .join(format!("{id}.json"));

        let read = fs::read(path)?;
        let read =
            serde_json::from_slice(&read).map_err(|err| internal_datafusion_err!("{err}"))?;
        Ok(read)
    }

    pub fn compare(&self, prev_query: &Self) {
        let prev_err = prev_query.iterations.iter().find_map(|v| v.error.clone());
        let new_err = self.iterations.iter().find_map(|v| v.error.clone());
        match (prev_err, new_err) {
            (Some(_prev_err), None) => {
                println!("{}: Previously failed, but now succeeded 🟠", self.id);
                return;
            }
            (None, Some(_new_err)) => {
                println!("{}: Previously succeeded, but now failed ❌", self.id);
                return;
            }
            (Some(_prev_err), Some(_new_err)) => {
                println!("{}: Previously failed, and now also failed ❌", self.id);
                return;
            }
            (None, None) => {}
        }

        let p50_prev = prev_query.p50();
        let p50 = self.p50();
        let (f, tag, emoji) = if p50 < p50_prev {
            let f = p50_prev as f64 / p50 as f64;
            (f, "faster", if f > 1.2 { "✅" } else { "✔" })
        } else {
            let f = p50 as f64 / p50_prev as f64;
            (f, "slower", if f > 1.2 { "❌" } else { "✖" })
        };
        println!(
            "{:>8}: prev={p50_prev:>4} ms, new={p50:>4} ms, diff={f:.2} {tag} {emoji}",
            self.id
        );
    }
}

/// Prints an iteration-count-independent suite TOTAL: the sum of per-query p50s over the queries
/// present in both runs that errored in neither. Mirrors the per-query comparison format so the
/// two harnesses (local and remote) read the same way.
pub fn print_comparison_total(base: &[BenchResult], new: &[BenchResult]) {
    let mut total_prev: u128 = 0;
    let mut total_new: u128 = 0;
    let mut stats_q_error_p50_prev = vec![];
    let mut stats_q_error_p50_new = vec![];
    let mut stats_q_error_p95_prev = vec![];
    let mut stats_q_error_p95_new = vec![];
    for query in new {
        let Some(prev) = base.iter().find(|v| v.id == query.id) else {
            continue;
        };
        if let (Some(p), Some(n)) = (prev.representative_time(), query.representative_time()) {
            total_prev += p;
            total_new += n;
            stats_q_error_p50_prev.extend(
                prev.iterations
                    .iter()
                    .filter_map(|iteration| iteration.stats_q_error_p50),
            );
            stats_q_error_p50_new.extend(
                query
                    .iterations
                    .iter()
                    .filter_map(|iteration| iteration.stats_q_error_p50),
            );
            stats_q_error_p95_prev.extend(
                prev.iterations
                    .iter()
                    .filter_map(|iteration| iteration.stats_q_error_p95),
            );
            stats_q_error_p95_new.extend(
                query
                    .iterations
                    .iter()
                    .filter_map(|iteration| iteration.stats_q_error_p95),
            );
        }
    }

    if total_prev != 0 || total_new != 0 {
        let (f, tag, emoji) = if total_new < total_prev {
            let f = total_prev as f64 / total_new as f64;
            (f, "faster", if f > 1.2 { "✅" } else { "✔" })
        } else {
            let f = total_new as f64 / total_prev.max(1) as f64;
            (f, "slower", if f > 1.2 { "❌" } else { "✖" })
        };
        println!(
            "{:>8}: prev={total_prev} ms, new={total_new} ms, diff={f:.2} {tag} {emoji}",
            "TOTAL"
        );
    }

    print_q_error_comparison("QERR P50", stats_q_error_p50_prev, stats_q_error_p50_new);
    print_q_error_comparison("QERR P95", stats_q_error_p95_prev, stats_q_error_p95_new);
}

fn print_q_error_comparison(label: &str, prev: Vec<f64>, new: Vec<f64>) {
    match (median(prev), median(new)) {
        (Some(prev), Some(new)) => println!("{label:>8}: prev={prev:.2}x, new={new:.2}x"),
        (Some(prev), None) => println!("{label:>8}: prev={prev:.2}x, new=n/a"),
        (None, Some(new)) => println!("{label:>8}: prev=n/a, new={new:.2}x"),
        (None, None) => {}
    }
}

fn serialize_bench_results<S: Serializer>(
    _bench_result: &[BenchResult],
    ser: S,
) -> Result<S::Ok, S::Error> {
    // We want to avoid serializing these here on purpose.
    ser.serialize_seq(Some(0))?.end()
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
