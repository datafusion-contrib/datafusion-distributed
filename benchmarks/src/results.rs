use crate::{DATA_PATH, RESULTS_DIR, built_info};
use chrono::{DateTime, Utc};
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{Result, internal_datafusion_err};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

/// A single iteration of a benchmark query
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryIter {
    pub row_count: usize,
    pub n_tasks: usize,
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

    pub fn load_previous(&self) -> Option<Self> {
        let path = PathBuf::from(DATA_PATH)
            .join(&self.dataset)
            .join("previous.json");
        let Ok(prev) = fs::read(path) else {
            return None;
        };

        let Ok(mut prev_output) = serde_json::from_slice::<Self>(&prev) else {
            return None;
        };

        prev_output.load_results();
        Some(prev_output)
    }

    fn load_results(&mut self) {
        let dir = PathBuf::from(DATA_PATH)
            .join(&self.dataset)
            .join(RESULTS_DIR)
            .join(&self.branch);

        let Ok(dir) = fs::read_dir(dir) else { return };

        for file in dir {
            let Ok(file) = file else { continue };
            let file_name = file.file_name().to_string_lossy().to_string();
            let id = if file_name.ends_with(".json") {
                file_name.trim_end_matches(".json")
            } else {
                continue;
            };
            let Ok(result) = BenchResult::load(&self.dataset, &self.branch, id) else {
                continue;
            };
            self.results.push(result);
        }
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
        let Some(previous) = self.load_previous() else {
            return Ok(());
        };

        let mut header_printed = false;
        for query in self.results.iter() {
            let Some(prev_query) = previous.results.iter().find(|v| v.id == query.id) else {
                continue;
            };

            if !header_printed {
                header_printed = true;
                let datetime: DateTime<Utc> = self.start_time.into();
                let header = format!(
                    "==== Comparison with the previous benchmark from {} ====",
                    datetime.format("%Y-%m-%d %H:%M:%S UTC")
                );
                println!("{header}");
                // Print machine information
                println!("os:        {}", std::env::consts::OS);
                println!("arch:      {}", std::env::consts::ARCH);
                println!("cpu cores: {}", get_available_parallelism());
                println!("threads:   {} -> {}", previous.threads, self.threads);
                println!("workers:   {} -> {}", previous.workers, self.workers);
                println!("{}", "=".repeat(header.len()))
            }

            let prev_err = prev_query.iterations.iter().find_map(|v| v.error.clone());
            let new_err = query.iterations.iter().find_map(|v| v.error.clone());
            match (prev_err, new_err) {
                (Some(_prev_err), None) => {
                    println!("{}: Previously failed, but now succeeded 🟠", query.id);
                    continue;
                }
                (None, Some(_new_err)) => {
                    println!("{}: Previously succeeded, but now failed ❌", query.id);
                    continue;
                }
                (Some(_prev_err), Some(_new_err)) => {
                    println!("{}: Previously failed, and now also failed ❌", query.id);
                    continue;
                }
                (None, None) => {}
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
            println!(
                "{:>8}: prev={avg_prev:>4} ms, new={avg:>4} ms, diff={f:.2} {tag} {emoji}",
                query.id
            );
        }

        Ok(())
    }
}

fn get_current_branch() -> String {
    let head_ref = built_info::GIT_HEAD_REF.expect("GIT_HEAD_REF not set");
    head_ref.split("/").last().unwrap().to_string()
}

impl BenchResult {
    pub fn avg(&self) -> u128 {
        self.iterations
            .iter()
            .map(|v| v.elapsed.as_millis())
            .sum::<u128>()
            / self.iterations.len() as u128
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
