use crate::state::{ClusterViewState, SortColumn, SortDirection, View, WorkerViewState};
use crate::worker::{ConnectionStatus, WorkerConn, discover_cluster_workers};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use url::Url;

/// App holds the main application state.
pub(crate) struct App {
    pub(crate) workers: Vec<WorkerConn>,
    active_query_count: usize,
    pub(crate) current_view: View,
    pub(crate) cluster_state: ClusterViewState,
    pub(crate) worker_state: WorkerViewState,
    pub(crate) paused: bool,
    pub(crate) show_help: bool,
    pub(crate) should_quit: bool,
    pub(crate) start_time: Instant,
    poll_count: u64,
    /// Previous tick's cluster-wide output rows total (for throughput delta).
    prev_output_rows_total: u64,
    prev_output_rows_time: Option<Instant>,
    /// Smoothed cluster-wide throughput in rows/s.
    pub(crate) current_throughput: f64,
    /// Seed URL for worker discovery via `GetClusterWorkers`.
    seed_url: Url,
    /// Last time we ran worker discovery.
    last_discovery: Option<Instant>,
}

/// Cluster-wide statistics for the header.
pub(crate) struct ClusterStats {
    pub(crate) total: usize,
    pub(crate) active_count: usize,
    pub(crate) idle_count: usize,
    pub(crate) connecting_count: usize,
    pub(crate) disconnected_count: usize,
    pub(crate) total_tasks: usize,
    pub(crate) total_completed: usize,
    pub(crate) active_queries: usize,
}

/// Interval between worker discovery polls.
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(5);

impl App {
    /// Create a new App that discovers workers via `GetClusterWorkers` on the seed URL.
    pub(crate) fn new(seed_url: Url) -> Self {
        App {
            workers: Vec::new(),
            active_query_count: 0,
            current_view: View::ClusterOverview,
            cluster_state: ClusterViewState::default(),
            worker_state: WorkerViewState::default(),
            paused: false,
            show_help: false,
            should_quit: false,
            start_time: Instant::now(),
            poll_count: 0,
            prev_output_rows_total: 0,
            prev_output_rows_time: None,
            current_throughput: 0.0,
            seed_url,
            last_discovery: None,
        }
    }

    /// Poll all workers for task progress. Called on the gRPC tick interval.
    pub(crate) async fn tick(&mut self) {
        if self.paused {
            return;
        }

        // Run worker discovery if in auto mode
        self.maybe_discover_workers().await;

        // Attempt connection for workers in Connecting or Disconnected state
        for worker in &mut self.workers {
            if worker.should_retry_connection() {
                worker.try_connect().await;
            }
        }

        // Poll all connected workers in parallel with timeout
        let poll_workers: Vec<_> = self
            .workers
            .iter_mut()
            .map(|worker| async {
                worker.poll().await;
            })
            .collect();

        let _ = tokio::time::timeout(Duration::from_millis(50), async {
            futures::future::join_all(poll_workers).await;
        })
        .await;

        self.poll_count += 1;
        self.rebuild_queries();
        self.update_throughput();
    }

    /// Periodically discovers workers via `GetClusterWorkers` on the seed URL.
    async fn maybe_discover_workers(&mut self) {
        let should_discover = match self.last_discovery {
            None => true,
            Some(last) => last.elapsed() >= DISCOVERY_INTERVAL,
        };

        if !should_discover {
            return;
        }

        self.last_discovery = Some(Instant::now());

        let discovered_urls = match discover_cluster_workers(&self.seed_url).await {
            Ok(urls) => urls,
            Err(_) => return,
        };

        // Build set of currently known URLs (owned to avoid borrow conflict)
        let known_urls: HashSet<Url> = self.workers.iter().map(|w| w.url.clone()).collect();

        // Add new workers
        for url in &discovered_urls {
            if !known_urls.contains(url) {
                self.workers.push(WorkerConn::new(url.clone()));
            }
        }

        // Remove workers that are no longer in the discovered set
        let discovered_set: HashSet<&Url> = discovered_urls.iter().collect();
        self.workers.retain(|w| discovered_set.contains(&w.url));
    }

    /// Update cluster-wide throughput from output rows delta.
    fn update_throughput(&mut self) {
        let current_total: u64 = self.workers.iter().map(|w| w.output_rows_total).sum();
        let now = Instant::now();

        if let Some(prev_time) = self.prev_output_rows_time {
            let elapsed = prev_time.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                let delta = current_total.saturating_sub(self.prev_output_rows_total);
                let instantaneous = delta as f64 / elapsed;
                // Exponential smoothing
                self.current_throughput = 0.7 * instantaneous + 0.3 * self.current_throughput;
            }
        }

        self.prev_output_rows_total = current_total;
        self.prev_output_rows_time = Some(now);
    }

    /// Count distinct active queries across all workers.
    fn rebuild_queries(&mut self) {
        let mut query_ids = HashSet::new();
        for worker in &self.workers {
            for task in &worker.tasks {
                if let Some(sk) = &task.stage_key {
                    query_ids.insert(&sk.query_id);
                }
            }
        }
        self.active_query_count = query_ids.len();
    }

    /// Get cluster-wide statistics.
    pub(crate) fn cluster_stats(&self) -> ClusterStats {
        let mut stats = ClusterStats {
            total: self.workers.len(),
            active_count: 0,
            idle_count: 0,
            connecting_count: 0,
            disconnected_count: 0,
            total_tasks: 0,
            total_completed: 0,
            active_queries: self.active_query_count,
        };

        for worker in &self.workers {
            match worker.connection_status {
                ConnectionStatus::Active => stats.active_count += 1,
                ConnectionStatus::Idle => stats.idle_count += 1,
                ConnectionStatus::Connecting => stats.connecting_count += 1,
                ConnectionStatus::Disconnected { .. } => stats.disconnected_count += 1,
            }
            stats.total_tasks += worker.tasks.len();
            stats.total_completed += worker.completed_tasks.len();
        }

        stats
    }

    /// Get the sorted worker indices for the cluster view.
    pub(crate) fn sorted_worker_indices(&self) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..self.workers.len()).collect();
        let direction = self.cluster_state.sort_direction;

        if direction == SortDirection::Unsorted {
            return indices;
        }

        let ascending = direction == SortDirection::Ascending;

        match self.cluster_state.selected_column {
            SortColumn::Worker => {
                indices.sort_by(|&a, &b| {
                    let cmp = self.workers[a].url.cmp(&self.workers[b].url);
                    if ascending { cmp } else { cmp.reverse() }
                });
            }
            SortColumn::Status => {
                indices.sort_by(|&a, &b| {
                    let cmp = self.workers[a]
                        .status_sort_key()
                        .cmp(&self.workers[b].status_sort_key());
                    if ascending { cmp } else { cmp.reverse() }
                });
            }
            SortColumn::Tasks => {
                indices.sort_by(|&a, &b| {
                    let cmp = self.workers[a]
                        .tasks
                        .len()
                        .cmp(&self.workers[b].tasks.len());
                    if ascending { cmp } else { cmp.reverse() }
                });
            }
            SortColumn::Queries => {
                indices.sort_by(|&a, &b| {
                    let cmp = self.workers[a]
                        .distinct_query_count()
                        .cmp(&self.workers[b].distinct_query_count());
                    if ascending { cmp } else { cmp.reverse() }
                });
            }
            SortColumn::Cpu => {
                indices.sort_by(|&a, &b| {
                    let cmp = self.workers[a]
                        .cpu_usage_percent
                        .partial_cmp(&self.workers[b].cpu_usage_percent)
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if ascending { cmp } else { cmp.reverse() }
                });
            }
            SortColumn::Memory => {
                indices.sort_by(|&a, &b| {
                    let cmp = self.workers[a].rss_bytes.cmp(&self.workers[b].rss_bytes);
                    if ascending { cmp } else { cmp.reverse() }
                });
            }
        }
        indices
    }

    /// Average observed_duration across all workers' completed tasks.
    pub(crate) fn cluster_avg_task_duration(&self) -> Option<Duration> {
        let mut total_secs = 0.0f64;
        let mut count = 0usize;
        for worker in &self.workers {
            for ct in &worker.completed_tasks {
                total_secs += ct.observed_duration.as_secs_f64();
                count += 1;
            }
        }
        if count > 0 {
            Some(Duration::from_secs_f64(total_secs / count as f64))
        } else {
            None
        }
    }

    /// Longest currently-running task across all workers.
    pub(crate) fn cluster_longest_active_task(&self) -> Duration {
        self.workers
            .iter()
            .map(|w| w.longest_task_duration())
            .max()
            .unwrap_or_default()
    }

    /// Get average task count across all workers (for hot spot detection).
    pub(crate) fn avg_tasks_per_worker(&self) -> f64 {
        if self.workers.is_empty() {
            return 0.0;
        }
        let total: usize = self.workers.iter().map(|w| w.tasks.len()).sum();
        total as f64 / self.workers.len() as f64
    }
}
