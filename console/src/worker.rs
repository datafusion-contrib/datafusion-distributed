use datafusion_distributed::{
    GetClusterWorkersRequest, GetTaskProgressRequest, ObservabilityServiceClient, PingRequest,
    TaskProgress, TaskStatus,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};
use tonic::transport::Channel;
use url::Url;

/// Maximum number of completed tasks to retain per worker.
const MAX_COMPLETED_TASKS: usize = 50;

/// Number of metric samples to keep per worker (300 * 100ms = 30s of history).
pub(crate) const METRIC_HISTORY_LEN: usize = 300;

/// Tracks connection and task state for a single worker.
pub(crate) struct WorkerConn {
    pub(crate) url: Url,
    client: Option<ObservabilityServiceClient<Channel>>,
    pub(crate) connection_status: ConnectionStatus,
    pub(crate) tasks: Vec<TaskProgress>,
    pub(crate) completed_tasks: VecDeque<CompletedTaskRecord>,
    task_first_seen: HashMap<TaskKey, Instant>,
    pub(crate) connected_since: Option<Instant>,
    pub(crate) poll_count: u64,
    last_reconnect_attempt: Option<Instant>,
    last_seen_query_ids: HashSet<Vec<u8>>,
    /// Worker RSS memory in bytes (from WorkerMetrics).
    pub(crate) rss_bytes: u64,
    /// Worker CPU usage percentage (from WorkerMetrics).
    pub(crate) cpu_usage_percent: f64,
    /// Sum of output_rows across all running tasks on this worker.
    pub(crate) output_rows_total: u64,
    /// Time-series history for sparkline graphs.
    pub(crate) cpu_history: VecDeque<u64>,
    pub(crate) rss_history: VecDeque<u64>,
    pub(crate) rows_history: VecDeque<u64>,
    /// Previous output_rows_total for computing per-poll delta.
    prev_output_rows: u64,
}

/// Unique key for a task: (query_id, stage_id, task_number).
type TaskKey = (Vec<u8>, u64, u64);

/// Record of a completed task with observed duration.
#[derive(Clone, Debug)]
pub(crate) struct CompletedTaskRecord {
    pub(crate) query_id: Vec<u8>,
    pub(crate) stage_id: u64,
    pub(crate) task_number: u64,
    pub(crate) observed_duration: Duration,
}

/// Connection status for a worker.
#[derive(Clone)]
pub(crate) enum ConnectionStatus {
    Connecting,
    Idle,
    Active,
    Disconnected { reason: String },
}

impl WorkerConn {
    /// Create a new WorkerConn in the initial Connecting state.
    pub(crate) fn new(url: Url) -> Self {
        Self {
            url,
            client: None,
            connection_status: ConnectionStatus::Connecting,
            tasks: Vec::new(),
            completed_tasks: VecDeque::new(),
            task_first_seen: HashMap::new(),
            connected_since: None,
            poll_count: 0,
            last_reconnect_attempt: None,
            last_seen_query_ids: HashSet::new(),
            rss_bytes: 0,
            cpu_usage_percent: 0.0,
            output_rows_total: 0,
            cpu_history: VecDeque::with_capacity(METRIC_HISTORY_LEN),
            rss_history: VecDeque::with_capacity(METRIC_HISTORY_LEN),
            rows_history: VecDeque::with_capacity(METRIC_HISTORY_LEN),
            prev_output_rows: 0,
        }
    }

    /// Attempts to establish a gRPC connection to a worker.
    pub(crate) async fn try_connect(&mut self) {
        self.last_reconnect_attempt = Some(Instant::now());

        match ObservabilityServiceClient::connect(self.url.to_string()).await {
            Ok(mut client) => match client.ping(PingRequest {}).await {
                Ok(_) => {
                    self.client = Some(client);
                    self.connection_status = ConnectionStatus::Idle;
                    self.connected_since = Some(Instant::now());
                    self.tasks.clear();
                    self.task_first_seen.clear();
                }
                Err(e) => {
                    self.client = None;
                    self.connected_since = None;
                    self.connection_status = ConnectionStatus::Disconnected {
                        reason: format!("Ping failed: {e}"),
                    };
                }
            },
            Err(e) => {
                self.client = None;
                self.connected_since = None;
                self.connection_status = ConnectionStatus::Disconnected {
                    reason: format!("Connection failed: {e}"),
                };
            }
        }
    }

    /// Returns true if the worker should attempt a (re)connection.
    pub(crate) fn should_retry_connection(&self) -> bool {
        match &self.connection_status {
            ConnectionStatus::Connecting => self.last_reconnect_attempt.is_none(),
            ConnectionStatus::Disconnected { .. } => {
                if let Some(last_attempt) = self.last_reconnect_attempt {
                    last_attempt.elapsed() >= Duration::from_secs(5)
                } else {
                    true
                }
            }
            _ => false,
        }
    }

    /// Queries a worker for task progress.
    pub(crate) async fn poll(&mut self) {
        let Some(client) = &mut self.client else {
            return;
        };

        match client.get_task_progress(GetTaskProgressRequest {}).await {
            Ok(response) => {
                let response = response.into_inner();
                let new_tasks = response.tasks;

                // Store worker-level metrics
                if let Some(wm) = &response.worker_metrics {
                    self.rss_bytes = wm.rss_bytes;
                    self.cpu_usage_percent = wm.cpu_usage_percent;
                }

                // Compute output rows total across running tasks
                self.output_rows_total = new_tasks.iter().map(|t| t.output_rows).sum();

                // Record metric history samples for sparkline graphs
                // Scale CPU% (0.0–100.0) by 100 → 0–10000 range for sparkline precision.
                push_history(
                    &mut self.cpu_history,
                    (self.cpu_usage_percent * 100.0) as u64,
                );
                push_history(&mut self.rss_history, self.rss_bytes);
                let rows_delta = self.output_rows_total.saturating_sub(self.prev_output_rows);
                push_history(&mut self.rows_history, rows_delta);
                self.prev_output_rows = self.output_rows_total;

                self.poll_count += 1;

                // Build set of new task keys for quick lookup
                let new_task_keys: HashSet<TaskKey> = new_tasks
                    .iter()
                    .filter_map(|t| {
                        t.stage_key
                            .as_ref()
                            .map(|sk| (sk.query_id.clone(), sk.stage_id, sk.task_number))
                    })
                    .collect();

                // Detect completed tasks: tasks that were running but disappeared
                for old_task in &self.tasks {
                    if old_task.status == TaskStatus::Running as i32 {
                        if let Some(sk) = &old_task.stage_key {
                            let key = (sk.query_id.clone(), sk.stage_id, sk.task_number);
                            if !new_task_keys.contains(&key) {
                                // Task disappeared — assume completed
                                let observed_duration = self
                                    .task_first_seen
                                    .get(&key)
                                    .map(|first| first.elapsed())
                                    .unwrap_or_default();

                                self.completed_tasks.push_front(CompletedTaskRecord {
                                    query_id: sk.query_id.clone(),
                                    stage_id: sk.stage_id,
                                    task_number: sk.task_number,
                                    observed_duration,
                                });

                                // Maintain bounded size
                                while self.completed_tasks.len() > MAX_COMPLETED_TASKS {
                                    self.completed_tasks.pop_back();
                                }

                                // Remove from first_seen tracking
                                self.task_first_seen.remove(&key);
                            }
                        }
                    }
                }

                // Track first_seen for new tasks
                let now = Instant::now();
                for task in &new_tasks {
                    if let Some(sk) = &task.stage_key {
                        let key = (sk.query_id.clone(), sk.stage_id, sk.task_number);
                        self.task_first_seen.entry(key).or_insert(now);
                    }
                }

                // Clean up first_seen for tasks no longer present
                self.task_first_seen
                    .retain(|k, _| new_task_keys.contains(k));

                // Update current tasks
                self.tasks = new_tasks;

                // Collect current query IDs
                let mut current_query_ids = HashSet::new();
                let mut has_running = false;

                for task in &self.tasks {
                    if let Some(sk) = &task.stage_key {
                        current_query_ids.insert(sk.query_id.clone());
                        if task.status == TaskStatus::Running as i32 {
                            has_running = true;
                        }
                    }
                }

                // If a new query starts, clear old completed tasks from previous queries
                if has_running && !self.completed_tasks.is_empty() {
                    let completed_query_ids: HashSet<_> = self
                        .completed_tasks
                        .iter()
                        .map(|t| t.query_id.clone())
                        .collect();

                    if !current_query_ids
                        .iter()
                        .any(|id| completed_query_ids.contains(id))
                    {
                        self.completed_tasks.clear();
                    }
                }

                // Update connection status
                if has_running {
                    self.connection_status = ConnectionStatus::Active;
                } else {
                    match &self.connection_status {
                        ConnectionStatus::Active | ConnectionStatus::Connecting => {
                            self.connection_status = ConnectionStatus::Idle;
                        }
                        ConnectionStatus::Idle => {}
                        ConnectionStatus::Disconnected { .. } => {
                            self.connection_status = ConnectionStatus::Idle;
                        }
                    }
                }

                self.last_seen_query_ids = current_query_ids;
            }
            Err(e) => {
                self.client = None;
                self.connected_since = None;
                self.tasks.clear();
                self.task_first_seen.clear();
                self.connection_status = ConnectionStatus::Disconnected {
                    reason: format!("Poll failed: {e}"),
                };
                self.last_seen_query_ids.clear();
                // Push zeros so sparkline shows the gap
                push_history(&mut self.cpu_history, 0);
                push_history(&mut self.rss_history, 0);
                push_history(&mut self.rows_history, 0);
            }
        }
    }

    /// Status text for display.
    pub(crate) fn status_text(&self) -> &'static str {
        match &self.connection_status {
            ConnectionStatus::Connecting => "CONNECTING",
            ConnectionStatus::Idle => "IDLE",
            ConnectionStatus::Active => "ACTIVE",
            ConnectionStatus::Disconnected { .. } => "DISCONNECTED",
        }
    }

    /// Status color for display.
    pub(crate) fn status_color(&self) -> ratatui::style::Color {
        use ratatui::style::Color;
        match self.connection_status {
            ConnectionStatus::Connecting => Color::Blue,
            ConnectionStatus::Idle => Color::Yellow,
            ConnectionStatus::Active => Color::Green,
            ConnectionStatus::Disconnected { .. } => Color::Red,
        }
    }

    /// Sort key for status ordering (disconnected first, then active, idle, connecting).
    pub(crate) fn status_sort_key(&self) -> u8 {
        match self.connection_status {
            ConnectionStatus::Disconnected { .. } => 0,
            ConnectionStatus::Active => 1,
            ConnectionStatus::Idle => 2,
            ConnectionStatus::Connecting => 3,
        }
    }

    /// Disconnect reason if applicable.
    pub(crate) fn disconnect_reason(&self) -> Option<&str> {
        if let ConnectionStatus::Disconnected { reason } = &self.connection_status {
            Some(reason)
        } else {
            None
        }
    }

    /// Duration of the longest-running task on this worker.
    pub(crate) fn longest_task_duration(&self) -> Duration {
        self.task_first_seen
            .values()
            .map(|first| first.elapsed())
            .max()
            .unwrap_or_default()
    }

    /// Number of distinct queries this worker has tasks for.
    pub(crate) fn distinct_query_count(&self) -> usize {
        let ids: HashSet<_> = self
            .tasks
            .iter()
            .filter_map(|t| t.stage_key.as_ref().map(|sk| &sk.query_id))
            .collect();
        ids.len()
    }

    /// Get task duration for a specific task.
    pub(crate) fn task_duration(
        &self,
        query_id: &[u8],
        stage_id: u64,
        task_number: u64,
    ) -> Duration {
        let key = (query_id.to_vec(), stage_id, task_number);
        self.task_first_seen
            .get(&key)
            .map(|first| first.elapsed())
            .unwrap_or_default()
    }
}

/// Push a value into a ring buffer, evicting the oldest if at capacity.
fn push_history(buf: &mut VecDeque<u64>, value: u64) {
    if buf.len() >= METRIC_HISTORY_LEN {
        buf.pop_front();
    }
    buf.push_back(value);
}

/// Connects to a seed worker and calls `GetClusterWorkers` to discover all worker URLs.
pub(crate) async fn discover_cluster_workers(seed_url: &Url) -> Result<Vec<Url>, String> {
    let mut client = ObservabilityServiceClient::connect(seed_url.to_string())
        .await
        .map_err(|e| format!("Failed to connect to seed worker {seed_url}: {e}"))?;

    client
        .ping(PingRequest {})
        .await
        .map_err(|e| format!("Seed worker {seed_url} ping failed: {e}"))?;

    let response = client
        .get_cluster_workers(GetClusterWorkersRequest {})
        .await
        .map_err(|e| format!("GetClusterWorkers failed on {seed_url}: {e}"))?;

    let urls = response
        .into_inner()
        .worker_urls
        .into_iter()
        .filter_map(|s| Url::parse(&s).ok())
        .collect();

    Ok(urls)
}
