use crate::state::{ClusterViewState, SortMode, View, WorkerViewState};
use datafusion_distributed::{
    GetTaskProgressRequest, ObservabilityServiceClient, ObservabilityStageKey, PingRequest,
    TaskProgress, TaskStatus,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};
use tonic::transport::Channel;
use url::Url;

/// Maximum number of completed tasks to retain per worker.
const MAX_COMPLETED_TASKS: usize = 50;

/// App holds the main application state.
pub struct App {
    pub workers: Vec<WorkerConn>,
    pub queries: HashMap<Vec<u8>, QuerySummary>,
    pub current_view: View,
    pub cluster_state: ClusterViewState,
    pub worker_state: WorkerViewState,
    pub paused: bool,
    pub show_help: bool,
    pub sort_mode: SortMode,
    pub should_quit: bool,
    pub start_time: Instant,
    pub poll_count: u64,
}

/// Summary of a query aggregated across all workers.
pub struct QuerySummary {
    pub query_id: Vec<u8>,
    pub worker_count: usize,
    pub task_count: usize,
    pub stage_count: usize,
    pub first_seen: Instant,
}

/// Cluster-wide statistics for the header.
pub struct ClusterStats {
    pub total: usize,
    pub active_count: usize,
    pub idle_count: usize,
    pub connecting_count: usize,
    pub disconnected_count: usize,
    pub total_tasks: usize,
    pub total_completed: usize,
    pub active_queries: usize,
}

/// Tracks connection and task state for a single worker.
pub struct WorkerConn {
    pub url: Url,
    client: Option<ObservabilityServiceClient<Channel>>,
    pub connection_status: ConnectionStatus,
    pub tasks: Vec<TaskProgress>,
    pub completed_tasks: VecDeque<CompletedTaskRecord>,
    pub task_first_seen: HashMap<TaskKey, Instant>,
    pub connected_since: Option<Instant>,
    pub poll_count: u64,
    last_reconnect_attempt: Option<Instant>,
    last_seen_query_ids: HashSet<Vec<u8>>,
}

/// Unique key for a task: (query_id, stage_id, task_number).
type TaskKey = (Vec<u8>, u64, u64);

/// Record of a completed task with observed duration.
#[derive(Clone, Debug)]
pub struct CompletedTaskRecord {
    pub stage_key: ObservabilityStageKey,
    pub query_id: Vec<u8>,
    pub stage_id: u64,
    pub task_number: u64,
    pub observed_duration: Duration,
}

/// Connection status for a worker.
#[derive(Clone)]
pub enum ConnectionStatus {
    Connecting,
    Idle,
    Active,
    Disconnected { reason: String },
}

impl App {
    /// Create a new App with the given worker URLs.
    pub fn new(worker_urls: Vec<Url>) -> Self {
        let workers = worker_urls
            .into_iter()
            .map(|url| WorkerConn {
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
            })
            .collect();

        App {
            workers,
            queries: HashMap::new(),
            current_view: View::ClusterOverview,
            cluster_state: ClusterViewState::default(),
            worker_state: WorkerViewState::default(),
            paused: false,
            show_help: false,
            sort_mode: SortMode::Name,
            should_quit: false,
            start_time: Instant::now(),
            poll_count: 0,
        }
    }

    /// Poll all workers for task progress. Called on the gRPC tick interval.
    pub async fn tick(&mut self) {
        if self.paused {
            return;
        }

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
    }

    /// Rebuild the queries HashMap from all workers' task data.
    fn rebuild_queries(&mut self) {
        self.queries.clear();

        for worker in &self.workers {
            for task in &worker.tasks {
                if let Some(sk) = &task.stage_key {
                    let entry =
                        self.queries
                            .entry(sk.query_id.clone())
                            .or_insert_with(|| QuerySummary {
                                query_id: sk.query_id.clone(),
                                worker_count: 0,
                                task_count: 0,
                                stage_count: 0,
                                first_seen: Instant::now(),
                            });
                    entry.task_count += 1;
                }
            }
        }

        // Second pass: compute per-query worker count and stage count
        for summary in self.queries.values_mut() {
            let mut workers_with_query = HashSet::new();
            let mut stages = HashSet::new();

            for worker in &self.workers {
                let has_tasks = worker.tasks.iter().any(|t| {
                    t.stage_key
                        .as_ref()
                        .is_some_and(|sk| sk.query_id == summary.query_id)
                });
                if has_tasks {
                    workers_with_query.insert(&worker.url);
                }

                for task in &worker.tasks {
                    if let Some(sk) = &task.stage_key {
                        if sk.query_id == summary.query_id {
                            stages.insert(sk.stage_id);
                        }
                    }
                }
            }

            summary.worker_count = workers_with_query.len();
            summary.stage_count = stages.len();
        }
    }

    /// Get cluster-wide statistics.
    pub fn cluster_stats(&self) -> ClusterStats {
        let mut stats = ClusterStats {
            total: self.workers.len(),
            active_count: 0,
            idle_count: 0,
            connecting_count: 0,
            disconnected_count: 0,
            total_tasks: 0,
            total_completed: 0,
            active_queries: self.queries.len(),
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
    pub fn sorted_worker_indices(&self) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..self.workers.len()).collect();
        match self.sort_mode {
            SortMode::Name => {
                indices.sort_by(|&a, &b| self.workers[a].url.cmp(&self.workers[b].url));
            }
            SortMode::Tasks => {
                indices.sort_by(|&a, &b| {
                    self.workers[b]
                        .tasks
                        .len()
                        .cmp(&self.workers[a].tasks.len())
                });
            }
            SortMode::Status => {
                indices.sort_by(|&a, &b| {
                    self.workers[a]
                        .status_sort_key()
                        .cmp(&self.workers[b].status_sort_key())
                });
            }
            SortMode::LongestTask => {
                indices.sort_by(|&a, &b| {
                    let dur_a = self.workers[a].longest_task_duration();
                    let dur_b = self.workers[b].longest_task_duration();
                    dur_b.cmp(&dur_a)
                });
            }
        }
        indices
    }

    /// Get average task count across all workers (for hot spot detection).
    pub fn avg_tasks_per_worker(&self) -> f64 {
        if self.workers.is_empty() {
            return 0.0;
        }
        let total: usize = self.workers.iter().map(|w| w.tasks.len()).sum();
        total as f64 / self.workers.len() as f64
    }
}

impl WorkerConn {
    /// Attempts to establish a gRPC connection to a worker.
    async fn try_connect(&mut self) {
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
    fn should_retry_connection(&self) -> bool {
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
    async fn poll(&mut self) {
        let Some(client) = &mut self.client else {
            return;
        };

        match client.get_task_progress(GetTaskProgressRequest {}).await {
            Ok(response) => {
                let new_tasks = response.into_inner().tasks;
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
                                    stage_key: sk.clone(),
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
            }
        }
    }

    /// Status text for display.
    pub fn status_text(&self) -> &'static str {
        match &self.connection_status {
            ConnectionStatus::Connecting => "CONNECTING",
            ConnectionStatus::Idle => "IDLE",
            ConnectionStatus::Active => "ACTIVE",
            ConnectionStatus::Disconnected { .. } => "DISCONNECTED",
        }
    }

    /// Status color for display.
    pub fn status_color(&self) -> ratatui::style::Color {
        use ratatui::style::Color;
        match self.connection_status {
            ConnectionStatus::Connecting => Color::Blue,
            ConnectionStatus::Idle => Color::Yellow,
            ConnectionStatus::Active => Color::Green,
            ConnectionStatus::Disconnected { .. } => Color::Red,
        }
    }

    /// Sort key for status ordering (disconnected first, then active, idle, connecting).
    pub fn status_sort_key(&self) -> u8 {
        match self.connection_status {
            ConnectionStatus::Disconnected { .. } => 0,
            ConnectionStatus::Active => 1,
            ConnectionStatus::Idle => 2,
            ConnectionStatus::Connecting => 3,
        }
    }

    /// Disconnect reason if applicable.
    pub fn disconnect_reason(&self) -> Option<&str> {
        if let ConnectionStatus::Disconnected { reason } = &self.connection_status {
            Some(reason)
        } else {
            None
        }
    }

    /// Duration of the longest-running task on this worker.
    pub fn longest_task_duration(&self) -> Duration {
        self.task_first_seen
            .values()
            .map(|first| first.elapsed())
            .max()
            .unwrap_or_default()
    }

    /// Number of distinct queries this worker has tasks for.
    pub fn distinct_query_count(&self) -> usize {
        let ids: HashSet<_> = self
            .tasks
            .iter()
            .filter_map(|t| t.stage_key.as_ref().map(|sk| &sk.query_id))
            .collect();
        ids.len()
    }

    /// Get task duration for a specific task.
    pub fn task_duration(&self, query_id: &[u8], stage_id: u64, task_number: u64) -> Duration {
        let key = (query_id.to_vec(), stage_id, task_number);
        self.task_first_seen
            .get(&key)
            .map(|first| first.elapsed())
            .unwrap_or_default()
    }
}
