use datafusion_distributed::{
    GetTaskProgressRequest, ObservabilityServiceClient, ObservabilityStageKey, PingRequest,
};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tonic::transport::Channel;
use url::Url;

pub struct App {
    pub workers: Vec<WorkerState>,
    pub should_quit: bool,
    pub console_state: ConsoleState,
    worker_registration_rx: mpsc::UnboundedReceiver<Vec<Url>>,
}

#[derive(Clone, PartialEq)]
pub enum ConsoleState {
    Idle,
    Active,
    Completed,
}

#[derive(Clone)]
pub enum ConnectionStatus {
    Connecting,
    Idle,
    Active,
    Disconnected { reason: String },
}

impl App {
    /// Create a new App in IDLE state (no workers initially)
    pub fn new() -> (Self, mpsc::UnboundedSender<Vec<Url>>) {
        let (tx, rx) = mpsc::unbounded_channel();

        let app = App {
            workers: Vec::new(),
            should_quit: false,
            console_state: ConsoleState::Idle,
            worker_registration_rx: rx,
        };

        (app, tx)
    }

    async fn register_workers(&mut self, worker_urls: Vec<Url>) {
        // Clear existing workers if switching to new query
        if !self.workers.is_empty() {
            self.workers.clear();
        }

        for url in worker_urls {
            let port = url.port().unwrap_or(80);
            let mut worker = WorkerState {
                url,
                port,
                client: None,
                connection_status: ConnectionStatus::Connecting,
                tasks: Vec::new(),
                completed_tasks: Vec::new(),
                last_poll: None,
                last_reconnect_attempt: None,
                last_seen_query_ids: HashSet::new(),
            };

            worker.try_connect().await;
            self.workers.push(worker);
        }

        // Transition to Active if workers are registered
        if !self.workers.is_empty() {
            self.console_state = ConsoleState::Active;
        }
    }

    /// Handle keyboard events
    pub fn handle_key_event(&mut self, key: crossterm::event::KeyEvent) {
        use crossterm::event::{KeyCode, KeyEventKind};

        if key.kind == KeyEventKind::Press {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
                KeyCode::Char('r') => {
                    // Clear completed tasks but keep connections
                    for worker in &mut self.workers {
                        worker.completed_tasks.clear();
                    }
                    self.console_state = ConsoleState::Idle;
                }
                _ => {}
            }
        }
    }

    /// Poll all workers for task progress
    pub async fn tick(&mut self) {
        // Check for new worker registrations
        if let Ok(worker_urls) = self.worker_registration_rx.try_recv() {
            self.register_workers(worker_urls).await;
        }

        // Attempt reconnection for disconnected workers
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

        self.update_console_state();
    }

    /// Update overall console state based on worker states
    fn update_console_state(&mut self) {
        if self.workers.is_empty() {
            self.console_state = ConsoleState::Idle;
            return;
        }

        let has_active = self
            .workers
            .iter()
            .any(|w| matches!(w.connection_status, ConnectionStatus::Active));

        let has_running_tasks = self.workers.iter().any(|w| !w.tasks.is_empty());
        let has_completed_tasks = self.workers.iter().any(|w| w.has_completed_tasks());

        if has_active || has_running_tasks {
            self.console_state = ConsoleState::Active;
        } else if has_completed_tasks {
            // All tasks completed, no running tasks
            self.console_state = ConsoleState::Completed;
        } else {
            self.console_state = ConsoleState::Idle;
        }
    }

    /// Get cluster-wide statistics
    pub fn cluster_stats(&self) -> ClusterStats {
        let mut stats = ClusterStats {
            active_count: 0,
            idle_count: 0,
            completed_count: 0,
            disconnected_count: 0,
            total_tasks: 0,
        };

        for worker in &self.workers {
            match worker.connection_status {
                ConnectionStatus::Active => stats.active_count += 1,
                ConnectionStatus::Idle => stats.idle_count += 1,
                ConnectionStatus::Disconnected { .. } => stats.disconnected_count += 1,
                ConnectionStatus::Connecting => {}
            }
            stats.total_tasks += worker.tasks.len();
        }

        stats
    }
}

/// Cluster-wide statistics
pub struct ClusterStats {
    pub active_count: usize,
    pub idle_count: usize,
    pub completed_count: usize,
    pub disconnected_count: usize,
    pub total_tasks: usize,
}

pub struct WorkerState {
    pub url: Url,
    pub port: u16,
    pub client: Option<ObservabilityServiceClient<Channel>>,
    pub connection_status: ConnectionStatus,
    pub tasks: Vec<datafusion_distributed::TaskProgress>,
    pub completed_tasks: Vec<CompletedTask>,
    pub last_poll: Option<Instant>,
    pub last_reconnect_attempt: Option<Instant>,
    pub last_seen_query_ids: HashSet<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct CompletedTask {
    pub stage_key: ObservabilityStageKey,
    pub total_partitions: u64,
    pub query_id: Vec<u8>,
}

impl WorkerState {
    /// Try to establish connection to worker
    async fn try_connect(&mut self) {
        self.last_reconnect_attempt = Some(Instant::now());

        match ObservabilityServiceClient::connect(self.url.to_string()).await {
            Ok(mut client) => match client.ping(PingRequest {}).await {
                Ok(_) => {
                    self.client = Some(client);
                    self.connection_status = ConnectionStatus::Idle;
                    self.tasks.clear();
                }
                Err(e) => {
                    self.client = None;
                    self.connection_status = ConnectionStatus::Disconnected {
                        reason: format!("Ping failed: {e}"),
                    };
                }
            },
            Err(e) => {
                self.client = None;
                self.connection_status = ConnectionStatus::Disconnected {
                    reason: format!("Connection failed: {e}"),
                };
            }
        }
    }

    /// Check if we should retry connection (every 5 seconds)
    fn should_retry_connection(&self) -> bool {
        if matches!(
            self.connection_status,
            ConnectionStatus::Disconnected { .. }
        ) {
            if let Some(last_attempt) = self.last_reconnect_attempt {
                last_attempt.elapsed() >= Duration::from_secs(5)
            } else {
                true
            }
        } else {
            false
        }
    }

    /// Poll worker for task progress
    async fn poll(&mut self) {
        use datafusion_distributed::TaskStatus;

        if let Some(client) = &mut self.client {
            match client.get_task_progress(GetTaskProgressRequest {}).await {
                Ok(response) => {
                    let new_tasks = response.into_inner().tasks;
                    self.last_poll = Some(Instant::now());

                    // Detect completed tasks: tasks that were running but now disappeared
                    for old_task in &self.tasks {
                        if old_task.status == TaskStatus::Running as i32 {
                            let still_exists = new_tasks.iter().any(|t| {
                                if let (Some(old_key), Some(new_key)) = (&old_task.stage_key, &t.stage_key) {
                                    old_key.query_id == new_key.query_id
                                        && old_key.stage_id == new_key.stage_id
                                        && old_key.task_number == new_key.task_number
                                } else {
                                    false
                                }
                            });

                            if !still_exists {
                                // Task disappeared - assume it completed
                                if let Some(stage_key) = &old_task.stage_key {
                                    self.completed_tasks.push(CompletedTask {
                                        stage_key: stage_key.clone(),
                                        total_partitions: old_task.total_partitions,
                                        query_id: stage_key.query_id.clone(),
                                    });
                                }
                            }
                        }
                    }

                    // Update current tasks
                    self.tasks = new_tasks;

                    // Collect query IDs from current tasks
                    let mut current_query_ids = HashSet::new();
                    let mut has_running = false;

                    for task in &self.tasks {
                        if let Some(stage_key) = &task.stage_key {
                            current_query_ids.insert(stage_key.query_id.clone());

                            if task.status == TaskStatus::Running as i32 {
                                has_running = true;
                            }
                        }
                    }

                    // If new work starts, clear old completed tasks
                    if has_running && !self.completed_tasks.is_empty() {
                        // Check if this is a new query (different from completed tasks)
                        let completed_query_ids: HashSet<_> = self.completed_tasks
                            .iter()
                            .map(|t| t.query_id.clone())
                            .collect();

                        if !current_query_ids.iter().any(|id| completed_query_ids.contains(id)) {
                            // New query started, clear old completed tasks
                            self.completed_tasks.clear();
                        }
                    }

                    // Update connection status based on task activity
                    match &self.connection_status {
                        ConnectionStatus::Active => {
                            if !has_running {
                                // All tasks disappeared, go to Idle
                                self.connection_status = ConnectionStatus::Idle;
                            }
                            // Otherwise stay Active
                        }
                        ConnectionStatus::Idle => {
                            if has_running {
                                // New tasks started
                                self.connection_status = ConnectionStatus::Active;
                            }
                        }
                        _ => {
                            // For Connecting or Disconnected states
                            if has_running {
                                self.connection_status = ConnectionStatus::Active;
                            } else {
                                self.connection_status = ConnectionStatus::Idle;
                            }
                        }
                    }

                    // Update tracked query IDs
                    self.last_seen_query_ids = current_query_ids;
                }
                Err(e) => {
                    // Connection lost
                    self.client = None;
                    self.tasks.clear();
                    self.connection_status = ConnectionStatus::Disconnected {
                        reason: format!("Poll failed: {}", e),
                    };
                    self.last_seen_query_ids.clear();
                }
            }
        }
    }

    /// Get status text for display
    pub fn status_text(&self) -> String {
        match &self.connection_status {
            ConnectionStatus::Connecting => "CONNECTING".to_string(),
            ConnectionStatus::Idle => "IDLE".to_string(),
            ConnectionStatus::Active => "ACTIVE".to_string(),
            ConnectionStatus::Disconnected { .. } => "DISCONNECTED".to_string(),
        }
    }

    /// Get status color for display
    pub fn status_color(&self) -> ratatui::style::Color {
        use ratatui::style::Color;
        match self.connection_status {
            ConnectionStatus::Connecting => Color::Blue,
            ConnectionStatus::Idle => Color::Yellow,
            ConnectionStatus::Active => Color::Green,
            ConnectionStatus::Disconnected { .. } => Color::Red,
        }
    }

    /// Get disconnection reason if applicable
    pub fn disconnect_reason(&self) -> Option<&str> {
        if let ConnectionStatus::Disconnected { reason } = &self.connection_status {
            Some(reason)
        } else {
            None
        }
    }

    /// Get aggregated progress across all tasks on this worker
    pub fn aggregate_progress(&self) -> (u64, u64) {
        let running_completed: u64 = self.tasks.iter().map(|t| t.completed_partitions).sum();
        let running_total: u64 = self.tasks.iter().map(|t| t.total_partitions).sum();

        // Add completed task partitions (all completed, so total = completed)
        let completed_total: u64 = self.completed_tasks.iter().map(|t| t.total_partitions).sum();

        (running_completed + completed_total, running_total + completed_total)
    }

    /// Check if worker has any completed tasks
    pub fn has_completed_tasks(&self) -> bool {
        !self.completed_tasks.is_empty()
    }

    /// Check if all tasks are completed (no running tasks, but has completed tasks)
    pub fn all_tasks_completed(&self) -> bool {
        self.tasks.is_empty() && !self.completed_tasks.is_empty()
    }
}
