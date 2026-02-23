use datafusion_distributed::{
    GetTaskMetricsRequest, ObservabilityServiceClient, PingRequest, TaskMetricsSummary,
};
use ratatui::widgets::TableState;
use std::time::{Duration, Instant};
use tonic::transport::Channel;
use url::Url;

/// Number of columns in the task table.
pub const COLUMN_COUNT: usize = 10;

/// Sort direction for table columns.
#[derive(Clone, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// App holds the main application state.
pub struct App {
    pub workers: Vec<WorkerState>,
    pub should_quit: bool,
    pub console_state: ConsoleState,
    pub task_table_state: TableState,
    pub selected_column: usize,
    pub sort_column: Option<usize>,
    pub sort_direction: SortDirection,
}

/// Represents overall state of the console application.
#[derive(Clone, PartialEq)]
pub enum ConsoleState {
    Idle,
    Active,
}

/// Tracks individual worker connection states.
#[derive(Clone)]
pub enum ConnectionStatus {
    Connecting,
    Idle,
    Active,
    Disconnected { reason: String },
}

#[derive(Clone, PartialEq)]
pub enum TaskRowStatus {
    Running,
    Completed,
}

/// A flattened view of a task with its worker context, used for the global task table.
pub struct TaskRow {
    pub worker_url: String,
    pub stage_id: u64,
    pub task_number: u64,
    pub query_id_short: String,
    pub output_rows: u64,
    pub elapsed_compute: u64,
    pub build_mem_used: u64,
    pub peak_mem_used: u64,
    pub spill_count: u64,
    pub status: TaskRowStatus,
}

impl App {
    /// Create a new App with the given worker URLs.
    pub fn new(worker_urls: Vec<Url>) -> Self {
        let workers = worker_urls
            .into_iter()
            .map(|url| WorkerState {
                url,
                client: None,
                connection_status: ConnectionStatus::Connecting,
                tasks: Vec::new(),
                completed_tasks: Vec::new(),
                last_poll: None,
                last_reconnect_attempt: None,
            })
            .collect();

        App {
            workers,
            should_quit: false,
            console_state: ConsoleState::Idle,
            task_table_state: TableState::default(),
            selected_column: 0,
            sort_column: None,
            sort_direction: SortDirection::Ascending,
        }
    }

    /// Handle keyboard events.
    pub fn handle_key_event(&mut self, key: crossterm::event::KeyEvent) {
        use crossterm::event::{KeyCode, KeyEventKind};

        if key.kind != KeyEventKind::Press {
            return;
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
            KeyCode::Down | KeyCode::Char('j') => {
                self.task_table_state.select_next();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.task_table_state.select_previous();
            }
            KeyCode::Left | KeyCode::Char('h') => {
                self.selected_column = self.selected_column.saturating_sub(1);
            }
            KeyCode::Right | KeyCode::Char('l') => {
                if self.selected_column < COLUMN_COUNT - 1 {
                    self.selected_column += 1;
                }
            }
            KeyCode::Char(' ') => {
                if self.sort_column == Some(self.selected_column) {
                    match self.sort_direction {
                        SortDirection::Ascending => {
                            self.sort_direction = SortDirection::Descending;
                        }
                        SortDirection::Descending => {
                            self.sort_column = None;
                            self.sort_direction = SortDirection::Ascending;
                        }
                    }
                } else {
                    self.sort_column = Some(self.selected_column);
                    self.sort_direction = SortDirection::Ascending;
                }
            }
            _ => {}
        }
    }

    /// Poll all workers for task metrics.
    pub async fn tick(&mut self) {
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

        let _ = tokio::time::timeout(Duration::from_millis(100), async {
            futures::future::join_all(poll_workers).await;
        })
        .await;

        self.update_console_state();
    }

    /// Update overall console state based on worker states.
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

        if has_active || has_running_tasks {
            self.console_state = ConsoleState::Active;
        } else {
            self.console_state = ConsoleState::Idle;
        }
    }

    /// Get cluster-wide statistics.
    pub fn cluster_stats(&self) -> ClusterStats {
        let mut stats = ClusterStats {
            active_count: 0,
            idle_count: 0,
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

    /// Build a flattened list of task rows across all workers for the global table.
    /// Running tasks appear first, followed by completed tasks.
    pub fn task_rows(&self) -> Vec<TaskRow> {
        let mut rows = Vec::new();

        for worker in &self.workers {
            for task in &worker.tasks {
                if let Some(row) = task_summary_to_row(task, &worker.url, TaskRowStatus::Running) {
                    rows.push(row);
                }
            }
            for task in &worker.completed_tasks {
                if let Some(row) = task_summary_to_row(task, &worker.url, TaskRowStatus::Completed)
                {
                    rows.push(row);
                }
            }
        }
        self.sort_task_rows(&mut rows);
        rows
    }

    /// Sort task rows by the currently selected sort column and direction.
    fn sort_task_rows(&self, rows: &mut [TaskRow]) {
        let Some(col) = self.sort_column else {
            return;
        };
        rows.sort_by(|a, b| {
            let cmp = match col {
                0 => a.query_id_short.cmp(&b.query_id_short),
                1 => a.stage_id.cmp(&b.stage_id),
                2 => a.task_number.cmp(&b.task_number),
                3 => a.worker_url.cmp(&b.worker_url),
                4 => {
                    let rank = |s: &TaskRowStatus| if *s == TaskRowStatus::Running { 0 } else { 1 };
                    rank(&a.status).cmp(&rank(&b.status))
                }
                5 => a.output_rows.cmp(&b.output_rows),
                6 => a.elapsed_compute.cmp(&b.elapsed_compute),
                7 => a.build_mem_used.cmp(&b.build_mem_used),
                8 => a.peak_mem_used.cmp(&b.peak_mem_used),
                9 => a.spill_count.cmp(&b.spill_count),
                _ => std::cmp::Ordering::Equal,
            };
            match self.sort_direction {
                SortDirection::Ascending => cmp,
                SortDirection::Descending => cmp.reverse(),
            }
        });
    }
}

fn task_summary_to_row(
    task: &TaskMetricsSummary,
    worker_url: &Url,
    status: TaskRowStatus,
) -> Option<TaskRow> {
    let sk = task.stage_key.as_ref()?;
    let query_id_short = if sk.query_id.len() >= 4 {
        hex::encode(&sk.query_id[..4])
    } else {
        hex::encode(&sk.query_id)
    };
    Some(TaskRow {
        worker_url: worker_url.to_string(),
        stage_id: sk.stage_id,
        task_number: sk.task_number,
        query_id_short,
        output_rows: task.output_rows,
        elapsed_compute: task.elapsed_compute,
        build_mem_used: task.build_mem_used,
        peak_mem_used: task.peak_mem_used,
        spill_count: task.spill_count,
        status,
    })
}

/// Cluster-wide statistics for display in the UI.
pub struct ClusterStats {
    pub active_count: usize,
    pub idle_count: usize,
    pub disconnected_count: usize,
    pub total_tasks: usize,
}

/// Tracks state for a single worker.
pub struct WorkerState {
    pub url: Url,
    pub client: Option<ObservabilityServiceClient<Channel>>,
    pub connection_status: ConnectionStatus,
    pub tasks: Vec<TaskMetricsSummary>,
    pub completed_tasks: Vec<TaskMetricsSummary>,
    pub last_poll: Option<Instant>,
    pub last_reconnect_attempt: Option<Instant>,
}

impl WorkerState {
    /// Attempts to establish a gRPC connection to a worker.
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

    /// Returns true if the worker should attempt a connection. This covers the initial
    /// `Connecting` state (first tick) and `Disconnected` state with a 5-second backoff.
    fn should_retry_connection(&self) -> bool {
        match &self.connection_status {
            ConnectionStatus::Connecting => {
                // First tick: no attempt made yet
                self.last_reconnect_attempt.is_none()
            }
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

    /// Queries a worker for task metrics.
    async fn poll(&mut self) {
        if let Some(client) = &mut self.client {
            match client.get_task_metrics(GetTaskMetricsRequest {}).await {
                Ok(response) => {
                    let new_tasks = response.into_inner().task_summaries;
                    self.last_poll = Some(Instant::now());

                    // Tasks that are in the oldtask list but not in the new task list are assumed
                    // to be completed.
                    for old_task in &self.tasks {
                        let still_exists = new_tasks.iter().any(|new_task| {
                            match (&old_task.stage_key, &new_task.stage_key) {
                                (Some(old_sk), Some(new_sk)) => {
                                    old_sk.query_id == new_sk.query_id
                                        && old_sk.stage_id == new_sk.stage_id
                                        && old_sk.task_number == new_sk.task_number
                                }
                                _ => false,
                            }
                        });
                        if !still_exists {
                            self.completed_tasks.push(old_task.clone());
                        }
                    }

                    // If new tasks belong to a different query, clear old completed tasks.
                    if !new_tasks.is_empty() && !self.completed_tasks.is_empty() {
                        let new_query_ids: Vec<_> = new_tasks
                            .iter()
                            .filter_map(|t| t.stage_key.as_ref().map(|sk| &sk.query_id))
                            .collect();
                        let any_match = self.completed_tasks.iter().any(|ct| {
                            ct.stage_key
                                .as_ref()
                                .is_some_and(|sk| new_query_ids.contains(&&sk.query_id))
                        });
                        if !any_match {
                            self.completed_tasks.clear();
                        }
                    }

                    self.tasks = new_tasks;
                    let has_running = !self.tasks.is_empty();

                    match &self.connection_status {
                        ConnectionStatus::Active => {
                            if !has_running {
                                self.connection_status = ConnectionStatus::Idle;
                            }
                        }
                        ConnectionStatus::Idle => {
                            if has_running {
                                self.connection_status = ConnectionStatus::Active;
                            }
                        }
                        _ => {
                            if has_running {
                                self.connection_status = ConnectionStatus::Active;
                            } else {
                                self.connection_status = ConnectionStatus::Idle;
                            }
                        }
                    }
                }
                Err(e) => {
                    self.client = None;
                    self.tasks.clear();
                    self.connection_status = ConnectionStatus::Disconnected {
                        reason: format!("Poll failed: {e}"),
                    };
                }
            }
        }
    }
}
