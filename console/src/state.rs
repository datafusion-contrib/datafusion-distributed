use ratatui::widgets::TableState;

/// Which view is currently active.
#[derive(Clone, PartialEq)]
pub enum View {
    ClusterOverview,
    WorkerDetail,
}

/// Sort mode for the worker table in cluster overview.
#[derive(Clone, Copy, PartialEq)]
pub enum SortMode {
    Name,
    Tasks,
    Status,
    LongestTask,
}

impl SortMode {
    pub fn next(self) -> Self {
        match self {
            SortMode::Name => SortMode::Tasks,
            SortMode::Tasks => SortMode::Status,
            SortMode::Status => SortMode::LongestTask,
            SortMode::LongestTask => SortMode::Name,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SortMode::Name => "name",
            SortMode::Tasks => "tasks",
            SortMode::Status => "status",
            SortMode::LongestTask => "longest task",
        }
    }
}

/// Which panel is focused in worker detail view.
#[derive(Clone, Copy, PartialEq)]
pub enum WorkerPanel {
    ActiveTasks,
    CompletedTasks,
}

/// State for the Cluster Overview view.
pub struct ClusterViewState {
    pub table: TableState,
}

impl Default for ClusterViewState {
    fn default() -> Self {
        let mut table = TableState::default();
        table.select(Some(0));
        Self { table }
    }
}

/// State for the Worker Detail view.
pub struct WorkerViewState {
    pub worker_idx: usize,
    pub active_table: TableState,
    pub completed_table: TableState,
    pub focused_panel: WorkerPanel,
}

impl Default for WorkerViewState {
    fn default() -> Self {
        Self {
            worker_idx: 0,
            active_table: TableState::default(),
            completed_table: TableState::default(),
            focused_panel: WorkerPanel::ActiveTasks,
        }
    }
}
