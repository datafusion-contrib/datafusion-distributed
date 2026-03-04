use ratatui::widgets::TableState;

/// Which view is currently active.
#[derive(Clone, PartialEq)]
pub enum View {
    ClusterOverview,
    WorkerDetail,
}

/// Which column is selected for sorting in the cluster table.
#[derive(Clone, Copy, PartialEq, Default)]
pub enum SortColumn {
    #[default]
    Worker,
    Status,
    Tasks,
    Queries,
    Cpu,
    Rss,
}

impl SortColumn {
    /// Move to the next column. When `wide` is false, skip Queries.
    pub fn next(self, wide: bool) -> Self {
        match self {
            SortColumn::Worker => SortColumn::Status,
            SortColumn::Status => SortColumn::Tasks,
            SortColumn::Tasks => {
                if wide {
                    SortColumn::Queries
                } else {
                    SortColumn::Cpu
                }
            }
            SortColumn::Queries => SortColumn::Cpu,
            SortColumn::Cpu => SortColumn::Rss,
            SortColumn::Rss => SortColumn::Worker,
        }
    }

    /// Move to the previous column. When `wide` is false, skip Queries.
    pub fn prev(self, wide: bool) -> Self {
        match self {
            SortColumn::Worker => SortColumn::Rss,
            SortColumn::Status => SortColumn::Worker,
            SortColumn::Tasks => SortColumn::Status,
            SortColumn::Queries => SortColumn::Tasks,
            SortColumn::Cpu => {
                if wide {
                    SortColumn::Queries
                } else {
                    SortColumn::Tasks
                }
            }
            SortColumn::Rss => SortColumn::Cpu,
        }
    }
}

/// Sort direction for the selected column.
#[derive(Clone, Copy, PartialEq, Default)]
pub enum SortDirection {
    #[default]
    Unsorted,
    Ascending,
    Descending,
}

impl SortDirection {
    pub fn next(self) -> Self {
        match self {
            SortDirection::Unsorted => SortDirection::Ascending,
            SortDirection::Ascending => SortDirection::Descending,
            SortDirection::Descending => SortDirection::Unsorted,
        }
    }

    pub fn indicator(self) -> &'static str {
        match self {
            SortDirection::Unsorted => "",
            SortDirection::Ascending => " ▲",
            SortDirection::Descending => " ▼",
        }
    }
}

/// Which panel is focused in worker detail view.
#[derive(Clone, Copy, PartialEq)]
pub enum WorkerPanel {
    Metrics,
    ActiveTasks,
    CompletedTasks,
}

/// State for the Cluster Overview view.
pub struct ClusterViewState {
    pub table: TableState,
    pub selected_column: SortColumn,
    pub sort_direction: SortDirection,
}

impl Default for ClusterViewState {
    fn default() -> Self {
        let mut table = TableState::default();
        table.select(Some(0));
        Self {
            table,
            selected_column: SortColumn::default(),
            sort_direction: SortDirection::default(),
        }
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
