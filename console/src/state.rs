use ratatui::widgets::TableState;

/// Which view is currently active.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum View {
    ClusterOverview,
    WorkerDetail,
}

/// Which column is selected for sorting in the cluster table.
#[derive(Clone, Copy, PartialEq, Default)]
pub(crate) enum SortColumn {
    #[default]
    Worker,
    Status,
    Tasks,
    Queries,
    Cpu,
    Memory,
}

impl SortColumn {
    /// Move to the next column. When `wide` is false, skip Queries.
    pub(crate) fn next(self, wide: bool) -> Self {
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
            SortColumn::Cpu => SortColumn::Memory,
            SortColumn::Memory => SortColumn::Worker,
        }
    }

    /// Move to the previous column. When `wide` is false, skip Queries.
    pub(crate) fn prev(self, wide: bool) -> Self {
        match self {
            SortColumn::Worker => SortColumn::Memory,
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
            SortColumn::Memory => SortColumn::Cpu,
        }
    }
}

/// Sort direction for the selected column.
#[derive(Clone, Copy, PartialEq, Default)]
pub(crate) enum SortDirection {
    #[default]
    Unsorted,
    Ascending,
    Descending,
}

impl SortDirection {
    pub(crate) fn next(self) -> Self {
        match self {
            SortDirection::Unsorted => SortDirection::Ascending,
            SortDirection::Ascending => SortDirection::Descending,
            SortDirection::Descending => SortDirection::Unsorted,
        }
    }

    pub(crate) fn indicator(self) -> &'static str {
        match self {
            SortDirection::Unsorted => "",
            SortDirection::Ascending => " ▲",
            SortDirection::Descending => " ▼",
        }
    }
}

/// Which panel is focused in worker detail view.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum WorkerPanel {
    Metrics,
    ActiveTasks,
    CompletedTasks,
}

/// State for the Cluster Overview view.
pub(crate) struct ClusterViewState {
    pub(crate) table: TableState,
    pub(crate) selected_column: SortColumn,
    pub(crate) sort_direction: SortDirection,
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
pub(crate) struct WorkerViewState {
    pub(crate) worker_idx: usize,
    pub(crate) active_table: TableState,
    pub(crate) completed_table: TableState,
    pub(crate) focused_panel: WorkerPanel,
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
