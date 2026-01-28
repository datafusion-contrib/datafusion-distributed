use crate::app::{App, ConsoleState, WorkerState};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph},
};

/// Main rendering function
pub fn render(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header with cluster summary
            Constraint::Min(0),    // Workers
            Constraint::Length(3), // Footer
        ])
        .split(frame.area());

    render_header(frame, chunks[0], app);
    render_workers(frame, chunks[1], app);
    render_footer(frame, chunks[2], app);
}

/// Render header with cluster summary
fn render_header(frame: &mut Frame, area: Rect, app: &App) {
    let stats = app.cluster_stats();
    let worker_count = app.workers.len();

    // Show console state prominently
    let (state_text, state_color) = match app.console_state {
        ConsoleState::Idle => ("IDLE", Color::Yellow),
        ConsoleState::Active => ("ACTIVE", Color::Green),
        ConsoleState::Completed => ("COMPLETED", Color::Cyan),
    };

    let header_text = vec![Line::from(vec![
        Span::styled("Console: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(
            state_text,
            Style::default()
                .fg(state_color)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  |  "),
        Span::styled(
            format!("{worker_count} workers"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("  |  "),
        Span::styled("Workers: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(
            format!("{} ACTIVE", stats.active_count),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(", "),
        Span::styled(
            format!("{} IDLE", stats.idle_count),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(", "),
        Span::styled(
            format!("{} DISCONNECTED", stats.disconnected_count),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  |  "),
        Span::styled("Tasks: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(
            stats.total_tasks.to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    ])];

    let header = Paragraph::new(header_text).block(
        Block::default()
            .borders(Borders::ALL)
            .title("DataFusion Distributed Console"),
    );

    frame.render_widget(header, area);
}

/// Render all workers in vertical layout
fn render_workers(frame: &mut Frame, area: Rect, app: &App) {
    if app.workers.is_empty() {
        let empty_msg = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                "No workers configured",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::ITALIC),
            )),
        ])
        .block(Block::default().borders(Borders::ALL).title("Workers"))
        .centered();

        frame.render_widget(empty_msg, area);
        return;
    }

    // Each worker needs only ~5 lines now (border + label + bar + spacing)
    let worker_height = 5;
    let constraints: Vec<_> = app
        .workers
        .iter()
        .map(|_| Constraint::Length(worker_height))
        .collect();

    let worker_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    for (i, worker) in app.workers.iter().enumerate() {
        if i < worker_chunks.len() {
            render_worker_section(frame, worker_chunks[i], worker);
        }
    }
}

/// Render a single worker section
fn render_worker_section(frame: &mut Frame, area: Rect, worker: &WorkerState) {
    let status = worker.status_text();
    let status_color = worker.status_color();

    let title = format!("Worker: :{} [{}]", worker.url.as_str(), status);

    let block = Block::default().borders(Borders::ALL).title(Span::styled(
        title,
        Style::default()
            .fg(status_color)
            .add_modifier(Modifier::BOLD),
    ));

    let inner_area = block.inner(area);
    frame.render_widget(block, area);

    // Render content based on state
    if let Some(reason) = worker.disconnect_reason() {
        // Show disconnection reason
        let disconnect_msg = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                format!("Connection failed: {reason}"),
                Style::default().fg(Color::Red),
            )),
        ])
        .centered();

        frame.render_widget(disconnect_msg, inner_area);
    } else if worker.all_tasks_completed() {
        // Show completed state (100% progress bar)
        render_completed_state(frame, inner_area, worker);
    } else if worker.tasks.is_empty() && !worker.has_completed_tasks() {
        // Show idle message
        let idle_msg = Paragraph::new(vec![Line::from(Span::styled(
            "No active tasks",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC),
        ))])
        .centered();

        frame.render_widget(idle_msg, inner_area);
    } else {
        // Show aggregated progress bar (running + completed)
        render_aggregated_progress(frame, inner_area, worker);
    }
}

/// Render aggregated progress bar for all tasks on a worker
fn render_aggregated_progress(frame: &mut Frame, area: Rect, worker: &WorkerState) {
    let (completed, total) = worker.aggregate_progress();
    let task_count = worker.tasks.len();

    let ratio = if total > 0 {
        completed as f64 / total as f64
    } else {
        0.0
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Label line
            Constraint::Length(2), // Progress bar
        ])
        .split(area);

    // Label: "Overall Progress (N tasks)"
    let label = Line::from(Span::styled(
        format!("Overall Progress ({task_count} tasks)"),
        Style::default().fg(Color::Cyan),
    ));
    frame.render_widget(Paragraph::new(label), chunks[0]);

    // Progress bar
    let progress_label = format!("{}/{} partitions ({:.1}%)", completed, total, ratio * 100.0);

    let gauge = Gauge::default()
        .gauge_style(
            Style::default()
                .fg(Color::Green)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .label(progress_label)
        .use_unicode(true)
        .ratio(ratio);

    frame.render_widget(gauge, chunks[1]);
}

/// Render completed state for a worker
fn render_completed_state(frame: &mut Frame, area: Rect, worker: &WorkerState) {
    let total: u64 = worker.completed_tasks.iter().map(|t| t.total_partitions).sum();
    let task_count = worker.completed_tasks.len();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Label line
            Constraint::Length(2), // Progress bar
        ])
        .split(area);

    // Label: "Tasks Completed ✓ (N tasks)"
    let label = Line::from(Span::styled(
        format!("Tasks Completed ✓ ({task_count} tasks)"),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(Paragraph::new(label), chunks[0]);

    // Progress bar showing 100% complete in cyan
    let progress_label = format!("{total}/{total} partitions (100.0%)");

    let gauge = Gauge::default()
        .gauge_style(
            Style::default()
                .fg(Color::Cyan)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .label(progress_label)
        .use_unicode(true)
        .ratio(1.0);

    frame.render_widget(gauge, chunks[1]);
}

/// Render footer with controls
fn render_footer(frame: &mut Frame, area: Rect, app: &App) {
    let footer_text = match app.console_state {
        ConsoleState::Idle => Line::from(vec![
            Span::styled(
                "Waiting for worker registration...  ",
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled("Press ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "'q'",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" to quit", Style::default().fg(Color::DarkGray)),
        ]),
        ConsoleState::Completed => Line::from(vec![
            Span::styled(
                "Query completed!  Press ",
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(
                "'r'",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" to reset  |  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "'q'",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" to quit", Style::default().fg(Color::DarkGray)),
        ]),
        ConsoleState::Active => Line::from(vec![
            Span::styled("Press ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "'q'",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" or ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "ESC",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                " to quit  |  Updates every 10ms",
                Style::default().fg(Color::DarkGray),
            ),
        ]),
    };

    let footer = Paragraph::new(footer_text)
        .block(Block::default().borders(Borders::ALL))
        .centered();

    frame.render_widget(footer, area);
}
