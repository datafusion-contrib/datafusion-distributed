use crate::app::{App, ConsoleState, TaskRowStatus};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
};

/// Main rendering function.
pub fn render(frame: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Task table
            Constraint::Length(3), // Footer
        ])
        .split(frame.area());

    render_header(frame, chunks[0], app);
    render_task_table(frame, chunks[1], app);
    render_footer(frame, chunks[2], app);
}

/// Render header with cluster summary.
fn render_header(frame: &mut Frame, area: Rect, app: &App) {
    let stats = app.cluster_stats();
    let worker_count = app.workers.len();

    let (state_text, state_color) = match app.console_state {
        ConsoleState::Idle => ("IDLE", Color::Yellow),
        ConsoleState::Active => ("ACTIVE", Color::Green),
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

/// Render the global task table.
fn render_task_table(frame: &mut Frame, area: Rect, app: &mut App) {
    let rows = app.task_rows();

    if rows.is_empty() {
        let empty_msg = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                "No active tasks",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::ITALIC),
            )),
        ])
        .block(Block::default().borders(Borders::ALL).title("Tasks"))
        .centered();

        frame.render_widget(empty_msg, area);
        return;
    }

    let header = Row::new(vec![
        "Query ID",
        "Stage No.",
        "Task No.",
        "Worker",
        "Status",
        "Output Rows",
        "Elapsed Compute",
        "Memory Usage",
        "Rows Spilled",
    ])
    .style(
        Style::default()
            .add_modifier(Modifier::BOLD)
            .fg(Color::Cyan),
    );

    let table_rows: Vec<Row> = rows
        .iter()
        .map(|r| {
            let (status_text, status_color) = match r.status {
                TaskRowStatus::Running => ("RUNNING", Color::Green),
                TaskRowStatus::Completed => ("DONE", Color::Cyan),
            };
            let row_style = match r.status {
                TaskRowStatus::Running => Style::default(),
                TaskRowStatus::Completed => Style::default().fg(Color::DarkGray),
            };
            Row::new(vec![
                Cell::from(r.query_id_short.clone()),
                Cell::from(r.stage_id.to_string()),
                Cell::from(r.task_number.to_string()),
                Cell::from(format_worker_url(&r.worker_url)),
                Cell::from(status_text).style(Style::default().fg(status_color)),
                Cell::from(format_count(r.output_rows)),
                Cell::from(format_duration_ns(r.elapsed_compute)),
                Cell::from(format_bytes(r.current_memory_usage)),
                Cell::from(format_count(r.spill_count)),
            ])
            .style(row_style)
        })
        .collect();

    let table = Table::new(
        table_rows,
        [
            Constraint::Percentage(10), // Query
            Constraint::Percentage(8),  // Stage
            Constraint::Percentage(5),  // Task No.
            Constraint::Percentage(12), // Worker
            Constraint::Percentage(10), // Status
            Constraint::Percentage(14), // Output Rows
            Constraint::Percentage(17), // Elapsed Compute
            Constraint::Percentage(12), // Memory Usage
            Constraint::Percentage(12), // Spilled Rows
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title("Tasks"))
    .row_highlight_style(
        Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    );

    frame.render_stateful_widget(table, area, &mut app.task_table_state);
}

/// Render footer with key hints.
fn render_footer(frame: &mut Frame, area: Rect, app: &App) {
    let footer_text = match app.console_state {
        ConsoleState::Idle => Line::from(vec![
            Span::styled(
                "Waiting for tasks...  ",
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
        ConsoleState::Active => Line::from(vec![
            Span::styled(
                "j/k",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(": navigate  |  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "'q'",
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

/// Extract port from worker URL for compact display.
fn format_worker_url(url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(url) {
        if let Some(port) = parsed.port() {
            return format!(":{port}");
        }
    }
    url.to_string()
}

fn format_count(value: u64) -> String {
    let s = value.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn format_duration_ns(ns: u64) -> String {
    if ns == 0 {
        return "--".to_string();
    }
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
