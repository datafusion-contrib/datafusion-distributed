use crate::app::App;
use crate::state::WorkerPanel;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

pub fn render(frame: &mut Frame, area: Rect, app: &mut App) {
    let idx = app.worker_state.worker_idx;
    if idx >= app.workers.len() {
        let msg = Paragraph::new("No worker selected")
            .style(Style::default().fg(Color::DarkGray))
            .centered();
        frame.render_widget(msg, area);
        return;
    }

    let [summary_area, active_area, completed_area, conn_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Percentage(50),
        Constraint::Min(4),
        Constraint::Length(1),
    ])
    .areas(area);

    render_summary(frame, summary_area, app, idx);
    render_active_tasks(frame, active_area, app, idx);
    render_completed_tasks(frame, completed_area, app, idx);
    render_connection_info(frame, conn_area, app, idx);
}

fn render_summary(frame: &mut Frame, area: Rect, app: &App, idx: usize) {
    let worker = &app.workers[idx];
    let status_color = worker.status_color();

    let line = Line::from(vec![
        Span::styled(" Worker: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(
            worker.url.as_str(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("    "),
        Span::styled("Status: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            worker.status_text(),
            Style::default()
                .fg(status_color)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("    "),
        Span::styled("Tasks: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            worker.tasks.len().to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("    "),
        Span::styled("Queries: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            worker.distinct_query_count().to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("    [{}/{}]", idx + 1, app.workers.len())),
    ]);

    frame.render_widget(Paragraph::new(line), area);
}

fn render_active_tasks(frame: &mut Frame, area: Rect, app: &mut App, idx: usize) {
    let worker = &app.workers[idx];
    let focused = app.worker_state.focused_panel == WorkerPanel::ActiveTasks;

    let header = Row::new(vec!["Query", "Stage", "Task#", "Duration"]).style(
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    );

    // Sort tasks by duration descending (longest first)
    let mut task_indices: Vec<usize> = (0..worker.tasks.len()).collect();
    task_indices.sort_by(|&a, &b| {
        let dur_a = worker.tasks[a]
            .stage_key
            .as_ref()
            .map(|sk| worker.task_duration(&sk.query_id, sk.stage_id, sk.task_number))
            .unwrap_or_default();
        let dur_b = worker.tasks[b]
            .stage_key
            .as_ref()
            .map(|sk| worker.task_duration(&sk.query_id, sk.stage_id, sk.task_number))
            .unwrap_or_default();
        dur_b.cmp(&dur_a)
    });

    let rows: Vec<Row> = task_indices
        .iter()
        .map(|&i| {
            let task = &worker.tasks[i];
            if let Some(sk) = &task.stage_key {
                let query_hex = hex_prefix(&sk.query_id, 8);
                let duration = worker.task_duration(&sk.query_id, sk.stage_id, sk.task_number);
                let dur_str = format_duration(duration);
                let dur_style = if duration.as_secs() > 60 {
                    Style::default().fg(Color::Red)
                } else if duration.as_secs() > 30 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default()
                };

                Row::new(vec![
                    Cell::from(query_hex).style(Style::default().fg(Color::Cyan)),
                    Cell::from(format!("S{}", sk.stage_id)),
                    Cell::from(format!("T{}", sk.task_number)),
                    Cell::from(dur_str).style(dur_style),
                ])
            } else {
                Row::new(vec![
                    Cell::from("?"),
                    Cell::from("?"),
                    Cell::from("?"),
                    Cell::from("-"),
                ])
            }
        })
        .collect();

    let title_style = if focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(30),
            Constraint::Percentage(20),
            Constraint::Percentage(20),
            Constraint::Percentage(30),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title(Span::styled(
        format!(" Active Tasks ({}) ", worker.tasks.len()),
        title_style,
    )))
    .row_highlight_style(
        Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    )
    .highlight_symbol("▸ ");

    frame.render_stateful_widget(table, area, &mut app.worker_state.active_table);
}

fn render_completed_tasks(frame: &mut Frame, area: Rect, app: &mut App, idx: usize) {
    let worker = &app.workers[idx];
    let focused = app.worker_state.focused_panel == WorkerPanel::CompletedTasks;

    let header = Row::new(vec!["Query", "Stage", "Task#", "Duration"]).style(
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row> = worker
        .completed_tasks
        .iter()
        .map(|ct| {
            let query_hex = hex_prefix(&ct.query_id, 8);
            let dur_str = format!("~{}", format_duration(ct.observed_duration));

            Row::new(vec![
                Cell::from(query_hex).style(Style::default().fg(Color::DarkGray)),
                Cell::from(format!("S{}", ct.stage_id)),
                Cell::from(format!("T{}", ct.task_number)),
                Cell::from(dur_str).style(Style::default().fg(Color::DarkGray)),
            ])
        })
        .collect();

    let title_style = if focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(30),
            Constraint::Percentage(20),
            Constraint::Percentage(20),
            Constraint::Percentage(30),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title(Span::styled(
        format!(" Recently Completed ({}) ", worker.completed_tasks.len()),
        title_style,
    )))
    .row_highlight_style(
        Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    );

    frame.render_stateful_widget(table, area, &mut app.worker_state.completed_table);
}

fn render_connection_info(frame: &mut Frame, area: Rect, app: &App, idx: usize) {
    let worker = &app.workers[idx];

    let connected_str = worker
        .connected_since
        .map(|since| format_duration(since.elapsed()))
        .unwrap_or_else(|| "-".to_string());

    let line = if let Some(reason) = worker.disconnect_reason() {
        Line::from(vec![
            Span::styled(" Disconnected: ", Style::default().fg(Color::Red)),
            Span::styled(reason, Style::default().fg(Color::Red)),
        ])
    } else {
        Line::from(vec![
            Span::styled(" Connected: ", Style::default().fg(Color::DarkGray)),
            Span::raw(connected_str),
            Span::styled("   Polls: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", worker.poll_count)),
        ])
    };

    frame.render_widget(Paragraph::new(line), area);
}

/// Format the first `n` bytes of a byte slice as hex.
fn hex_prefix(bytes: &[u8], n: usize) -> String {
    bytes
        .iter()
        .take(n)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn format_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    let millis = d.subsec_millis();
    if secs == 0 {
        format!("{millis}ms")
    } else if secs < 60 {
        format!("{secs}.{:01}s", millis / 100)
    } else {
        format!("{}m {}s", secs / 60, secs % 60)
    }
}
