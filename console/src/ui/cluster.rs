use crate::app::App;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

pub fn render(frame: &mut Frame, area: Rect, app: &mut App) {
    let [table_area, summary_area] =
        Layout::vertical([Constraint::Min(3), Constraint::Length(1)]).areas(area);

    render_worker_table(frame, table_area, app);
    render_task_distribution(frame, summary_area, app);
}

fn render_worker_table(frame: &mut Frame, area: Rect, app: &mut App) {
    let sorted_indices = app.sorted_worker_indices();
    let avg_tasks = app.avg_tasks_per_worker();
    let wide = area.width >= 100;

    let header_cells = if wide {
        vec!["Worker", "Status", "Tasks", "Queries", "Longest Task"]
    } else {
        vec!["Worker", "Status", "Tasks", "Longest"]
    };

    let header = Row::new(header_cells.iter().map(|h| {
        Cell::from(*h).style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
    }))
    .height(1);

    let rows: Vec<Row> = sorted_indices
        .iter()
        .map(|&idx| {
            let worker = &app.workers[idx];
            let task_count = worker.tasks.len();
            let status_color = worker.status_color();

            // URL display
            let url_str = worker.url.as_str();
            let url_display = if wide || url_str.len() <= 30 {
                url_str.to_string()
            } else {
                // Show last 28 chars with ".."
                format!("..{}", &url_str[url_str.len().saturating_sub(28)..])
            };

            // Task count with hot spot highlighting
            let task_style =
                if task_count > 0 && avg_tasks > 0.0 && task_count as f64 > avg_tasks * 2.0 {
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                } else if task_count > 0 {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::DarkGray)
                };

            let task_str = if matches!(
                worker.connection_status,
                crate::app::ConnectionStatus::Disconnected { .. }
            ) {
                "-".to_string()
            } else {
                task_count.to_string()
            };

            // Longest task duration
            let longest = worker.longest_task_duration();
            let longest_str = if longest.is_zero() {
                "-".to_string()
            } else {
                format_task_duration(longest)
            };
            let longest_style = if longest.as_secs() > 60 {
                Style::default().fg(Color::Red)
            } else if longest.as_secs() > 30 {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default().fg(Color::DarkGray)
            };

            if wide {
                let query_count = worker.distinct_query_count();
                let query_str = if matches!(
                    worker.connection_status,
                    crate::app::ConnectionStatus::Disconnected { .. }
                ) {
                    "-".to_string()
                } else {
                    query_count.to_string()
                };

                Row::new(vec![
                    Cell::from(url_display),
                    Cell::from(worker.status_text()).style(Style::default().fg(status_color)),
                    Cell::from(task_str).style(task_style),
                    Cell::from(query_str).style(Style::default().fg(Color::DarkGray)),
                    Cell::from(longest_str).style(longest_style),
                ])
            } else {
                Row::new(vec![
                    Cell::from(url_display),
                    Cell::from(worker.status_text()).style(Style::default().fg(status_color)),
                    Cell::from(task_str).style(task_style),
                    Cell::from(longest_str).style(longest_style),
                ])
            }
        })
        .collect();

    let widths = if wide {
        vec![
            Constraint::Percentage(35),
            Constraint::Percentage(15),
            Constraint::Percentage(10),
            Constraint::Percentage(15),
            Constraint::Percentage(25),
        ]
    } else {
        vec![
            Constraint::Percentage(40),
            Constraint::Percentage(20),
            Constraint::Percentage(15),
            Constraint::Percentage(25),
        ]
    };

    let sort_label = app.sort_mode.label();
    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Workers (sorted by {sort_label}) ")),
        )
        .row_highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");

    frame.render_stateful_widget(table, area, &mut app.cluster_state.table);
}

fn render_task_distribution(frame: &mut Frame, area: Rect, app: &App) {
    if app.workers.is_empty() {
        return;
    }

    let mut task_counts: Vec<usize> = app.workers.iter().map(|w| w.tasks.len()).collect();
    task_counts.sort();

    let min = task_counts.first().copied().unwrap_or(0);
    let max = task_counts.last().copied().unwrap_or(0);
    let sum: usize = task_counts.iter().sum();
    let avg = sum as f64 / task_counts.len() as f64;
    let median = task_counts[task_counts.len() / 2];

    let line = Line::from(vec![
        Span::styled(" Tasks/worker: ", Style::default().fg(Color::DarkGray)),
        Span::styled(format!("min={min}"), Style::default().fg(Color::White)),
        Span::raw("  "),
        Span::styled(
            format!("max={max}"),
            if max > 0 && max as f64 > avg * 2.0 {
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            },
        ),
        Span::raw("  "),
        Span::styled(format!("avg={avg:.1}"), Style::default().fg(Color::White)),
        Span::raw("  "),
        Span::styled(
            format!("median={median}"),
            Style::default().fg(Color::White),
        ),
        Span::styled(
            format!("   ({} workers)", app.workers.len()),
            Style::default().fg(Color::DarkGray),
        ),
    ]);

    frame.render_widget(Paragraph::new(line), area);
}

fn format_task_duration(d: std::time::Duration) -> String {
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
