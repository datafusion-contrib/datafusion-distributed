use super::format::{format_bytes, format_duration, format_rows_throughput};
use crate::app::App;
use crate::state::{SortColumn, SortDirection};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

pub(super) fn render(frame: &mut Frame, area: Rect, app: &mut App) {
    let [metrics_area, table_area, summary_area] = Layout::vertical([
        Constraint::Length(4),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .areas(area);

    render_cluster_metrics(frame, metrics_area, app);
    render_worker_table(frame, table_area, app);
    render_task_distribution(frame, summary_area, app);
}

fn render_cluster_metrics(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::default().borders(Borders::ALL).title(Span::styled(
        " Cluster Metrics ",
        Style::default().add_modifier(Modifier::BOLD),
    ));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let [line1_area, line2_area] =
        Layout::vertical([Constraint::Length(1), Constraint::Length(1)]).areas(inner);

    let stats = app.cluster_stats();

    // Line 1: Throughput and active workers
    let throughput_str = if app.current_throughput > 0.0 {
        format_rows_throughput(app.current_throughput)
    } else {
        "--".to_string()
    };
    let throughput_color = if app.current_throughput > 0.0 {
        Color::Green
    } else {
        Color::DarkGray
    };

    let active_str = format!("{}/{} workers", stats.active_count, stats.total);
    let active_color = if stats.active_count > 0 {
        Color::Green
    } else {
        Color::DarkGray
    };

    let queries_str = format!("{}", stats.active_queries);
    let queries_color = if stats.active_queries > 0 {
        Color::Cyan
    } else {
        Color::DarkGray
    };

    let line1 = Line::from(vec![
        Span::styled(
            " Throughput: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(throughput_str, Style::default().fg(throughput_color)),
        Span::raw("     "),
        Span::styled(" Active: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(active_str, Style::default().fg(active_color)),
        Span::raw("     "),
        Span::styled(
            " Queries in flight: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(queries_str, Style::default().fg(queries_color)),
    ]);
    frame.render_widget(Paragraph::new(line1), line1_area);

    // Line 2: Completed tasks, avg duration, longest active task
    let completed_str = format!("{} tasks", stats.total_completed);

    let avg_dur_str = app
        .cluster_avg_task_duration()
        .map(format_duration)
        .unwrap_or_else(|| "--".to_string());

    let longest = app.cluster_longest_active_task();
    let longest_str = if longest.is_zero() {
        "--".to_string()
    } else {
        format_duration(longest)
    };
    let longest_color = if longest.as_secs() > 60 {
        Color::Red
    } else if longest.as_secs() > 30 {
        Color::Yellow
    } else {
        Color::DarkGray
    };

    let line2 = Line::from(vec![
        Span::styled(
            " Completed: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(completed_str, Style::default().fg(Color::Cyan)),
        Span::raw("     "),
        Span::styled(
            "Avg duration: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(avg_dur_str, Style::default().fg(Color::DarkGray)),
        Span::raw("     "),
        Span::styled("Longest: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(longest_str, Style::default().fg(longest_color)),
    ]);
    frame.render_widget(Paragraph::new(line2), line2_area);
}

fn render_worker_table(frame: &mut Frame, area: Rect, app: &mut App) {
    let sorted_indices = app.sorted_worker_indices();
    let avg_tasks = app.avg_tasks_per_worker();
    let wide = area.width >= 100;

    let columns: Vec<(&str, SortColumn)> = if wide {
        vec![
            ("Worker", SortColumn::Worker),
            ("Status", SortColumn::Status),
            ("Tasks", SortColumn::Tasks),
            ("Queries", SortColumn::Queries),
            ("CPU", SortColumn::Cpu),
            ("Memory", SortColumn::Rss),
        ]
    } else {
        vec![
            ("Worker", SortColumn::Worker),
            ("Status", SortColumn::Status),
            ("Tasks", SortColumn::Tasks),
            ("CPU", SortColumn::Cpu),
            ("Memory", SortColumn::Rss),
        ]
    };

    let selected_col = app.cluster_state.selected_column;
    let sort_dir = app.cluster_state.sort_direction;

    let header = Row::new(columns.iter().map(|(label, col)| {
        let is_selected = *col == selected_col;
        let indicator = if is_selected {
            sort_dir.indicator()
        } else {
            ""
        };
        let text = format!("{label}{indicator}");

        let style = if is_selected && sort_dir != SortDirection::Unsorted {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
        } else if is_selected {
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
        } else {
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD)
        };

        Cell::from(text).style(style)
    }))
    .height(1);

    let rows: Vec<Row> = sorted_indices
        .iter()
        .map(|&idx| {
            let worker = &app.workers[idx];
            let task_count = worker.tasks.len();
            let status_color = worker.status_color();
            let is_disconnected = matches!(
                worker.connection_status,
                crate::app::ConnectionStatus::Disconnected { .. }
            );

            // URL display
            let url_str = worker.url.as_str();
            let url_display = if wide || url_str.len() <= 30 {
                url_str.to_string()
            } else {
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

            let task_str = if is_disconnected {
                "-".to_string()
            } else {
                task_count.to_string()
            };

            // CPU usage
            let (cpu_str, cpu_style) = if is_disconnected {
                ("-".to_string(), Style::default().fg(Color::DarkGray))
            } else if worker.cpu_usage_percent > 0.0 {
                let style = if worker.cpu_usage_percent > 95.0 {
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                } else if worker.cpu_usage_percent > 80.0 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default().fg(Color::Green)
                };
                (format!("{:.1}%", worker.cpu_usage_percent), style)
            } else {
                ("--".to_string(), Style::default().fg(Color::DarkGray))
            };

            // RSS memory
            let (rss_str, rss_style) = if is_disconnected {
                ("-".to_string(), Style::default().fg(Color::DarkGray))
            } else if worker.rss_bytes > 0 {
                (
                    format_bytes(worker.rss_bytes),
                    Style::default().fg(Color::White),
                )
            } else {
                ("--".to_string(), Style::default().fg(Color::DarkGray))
            };

            if wide {
                let query_count = worker.distinct_query_count();
                let query_str = if is_disconnected {
                    "-".to_string()
                } else {
                    query_count.to_string()
                };

                Row::new(vec![
                    Cell::from(url_display),
                    Cell::from(worker.status_text()).style(Style::default().fg(status_color)),
                    Cell::from(task_str).style(task_style),
                    Cell::from(query_str).style(Style::default().fg(Color::DarkGray)),
                    Cell::from(cpu_str).style(cpu_style),
                    Cell::from(rss_str).style(rss_style),
                ])
            } else {
                Row::new(vec![
                    Cell::from(url_display),
                    Cell::from(worker.status_text()).style(Style::default().fg(status_color)),
                    Cell::from(task_str).style(task_style),
                    Cell::from(cpu_str).style(cpu_style),
                    Cell::from(rss_str).style(rss_style),
                ])
            }
        })
        .collect();

    let widths = if wide {
        vec![
            Constraint::Percentage(30),
            Constraint::Percentage(12),
            Constraint::Percentage(10),
            Constraint::Percentage(12),
            Constraint::Percentage(13),
            Constraint::Percentage(13),
        ]
    } else {
        vec![
            Constraint::Percentage(35),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(20),
        ]
    };

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" Workers "))
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
