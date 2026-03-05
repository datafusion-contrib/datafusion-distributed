use super::format::format_duration;
use crate::app::App;
use crate::state::View;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

pub(super) fn render(frame: &mut Frame, area: Rect, app: &App) {
    let stats = app.cluster_stats();

    let view_name = match app.current_view {
        View::ClusterOverview => "Cluster Overview",
        View::WorkerDetail => "Worker Detail",
    };

    let live_badge = if app.paused {
        Span::styled(
            " PAUSED ",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(
            " LIVE ",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
    };

    let uptime = app.start_time.elapsed();
    let uptime_str = format_duration(uptime);

    let line = Line::from(vec![
        Span::styled(
            format!("Workers: {} total  ", stats.total),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{} active", stats.active_count),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(
            format!("{} idle", stats.idle_count),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(
            format!("{} disconnected", stats.disconnected_count),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        Span::raw("    "),
        Span::styled(
            format!("Queries: {}", stats.active_queries),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(
            format!("Tasks: {}", stats.total_tasks),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("    "),
        Span::styled(uptime_str, Style::default().fg(Color::DarkGray)),
        Span::raw("  "),
        live_badge,
    ]);

    let title = format!(" ddf-console — {view_name} ");
    let header = Paragraph::new(line).block(Block::default().borders(Borders::BOTTOM).title(
        Span::styled(title, Style::default().add_modifier(Modifier::BOLD)),
    ));

    frame.render_widget(header, area);
}
