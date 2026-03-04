use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

pub fn render_overlay(frame: &mut Frame) {
    let area = centered_rect(50, 60, frame.area());

    // Clear the background
    frame.render_widget(Clear, area);

    let lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  Global",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled("  1 / 2       ", Style::default().fg(Color::Yellow)),
            Span::raw("Switch views"),
        ]),
        Line::from(vec![
            Span::styled("  Tab         ", Style::default().fg(Color::Yellow)),
            Span::raw("Switch views"),
        ]),
        Line::from(vec![
            Span::styled("  p           ", Style::default().fg(Color::Yellow)),
            Span::raw("Pause/resume polling"),
        ]),
        Line::from(vec![
            Span::styled("  q / Ctrl+C  ", Style::default().fg(Color::Yellow)),
            Span::raw("Quit"),
        ]),
        Line::from(vec![
            Span::styled("  ?           ", Style::default().fg(Color::Yellow)),
            Span::raw("Toggle this help"),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "  Navigation",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled("  j / k       ", Style::default().fg(Color::Yellow)),
            Span::raw("Move selection up/down"),
        ]),
        Line::from(vec![
            Span::styled("  Enter       ", Style::default().fg(Color::Yellow)),
            Span::raw("Drill into worker"),
        ]),
        Line::from(vec![
            Span::styled("  Esc         ", Style::default().fg(Color::Yellow)),
            Span::raw("Go back"),
        ]),
        Line::from(vec![
            Span::styled("  h / l       ", Style::default().fg(Color::Yellow)),
            Span::raw("Prev/next worker (detail)"),
        ]),
        Line::from(vec![
            Span::styled("  Shift+Tab   ", Style::default().fg(Color::Yellow)),
            Span::raw("Cycle panels (Metrics/Tasks/Completed)"),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "  Cluster View",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled(
                "  \u{2190} / \u{2192}     ",
                Style::default().fg(Color::Yellow),
            ),
            Span::raw("Select sort column"),
        ]),
        Line::from(vec![
            Span::styled("  Space       ", Style::default().fg(Color::Yellow)),
            Span::raw("Cycle sort direction (\u{25b2}/\u{25bc})"),
        ]),
        Line::from(vec![
            Span::styled("  r           ", Style::default().fg(Color::Yellow)),
            Span::raw("Reset completed tasks"),
        ]),
        Line::from(vec![
            Span::styled("  g / G       ", Style::default().fg(Color::Yellow)),
            Span::raw("Jump to top/bottom"),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "  Press any key to close",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let help = Paragraph::new(lines).wrap(Wrap { trim: false }).block(
        Block::default()
            .borders(Borders::ALL)
            .title(Span::styled(
                " Help ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ))
            .style(Style::default().bg(Color::Black)),
    );

    frame.render_widget(help, area);
}

/// Create a centered rect using percentages of the parent area.
fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let [_, center_v, _] = Layout::vertical([
        Constraint::Percentage((100 - percent_y) / 2),
        Constraint::Percentage(percent_y),
        Constraint::Percentage((100 - percent_y) / 2),
    ])
    .areas(area);

    let [_, center, _] = Layout::horizontal([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ])
    .areas(center_v);

    center
}
