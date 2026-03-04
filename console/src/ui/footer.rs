use crate::app::App;
use crate::state::View;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let tab_spans = |view: &View| -> Vec<Span> {
        let cluster_style = if *view == View::ClusterOverview {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let worker_style = if *view == View::WorkerDetail {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        vec![
            Span::styled("[1]Cluster", cluster_style),
            Span::raw(" "),
            Span::styled("[2]Worker", worker_style),
        ]
    };

    let context_hints = match app.current_view {
        View::ClusterOverview => vec![
            Span::styled(" j/k", Style::default().fg(Color::Yellow)),
            Span::styled(":select", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("Enter", Style::default().fg(Color::Yellow)),
            Span::styled(":detail", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("\u{2190}\u{2192}", Style::default().fg(Color::Yellow)),
            Span::styled(":column", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("Space", Style::default().fg(Color::Yellow)),
            Span::styled(":sort", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("p", Style::default().fg(Color::Yellow)),
            Span::styled(":pause", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("?", Style::default().fg(Color::Yellow)),
            Span::styled(":help", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("q", Style::default().fg(Color::Yellow)),
            Span::styled(":quit", Style::default().fg(Color::DarkGray)),
        ],
        View::WorkerDetail => vec![
            Span::styled(" h/l", Style::default().fg(Color::Yellow)),
            Span::styled(":prev/next", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("j/k", Style::default().fg(Color::Yellow)),
            Span::styled(":select", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("S-Tab", Style::default().fg(Color::Yellow)),
            Span::styled(":panel", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("Esc", Style::default().fg(Color::Yellow)),
            Span::styled(":back", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("p", Style::default().fg(Color::Yellow)),
            Span::styled(":pause", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("?", Style::default().fg(Color::Yellow)),
            Span::styled(":help", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled("q", Style::default().fg(Color::Yellow)),
            Span::styled(":quit", Style::default().fg(Color::DarkGray)),
        ],
    };

    let mut spans = tab_spans(&app.current_view);
    spans.push(Span::styled(" │", Style::default().fg(Color::DarkGray)));
    spans.extend(context_hints);

    let footer = Paragraph::new(Line::from(spans));
    frame.render_widget(footer, area);
}
