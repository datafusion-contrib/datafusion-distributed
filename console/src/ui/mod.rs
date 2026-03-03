mod cluster;
mod footer;
mod header;
mod help;
mod worker;

use crate::app::App;
use crate::state::View;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};

/// Top-level render dispatch.
pub fn render(frame: &mut Frame, app: &mut App) {
    let [header_area, content_area, footer_area] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Min(5),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    header::render(frame, header_area, app);

    match app.current_view {
        View::ClusterOverview => cluster::render(frame, content_area, app),
        View::WorkerDetail => worker::render(frame, content_area, app),
    }

    footer::render(frame, footer_area, app);

    if app.show_help {
        help::render_overlay(frame);
    }
}
