use crate::app::App;
use crate::state::{View, WorkerPanel};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

/// Handle a key event, dispatching to the appropriate view handler.
pub fn handle_key_event(app: &mut App, key: KeyEvent) {
    if key.kind != KeyEventKind::Press {
        return;
    }

    // Help overlay takes priority — any key closes it
    if app.show_help {
        app.show_help = false;
        return;
    }

    // Global keys
    match (key.code, key.modifiers) {
        (KeyCode::Char('q'), _) | (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
            app.should_quit = true;
            return;
        }
        (KeyCode::Char('?'), _) => {
            app.show_help = true;
            return;
        }
        (KeyCode::Char('p'), _) => {
            app.paused = !app.paused;
            return;
        }
        (KeyCode::Char('1'), _) => {
            app.current_view = View::ClusterOverview;
            return;
        }
        (KeyCode::Char('2'), _) => {
            if !app.workers.is_empty() {
                app.current_view = View::WorkerDetail;
            }
            return;
        }
        _ => {}
    }

    // View-specific keys
    match app.current_view {
        View::ClusterOverview => handle_cluster_keys(app, key),
        View::WorkerDetail => handle_worker_keys(app, key),
    }
}

fn handle_cluster_keys(app: &mut App, key: KeyEvent) {
    let worker_count = app.workers.len();
    if worker_count == 0 {
        return;
    }

    match key.code {
        KeyCode::Down | KeyCode::Char('j') => {
            let selected = app
                .cluster_state
                .table
                .selected()
                .unwrap_or(0)
                .saturating_add(1)
                .min(worker_count.saturating_sub(1));
            app.cluster_state.table.select(Some(selected));
        }
        KeyCode::Up | KeyCode::Char('k') => {
            let selected = app
                .cluster_state
                .table
                .selected()
                .unwrap_or(0)
                .saturating_sub(1);
            app.cluster_state.table.select(Some(selected));
        }
        KeyCode::Home | KeyCode::Char('g') => {
            app.cluster_state.table.select(Some(0));
        }
        KeyCode::End | KeyCode::Char('G') => {
            app.cluster_state
                .table
                .select(Some(worker_count.saturating_sub(1)));
        }
        KeyCode::Enter => {
            if let Some(selected) = app.cluster_state.table.selected() {
                let sorted = app.sorted_worker_indices();
                if let Some(&real_idx) = sorted.get(selected) {
                    app.worker_state.worker_idx = real_idx;
                    app.worker_state.active_table = Default::default();
                    app.worker_state.completed_table = Default::default();
                    app.worker_state.focused_panel = WorkerPanel::ActiveTasks;
                    app.current_view = View::WorkerDetail;
                }
            }
        }
        KeyCode::Left => {
            app.cluster_state.selected_column = app.cluster_state.selected_column.prev(true);
        }
        KeyCode::Right => {
            app.cluster_state.selected_column = app.cluster_state.selected_column.next(true);
        }
        KeyCode::Char(' ') => {
            app.cluster_state.sort_direction = app.cluster_state.sort_direction.next();
        }
        KeyCode::Char('r') => {
            for worker in &mut app.workers {
                worker.completed_tasks.clear();
            }
        }
        KeyCode::Tab => {
            if let Some(selected) = app.cluster_state.table.selected() {
                let sorted = app.sorted_worker_indices();
                if let Some(&real_idx) = sorted.get(selected) {
                    app.worker_state.worker_idx = real_idx;
                    app.current_view = View::WorkerDetail;
                }
            }
        }
        _ => {}
    }
}

fn handle_worker_keys(app: &mut App, key: KeyEvent) {
    if app.worker_state.worker_idx >= app.workers.len() {
        return;
    }

    match key.code {
        KeyCode::Esc | KeyCode::Tab => {
            app.current_view = View::ClusterOverview;
        }
        KeyCode::Left | KeyCode::Char('h') => {
            // Previous worker
            if !app.workers.is_empty() {
                if app.worker_state.worker_idx == 0 {
                    app.worker_state.worker_idx = app.workers.len() - 1;
                } else {
                    app.worker_state.worker_idx -= 1;
                }
                app.worker_state.active_table = Default::default();
                app.worker_state.completed_table = Default::default();
            }
        }
        KeyCode::Right | KeyCode::Char('l') => {
            // Next worker
            if !app.workers.is_empty() {
                app.worker_state.worker_idx = (app.worker_state.worker_idx + 1) % app.workers.len();
                app.worker_state.active_table = Default::default();
                app.worker_state.completed_table = Default::default();
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            let worker = &app.workers[app.worker_state.worker_idx];
            match app.worker_state.focused_panel {
                WorkerPanel::Metrics => {} // Sparklines are not scrollable
                WorkerPanel::ActiveTasks => {
                    let count = worker.tasks.len();
                    if count > 0 {
                        let selected = app
                            .worker_state
                            .active_table
                            .selected()
                            .map(|s| (s + 1).min(count - 1))
                            .unwrap_or(0);
                        app.worker_state.active_table.select(Some(selected));
                    }
                }
                WorkerPanel::CompletedTasks => {
                    let count = worker.completed_tasks.len();
                    if count > 0 {
                        let selected = app
                            .worker_state
                            .completed_table
                            .selected()
                            .map(|s| (s + 1).min(count - 1))
                            .unwrap_or(0);
                        app.worker_state.completed_table.select(Some(selected));
                    }
                }
            }
        }
        KeyCode::Up | KeyCode::Char('k') => match app.worker_state.focused_panel {
            WorkerPanel::Metrics => {} // Sparklines are not scrollable
            WorkerPanel::ActiveTasks => {
                let selected = app
                    .worker_state
                    .active_table
                    .selected()
                    .map(|s| s.saturating_sub(1))
                    .unwrap_or(0);
                app.worker_state.active_table.select(Some(selected));
            }
            WorkerPanel::CompletedTasks => {
                let selected = app
                    .worker_state
                    .completed_table
                    .selected()
                    .map(|s| s.saturating_sub(1))
                    .unwrap_or(0);
                app.worker_state.completed_table.select(Some(selected));
            }
        },
        KeyCode::Char('\t') => {
            // This is a literal tab character, not the Tab key — for panel switching
            // The Tab key without shift is handled above (goes back to cluster)
        }
        KeyCode::BackTab => {
            // Shift+Tab: cycle focus between panels
            app.worker_state.focused_panel = match app.worker_state.focused_panel {
                WorkerPanel::Metrics => WorkerPanel::ActiveTasks,
                WorkerPanel::ActiveTasks => WorkerPanel::CompletedTasks,
                WorkerPanel::CompletedTasks => WorkerPanel::Metrics,
            };
        }
        KeyCode::Char('r') => {
            if let Some(worker) = app.workers.get_mut(app.worker_state.worker_idx) {
                worker.completed_tasks.clear();
            }
        }
        _ => {}
    }
}
