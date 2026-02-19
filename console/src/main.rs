mod app;
mod ui;

use app::App;
use crossterm::event::{self, Event};
use ratatui::DefaultTerminal;
use std::time::Duration;
use structopt::StructOpt;
use url::Url;

#[derive(StructOpt)]
#[structopt(
    name = "datafusion-distributed-console",
    about = "Console for monitoring DataFusion distributed workers"
)]
struct Args {
    /// Comma-delimited list of worker ports (e.g. 8080,8081)
    #[structopt(long = "cluster-ports", use_delimiter = true)]
    cluster_ports: Vec<u16>,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let args = Args::from_args();

    let worker_urls: Vec<Url> = args
        .cluster_ports
        .iter()
        .map(|port| Url::parse(&format!("http://localhost:{port}")).expect("valid localhost URL"))
        .collect();

    let mut app = App::new(worker_urls);

    // Initialize terminal
    let mut terminal = ratatui::init();
    terminal.clear()?;

    // Run TUI loop
    let result = run_app(&mut terminal, &mut app).await;

    ratatui::restore();

    result
}

/// Main application loop
async fn run_app(terminal: &mut DefaultTerminal, app: &mut App) -> color_eyre::Result<()> {
    loop {
        app.tick().await;

        terminal.draw(|frame| ui::render(frame, app))?;

        if event::poll(Duration::from_millis(10))? {
            if let Event::Key(key) = event::read()? {
                app.handle_key_event(key);
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}
