mod app;
mod input;
mod state;
mod ui;

use app::App;
use clap::Parser;
use crossterm::event::{self, Event};
use ratatui::DefaultTerminal;
use std::time::{Duration, Instant};
use url::Url;

#[derive(Parser)]
#[command(
    name = "datafusion-distributed-console",
    about = "Console for monitoring DataFusion distributed workers"
)]
struct Args {
    /// Comma-delimited list of worker ports (assumed localhost)
    #[arg(long = "cluster-ports", value_delimiter = ',')]
    cluster_ports: Vec<u16>,

    /// Polling interval in milliseconds
    #[arg(long = "poll-interval", default_value = "100")]
    poll_interval: u64,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let args = Args::parse();

    let worker_urls: Vec<Url> = args
        .cluster_ports
        .iter()
        .map(|port| Url::parse(&format!("http://localhost:{port}")).expect("valid localhost URL"))
        .collect();

    let poll_interval = Duration::from_millis(args.poll_interval);
    let mut app = App::new(worker_urls);

    let mut terminal = ratatui::init();
    terminal.clear()?;

    let result = run_app(&mut terminal, &mut app, poll_interval).await;

    ratatui::restore();

    result
}

async fn run_app(
    terminal: &mut DefaultTerminal,
    app: &mut App,
    poll_interval: Duration,
) -> color_eyre::Result<()> {
    let mut last_poll = Instant::now();

    loop {
        // Poll workers on the gRPC tick interval
        if last_poll.elapsed() >= poll_interval {
            app.tick().await;
            last_poll = Instant::now();
        }

        // Render
        terminal.draw(|frame| ui::render(frame, app))?;

        // Check for keyboard input (16ms timeout ~ 60fps responsiveness)
        if event::poll(Duration::from_millis(16))? {
            if let Event::Key(key) = event::read()? {
                input::handle_key_event(app, key);
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}
