mod app;
mod input;
mod state;
mod ui;
mod worker;

use app::App;
use crossterm::event::{self, Event};
use ratatui::DefaultTerminal;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use url::Url;

#[derive(StructOpt)]
#[structopt(
    name = "datafusion-distributed-console",
    about = "Console for monitoring DataFusion distributed workers"
)]
struct Args {
    /// Port of a worker to connect to for auto-discovery.
    /// The console calls GetClusterWorkers on this worker to discover the full cluster.
    port: u16,

    /// Polling interval in milliseconds
    #[structopt(long = "poll-interval", default_value = "100")]
    poll_interval: u64,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let args = Args::from_args();

    let seed_url =
        Url::parse(&format!("http://localhost:{}", args.port)).expect("valid URL");

    let poll_interval = Duration::from_millis(args.poll_interval);
    let mut app = App::new(seed_url);

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
        if last_poll.elapsed() >= poll_interval {
            app.tick().await;
            last_poll = Instant::now();
        }

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
