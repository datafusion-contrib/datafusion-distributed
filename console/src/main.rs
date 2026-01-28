mod app;
mod ui;

use app::App;
use crossterm::event::{self, Event};
use datafusion_distributed::{ConsoleControlServiceImpl, ConsoleControlServiceServer};
use ratatui::DefaultTerminal;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use structopt::StructOpt;
use tonic::transport::Server;

#[derive(StructOpt)]
#[structopt(
    name = "datafusion-distributed-console",
    about = "Console for monitoring DataFusion distributed workers"
)]
struct Args {
    /// Port for console control gRPC service
    #[structopt(long = "console-port", default_value = "9090")]
    console_port: u16,

    /// Address to bind console control service
    #[structopt(long = "console-bind-addr", default_value = "0.0.0.0")]
    console_bind_addr: String,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let args = Args::from_args();

    // Parse bind address
    let bind_addr: IpAddr = args.console_bind_addr.parse().unwrap_or_else(|_| {
        eprintln!(
            "Error: Invalid bind address '{}', using 0.0.0.0",
            args.console_bind_addr
        );
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))
    });

    // Start in IDLE state with gRPC server
    let (mut app, worker_tx) = App::new();

    // Start gRPC server in background
    let console_addr = SocketAddr::new(bind_addr, args.console_port);

    let control_service = ConsoleControlServiceImpl::new(worker_tx);
    let control_server = ConsoleControlServiceServer::new(control_service);

    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(control_server)
            .serve(console_addr)
            .await
        {
            eprintln!("Console gRPC server error: {e}");
        }
    });

    println!("Console control service listening on {console_addr}");

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
