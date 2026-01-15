use datafusion_distributed::PingRequest;
use datafusion_distributed::protobuf::observability::observability_service_client::ObservabilityServiceClient;
use ratatui::widgets::Paragraph;
use ratatui::{DefaultTerminal, Frame};

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let mut client =
        ObservabilityServiceClient::connect("http://127.0.0.1:8080".to_string()).await?;
    let resp = client.ping(PingRequest {}).await?;
    let message = format!("ping ok (value: {})", resp.into_inner().value);

    color_eyre::install()?;
    ratatui::run(|terminal| app(terminal, &message))?;
    Ok(())
}

fn app(terminal: &mut DefaultTerminal, message: &str) -> std::io::Result<()> {
    loop {
        terminal.draw(|frame| render(frame, message))?;
        if crossterm::event::read()?.is_key_press() {
            break Ok(());
        }
    }
}

fn render(frame: &mut Frame, message: &str) {
    frame.render_widget(Paragraph::new(message), frame.area());
}
