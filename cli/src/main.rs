use clap::{Parser, Subcommand};
use std::process::ExitCode;

mod query;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand, PartialEq)]
enum Command {
    /// Run queries in an interactive shell, or from commands and files, like datafusion-cli does,
    /// but distributed across in-memory workers.
    Query(query::Args),
}

#[tokio::main]
/// Runs the given subcommand, then handles printing errors and returning the correct exit code
pub async fn main() -> ExitCode {
    env_logger::init();
    let result = match Args::parse().command {
        Command::Query(args) => query::run(args).await,
    };
    if let Err(e) = result {
        println!("Error: {e}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
