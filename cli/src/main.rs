// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// File mainly copied from https://github.com/apache/datafusion/blob/main/datafusion-cli/src/main.rs

use clap::Parser;
use datafusion::common::config_err;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::ExplainFormat;
use datafusion::prelude::SessionContext;
use datafusion_cli::catalog::DynamicObjectStoreCatalog;
use datafusion_cli::object_storage::instrumented::InstrumentedObjectStoreRegistry;
use datafusion_cli::{
    DATAFUSION_CLI_VERSION, exec,
    print_format::PrintFormat,
    print_options::{MaxRows, PrintOptions},
};
use datafusion_distributed::test_utils::in_memory_channel_resolver::{
    InMemoryChannelResolver, InMemoryWorkerResolver,
};
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule};
use std::env;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short = 'p',
        long,
        help = "Path to your data, default to current directory",
        value_parser(parse_valid_data_dir)
    )]
    data_path: Option<String>,

    #[clap(
        short = 'b',
        long,
        help = "The batch size of each query, or use DataFusion default",
        value_parser(parse_batch_size)
    )]
    batch_size: Option<usize>,

    #[clap(
        short = 'c',
        long,
        num_args = 0..,
        help = "Execute the given command string(s), then exit. Commands are expected to be non empty.",
        value_parser(parse_command)
    )]
    command: Vec<String>,

    #[clap(
        short,
        long,
        num_args = 0..,
        help = "Execute commands from file(s), then exit",
        value_parser(parse_valid_file)
    )]
    file: Vec<String>,

    #[clap(
        short = 'r',
        long,
        num_args = 0..,
        help = "Run the provided files on startup instead of ~/.datafusionrc",
        value_parser(parse_valid_file),
        conflicts_with = "file"
    )]
    rc: Option<Vec<String>>,

    #[clap(long, value_enum, default_value_t = PrintFormat::Automatic)]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,

    #[clap(
        long,
        help = "The max number of rows to display for 'Table' format\n[possible values: numbers(0/10/...), inf(no limit)]",
        default_value = "40"
    )]
    maxrows: MaxRows,

    #[clap(long, help = "Enables console syntax highlighting")]
    color: bool,
}

#[tokio::main]
/// Calls [`main_inner`], then handles printing errors and returning the correct exit code
pub async fn main() -> ExitCode {
    if let Err(e) = main_inner().await {
        println!("Error: {e}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

/// Main CLI entrypoint
async fn main_inner() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if !args.quiet {
        println!("Distributed DataFusion CLI v{DATAFUSION_CLI_VERSION}");
    }

    if let Some(ref path) = args.data_path {
        let p = Path::new(path);
        env::set_current_dir(p)?;
    };

    let session_config = get_session_config(&args)?;

    let mut rt_builder = RuntimeEnvBuilder::new();

    let instrumented_registry = Arc::new(InstrumentedObjectStoreRegistry::new());
    rt_builder = rt_builder.with_object_store_registry(instrumented_registry.clone());

    let runtime_env = rt_builder.build_arc()?;

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(16))
        .with_distributed_channel_resolver(InMemoryChannelResolver::default())
        .build();

    // enable dynamic file query
    let ctx = SessionContext::from(state).enable_url_table();
    ctx.refresh_catalogs().await?;
    // install dynamic catalog provider that can register required object stores
    ctx.register_catalog_list(Arc::new(DynamicObjectStoreCatalog::new(
        ctx.state().catalog_list().clone(),
        ctx.state_weak_ref(),
    )));

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
        maxrows: args.maxrows,
        color: args.color,
        instrumented_registry: Arc::clone(&instrumented_registry),
    };

    let commands = args.command;
    let files = args.file;
    let rc = args.rc.unwrap_or_else(|| {
        let mut files = Vec::new();
        let home = dirs::home_dir();
        if let Some(p) = home {
            let home_rc = p.join(".datafusionrc");
            if home_rc.exists() {
                files.push(home_rc.into_os_string().into_string().unwrap());
            }
        }
        files
    });

    if commands.is_empty() && files.is_empty() {
        if !rc.is_empty() {
            exec::exec_from_files(&ctx, rc, &print_options).await?;
        }
        return exec::exec_from_repl(&ctx, &mut print_options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)));
    }

    if !files.is_empty() {
        exec::exec_from_files(&ctx, files, &print_options).await?;
    }

    if !commands.is_empty() {
        exec::exec_from_commands(&ctx, commands, &print_options).await?;
    }

    Ok(())
}

/// Get the session configuration based on the provided arguments
/// and environment settings.
fn get_session_config(args: &Args) -> Result<SessionConfig> {
    // Read options from environment variables and merge with command line options
    let mut config_options = ConfigOptions::from_env()?;

    if let Some(batch_size) = args.batch_size {
        if batch_size == 0 {
            return config_err!("batch_size must be greater than 0");
        }
        config_options.execution.batch_size = batch_size;
    };

    // use easier to understand "tree" mode by default
    // if the user hasn't specified an explain format in the environment
    if env::var_os("DATAFUSION_EXPLAIN_FORMAT").is_none() {
        config_options.explain.format = ExplainFormat::Indent;
    }

    // in the CLI, we want to show NULL values rather the empty strings
    if env::var_os("DATAFUSION_FORMAT_NULL").is_none() {
        config_options.format.null = String::from("NULL");
    }

    let session_config = SessionConfig::from(config_options).with_information_schema(true);
    Ok(session_config)
}

fn parse_valid_file(dir: &str) -> Result<String, String> {
    if Path::new(dir).is_file() {
        Ok(dir.to_string())
    } else {
        Err(format!("Invalid file '{dir}'"))
    }
}

fn parse_valid_data_dir(dir: &str) -> Result<String, String> {
    if Path::new(dir).is_dir() {
        Ok(dir.to_string())
    } else {
        Err(format!("Invalid data directory '{dir}'"))
    }
}

fn parse_batch_size(size: &str) -> Result<usize, String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(size),
        _ => Err(format!("Invalid batch size '{size}'")),
    }
}

fn parse_command(command: &str) -> Result<String, String> {
    if !command.is_empty() {
        Ok(command.to_string())
    } else {
        Err("-c flag expects only non empty commands".to_string())
    }
}
