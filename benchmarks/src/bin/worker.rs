use std::error::Error;

use datafusion_distributed_benchmarks::remote_worker::{RemoteBenchmarkWorker, RemoteWorkerOpt};
use structopt::StructOpt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    RemoteBenchmarkWorker::builder(RemoteWorkerOpt::from_args())
        .serve()
        .await
}
