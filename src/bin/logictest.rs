use clap::Parser;
use datafusion_distributed::test_utils::sqllogictest::DatafusionDistributedDB;
use sqllogictest::Runner;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "logictest")]
#[command(
    about = "A SQLLogicTest runner for DataFusion Distributed. Docs: https://sqlite.org/sqllogictest/doc/trunk/about.wiki"
)]
struct Args {
    /// Files or directories to run
    #[arg(required = true)]
    files: Vec<PathBuf>,

    /// Update test files with actual output rather than verifying the existing output
    #[arg(long = "override")]
    override_mode: bool,

    /// Number of workers
    #[arg(long, default_value = "3")]
    num_workers: usize,
}

async fn run<D, M>(
    paths: Vec<PathBuf>,
    runner: &mut Runner<D, M>,
    override_mode: bool,
) -> Result<(), Box<dyn std::error::Error>>
where
    D: sqllogictest::AsyncDB,
    M: sqllogictest::MakeConnection<Conn = D>,
{
    let mut queue = paths;
    let mut idx = 0;
    while idx < queue.len() {
        let file_path = &queue[idx];
        idx += 1;
        if !file_path.is_file() {
            queue.extend(
                expand_directory(file_path).await.unwrap_or_else(|_| {
                    panic!("Failed to expand directory: {}", file_path.display())
                }),
            );
            continue;
        }
        let file_path_str = file_path.to_str().expect("Invalid file path");

        let result = match override_mode {
            true => {
                runner
                    .update_test_file(
                        file_path_str,
                        " ",
                        sqllogictest::default_validator,
                        sqllogictest::default_column_validator,
                    )
                    .await
            }

            false => runner.run_file_async(file_path).await.map_err(|e| e.into()),
        };
        match result {
            Ok(_) => println!("🟢 Success: {}", file_path.display()),
            Err(e) => {
                eprintln!("{e}");
                return Err(format!("🔴 Failure: {}", file_path.display()).into());
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut runner =
        Runner::new(
            move || async move { Ok(DatafusionDistributedDB::new(args.num_workers).await) },
        );

    run(args.files, &mut runner, args.override_mode).await?;

    Ok(())
}

async fn expand_directory(dir_path: &PathBuf) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut entries: Vec<_> = std::fs::read_dir(dir_path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "slt")
                .unwrap_or(false)
        })
        .map(|entry| entry.path())
        .collect();

    // Sort entries for consistent order
    entries.sort();

    Ok(entries)
}
