use datafusion::{
    common::{internal_datafusion_err, internal_err},
    error::Result,
    execution::context::SessionContext,
    prelude::ParquetReadOptions,
};
use std::fs;
use std::path::Path;
use std::process::Command;

pub fn get_data_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/tpcds/data")
}

pub fn get_queries_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/tpcds/queries")
}

pub fn get_tpcds_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/tpcds")
}

/// Load a single TPC-DS query by ID (1-99).
pub fn get_test_tpcds_query(id: usize) -> Result<String> {
    let queries_dir = get_queries_dir();

    if !queries_dir.exists() {
        return internal_err!(
            "TPC-DS queries directory not found: {}",
            queries_dir.display()
        );
    }

    let query_file = queries_dir.join(format!("q{id}.sql"));

    if !query_file.exists() {
        return internal_err!("Query file not found: {}", query_file.display());
    }

    let query_sql = fs::read_to_string(&query_file)
        .map_err(|e| {
            internal_datafusion_err!("Failed to read query file {}: {}", query_file.display(), e)
        })?
        .trim()
        .to_string();

    Ok(query_sql)
}

pub const TPCDS_TABLES: &[&str] = &[
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

pub async fn register_tpcds_table(
    ctx: &SessionContext,
    table_name: &str,
    data_dir: Option<&Path>,
) -> Result<()> {
    let default_data_dir = get_data_dir();
    let data_path = data_dir.unwrap_or(&default_data_dir);

    // Check if this is a single parquet file
    let table_file_path = data_path.join(format!("{table_name}.parquet"));
    if table_file_path.is_file() {
        ctx.register_parquet(
            table_name,
            &table_file_path.to_string_lossy(),
            ParquetReadOptions::default(),
        )
        .await?;
        return Ok(());
    }

    // Check if this is a directory with multiple parquet files
    let table_dir_path = data_path.join(table_name);
    if table_dir_path.is_dir() {
        ctx.register_parquet(
            table_name,
            &table_dir_path.to_string_lossy(),
            ParquetReadOptions::default(),
        )
        .await?;
        return Ok(());
    }

    internal_err!(
        "TPCDS table not found: {} (looked for both file and directory)",
        table_name
    )
}

pub async fn register_tables(ctx: &SessionContext) -> Result<Vec<String>> {
    let mut registered_tables = Vec::new();

    for &table_name in TPCDS_TABLES {
        register_tpcds_table(ctx, table_name, None).await?;
        registered_tables.push(table_name.to_string());
    }

    Ok(registered_tables)
}

/// Generate TPC-DS data using the generation script
pub fn generate_tpcds_data(scale_factor: &str) -> Result<()> {
    let tpcds_dir = get_tpcds_dir();
    let generate_script = tpcds_dir.join("generate.sh");

    if !generate_script.exists() {
        return internal_err!(
            "TPC-DS generation script not found: {}",
            generate_script.display()
        );
    }

    let output = Command::new("bash")
        .arg(&generate_script)
        .arg(scale_factor)
        .current_dir(&tpcds_dir)
        .output()
        .map_err(|e| {
            internal_datafusion_err!("Failed to execute TPC-DS generation script: {}", e)
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return internal_err!(
            "TPC-DS generation failed:\nstdout: {}\nstderr: {}",
            stdout,
            stderr
        );
    }

    Ok(())
}
