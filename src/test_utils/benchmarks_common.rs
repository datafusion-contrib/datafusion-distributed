use datafusion::common::{DataFusionError, internal_datafusion_err, internal_err};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::fs;
use std::path::Path;

pub(crate) fn get_queries(path: &str) -> Vec<String> {
    let queries_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    let mut result = vec![];
    for file in queries_dir.read_dir().unwrap() {
        let file = file.unwrap();
        let file_name = file.file_name().display().to_string();
        if file_name.ends_with(".sql") {
            result.push(file_name.trim_end_matches(".sql").to_string());
        }
    }
    result
}

pub(crate) fn get_query(path: &str, id: &str) -> Result<String, DataFusionError> {
    let queries_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);

    if !queries_dir.exists() {
        return internal_err!(
            "Benchmark queries directory not found: {}",
            queries_dir.display()
        );
    }

    let query_file = queries_dir.join(format!("{id}.sql"));

    if !query_file.exists() {
        return internal_err!("Query file not found: {}", query_file.display());
    }

    let query_sql = fs::read_to_string(&query_file)
        .map_err(|e| {
            internal_datafusion_err!("Failed to read query file {}: {e}", query_file.display())
        })?
        .trim()
        .to_string();

    Ok(query_sql)
}

pub async fn register_tables(
    ctx: &SessionContext,
    data_path: &Path,
) -> Result<(), DataFusionError> {
    for entry in fs::read_dir(data_path)? {
        let path = entry?.path();
        if path.is_dir() {
            let table_name = path.file_name().unwrap().to_str().unwrap();
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        }
    }
    Ok(())
}
