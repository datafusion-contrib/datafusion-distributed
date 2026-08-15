use datafusion::common::{DataFusionError, exec_err, internal_datafusion_err, internal_err};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::fs;
use std::path::Path;

/// Returns the workspace root directory (parent of the benchmarks crate).
fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("benchmarks crate should be inside a workspace")
}

pub fn get_queries(path: &str) -> Vec<String> {
    let queries_dir = workspace_root().join(path);
    let mut result = vec![];
    for file in queries_dir.read_dir().unwrap() {
        let file = file.unwrap();
        let file_name = file.file_name().display().to_string();
        if file_name.ends_with(".sql") {
            result.push(file_name.trim_end_matches(".sql").to_string());
        }
    }

    // Each element might be something like q12.sql or custom2.sql.
    // This orders the string list by the parsed integer number inside an arbitrary string.
    result.sort_by(|a, b| {
        // Extract numbers from both strings
        let extract_number = |s: &str| -> Option<u32> {
            s.chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<u32>()
                .ok()
        };

        match (extract_number(a), extract_number(b)) {
            (Some(num_a), Some(num_b)) => num_a.cmp(&num_b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.cmp(b), // Fall back to lexicographic ordering
        }
    });
    result
}

pub fn get_query(path: &str, id: &str) -> Result<String, DataFusionError> {
    let queries_dir = workspace_root().join(path);

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

/// Register each parquet table directory under `data_path`.
///
/// `--dataset` must point at a generated *variant* (e.g. `testdata/clickbench/0-100`),
/// not the suite root. Suite roots also contain `queries/` and other non-table
/// directories; those are skipped. Directories that do not contain parquet files
/// are skipped. If nothing is registered, return an error instead of silently
/// running queries against missing tables.
pub async fn register_tables(
    ctx: &SessionContext,
    data_path: &Path,
) -> Result<(), DataFusionError> {
    let mut registered = 0usize;
    for entry in fs::read_dir(data_path)? {
        let path = entry?.path();
        if !path.is_dir() {
            continue;
        }
        let table_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if table_name.is_empty() || table_name == "queries" || table_name.starts_with('.') {
            continue;
        }
        if !dir_contains_parquet(&path)? {
            continue;
        }
        ctx.register_parquet(
            table_name,
            path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;
        registered += 1;
    }
    if registered == 0 {
        return exec_err!(
            "No parquet tables found under {}. Pass a generated variant \
             (`tpch/sf1`, `tpcds/sf1`, `clickbench/0-100`), not the suite root. \
             See benchmarks/README.md.",
            data_path.display()
        );
    }
    Ok(())
}

fn dir_contains_parquet(path: &Path) -> Result<bool, DataFusionError> {
    for entry in fs::read_dir(path)? {
        if entry?.path().extension().and_then(|e| e.to_str()) == Some("parquet") {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dfd-register-tables-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_int_parquet(path: &Path) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn register_tables_skips_queries_and_requires_parquet() {
        let root = unique_temp_dir();
        fs::create_dir_all(root.join("queries")).unwrap();
        fs::write(root.join("queries").join("q1.sql"), "select 1").unwrap();
        fs::create_dir_all(root.join("notes")).unwrap();
        fs::write(root.join("notes").join("readme.txt"), "hi").unwrap();

        let ctx = SessionContext::new();
        let err = register_tables(&ctx, &root).await.unwrap_err();
        assert!(
            err.to_string().contains("No parquet tables"),
            "unexpected error: {err}"
        );

        write_int_parquet(&root.join("hits").join("0.parquet"));
        register_tables(&ctx, &root).await.unwrap();
        assert!(ctx.table("hits").await.is_ok());
        assert!(ctx.table("queries").await.is_err());

        fs::remove_dir_all(&root).ok();
    }
}
