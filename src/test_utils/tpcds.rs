use arrow::datatypes::{DataType, Field};
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::expressions::{CastColumnExpr, Column};
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use parquet::file::properties::WriterProperties;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const URL: &str = "https://github.com/apache/datafusion-benchmarks/archive/refs/heads/main.zip";

/// Load a single TPC-DS query by ID (1-99).
pub fn get_test_tpcds_query(id: usize) -> Result<String, DataFusionError> {
    let queries_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/tpcds/queries");

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
            internal_datafusion_err!("Failed to read query file {}: {e}", query_file.display())
        })?
        .trim()
        .to_string();

    Ok(query_sql)
}

/// Downloads the datafusion-benchmarks repository as a zip file
async fn download_benchmarks(dest_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    if dest_path.exists() {
        return Ok(());
    }

    // Create directory if it doesn't exist
    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Download the file
    let response = reqwest::get(URL).await?;
    let bytes = response.bytes().await?;

    // Write to file
    let mut file = fs::File::create(&dest_path)?;
    file.write_all(&bytes)?;

    Ok(())
}

/// Unzips the downloaded benchmarks zip file
fn unzip_benchmarks(
    zip_path: PathBuf,
    extract_to: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    if extract_to.exists() {
        return Ok(());
    }

    let file = fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut zip_file = archive.by_index(i)?;
        let file_name = zip_file.name();
        if !(file_name.contains("tpcds") && file_name.ends_with(".parquet")) {
            continue;
        }
        let outpath = extract_to.join(zip_file.mangled_name().file_name().unwrap());

        if let Some(parent) = outpath.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut outfile = fs::File::create(&outpath)?;
        std::io::copy(&mut zip_file, &mut outfile)?;
    }

    Ok(())
}

async fn repartition_parquet_file(
    file_path: PathBuf,
    dest_path: PathBuf,
    partitions: usize,
    use_dict_encoding: bool,
) -> Result<(), DataFusionError> {
    if !file_path.exists() {
        return internal_err!("Path {} does not exist", file_path.display());
    }
    let file_name = file_path.file_name().unwrap().to_str().unwrap();
    if !file_name.ends_with(".parquet") {
        return internal_err!("Path {} is not parquet", file_path.display());
    }
    let table_name = file_name.trim_end_matches(".parquet");

    if let Ok(dir) = fs::read_dir(&dest_path) {
        if dir.count() >= 1 {
            return Ok(());
        }
    }

    let ctx = SessionContext::new();
    ctx.sql("SET datafusion.execution.target_partitions=1")
        .await?;

    ctx.register_parquet(
        table_name,
        &file_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let table = ctx.table(table_name).await?;
    let mut plan = table.create_physical_plan().await?;
    if use_dict_encoding && table_name == "item" {
        let cols = ["i_brand", "i_category", "i_class", "i_color", "i_size"];
        plan = project_cols_as_dict(plan, &cols)?;
    } else if use_dict_encoding && table_name == "customer" {
        let cols = ["c_salutation"];
        plan = project_cols_as_dict(plan, &cols)?;
    } else if use_dict_encoding && table_name == "store" {
        let cols = ["s_state", "s_country"];
        plan = project_cols_as_dict(plan, &cols)?;
    }

    let plan = RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(partitions))?;
    ctx.write_parquet(
        Arc::new(plan),
        dest_path.to_str().unwrap(),
        Some(
            WriterProperties::builder()
                .set_dictionary_enabled(true)
                .build(),
        ),
    )
    .await?;

    Ok(())
}

fn project_cols_as_dict(
    plan: Arc<dyn ExecutionPlan>,
    cols: &[&str],
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let project = ProjectionExec::try_new(
        plan.schema()
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ProjectionExpr {
                expr: if cols.contains(&f.name().as_str()) {
                    Arc::new(CastColumnExpr::new(
                        Arc::new(Column::new(f.name(), i)),
                        f.clone(),
                        Arc::new(Field::new(
                            f.name(),
                            DataType::Dictionary(
                                Box::new(DataType::UInt16),
                                Box::new(DataType::Utf8),
                            ),
                            f.is_nullable(),
                        )),
                        None,
                    ))
                } else {
                    Arc::new(Column::new(f.name(), i))
                },
                alias: f.name().to_string(),
            }),
        plan,
    )?;
    Ok(Arc::new(project))
}

pub async fn prepare_tables(
    data_path: PathBuf,
    dest_path: PathBuf,
    partitions: usize,
) -> datafusion::common::Result<()> {
    for entry in fs::read_dir(data_path)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name = file_name.to_str().unwrap();
        if !file_name.ends_with(".parquet") {
            continue;
        }
        let table_name = file_name.trim_end_matches(".parquet");
        // Apply dictionary encoding if requested and materialize to disk
        /// Tables that should have dictionary encoding applied for testing
        const DICT_ENCODING_TABLES: &[&str] = &["item", "customer", "store"];

        repartition_parquet_file(
            entry.path(),
            dest_path.join(table_name),
            partitions,
            DICT_ENCODING_TABLES.contains(&table_name),
        )
        .await?;
    }
    Ok(())
}

pub async fn generate_tpcds_data(
    dir: &Path,
    sf: f64,
    partitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if sf != 1.0 {
        Err("Only scale factor 1.0 is supported for TPC-DS")?;
    }
    let base_path = dir.parent().unwrap();
    download_benchmarks(base_path.join("main.zip")).await?;
    unzip_benchmarks(base_path.join("main.zip"), base_path.join("downloaded"))?;
    prepare_tables(base_path.join("downloaded"), dir.to_path_buf(), partitions).await?;
    Ok(())
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
