use super::common;
use arrow::datatypes::{DataType, Field};
use datafusion::common::internal_err;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::expressions::{CastExpr, Column};
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

pub fn get_queries() -> Vec<String> {
    common::get_queries("testdata/tpcds/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    common::get_query("testdata/tpcds/queries", id)
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

/// Unzips TPC-DS parquet files for `sf` from the downloaded benchmarks zip.
fn unzip_benchmarks(
    zip_path: PathBuf,
    extract_to: PathBuf,
    sf: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    if extract_to.exists() {
        return Ok(());
    }

    let file = fs::File::open(&zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    let sf_marker = format!("tpcds/data/sf{}/", format_scale_factor(sf));
    let mut extracted = 0usize;

    for i in 0..archive.len() {
        let mut zip_file = archive.by_index(i)?;
        let file_name = zip_file.name();
        if !(file_name.contains(&sf_marker) && file_name.ends_with(".parquet")) {
            continue;
        }
        let outpath = extract_to.join(zip_file.mangled_name().file_name().unwrap());

        if let Some(parent) = outpath.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut outfile = fs::File::create(&outpath)?;
        std::io::copy(&mut zip_file, &mut outfile)?;
        extracted += 1;
    }

    if extracted == 0 {
        fs::remove_dir_all(&extract_to).ok();
        return Err(format!(
            "No TPC-DS parquet files for scale factor {} in {}",
            format_scale_factor(sf),
            zip_path.display()
        )
        .into());
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

    if let Ok(dir) = fs::read_dir(&dest_path)
        && dir.count() >= 1
    {
        return Ok(());
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
                    Arc::new(CastExpr::new_with_target_field(
                        Arc::new(Column::new(f.name(), i)),
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

async fn prepare_tables(
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

/// Directory suffix for a scale factor (`1.0` → `"1"`, `0.01` → `"0.01"`).
pub fn format_scale_factor(sf: f64) -> String {
    if sf.fract() == 0.0 && sf.is_finite() && sf.abs() <= i64::MAX as f64 {
        format!("{}", sf as i64)
    } else {
        format!("{sf}")
    }
}

/// TPC-DS scale factors supported by the generator: `(0, 100000]`.
pub fn validate_scale_factor(sf: f64) -> Result<(), Box<dyn std::error::Error>> {
    if !sf.is_finite() || sf <= 0.0 || sf > 100_000.0 {
        Err(format!(
            "TPC-DS scale factor must be in (0, 100000], got {sf}"
        ))?;
    }
    Ok(())
}

pub async fn generate_data(
    dir: &Path,
    sf: f64,
    partitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_scale_factor(sf)?;
    fs::create_dir_all(dir)?;
    let base_path = dir.parent().unwrap();
    let raw = raw_table_dir(base_path, sf).await?;
    prepare_tables(raw, dir.to_path_buf(), partitions).await?;
    Ok(())
}

async fn raw_table_dir(base_path: &Path, sf: f64) -> Result<PathBuf, Box<dyn std::error::Error>> {
    // SF1 is shipped as pre-built parquet in datafusion-benchmarks.
    if sf == 1.0 {
        download_benchmarks(base_path.join("main.zip")).await?;
        let extracted = base_path.join("downloaded");
        unzip_benchmarks(base_path.join("main.zip"), extracted.clone(), sf)?;
        return Ok(extracted);
    }

    let generated = base_path.join(format!("generated_sf{}", format_scale_factor(sf)));
    super::tpcds_gen::generate_raw_parquet(&generated, sf).await?;
    Ok(generated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_scale_factor_drops_trailing_integer_fraction() {
        assert_eq!(format_scale_factor(1.0), "1");
        assert_eq!(format_scale_factor(10.0), "10");
        assert_eq!(format_scale_factor(0.01), "0.01");
        // sf1/ must not be a prefix of sf10/
        assert!(
            !format!("tpcds/data/sf{}/", format_scale_factor(10.0))
                .contains(&format!("tpcds/data/sf{}/", format_scale_factor(1.0)))
        );
    }

    #[test]
    fn validate_scale_factor_accepts_supported_range() {
        validate_scale_factor(0.01).unwrap();
        validate_scale_factor(1.0).unwrap();
        validate_scale_factor(10.0).unwrap();
        validate_scale_factor(100_000.0).unwrap();
    }

    #[test]
    fn validate_scale_factor_rejects_out_of_range() {
        assert!(validate_scale_factor(0.0).is_err());
        assert!(validate_scale_factor(-1.0).is_err());
        assert!(validate_scale_factor(100_000.1).is_err());
        assert!(validate_scale_factor(f64::NAN).is_err());
    }
}
