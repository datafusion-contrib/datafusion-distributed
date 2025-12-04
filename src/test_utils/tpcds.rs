use datafusion::{
    arrow::{
        array::{Array, ArrayRef, DictionaryArray, StringArray, StringViewArray},
        datatypes::{DataType, Field, Schema, UInt16Type},
        record_batch::RecordBatch,
    },
    common::{internal_datafusion_err, internal_err},
    error::Result,
    execution::context::SessionContext,
    prelude::ParquetReadOptions,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

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

/// Tables that should have dictionary encoding applied for testing
const DICT_ENCODING_TABLES: &[&str] = &["item", "customer", "store"];

/// Force dictionary encoding for specific string columns in a table for extra test coverage.
fn force_dictionary_encoding_for_table(
    table_name: &str,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    let dict_columns = match table_name {
        "item" => vec!["i_brand", "i_category", "i_class", "i_color", "i_size"],
        "customer" => vec!["c_salutation"],
        "store" => vec!["s_state", "s_country"],
        _ => vec![], // No dictionary encoding for other tables
    };

    if dict_columns.is_empty() {
        return Ok(batch);
    }

    let schema = batch.schema();
    let mut new_fields = Vec::new();
    let mut new_columns = Vec::new();

    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);

        // Check if this column should be dictionary-encoded
        if dict_columns.contains(&field.name().as_str())
            && matches!(field.data_type(), DataType::Utf8 | DataType::Utf8View)
        {
            // Convert to dictionary encoding
            let string_data =
                if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                    string_array.iter().collect::<Vec<_>>()
                } else if let Some(view_array) = column.as_any().downcast_ref::<StringViewArray>() {
                    view_array.iter().collect::<Vec<_>>()
                } else {
                    return internal_err!("Expected string array for column {}", field.name());
                };

            let dict_array: DictionaryArray<UInt16Type> = string_data.into_iter().collect();
            let dict_field = Field::new(
                field.name(),
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                field.is_nullable(),
            );

            new_fields.push(dict_field);
            new_columns.push(Arc::new(dict_array) as ArrayRef);
        } else {
            new_fields.push((**field).clone());
            new_columns.push(column.clone());
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns).map_err(|e| internal_datafusion_err!("{}", e))
}

pub async fn register_tpcds_table(
    ctx: &SessionContext,
    table_name: &str,
    data_dir: Option<&Path>,
) -> Result<()> {
    register_tpcds_table_with_options(ctx, table_name, data_dir, false).await
}

pub async fn register_tpcds_table_with_options(
    ctx: &SessionContext,
    table_name: &str,
    data_dir: Option<&Path>,
    dict_encode_items_table: bool,
) -> Result<()> {
    let default_data_dir = get_data_dir();
    let data_path = data_dir.unwrap_or(&default_data_dir);

    // Apply dictionary encoding if requested and materialize to disk
    if dict_encode_items_table && DICT_ENCODING_TABLES.contains(&table_name) {
        let table_dir_path = data_path.join(table_name);
        if table_dir_path.is_dir() {
            let dict_table_path = data_path.join(format!("{table_name}_dict"));

            // Check if dictionary encoded version already exists
            if dict_table_path.exists() {
                // Use the existing dictionary encoded version
                ctx.register_parquet(
                    table_name,
                    &dict_table_path.to_string_lossy(),
                    ParquetReadOptions::default(),
                )
                .await?;
                return Ok(());
            }

            // Register temporarily to read the original data
            let temp_table_name = format!("temp_{table_name}");
            ctx.register_parquet(
                &temp_table_name,
                &table_dir_path.to_string_lossy(),
                ParquetReadOptions::default(),
            )
            .await?;

            // Read data and apply dictionary encoding
            let df = ctx.table(&temp_table_name).await?;
            let batches = df.collect().await?;

            let mut dict_batches = Vec::new();
            for batch in batches {
                dict_batches.push(force_dictionary_encoding_for_table(table_name, batch)?);
            }

            // Write dictionary-encoded data to disk
            if !dict_batches.is_empty() {
                fs::create_dir_all(&dict_table_path)?;
                let dict_file_path = dict_table_path.join("data.parquet");
                let file = fs::File::create(&dict_file_path)?;
                let props = WriterProperties::builder().build();
                let mut writer = ArrowWriter::try_new(file, dict_batches[0].schema(), Some(props))?;

                for batch in &dict_batches {
                    writer.write(batch)?;
                }
                writer.close()?;

                // Register the dictionary encoded table
                ctx.register_parquet(
                    table_name,
                    &dict_table_path.to_string_lossy(),
                    ParquetReadOptions::default(),
                )
                .await?;
            }

            // Deregister the temporary table
            ctx.deregister_table(&temp_table_name)?;
            return Ok(());
        }
    }

    // Use normal parquet registration for all tables
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
    register_tables_with_options(ctx, false).await
}

pub async fn register_tables_with_options(
    ctx: &SessionContext,
    dict_encode_items_table: bool,
) -> Result<Vec<String>> {
    let mut registered_tables = Vec::new();

    for &table_name in TPCDS_TABLES {
        register_tpcds_table_with_options(ctx, table_name, None, dict_encode_items_table).await?;
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
