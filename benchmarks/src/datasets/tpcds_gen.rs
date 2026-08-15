use super::tpcds_schema;
use arrow::datatypes::{Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use parquet::arrow::arrow_writer::ArrowWriter;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;
use tpcdsgen::config::{Session, Table};
use tpcdsgen::output::Iso8859Writer;
use tpcdsgen::row::{
    CallCenterRowGenerator, CatalogPageRowGenerator, CatalogSalesRowGenerator,
    CustomerAddressRowGenerator, CustomerDemographicsRowGenerator, CustomerRowGenerator,
    DateDimRowGenerator, HouseholdDemographicsRowGenerator, IncomeBandRowGenerator,
    InventoryRowGenerator, ItemRowGenerator, PromotionRowGenerator, ReasonRowGenerator,
    RowGenerator, ShipModeRowGenerator, StoreRowGenerator, StoreSalesRowGenerator,
    TimeDimRowGenerator, WarehouseRowGenerator, WebPageRowGenerator, WebSalesRowGenerator,
    WebSiteRowGenerator,
};
use tpcdsgen::types::Date;

/// Generate TPC-DS tables at `sf` as one `<table>.parquet` file each in `dest`.
pub async fn generate_raw_parquet(dest: &Path, sf: f64) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dest)?;
    if has_parquet_files(dest) {
        return Ok(());
    }

    let dat_dir = dest.join("dat");
    fs::create_dir_all(&dat_dir)?;
    generate_dat_files(&dat_dir, sf)?;
    convert_dat_dir(&dat_dir, dest).await?;
    let _ = fs::remove_dir_all(&dat_dir);
    Ok(())
}

fn has_parquet_files(dir: &Path) -> bool {
    fs::read_dir(dir)
        .map(|entries| {
            entries.filter_map(|e| e.ok()).any(|e| {
                e.path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext == "parquet")
            })
        })
        .unwrap_or(false)
}

fn generate_dat_files(dat_dir: &Path, sf: f64) -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::new(
        sf,
        dat_dir.to_string_lossy().into_owned(),
        ".dat".to_string(),
        None,
        String::new(),
        '|',
        false,
        false,
        1,
        true,
    );

    for table in Table::main_tables() {
        generate_table(table, &session)?;
    }
    Ok(())
}

fn generate_table(table: Table, session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    match table {
        Table::CallCenter => generate_simple::<CallCenterRowGenerator>(table, session),
        Table::CatalogPage => generate_simple::<CatalogPageRowGenerator>(table, session),
        Table::Customer => generate_simple::<CustomerRowGenerator>(table, session),
        Table::CustomerAddress => generate_simple::<CustomerAddressRowGenerator>(table, session),
        Table::CustomerDemographics => {
            generate_simple::<CustomerDemographicsRowGenerator>(table, session)
        }
        Table::DateDim => generate_simple::<DateDimRowGenerator>(table, session),
        Table::HouseholdDemographics => {
            generate_simple::<HouseholdDemographicsRowGenerator>(table, session)
        }
        Table::IncomeBand => generate_simple::<IncomeBandRowGenerator>(table, session),
        Table::Item => generate_simple::<ItemRowGenerator>(table, session),
        Table::Promotion => generate_simple::<PromotionRowGenerator>(table, session),
        Table::Reason => generate_simple::<ReasonRowGenerator>(table, session),
        Table::ShipMode => generate_simple::<ShipModeRowGenerator>(table, session),
        Table::Store => generate_simple::<StoreRowGenerator>(table, session),
        Table::TimeDim => generate_simple::<TimeDimRowGenerator>(table, session),
        Table::Warehouse => generate_simple::<WarehouseRowGenerator>(table, session),
        Table::WebPage => generate_simple::<WebPageRowGenerator>(table, session),
        Table::WebSite => generate_simple::<WebSiteRowGenerator>(table, session),
        Table::StoreSales => generate_store_sales(session),
        Table::StoreReturns => Ok(()),
        Table::CatalogSales => generate_catalog_sales(session),
        Table::CatalogReturns => Ok(()),
        Table::WebSales => generate_web_sales(session),
        Table::WebReturns => Ok(()),
        Table::Inventory => generate_inventory(session),
        Table::DbgenVersion => Ok(()),
        _ => Ok(()),
    }
}

trait RowGeneratorFactory: RowGenerator + Sized {
    fn create() -> Self;
}

macro_rules! impl_factory {
    ($($gen:ty),*) => {
        $(
            impl RowGeneratorFactory for $gen {
                fn create() -> Self {
                    Self::new()
                }
            }
        )*
    };
}

impl_factory!(
    CallCenterRowGenerator,
    CatalogPageRowGenerator,
    CustomerRowGenerator,
    CustomerAddressRowGenerator,
    CustomerDemographicsRowGenerator,
    DateDimRowGenerator,
    HouseholdDemographicsRowGenerator,
    IncomeBandRowGenerator,
    ItemRowGenerator,
    PromotionRowGenerator,
    ReasonRowGenerator,
    ShipModeRowGenerator,
    StoreRowGenerator,
    TimeDimRowGenerator,
    WarehouseRowGenerator,
    WebPageRowGenerator,
    WebSiteRowGenerator
);

fn output_path(table: Table, session: &Session) -> std::path::PathBuf {
    Path::new(session.get_target_directory()).join(format!(
        "{}{}",
        table.get_name(),
        session.get_suffix()
    ))
}

fn generate_simple<G: RowGeneratorFactory>(
    table: Table,
    session: &Session,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut generator = G::create();
    let row_count = session.get_scaling().get_row_count(table);
    let path = output_path(table, session);
    let mut writer = Iso8859Writer::new(BufWriter::new(File::create(&path)?));

    println!("Generating {} ({row_count} rows)...", table.get_name());
    for row_number in 1..=row_count {
        let result = generator.generate_row_and_child_rows(row_number, session, None, None)?;
        for row in result.get_rows() {
            row.write_to(&mut writer, session.get_separator())?;
        }
        generator.consume_remaining_seeds_for_row();
    }
    writer.flush()?;
    Ok(())
}

fn generate_store_sales(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    generate_sales_pair(
        session,
        Table::StoreSales,
        Table::StoreReturns,
        StoreSalesRowGenerator::new(),
        |g| g.consume_child_seeds(),
    )
}

fn generate_catalog_sales(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    generate_sales_pair(
        session,
        Table::CatalogSales,
        Table::CatalogReturns,
        CatalogSalesRowGenerator::new(),
        |g| g.consume_child_seeds(),
    )
}

fn generate_web_sales(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    generate_sales_pair(
        session,
        Table::WebSales,
        Table::WebReturns,
        WebSalesRowGenerator::new(),
        |g| g.consume_child_seeds(),
    )
}

fn generate_sales_pair<G, F>(
    session: &Session,
    sales: Table,
    returns: Table,
    mut generator: G,
    mut consume_child: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    G: RowGenerator,
    F: FnMut(&mut G),
{
    let num_orders = session.get_scaling().get_row_count(sales);
    let sales_path = output_path(sales, session);
    let returns_path = output_path(returns, session);
    let mut sales_writer = Iso8859Writer::new(BufWriter::new(File::create(&sales_path)?));
    let mut returns_writer = Iso8859Writer::new(BufWriter::new(File::create(&returns_path)?));

    println!(
        "Generating {} + {}...",
        sales.get_name(),
        returns.get_name()
    );
    let mut row_number = 1i64;
    while row_number <= num_orders {
        let result = generator.generate_row_and_child_rows(row_number, session, None, None)?;
        let rows = result.get_rows();
        if !rows.is_empty() {
            rows[0].write_to(&mut sales_writer, session.get_separator())?;
        }
        if rows.len() > 1 {
            rows[1].write_to(&mut returns_writer, session.get_separator())?;
        }
        if result.should_end_row() {
            generator.consume_remaining_seeds_for_row();
            consume_child(&mut generator);
            row_number += 1;
        }
    }
    sales_writer.flush()?;
    returns_writer.flush()?;
    Ok(())
}

fn generate_inventory(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    let mut generator = InventoryRowGenerator::new();
    let scaling = session.get_scaling();
    let item_count = scaling.get_id_count(Table::Item);
    let warehouse_count = scaling.get_row_count(Table::Warehouse);
    let n_days = Date::JULIAN_DATE_MAXIMUM - Date::JULIAN_DATE_MINIMUM;
    let n_weeks = (n_days + 7) / 7;
    let num_rows = item_count * warehouse_count * i64::from(n_weeks);

    let path = output_path(Table::Inventory, session);
    let mut writer = Iso8859Writer::new(BufWriter::new(File::create(&path)?));
    println!("Generating inventory ({num_rows} rows)...");
    for row_number in 1..=num_rows {
        let result = generator.generate_row_and_child_rows(row_number, session, None, None)?;
        for row in result.get_rows() {
            row.write_to(&mut writer, session.get_separator())?;
        }
        generator.consume_remaining_seeds_for_row();
    }
    writer.flush()?;
    Ok(())
}

async fn convert_dat_dir(dat_dir: &Path, dest: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for entry in fs::read_dir(dat_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("dat") {
            continue;
        }
        let table = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or("invalid DAT file name")?;
        let Some(schema) = tpcds_schema::table_schema(table) else {
            continue;
        };
        let parquet_path = dest.join(format!("{table}.parquet"));
        convert_dat_to_parquet(&path, &parquet_path, schema).await?;
    }
    Ok(())
}

async fn convert_dat_to_parquet(
    dat_path: &Path,
    parquet_path: &Path,
    schema: arrow::datatypes::SchemaRef,
) -> Result<(), Box<dyn std::error::Error>> {
    let metadata = fs::metadata(dat_path)?;
    if metadata.len() == 0 {
        let file = File::create(parquet_path)?;
        ArrowWriter::try_new(file, schema, None)?.close()?;
        return Ok(());
    }

    // DAT rows terminate with a trailing '|', which shows up as an extra empty field.
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let output_cols: Vec<String> = fields.iter().map(|f| f.name().to_string()).collect();
    fields.push(Field::new(
        "_trailing",
        arrow::datatypes::DataType::Utf8,
        true,
    ));
    let read_schema = Arc::new(Schema::new(fields));

    let ctx = SessionContext::new();
    let df = ctx
        .read_csv(
            dat_path.to_str().ok_or("DAT path is not valid UTF-8")?,
            CsvReadOptions::new()
                .delimiter(b'|')
                .has_header(false)
                .schema(read_schema.as_ref())
                .file_extension(".dat"),
        )
        .await?;
    let col_refs: Vec<&str> = output_cols.iter().map(String::as_str).collect();
    let df = df.select_columns(&col_refs)?;
    df.write_parquet(
        parquet_path
            .to_str()
            .ok_or("parquet path is not valid UTF-8")?,
        DataFrameWriteOptions::new().with_single_file_output(true),
        None,
    )
    .await?;
    Ok(())
}
