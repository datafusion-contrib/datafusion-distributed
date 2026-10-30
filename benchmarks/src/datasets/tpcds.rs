use super::parquet_layout::{ParquetLayout, TableColumns, TableWriter};
use super::progress::dataset_progress;
use super::{common, output::DatasetOutput};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::exec_datafusion_err;
use datafusion::error::DataFusionError;
use indicatif::{HumanCount, ProgressBar};
use std::sync::Arc;
use tpcdsgen::config::Options;
use tpcdsgen_arrow::{
    CallCenterArrow, CatalogPageArrow, CatalogReturnsArrow, CatalogSalesArrow,
    CustomerAddressArrow, CustomerArrow, CustomerDemographicsArrow, DateDimArrow,
    HouseholdDemographicsArrow, IncomeBandArrow, InventoryArrow, ItemArrow, PromotionArrow,
    ReasonArrow, ShipModeArrow, StoreArrow, StoreReturnsArrow, StoreSalesArrow, TimeDimArrow,
    WarehouseArrow, WebPageArrow, WebReturnsArrow, WebSalesArrow, WebSiteArrow,
};

/// The parameters a TPC-DS dataset is generated with.
#[derive(Debug, Clone, PartialEq)]
pub struct TpcdsSpec {
    pub location: String,
    pub scale_factor: f64,
    pub layout: ParquetLayout,
}

impl TpcdsSpec {
    /// The location of the dataset: `location` followed by a path that encodes every parameter.
    pub fn dataset(&self) -> String {
        format!(
            "{}/tpcds_parquet/scale_factor={}/{}",
            self.location.trim_end_matches('/'),
            self.scale_factor,
            self.layout.path()
        )
    }
}

/// Per-table columns, using the canonical column names. Sort keys are the columns that the pinned
/// tpcdsgen emits in ascending order: surrogate keys of dimension tables and ticket/order
/// numbers of fact tables. Sold dates are not ordered.
fn table_columns(table: &str) -> TableColumns {
    let sort_keys: &'static [&'static str] = match table {
        "call_center" => &["cc_call_center_sk"],
        "catalog_page" => &["cp_catalog_page_sk"],
        "catalog_returns" => &["cr_order_number"],
        "catalog_sales" => &["cs_order_number"],
        "customer" => &["c_customer_sk"],
        "customer_address" => &["ca_address_sk"],
        "customer_demographics" => &["cd_demo_sk"],
        "date_dim" => &["d_date_sk"],
        "household_demographics" => &["hd_demo_sk"],
        "income_band" => &["ib_income_band_sk"],
        "inventory" => &["inv_date_sk", "inv_item_sk", "inv_warehouse_sk"],
        "item" => &["i_item_sk"],
        "promotion" => &["p_promo_sk"],
        "reason" => &["r_reason_sk"],
        "ship_mode" => &["sm_ship_mode_sk"],
        "store" => &["s_store_sk"],
        "store_returns" => &["sr_ticket_number"],
        "store_sales" => &["ss_ticket_number"],
        "time_dim" => &["t_time_sk"],
        "warehouse" => &["w_warehouse_sk"],
        "web_page" => &["wp_web_page_sk"],
        "web_returns" => &["wr_order_number"],
        "web_sales" => &["ws_order_number"],
        "web_site" => &["web_site_sk"],
        _ => &[],
    };
    let dictionary: &'static [&'static str] = match table {
        "item" => &["i_brand", "i_category", "i_class", "i_color", "i_size"],
        "customer" => &["c_salutation"],
        "store" => &["s_state", "s_country"],
        _ => &[],
    };
    TableColumns {
        sort_keys,
        dictionary,
    }
}

pub fn get_queries() -> Vec<String> {
    common::get_queries("testdata/tpcds/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    common::get_query("testdata/tpcds/queries", id)
}

async fn generate_table<A>(
    data_source: A,
    table_name: &str,
    output: &DatasetOutput,
    spec: &TpcdsSpec,
    progress: &ProgressBar,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Iterator<Item = RecordBatch>,
{
    let mut writer = TableWriter::new(output, &spec.layout, table_name, table_columns(table_name));
    let mut rows = 0;
    for batch in data_source {
        rows += batch.num_rows() as u64;
        progress.set_message(format!("{table_name}: {} rows", HumanCount(rows)));
        writer.write(canonical_names(batch, table_name)?).await?;
    }
    writer.close().await?;
    progress.inc(1);
    Ok(())
}

/// Renames the columns whose tpcdsgen names differ from the TPC-DS specification.
fn canonical_names(
    batch: RecordBatch,
    table_name: &str,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = batch.schema();
    if schema
        .fields()
        .iter()
        .all(|field| canonical_column_name(table_name, field.name()) == field.name())
    {
        return Ok(batch);
    }
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            Arc::new(Field::new(
                canonical_column_name(table_name, field.name()),
                field.data_type().clone(),
                field.is_nullable(),
            ))
        })
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        batch.columns().to_vec(),
    )
}

fn canonical_column_name<'a>(table_name: &str, column_name: &'a str) -> &'a str {
    match (table_name, column_name) {
        ("catalog_returns", "cr_return_amount_inc_tax") => "cr_return_amt_inc_tax",
        ("income_band", "ib_income_band_id") => "ib_income_band_sk",
        ("reason", "r_reason_description") => "r_reason_desc",
        _ => column_name,
    }
}

/// Streams generated TPC-DS Parquet files directly to the destination, unless it already holds
/// the complete dataset.
pub async fn generate_data(spec: &TpcdsSpec) -> Result<(), Box<dyn std::error::Error>> {
    if !spec.scale_factor.is_finite() || spec.scale_factor <= 0.0 {
        return Err(exec_datafusion_err!("scale factor must be positive").into());
    }
    let output = &DatasetOutput::new(&spec.dataset()).await?;
    eprintln!("Generating {}", output.location());
    if output.is_complete().await? {
        return Ok(());
    }
    let mut options = Options::new();
    options.scale = spec.scale_factor;
    let session = options.to_session()?;

    // One step per generated table below.
    let progress = dataset_progress(24, "tables")?;
    macro_rules! generate_tpcds_table {
        ($generator:ident, $name:literal) => {
            generate_table(
                $generator::new(session.clone()).with_batch_size(8_000),
                $name,
                output,
                spec,
                &progress,
            )
            .await?
        };
    }

    generate_tpcds_table!(CallCenterArrow, "call_center");
    generate_tpcds_table!(CatalogPageArrow, "catalog_page");
    generate_tpcds_table!(CatalogReturnsArrow, "catalog_returns");
    generate_tpcds_table!(CatalogSalesArrow, "catalog_sales");
    generate_tpcds_table!(CustomerArrow, "customer");
    generate_tpcds_table!(CustomerAddressArrow, "customer_address");
    generate_tpcds_table!(CustomerDemographicsArrow, "customer_demographics");
    generate_tpcds_table!(DateDimArrow, "date_dim");
    generate_tpcds_table!(HouseholdDemographicsArrow, "household_demographics");
    generate_tpcds_table!(IncomeBandArrow, "income_band");
    generate_tpcds_table!(InventoryArrow, "inventory");
    generate_tpcds_table!(ItemArrow, "item");
    generate_tpcds_table!(PromotionArrow, "promotion");
    generate_tpcds_table!(ReasonArrow, "reason");
    generate_tpcds_table!(ShipModeArrow, "ship_mode");
    generate_tpcds_table!(StoreArrow, "store");
    generate_tpcds_table!(StoreReturnsArrow, "store_returns");
    generate_tpcds_table!(StoreSalesArrow, "store_sales");
    generate_tpcds_table!(TimeDimArrow, "time_dim");
    generate_tpcds_table!(WarehouseArrow, "warehouse");
    generate_tpcds_table!(WebPageArrow, "web_page");
    generate_tpcds_table!(WebReturnsArrow, "web_returns");
    generate_tpcds_table!(WebSalesArrow, "web_sales");
    generate_tpcds_table!(WebSiteArrow, "web_site");
    progress.finish_with_message("done");
    output.mark_complete().await?;
    Ok(())
}
