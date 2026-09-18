use super::{common, output::DatasetOutput};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::exec_datafusion_err;
use datafusion::error::DataFusionError;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tpcdsgen::config::Options;
use tpcdsgen_arrow::{
    CallCenterArrow, CatalogPageArrow, CatalogReturnsArrow, CatalogSalesArrow,
    CustomerAddressArrow, CustomerArrow, CustomerDemographicsArrow, DateDimArrow,
    HouseholdDemographicsArrow, IncomeBandArrow, InventoryArrow, ItemArrow, PromotionArrow,
    ReasonArrow, ShipModeArrow, StoreArrow, StoreReturnsArrow, StoreSalesArrow, TimeDimArrow,
    WarehouseArrow, WebPageArrow, WebReturnsArrow, WebSalesArrow, WebSiteArrow,
};

pub fn get_queries() -> Vec<String> {
    common::get_queries("testdata/tpcds/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    common::get_query("testdata/tpcds/queries", id)
}

async fn generate_table<A>(
    mut data_source: A,
    table_name: &str,
    output: &DatasetOutput,
    partitions: usize,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Iterator<Item = RecordBatch>,
{
    let Some(first_batch) = data_source.next() else {
        return Ok(());
    };

    let first_batch = dictionary_encode(first_batch, table_name)?;
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .build();
    let mut writers = (0..partitions)
        .map(|partition| {
            AsyncArrowWriter::try_new(
                output.writer(&format!("{table_name}/part-{partition}.parquet")),
                first_batch.schema(),
                Some(properties.clone()),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    write_batch(&mut writers, &first_batch).await?;
    for batch in data_source {
        let batch = dictionary_encode(batch, table_name)?;
        write_batch(&mut writers, &batch).await?;
    }
    for writer in writers {
        writer.close().await?;
    }
    Ok(())
}

async fn write_batch<W>(
    writers: &mut [AsyncArrowWriter<W>],
    batch: &RecordBatch,
) -> Result<(), parquet::errors::ParquetError>
where
    W: parquet::arrow::async_writer::AsyncFileWriter,
{
    let rows_per_writer = batch.num_rows() / writers.len();
    let remaining_rows = batch.num_rows() % writers.len();
    let mut offset = 0;
    for (index, writer) in writers.iter_mut().enumerate() {
        let rows = rows_per_writer + usize::from(index < remaining_rows);
        if rows > 0 {
            writer.write(&batch.slice(offset, rows)).await?;
            offset += rows;
        }
    }
    Ok(())
}

fn dictionary_encode(
    batch: RecordBatch,
    table_name: &str,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let dictionary_columns: &[&str] = match table_name {
        "item" => &["i_brand", "i_category", "i_class", "i_color", "i_size"],
        "customer" => &["c_salutation"],
        "store" => &["s_state", "s_country"],
        _ => &[],
    };
    let schema = batch.schema();
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            let field_name = canonical_column_name(table_name, field.name());
            if dictionary_columns.contains(&field.name().as_str()) {
                Arc::new(Field::new(
                    field_name,
                    DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                    field.is_nullable(),
                ))
            } else {
                Arc::new(Field::new(
                    field_name,
                    field.data_type().clone(),
                    field.is_nullable(),
                ))
            }
        })
        .collect();
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if dictionary_columns.contains(&field.name().as_str()) {
                cast(
                    column,
                    &DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                )
            } else {
                Ok(Arc::clone(column))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
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

async fn generate_tables(
    output: &DatasetOutput,
    sf: f64,
    partitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut options = Options::new();
    options.scale = sf;
    let session = options.to_session()?;

    macro_rules! generate_tpcds_table {
        ($generator:ident, $name:literal) => {
            generate_table(
                $generator::new(session.clone()).with_batch_size(8_000),
                $name,
                output,
                partitions,
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
    Ok(())
}

/// Streams generated TPC-DS Parquet files directly to the destination.
pub async fn generate_data(
    output: &DatasetOutput,
    sf: f64,
    partitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if !sf.is_finite() || sf <= 0.0 || partitions == 0 {
        return Err(exec_datafusion_err!("scale factor and partitions must be positive").into());
    }
    output.ensure_empty().await?;
    generate_tables(output, sf, partitions).await?;
    output.write("_SUCCESS", Vec::new()).await?;
    Ok(())
}
