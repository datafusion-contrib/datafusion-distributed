use super::common;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};
use std::fs;
use std::path::Path;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
};

pub fn get_queries() -> Vec<String> {
    common::get_queries("testdata/tpch/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    common::get_query("testdata/tpch/queries", id)
}

fn generate_table<A>(
    mut data_source: A,
    table_name: &str,
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Iterator<Item = Result<RecordBatch, ArrowError>>,
{
    let output_path = data_dir.join(format!("{table_name}.parquet"));

    if let Some(first_batch) = data_source.next() {
        let first_batch = first_batch?;
        let file = fs::File::create(&output_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, first_batch.schema(), Some(props))?;

        writer.write(&first_batch)?;

        for batch in data_source {
            writer.write(&batch?)?;
        }

        writer.close()?;
    }

    Ok(())
}

/// Generates all TPC-H tables as parquet files in the specified data directory.
pub fn generate_tpch_data(
    data_dir: &Path,
    sf: f64,
    parts: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(data_dir)?;

    macro_rules! generate_tpch_table {
        ($generator:ident, $arrow:ident, $name:literal) => {{
            let table_dir = data_dir.join($name);
            fs::create_dir_all(&table_dir)?;
            for part in 1..=(parts as i32) {
                generate_table(
                    $arrow::new($generator::new(sf, part, parts as i32)).with_batch_size(1000),
                    &format!("{part}"),
                    &table_dir,
                )?;
            }
        }};
    }

    generate_tpch_table!(RegionGenerator, RegionArrow, "region");
    generate_tpch_table!(NationGenerator, NationArrow, "nation");
    generate_tpch_table!(CustomerGenerator, CustomerArrow, "customer");
    generate_tpch_table!(SupplierGenerator, SupplierArrow, "supplier");
    generate_tpch_table!(PartGenerator, PartArrow, "part");
    generate_tpch_table!(PartSuppGenerator, PartSuppArrow, "partsupp");
    generate_tpch_table!(OrderGenerator, OrderArrow, "orders");
    generate_tpch_table!(LineItemGenerator, LineItemArrow, "lineitem");
    Ok(())
}
