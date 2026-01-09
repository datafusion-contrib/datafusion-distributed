use crate::test_utils::benchmarks_common;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};
use std::fs;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
};

pub fn get_queries() -> Vec<String> {
    benchmarks_common::get_queries("testdata/tpch/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    benchmarks_common::get_query("testdata/tpch/queries", id)
}

// generate_table creates a parquet file in the data directory from an arrow RecordBatch row
// source.
fn generate_table<A>(
    mut data_source: A,
    table_name: &str,
    data_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Iterator<Item = RecordBatch>,
{
    let output_path = data_dir.join(format!("{table_name}.parquet"));

    if let Some(first_batch) = data_source.next() {
        let file = fs::File::create(&output_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, first_batch.schema(), Some(props))?;

        writer.write(&first_batch)?;

        for batch in data_source {
            writer.write(&batch)?;
        }

        writer.close()?;
    }

    Ok(())
}

// generate_tpch_data generates all TPC-H tables in the specified data directory.
pub fn generate_tpch_data(data_dir: &std::path::Path, sf: f64, parts: i32) {
    fs::create_dir_all(data_dir).expect("Failed to create data directory");

    macro_rules! must_generate_tpch_table {
        ($generator:ident, $arrow:ident, $name:literal) => {
            let data_dir = data_dir.join($name);
            fs::create_dir_all(data_dir.clone()).expect("Failed to create data directory");
            // create three partitions for the table
            (1..=parts).for_each(|part| {
                generate_table(
                    // TODO: Consider adjusting the partitions and batch sizes.
                    $arrow::new($generator::new(sf, part, parts)).with_batch_size(1000),
                    &format!("{part}"),
                    &data_dir,
                )
                .expect(concat!("Failed to generate ", $name, " table"));
            });
        };
    }

    must_generate_tpch_table!(RegionGenerator, RegionArrow, "region");
    must_generate_tpch_table!(NationGenerator, NationArrow, "nation");
    must_generate_tpch_table!(CustomerGenerator, CustomerArrow, "customer");
    must_generate_tpch_table!(SupplierGenerator, SupplierArrow, "supplier");
    must_generate_tpch_table!(PartGenerator, PartArrow, "part");
    must_generate_tpch_table!(PartSuppGenerator, PartSuppArrow, "partsupp");
    must_generate_tpch_table!(OrderGenerator, OrderArrow, "orders");
    must_generate_tpch_table!(LineItemGenerator, LineItemArrow, "lineitem");
}
