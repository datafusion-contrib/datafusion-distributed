use super::common;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use parquet::file::metadata::SortingColumn;
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

/// Columns that the pinned tpchgen already emits in order, used as Parquet
/// `sorting_columns` for the `tpch/sorted_sf*` variant.
///
/// `partsupp` only records `ps_partkey`: `ps_suppkey` wraps within each part.
const TABLE_SORT_KEYS: &[(&str, &[&str])] = &[
    ("region", &["r_regionkey"]),
    ("nation", &["n_nationkey"]),
    ("customer", &["c_custkey"]),
    ("supplier", &["s_suppkey"]),
    ("part", &["p_partkey"]),
    ("partsupp", &["ps_partkey"]),
    ("orders", &["o_orderkey"]),
    ("lineitem", &["l_orderkey", "l_linenumber"]),
];

pub fn get_queries() -> Vec<String> {
    common::get_queries("testdata/tpch/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    common::get_query("testdata/tpch/queries", id)
}

fn sort_keys(table: &str) -> Option<&'static [&'static str]> {
    TABLE_SORT_KEYS
        .iter()
        .find(|(name, _)| *name == table)
        .map(|(_, keys)| *keys)
}

fn writer_props(
    schema: &Schema,
    sort_cols: Option<&[&str]>,
) -> Result<WriterProperties, Box<dyn std::error::Error>> {
    let mut builder = WriterProperties::builder();
    if let Some(sort_cols) = sort_cols {
        let sorting_columns = sort_cols
            .iter()
            .map(|name| {
                let idx = schema.index_of(name)?;
                Ok(SortingColumn {
                    column_idx: idx as i32,
                    descending: false,
                    nulls_first: false,
                })
            })
            .collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;
        builder = builder.set_sorting_columns(Some(sorting_columns));
    }
    Ok(builder.build())
}

fn generate_table<A>(
    mut data_source: A,
    table_name: &str,
    data_dir: &Path,
    sort_cols: Option<&[&str]>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Iterator<Item = RecordBatch>,
{
    let output_path = data_dir.join(format!("{table_name}.parquet"));

    if let Some(first_batch) = data_source.next() {
        let file = fs::File::create(&output_path)?;
        let props = writer_props(first_batch.schema().as_ref(), sort_cols)?;
        let mut writer = ArrowWriter::try_new(file, first_batch.schema(), Some(props))?;

        writer.write(&first_batch)?;

        for batch in data_source {
            writer.write(&batch)?;
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
    generate_tpch_tables(data_dir, sf, parts, false)
}

/// Same generators as [`generate_tpch_data`], with table-specific Parquet
/// `sorting_columns` metadata for the `tpch/sorted_sf*` variant.
pub fn generate_sorted_tpch_data(
    data_dir: &Path,
    sf: f64,
    parts: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    generate_tpch_tables(data_dir, sf, parts, true)
}

fn generate_tpch_tables(
    data_dir: &Path,
    sf: f64,
    parts: usize,
    sorted: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(data_dir)?;

    macro_rules! generate_tpch_table {
        ($generator:ident, $arrow:ident, $name:literal) => {{
            let table_dir = data_dir.join($name);
            fs::create_dir_all(&table_dir)?;
            let keys = if sorted { sort_keys($name) } else { None };
            for part in 1..=(parts as i32) {
                generate_table(
                    $arrow::new($generator::new(sf, part, parts as i32)).with_batch_size(1000),
                    &format!("{part}"),
                    &table_dir,
                    keys,
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
