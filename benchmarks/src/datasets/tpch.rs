use super::{common, output::DatasetOutput};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, exec_datafusion_err};
use parquet::arrow::AsyncArrowWriter;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::WriterProperties;
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

async fn generate_table<A>(
    mut data_source: A,
    table_name: &str,
    output: &DatasetOutput,
    sort_cols: Option<&[&str]>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Iterator<Item = RecordBatch>,
{
    if let Some(first_batch) = data_source.next() {
        let file = output.writer(&format!("{table_name}.parquet"));
        let props = writer_props(first_batch.schema().as_ref(), sort_cols)?;
        let mut writer = AsyncArrowWriter::try_new(file, first_batch.schema(), Some(props))?;

        writer.write(&first_batch).await?;

        for batch in data_source {
            writer.write(&batch).await?;
        }

        writer.close().await?;
    }

    Ok(())
}

/// Streams TPC-H Parquet files to an empty local directory or S3 prefix.
pub async fn generate_data(
    output: &DatasetOutput,
    sf: f64,
    parts: usize,
    sorted: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !sf.is_finite() || sf <= 0.0 || parts == 0 || parts > i32::MAX as usize {
        return Err(exec_datafusion_err!(
            "scale factor and partitions must be positive, with partitions <= i32::MAX"
        )
        .into());
    }
    output.ensure_empty().await?;

    macro_rules! generate_tpch_table {
        ($generator:ident, $arrow:ident, $name:literal, $parts:expr) => {{
            let table_parts = $parts;
            let keys = if sorted { sort_keys($name) } else { None };
            for part in 1..=(table_parts as i32) {
                generate_table(
                    $arrow::new($generator::new(sf, part, table_parts as i32))
                        .with_batch_size(1000),
                    &format!("{}/{part}", $name),
                    output,
                    keys,
                )
                .await?;
            }
        }};
    }

    // These generators ignore partition arguments and emit the entire fixed-size table.
    // Using the requested partition count would duplicate every row in benchmark datasets.
    generate_tpch_table!(RegionGenerator, RegionArrow, "region", 1);
    generate_tpch_table!(NationGenerator, NationArrow, "nation", 1);
    generate_tpch_table!(CustomerGenerator, CustomerArrow, "customer", parts);
    generate_tpch_table!(SupplierGenerator, SupplierArrow, "supplier", parts);
    generate_tpch_table!(PartGenerator, PartArrow, "part", parts);
    generate_tpch_table!(PartSuppGenerator, PartSuppArrow, "partsupp", parts);
    generate_tpch_table!(OrderGenerator, OrderArrow, "orders", parts);
    generate_tpch_table!(LineItemGenerator, LineItemArrow, "lineitem", parts);
    output.write("_SUCCESS", Vec::new()).await?;
    Ok(())
}
