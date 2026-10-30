use super::parquet_layout::{ParquetLayout, TableColumns, TableWriter};
use super::progress::dataset_progress;
use super::{common, output::DatasetOutput};
use datafusion::common::{DataFusionError, exec_datafusion_err};
use indicatif::HumanCount;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
};

/// The parameters a TPC-H dataset is generated with.
#[derive(Debug, Clone, PartialEq)]
pub struct TpchSpec {
    pub location: String,
    pub scale_factor: f64,
    pub layout: ParquetLayout,
}

impl TpchSpec {
    /// The location of the dataset: `location` followed by a path that encodes every parameter.
    pub fn dataset(&self) -> String {
        format!(
            "{}/tpch_parquet/scale_factor={}/{}",
            self.location.trim_end_matches('/'),
            self.scale_factor,
            self.layout.path()
        )
    }
}

/// Per-table columns. Sort keys are the columns that the pinned tpchgen already emits in order;
/// `partsupp` only records `ps_partkey` because `ps_suppkey` wraps within each part.
fn table_columns(table: &str) -> TableColumns {
    let (sort_keys, dictionary): (&'static [&'static str], &'static [&'static str]) = match table {
        "region" => (&["r_regionkey"], &["r_name"]),
        "nation" => (&["n_nationkey"], &["n_name"]),
        "customer" => (&["c_custkey"], &["c_mktsegment"]),
        "supplier" => (&["s_suppkey"], &[]),
        "part" => (
            &["p_partkey"],
            &["p_mfgr", "p_brand", "p_type", "p_container"],
        ),
        "partsupp" => (&["ps_partkey"], &[]),
        "orders" => (&["o_orderkey"], &["o_orderstatus", "o_orderpriority"]),
        "lineitem" => (
            &["l_orderkey", "l_linenumber"],
            &[
                "l_returnflag",
                "l_linestatus",
                "l_shipinstruct",
                "l_shipmode",
            ],
        ),
        _ => (&[], &[]),
    };
    TableColumns {
        sort_keys,
        dictionary,
    }
}

pub fn get_queries() -> Vec<String> {
    common::get_queries("testdata/tpch/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    common::get_query("testdata/tpch/queries", id)
}

/// Streams TPC-H Parquet files to a local directory or S3 prefix, unless it already holds the
/// complete dataset.
pub async fn generate_data(spec: &TpchSpec) -> Result<(), Box<dyn std::error::Error>> {
    let sf = spec.scale_factor;
    if !sf.is_finite() || sf <= 0.0 {
        return Err(exec_datafusion_err!("scale factor must be positive").into());
    }
    let output = &DatasetOutput::new(&spec.dataset()).await?;
    eprintln!("Generating {}", output.location());
    if output.is_complete().await? {
        return Ok(());
    }

    // One step per generated table below.
    let progress = dataset_progress(8, "tables")?;
    macro_rules! generate_tpch_table {
        ($generator:ident, $arrow:ident, $name:literal) => {
            let mut writer = TableWriter::new(output, &spec.layout, $name, table_columns($name));
            let mut rows = 0;
            for batch in $arrow::new($generator::new(sf, 1, 1)).with_batch_size(1000) {
                rows += batch.num_rows() as u64;
                progress.set_message(format!("{}: {} rows", $name, HumanCount(rows)));
                writer.write(batch).await?;
            }
            writer.close().await?;
            progress.inc(1);
        };
    }

    generate_tpch_table!(RegionGenerator, RegionArrow, "region");
    generate_tpch_table!(NationGenerator, NationArrow, "nation");
    generate_tpch_table!(CustomerGenerator, CustomerArrow, "customer");
    generate_tpch_table!(SupplierGenerator, SupplierArrow, "supplier");
    generate_tpch_table!(PartGenerator, PartArrow, "part");
    generate_tpch_table!(PartSuppGenerator, PartSuppArrow, "partsupp");
    generate_tpch_table!(OrderGenerator, OrderArrow, "orders");
    generate_tpch_table!(LineItemGenerator, LineItemArrow, "lineitem");
    progress.finish_with_message("done");
    output.mark_complete().await?;
    Ok(())
}
