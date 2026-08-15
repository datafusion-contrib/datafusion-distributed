use super::tpch;
use arrow::array::{Array, AsArray, BooleanArray};
use arrow::compute::kernels::cmp::{gt, lt};
use arrow::datatypes::{DataType, Int64Type, Schema, UInt64Type};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::TryStreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::WriterProperties;
use std::cmp::Ordering;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// TPC-H primary keys used as the physical sort order for each table.
///
/// Each table is generated with the same `tpchgen` data as the unsorted TPC-H
/// dataset, then globally sorted by these columns (ascending, nulls last).
/// Files under a table directory are range-partitioned: every row in
/// `i.parquet` compares `<=` every row in `(i+1).parquet` under this key.
pub const TABLE_SORT_KEYS: &[(&str, &[&str])] = &[
    ("region", &["r_regionkey"]),
    ("nation", &["n_nationkey"]),
    ("customer", &["c_custkey"]),
    ("supplier", &["s_suppkey"]),
    ("part", &["p_partkey"]),
    ("partsupp", &["ps_partkey", "ps_suppkey"]),
    ("orders", &["o_orderkey"]),
    ("lineitem", &["l_orderkey", "l_linenumber"]),
];

/// Cap in-memory sort so larger scale factors spill to disk instead of OOM.
/// The pool is a limit, not a reservation.
const SORT_MEMORY_BYTES: usize = 4 * 1024 * 1024 * 1024;

/// Sort keys for a TPC-H table, if the table is part of this dataset.
pub fn sort_keys(table: &str) -> Option<&'static [&'static str]> {
    TABLE_SORT_KEYS
        .iter()
        .find(|(name, _)| *name == table)
        .map(|(_, keys)| *keys)
}

/// Generates all TPC-H tables as globally sorted parquet files.
///
/// Files are written under `data_dir/<table>/<part>.parquet`, matching the
/// unsorted TPC-H layout.
pub async fn generate_tpch_sorted_data(
    data_dir: &Path,
    sf: f64,
    parts: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    tpch::generate_tpch_data(data_dir, sf, parts)?;
    rewrite_tables_globally_sorted(data_dir).await
}

/// Rewrite every TPC-H table directory under `data_dir` into a globally sorted
/// Parquet layout using [`TABLE_SORT_KEYS`].
pub async fn rewrite_tables_globally_sorted(
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for (table, keys) in TABLE_SORT_KEYS {
        let table_dir = data_dir.join(table);
        if !table_dir.is_dir() {
            continue;
        }
        let file_count = count_parquet_files(&table_dir)?;
        if file_count == 0 {
            continue;
        }
        rewrite_table(&table_dir, table, keys, file_count).await?;
    }
    Ok(())
}

async fn rewrite_table(
    table_dir: &Path,
    table: &str,
    keys: &[&str],
    file_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let tmp_dir = table_dir.with_file_name(format!(".{table}-sorted-tmp"));
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir)?;
    }
    fs::create_dir_all(&tmp_dir)?;

    let ctx = sort_session_context()?;
    let table_url = table_dir
        .to_str()
        .ok_or("TPC-H table path is not valid UTF-8")?;
    ctx.register_parquet(table, table_url, ParquetReadOptions::default())
        .await?;

    require_sort_columns(ctx.table(table).await?.schema().as_arrow(), keys)?;

    let total_rows = count_rows(&ctx, table).await?;
    if total_rows == 0 {
        fs::remove_dir_all(&tmp_dir)?;
        return Ok(());
    }

    let rows_per_file = total_rows.div_ceil(file_count);
    let order_by = keys
        .iter()
        .map(|col| format!("{col} ASC NULLS LAST"))
        .collect::<Vec<_>>()
        .join(", ");

    println!(
        "Sorting {total_rows} {table} rows into {file_count} file(s) by ({})",
        keys.join(", ")
    );

    let df = ctx
        .sql(&format!("SELECT * FROM {table} ORDER BY {order_by}"))
        .await?;
    let schema = df.schema().as_arrow().clone();
    let mut stream = df.execute_stream().await?;
    let props = sorted_writer_props(&schema, keys)?;

    let mut file_index = 0usize;
    let mut remaining_in_file = 0usize;
    let mut writer: Option<ArrowWriter<fs::File>> = None;

    while let Some(batch) = stream.try_next().await? {
        let mut offset = 0;
        while offset < batch.num_rows() {
            if remaining_in_file == 0 {
                if let Some(current) = writer.take() {
                    current.close()?;
                }
                file_index += 1;
                let path = tmp_dir.join(format!("{file_index}.parquet"));
                let file = fs::File::create(&path)?;
                writer = Some(ArrowWriter::try_new(
                    file,
                    batch.schema(),
                    Some(props.clone()),
                )?);
                remaining_in_file = rows_per_file;
            }

            let take = (batch.num_rows() - offset).min(remaining_in_file);
            if let Some(current) = writer.as_mut() {
                current.write(&batch.slice(offset, take))?;
            }
            offset += take;
            remaining_in_file -= take;
        }
    }

    if let Some(current) = writer.take() {
        current.close()?;
    }

    // Drop readers before replacing the table directory. On Windows the
    // original parquet files can otherwise stay locked.
    drop(stream);
    drop(df);
    drop(ctx);

    fs::remove_dir_all(table_dir)?;
    fs::rename(&tmp_dir, table_dir)?;
    Ok(())
}

fn sort_session_context() -> datafusion::common::Result<SessionContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(SORT_MEMORY_BYTES, 1.0)
        .build_arc()?;
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime)
        .build();
    Ok(SessionContext::new_with_state(state))
}

fn require_sort_columns(schema: &Schema, keys: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    for col in keys {
        if schema.index_of(col).is_err() {
            return Err(format!("sorted TPC-H dataset requires column `{col}`").into());
        }
    }
    Ok(())
}

async fn count_rows(
    ctx: &SessionContext,
    table: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let batches = ctx
        .sql(&format!("SELECT COUNT(*) FROM {table}"))
        .await?
        .collect()
        .await?;
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Ok(0);
    }
    let col = batches[0].column(0);
    match col.data_type() {
        DataType::Int64 => Ok(col.as_primitive::<Int64Type>().value(0) as usize),
        DataType::UInt64 => Ok(col.as_primitive::<UInt64Type>().value(0) as usize),
        other => Err(format!("unexpected COUNT(*) type: {other}").into()),
    }
}

fn sorted_writer_props(
    schema: &Schema,
    sort_cols: &[&str],
) -> Result<WriterProperties, Box<dyn std::error::Error>> {
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

    Ok(WriterProperties::builder()
        .set_sorting_columns(Some(sorting_columns))
        .build())
}

fn count_parquet_files(dir: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .count())
}

/// Returns true when `left[left_row]` is strictly less than or equal to
/// `right[right_row]` under `keys` with NULLS LAST.
fn row_is_sorted_before(
    left: &RecordBatch,
    left_row: usize,
    right: &RecordBatch,
    right_row: usize,
    keys: &[&str],
) -> Result<bool, Box<dyn std::error::Error>> {
    compare_sort_keys(left, left_row, right, right_row, keys).map(|ord| ord.is_le())
}

fn compare_sort_keys(
    left: &RecordBatch,
    left_row: usize,
    right: &RecordBatch,
    right_row: usize,
    keys: &[&str],
) -> Result<Ordering, Box<dyn std::error::Error>> {
    for col_name in keys {
        let l = left
            .column_by_name(col_name)
            .ok_or_else(|| format!("missing sort column {col_name}"))?;
        let r = right
            .column_by_name(col_name)
            .ok_or_else(|| format!("missing sort column {col_name}"))?;

        let l_null = l.is_null(left_row);
        let r_null = r.is_null(right_row);
        match (l_null, r_null) {
            (true, true) => continue,
            (false, true) => return Ok(Ordering::Less),
            (true, false) => return Ok(Ordering::Greater),
            (false, false) => {}
        }

        let l_slice = l.slice(left_row, 1);
        let r_slice = r.slice(right_row, 1);
        if bool_array_true(&lt(&l_slice, &r_slice)?)? {
            return Ok(Ordering::Less);
        }
        if bool_array_true(&gt(&l_slice, &r_slice)?)? {
            return Ok(Ordering::Greater);
        }
    }
    Ok(Ordering::Equal)
}

fn bool_array_true(arr: &BooleanArray) -> Result<bool, Box<dyn std::error::Error>> {
    if arr.is_empty() {
        return Err("empty comparison result".into());
    }
    Ok(arr.value(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::Field;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::path::PathBuf;

    #[tokio::test]
    async fn rewrites_tables_as_globally_sorted_files() -> Result<(), Box<dyn std::error::Error>> {
        let dest = test_dir("rewrite");
        let orders = dest.join("orders");

        // Two unsorted files whose keys interleave.
        write_orders_file(&orders.join("1.parquet"), &[4, 1])?;
        write_orders_file(&orders.join("2.parquet"), &[5, 2, 3])?;

        rewrite_tables_globally_sorted(&dest).await?;

        let files = read_table_files(&dest.join("orders"))?;
        assert_eq!(files.len(), 2, "expected two non-empty output files");

        let all = concat_table(&files)?;
        assert_eq!(all.num_rows(), 5);
        let keys = sort_keys("orders").unwrap();
        assert_eq!(int64_values(&all, "o_orderkey"), vec![1, 2, 3, 4, 5]);

        for i in 1..all.num_rows() {
            assert!(
                row_is_sorted_before(&all, i - 1, &all, i, keys)?,
                "row {i} is out of global sort order"
            );
        }

        let first = files[0].slice(files[0].num_rows() - 1, 1);
        let second = files[1].slice(0, 1);
        assert!(
            row_is_sorted_before(&first, 0, &second, 0, keys)?,
            "file 1 must not contain keys greater than file 2"
        );
        assert_sorting_metadata(&dest.join("orders"), keys);

        let _ = fs::remove_dir_all(&dest);
        Ok(())
    }

    #[tokio::test]
    async fn sort_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let dest = test_dir("idempotent");
        write_orders_file(&dest.join("orders").join("1.parquet"), &[3, 1, 2])?;

        rewrite_tables_globally_sorted(&dest).await?;
        let first = concat_table(&read_table_files(&dest.join("orders"))?)?;
        rewrite_tables_globally_sorted(&dest).await?;
        let second = concat_table(&read_table_files(&dest.join("orders"))?)?;

        assert_eq!(first, second);
        let _ = fs::remove_dir_all(&dest);
        Ok(())
    }

    #[tokio::test]
    async fn generates_globally_sorted_tables_at_smoke_scale()
    -> Result<(), Box<dyn std::error::Error>> {
        let dest = test_dir("smoke");
        generate_tpch_sorted_data(&dest, 0.01, 4).await?;

        for (table, keys) in TABLE_SORT_KEYS {
            let batches = read_table_files(&dest.join(table))?;
            assert!(!batches.is_empty(), "{table} should not be empty");
            let all = concat_table(&batches)?;
            assert!(all.num_rows() > 0, "{table} should not be empty");
            for i in 1..all.num_rows() {
                assert!(
                    row_is_sorted_before(&all, i - 1, &all, i, keys)?,
                    "{table} row {i} is out of global sort order"
                );
            }
        }

        let _ = fs::remove_dir_all(&dest);
        Ok(())
    }

    fn test_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dfd-tpch-sorted-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_orders_file(path: &Path, keys: &[i64]) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("payload", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from_iter_values(keys.iter().map(|k| k * 10))),
            ],
        )?;
        let file = fs::File::create(path)?;
        let mut writer =
            ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build()))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn read_table_files(table_dir: &Path) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        let mut paths: Vec<PathBuf> = fs::read_dir(table_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
            .collect();
        paths.sort_by_key(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(u32::MAX)
        });

        let mut batches = Vec::new();
        for path in paths {
            let file = fs::File::open(&path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            for batch in reader {
                batches.push(batch?);
            }
        }
        Ok(batches)
    }

    fn concat_table(batches: &[RecordBatch]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        Ok(arrow::compute::concat_batches(
            &batches[0].schema(),
            batches,
        )?)
    }

    fn int64_values(batch: &RecordBatch, name: &str) -> Vec<i64> {
        let arr = batch
            .column_by_name(name)
            .unwrap()
            .as_primitive::<Int64Type>();
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    fn assert_sorting_metadata(table_dir: &Path, keys: &[&str]) {
        let mut files: Vec<_> = fs::read_dir(table_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
            .collect();
        files.sort();
        assert!(!files.is_empty());

        let file = fs::File::open(&files[0]).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let rg = reader.metadata().row_group(0);
        let sorting = rg
            .sorting_columns()
            .expect("parquet sorting_columns metadata");
        assert_eq!(sorting.len(), keys.len());
        for col in sorting {
            assert!(!col.descending);
            assert!(!col.nulls_first);
        }
    }
}
