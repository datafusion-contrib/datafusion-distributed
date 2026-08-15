use super::clickbench;
use arrow::array::AsArray;
use arrow::datatypes::{DataType, Int64Type, Schema, UInt64Type};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::TryStreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

/// Global sort key for the `clickbench-sorted` dataset.
///
/// Matches the official ClickHouse `hits` MergeTree `ORDER BY`
/// (`CounterID, EventDate, UserID, EventTime, WatchID`). Output files are
/// written so every row in `i.parquet` compares `<=` every row in
/// `(i+1).parquet` under this key.
pub const CLICKBENCH_SORT_KEY: &[&str] =
    &["CounterID", "EventDate", "UserID", "EventTime", "WatchID"];

/// Cap in-memory sort so a full 100-file generation spills to disk instead of
/// OOM. The pool is a limit, not a reservation.
const SORT_MEMORY_BYTES: usize = 4 * 1024 * 1024 * 1024;

/// Download the ClickBench partition range and rewrite `hits/` as a globally
/// sorted Parquet layout.
pub async fn generate_clickbench_sorted_data(
    dest_path: &Path,
    range: Range<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_count = range.end.saturating_sub(range.start);
    clickbench::generate_clickbench_data(dest_path, range).await?;
    rewrite_hits_globally_sorted(dest_path, file_count).await
}

/// Rewrite `dest_path/hits/*.parquet` into `file_count` files that together
/// form one globally sorted sequence under [`CLICKBENCH_SORT_KEY`].
pub async fn rewrite_hits_globally_sorted(
    dest_path: &Path,
    file_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if file_count == 0 {
        return Ok(());
    }

    let hits_dir = dest_path.join("hits");
    if !hits_dir.is_dir() {
        return Err(format!(
            "ClickBench hits directory not found: {}",
            hits_dir.display()
        )
        .into());
    }

    let tmp_dir = dest_path.join(".hits-sorted-tmp");
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir)?;
    }
    fs::create_dir_all(&tmp_dir)?;

    let ctx = sort_session_context()?;
    let hits_url = hits_dir
        .to_str()
        .ok_or("ClickBench hits path is not valid UTF-8")?;
    ctx.register_parquet("hits", hits_url, ParquetReadOptions::default())
        .await?;

    require_sort_columns(ctx.table("hits").await?.schema().as_arrow())?;

    let total_rows = count_hits(&ctx).await?;
    if total_rows == 0 {
        fs::remove_dir_all(&tmp_dir)?;
        return Ok(());
    }

    let rows_per_file = total_rows.div_ceil(file_count);
    let order_by = CLICKBENCH_SORT_KEY
        .iter()
        .map(|col| format!("\"{col}\" ASC NULLS FIRST"))
        .collect::<Vec<_>>()
        .join(", ");

    println!(
        "Sorting {total_rows} ClickBench rows into {file_count} file(s) by ({})",
        CLICKBENCH_SORT_KEY.join(", ")
    );

    let df = ctx
        .sql(&format!("SELECT * FROM hits ORDER BY {order_by}"))
        .await?;
    let mut stream = df.execute_stream().await?;

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
                let path = tmp_dir.join(format!("{file_index}.parquet"));
                let file = fs::File::create(&path)?;
                let props = WriterProperties::builder().build();
                writer = Some(ArrowWriter::try_new(file, batch.schema(), Some(props))?);
                file_index += 1;
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

    let backup = dest_path.join(".hits-unsorted");
    if backup.exists() {
        fs::remove_dir_all(&backup)?;
    }
    fs::rename(&hits_dir, &backup)?;
    if let Err(err) = fs::rename(&tmp_dir, &hits_dir) {
        let _ = fs::rename(&backup, &hits_dir);
        return Err(err.into());
    }
    fs::remove_dir_all(&backup)?;
    println!(
        "Wrote globally sorted ClickBench hits to {}",
        hits_dir.display()
    );
    Ok(())
}

fn sort_session_context() -> datafusion::common::Result<SessionContext> {
    // FairSpillPool lets SortExec spill when the full ClickBench set exceeds RAM.
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(SORT_MEMORY_BYTES)))
        .build_arc()?;
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime)
        .build();
    Ok(SessionContext::new_with_state(state))
}

fn require_sort_columns(schema: &Schema) -> Result<(), Box<dyn std::error::Error>> {
    for col in CLICKBENCH_SORT_KEY {
        if schema.index_of(col).is_err() {
            return Err(
                format!("sorted ClickBench dataset requires column `{col}` in hits").into(),
            );
        }
    }
    Ok(())
}

async fn count_hits(ctx: &SessionContext) -> Result<usize, Box<dyn std::error::Error>> {
    let batches = ctx
        .sql("SELECT COUNT(*) FROM hits")
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Date32Array, Int32Array, Int64Array, StringArray};
    use arrow::compute::kernels::cmp::{gt, lt};
    use arrow::datatypes::Field;
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::cmp::Ordering;
    use std::path::PathBuf;

    #[tokio::test]
    async fn rewrites_hits_as_globally_sorted_files() -> Result<(), Box<dyn std::error::Error>> {
        let dest = test_dir();
        let hits_dir = dest.join("hits");

        // Two unsorted files whose keys interleave. Payload is the original
        // WatchID so we can confirm every row is preserved.
        write_hits_file(
            &hits_dir.join("0.parquet"),
            &[row(2, 1, 10, 100, 2), row(5, 1, 10, 100, 5)],
        )?;
        write_hits_file(
            &hits_dir.join("1.parquet"),
            &[
                row(1, 1, 10, 100, 1),
                row(3, 1, 10, 100, 3),
                row(2, 1, 10, 100, 20),
            ],
        )?;

        rewrite_hits_globally_sorted(&dest, 2).await?;

        let parquet_files = list_parquet_files(&dest.join("hits"))?;
        assert_eq!(
            parquet_files.len(),
            2,
            "expected two non-empty output files"
        );

        let files = read_hits_files(&dest.join("hits"))?;
        let all = concat_batches(&files)?;
        assert_eq!(all.num_rows(), 5);

        let watch_ids = all
            .column_by_name("WatchID")
            .unwrap()
            .as_primitive::<Int64Type>();
        let mut seen: Vec<i64> = (0..all.num_rows()).map(|i| watch_ids.value(i)).collect();
        seen.sort_unstable();
        assert_eq!(seen, vec![1, 2, 3, 5, 20]);

        for i in 1..all.num_rows() {
            assert!(
                row_is_sorted_before(&all, i - 1, &all, i)?,
                "row {i} is out of global sort order"
            );
        }

        let first = files[0].slice(files[0].num_rows() - 1, 1);
        let second = files[1].slice(0, 1);
        assert!(
            row_is_sorted_before(&first, 0, &second, 0)?,
            "file 0 must not contain keys greater than file 1"
        );

        let _ = fs::remove_dir_all(&dest);
        Ok(())
    }

    #[tokio::test]
    async fn sort_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let dest = test_dir();
        let hits_dir = dest.join("hits");
        write_hits_file(
            &hits_dir.join("0.parquet"),
            &[row(3, 1, 1, 1, 3), row(1, 1, 1, 1, 1), row(2, 1, 1, 1, 2)],
        )?;

        rewrite_hits_globally_sorted(&dest, 1).await?;
        let first = watch_ids(&concat_batches(&read_hits_files(&dest.join("hits"))?)?);
        rewrite_hits_globally_sorted(&dest, 1).await?;
        let second = watch_ids(&concat_batches(&read_hits_files(&dest.join("hits"))?)?);

        assert_eq!(first, second);
        let _ = fs::remove_dir_all(&dest);
        Ok(())
    }

    fn row_is_sorted_before(
        left: &RecordBatch,
        left_row: usize,
        right: &RecordBatch,
        right_row: usize,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(compare_sort_keys(left, left_row, right, right_row)?.is_le())
    }

    fn compare_sort_keys(
        left: &RecordBatch,
        left_row: usize,
        right: &RecordBatch,
        right_row: usize,
    ) -> Result<Ordering, Box<dyn std::error::Error>> {
        for col_name in CLICKBENCH_SORT_KEY {
            let l = left
                .column_by_name(col_name)
                .ok_or_else(|| format!("missing sort column {col_name}"))?;
            let r = right
                .column_by_name(col_name)
                .ok_or_else(|| format!("missing sort column {col_name}"))?;

            match (l.is_null(left_row), r.is_null(right_row)) {
                (true, true) => continue,
                (true, false) => return Ok(Ordering::Less),
                (false, true) => return Ok(Ordering::Greater),
                (false, false) => {}
            }

            let l_slice = l.slice(left_row, 1);
            let r_slice = r.slice(right_row, 1);
            if lt(&l_slice, &r_slice)?.value(0) {
                return Ok(Ordering::Less);
            }
            if gt(&l_slice, &r_slice)?.value(0) {
                return Ok(Ordering::Greater);
            }
        }
        Ok(Ordering::Equal)
    }

    fn watch_ids(batch: &RecordBatch) -> Vec<i64> {
        let col = batch
            .column_by_name("WatchID")
            .unwrap()
            .as_primitive::<Int64Type>();
        (0..batch.num_rows()).map(|i| col.value(i)).collect()
    }

    struct Row {
        counter_id: i32,
        event_date: i32,
        user_id: i64,
        event_time: i32,
        watch_id: i64,
    }

    fn row(counter_id: i32, event_date: i32, user_id: i64, event_time: i32, watch_id: i64) -> Row {
        Row {
            counter_id,
            event_date,
            user_id,
            event_time,
            watch_id,
        }
    }

    fn test_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dfd-clickbench-sorted-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(dir.join("hits")).unwrap();
        dir
    }

    fn write_hits_file(path: &Path, rows: &[Row]) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("CounterID", DataType::Int32, true),
            Field::new("EventDate", DataType::Date32, true),
            Field::new("UserID", DataType::Int64, true),
            Field::new("EventTime", DataType::Int32, true),
            Field::new("WatchID", DataType::Int64, true),
            Field::new("Payload", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter(
                    rows.iter().map(|r| Some(r.counter_id)),
                )),
                Arc::new(Date32Array::from_iter(
                    rows.iter().map(|r| Some(r.event_date)),
                )),
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| Some(r.user_id)))),
                Arc::new(Int32Array::from_iter(
                    rows.iter().map(|r| Some(r.event_time)),
                )),
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| Some(r.watch_id)))),
                Arc::new(StringArray::from_iter(
                    rows.iter().map(|r| Some(r.watch_id.to_string())),
                )),
            ],
        )?;
        let file = fs::File::create(path)?;
        let mut writer =
            ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build()))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn list_parquet_files(hits_dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let mut paths: Vec<PathBuf> = fs::read_dir(hits_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
            .collect();
        paths.sort();
        Ok(paths)
    }

    fn read_hits_files(hits_dir: &Path) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        let paths = list_parquet_files(hits_dir)?;

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

    fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        Ok(arrow::compute::concat_batches(
            &batches[0].schema(),
            batches,
        )?)
    }
}
