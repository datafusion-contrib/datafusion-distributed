use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::scalar::ScalarValue;

#[track_caller]
pub(crate) fn assert_scalar_result(
    batches: &[RecordBatch],
    column: &str,
    expected: ScalarValue,
) -> Result<()> {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1, "expected one row for {column}");
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() != 0)
        .expect("one total row guarantees a nonempty batch");
    assert_eq!(batch.num_columns(), 1, "expected one column for {column}");
    assert_eq!(batch.schema().field(0).name(), column);
    assert_eq!(
        ScalarValue::try_from_array(batch.column(0), 0)?,
        expected,
        "unexpected scalar for {column}"
    );
    Ok(())
}
