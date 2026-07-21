use datafusion::arrow::array::RecordBatch;

/// Returns the logical size of a batch's slices, excluding unused backing-buffer capacity.
pub(crate) fn logical_record_batch_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| column.to_data().get_slice_memory_size().unwrap_or(0))
        .sum()
}
