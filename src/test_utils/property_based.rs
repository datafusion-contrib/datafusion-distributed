use arrow::{
    array::{ArrayRef, Float16Array, Float32Array, Float64Array, UInt32Array},
    compute::{SortColumn, concat_batches, lexsort_to_indices},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{internal_datafusion_err, internal_err},
    error::{DataFusionError, Result},
    physical_expr::LexOrdering,
    physical_plan::ExecutionPlan,
};
use std::sync::Arc;

/// compares the set of record batches for equality
pub async fn compare_result_set(
    actual_result: &Result<Vec<RecordBatch>>,
    expected_result: &Result<Vec<RecordBatch>>,
) -> Result<()> {
    let test_batches = match actual_result.as_ref() {
        Ok(batches) => batches,
        Err(e) => {
            if expected_result.is_ok() {
                return internal_err!("expected no error but got: {}", e);
            }
            return Ok(()); // Both errored, so the query is valid
        }
    };

    let compare_batches = match expected_result.as_ref() {
        Ok(batches) => batches,
        Err(e) => {
            if actual_result.is_ok() {
                return internal_err!("expected error but got none, error: {}", e);
            }
            return Ok(()); // Both errored, so the query is valid
        }
    };

    records_equal_as_sets(test_batches, compare_batches)
        .map_err(|e| internal_datafusion_err!("result sets were not equal: {}", e))
}

// Ensures that the plans have the same ordering properties and that the actual result is sorted
// correctly.
pub async fn compare_ordering(
    actual_physical_plan: Arc<dyn ExecutionPlan>,
    expected_physical_plan: Arc<dyn ExecutionPlan>,
    actual_result: &Result<Vec<RecordBatch>>,
) -> Result<()> {
    // Only validate if the query succeeded
    let test_batches = match actual_result.as_ref() {
        Ok(batches) => batches,
        Err(_) => return Ok(()),
    };

    let actual_ordering = actual_physical_plan.properties().output_ordering();
    let expected_ordering = expected_physical_plan.properties().output_ordering();

    if actual_ordering != expected_ordering {
        return internal_err!(
            "ordering mismatch: expected ordering: {:?}, actual ordering: {:?}",
            expected_ordering,
            actual_ordering
        );
    }

    // If there's no ordering, there's nothing to validate.
    let Some(lex_ordering) = actual_ordering else {
        return Ok(());
    };

    // Coalesce all batches into a single batch to check ordering across the entire result set
    if !test_batches.is_empty() {
        let coalesced_batch = if test_batches.len() == 1 {
            test_batches[0].clone()
        } else {
            concat_batches(&test_batches[0].schema(), test_batches)?
        };

        let is_sorted = is_table_same_after_sort(lex_ordering, &coalesced_batch)?;
        if !is_sorted {
            return internal_err!(
                "ordering validation failed: results are not properly sorted according to expected ordering: {:?}",
                lex_ordering
            );
        }
    }

    Ok(())
}

/// Compare two sets of record batches for equality (order-independent)
fn records_equal_as_sets(
    left: &[RecordBatch],
    right: &[RecordBatch],
) -> Result<(), DataFusionError> {
    // First check if total row counts match
    let left_rows: usize = left.iter().map(|b| b.num_rows()).sum();
    let right_rows: usize = right.iter().map(|b| b.num_rows()).sum();

    if left_rows != right_rows {
        return internal_err!(
            "Row counts differ: left={}, right={}",
            left_rows,
            right_rows
        );
    }

    // Check if schemas match
    if !left.is_empty() && !right.is_empty() && left[0].schema() != right[0].schema() {
        return internal_err!(
            "Schemas differ between result sets\nLeft schema: {:?}\nRight schema: {:?}",
            left[0].schema(),
            right[0].schema()
        );
    }

    detailed_batch_comparison(left, right)
}

/// Perform detailed comparison of record batches
fn detailed_batch_comparison(
    left: &[RecordBatch],
    right: &[RecordBatch],
) -> Result<(), DataFusionError> {
    // Convert both sides to sets of string representations of rows
    let left_rows = batch_rows_to_strings(left);
    let right_rows = batch_rows_to_strings(right);

    if left_rows.len() != right_rows.len() {
        return internal_err!(
            "Row set sizes differ: left={}, right={}",
            left_rows.len(),
            right_rows.len()
        );
    }

    let left_set: std::collections::HashSet<_> = left_rows.iter().collect();
    let right_set: std::collections::HashSet<_> = right_rows.iter().collect();

    if left_set != right_set {
        // Get all differences (not just samples)
        let left_only: Vec<_> = left_rows
            .iter()
            .filter(|row| !right_set.contains(row))
            .collect();
        let right_only: Vec<_> = right_rows
            .iter()
            .filter(|row| !left_set.contains(row))
            .collect();

        let mut error_msg = format!(
            "Row content differs between result sets\nLeft set size: {}, Right set size: {}",
            left_set.len(),
            right_set.len()
        );

        if !left_only.is_empty() {
            error_msg.push_str(&format!(
                "\n\nRows only in left ({} total):",
                left_only.len()
            ));
            for row in left_only {
                error_msg.push_str(&format!("\n  {row}"));
            }
        }

        if !right_only.is_empty() {
            error_msg.push_str(&format!(
                "\n\nRows only in right ({} total):",
                right_only.len()
            ));
            for row in right_only {
                error_msg.push_str(&format!("\n  {row}"));
            }
        }

        return internal_err!("{}", error_msg);
    }

    Ok(())
}

/// Convert record batches to string representations of individual rows
fn batch_rows_to_strings(batches: &[RecordBatch]) -> Vec<String> {
    use arrow::util::display::array_value_to_string;

    let mut result = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row_values = Vec::new();

            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);

                if array.is_null(row_idx) {
                    row_values.push("NULL".to_string());
                } else if let Some(arr) = array.as_any().downcast_ref::<Float16Array>() {
                    row_values.push(format!("{:.1$}", arr.value(row_idx), 2));
                } else if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                    row_values.push(format!("{:.1$}", arr.value(row_idx), 2));
                } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                    row_values.push(format!("{:.1$}", arr.value(row_idx), 2));
                } else {
                    // Use Arrow's deterministic string representation
                    let value_str = array_value_to_string(array, row_idx)
                        .unwrap_or_else(|_| "ERROR".to_string());
                    row_values.push(value_str);
                }
            }

            // Join columns with a delimiter for deterministic representation
            result.push(row_values.join("|"));
        }
    }

    result
}

/// Checks if the table remains unchanged when sorted according to the provided ordering.
///
/// This implementation is copied from datafusion/core/tests/fuzz_cases/equivalence/utils.rs
pub fn is_table_same_after_sort(
    required_ordering: &LexOrdering,
    batch: &RecordBatch,
) -> Result<bool> {
    if required_ordering.is_empty() {
        return Ok(true);
    }

    let n_row = batch.num_rows();
    if n_row == 0 {
        return Ok(true);
    }

    // Add a unique column of ascending integers to break ties
    let unique_column = Arc::new(UInt32Array::from_iter_values(0..n_row as u32)) as ArrayRef;

    let mut columns = batch.columns().to_vec();
    columns.push(unique_column);

    let mut fields: Vec<Arc<arrow::datatypes::Field>> =
        batch.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(arrow::datatypes::Field::new(
        "unique_col",
        arrow::datatypes::DataType::UInt32,
        false,
    )));

    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let batch_with_unique = RecordBatch::try_new(schema, columns)?;

    // Convert to sort columns
    let mut sort_columns = Vec::new();
    for sort_expr in required_ordering {
        let sort_column = sort_expr.evaluate_to_sort_column(&batch_with_unique)?;
        sort_columns.push(sort_column);
    }

    // Add the unique column sort
    sort_columns.push(SortColumn {
        values: batch_with_unique
            .column(batch_with_unique.num_columns() - 1)
            .clone(),
        options: Some(arrow::compute::SortOptions::default()),
    });

    // Get sorted indices
    let sorted_indices = lexsort_to_indices(&sort_columns, None)?;

    // Check if indices are in natural order [0, 1, 2, ...]
    let expected: Vec<u32> = (0..n_row as u32).collect();
    let actual: Vec<u32> = sorted_indices.values().iter().cloned().collect();
    Ok(actual == expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use std::sync::Arc;

    #[tokio::test]
    async fn test_records_equal_as_sets_empty() {
        let left: Vec<RecordBatch> = vec![];
        let right: Vec<RecordBatch> = vec![];
        assert!(records_equal_as_sets(&left, &right).is_ok());
    }

    #[tokio::test]
    async fn test_records_equal_as_sets_with_data() {
        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 1, 2])), // Different order
                Arc::new(StringArray::from(vec!["c", "a", "b"])),
            ],
        )
        .unwrap();

        let left = vec![batch1];
        let right = vec![batch2];

        // Should be equal as sets (order independent)
        assert!(records_equal_as_sets(&left, &right).is_ok());
    }

    #[tokio::test]
    async fn test_records_equal_different_counts() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))], // Different count
        )
        .unwrap();

        let left = vec![batch1];
        let right = vec![batch2];

        assert!(records_equal_as_sets(&left, &right).is_err());
    }

    #[tokio::test]
    async fn test_batch_rows_to_strings() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true), // Nullable
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();

        let strings = batch_rows_to_strings(&[batch]);

        assert_eq!(strings.len(), 2);
        // The exact format might vary, but we should have 2 strings representing the rows
        assert!(!strings[0].is_empty());
        assert!(!strings[1].is_empty());
        assert!(strings[1].contains("NULL")); // Second row has null value
    }

    #[tokio::test]
    async fn test_detailed_batch_comparison_identical() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let left = vec![batch.clone()];
        let right = vec![batch];

        assert!(detailed_batch_comparison(&left, &right).is_ok());
    }

    #[tokio::test]
    async fn test_detailed_batch_comparison_different() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 4]))], // Different last value
        )
        .unwrap();

        let left = vec![batch1];
        let right = vec![batch2];

        assert!(detailed_batch_comparison(&left, &right).is_err());
    }

    #[tokio::test]
    async fn test_ordering_validation() {
        let actual_ctx = SessionContext::new();
        let expected_ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["c", "b", "a"])),
            ],
        )
        .unwrap();

        actual_ctx
            .register_batch("test_table", batch.clone())
            .unwrap();
        expected_ctx
            .register_batch("test_table", batch.clone())
            .unwrap();

        // Query which sorted by id should pass.
        let ordered_query = "SELECT * FROM test_table ORDER BY value";

        let df = actual_ctx.sql(ordered_query).await.unwrap();
        let task_ctx = actual_ctx.task_ctx();
        let actual_plan = df.create_physical_plan().await.unwrap();
        let results = collect(actual_plan.clone(), task_ctx).await;

        let df = expected_ctx.sql(ordered_query).await.unwrap();
        let expected_plan = df.create_physical_plan().await.unwrap();

        assert!(
            compare_ordering(actual_plan.clone(), expected_plan.clone(), &results)
                .await
                .is_ok()
        );

        // This should fail because the batch is not sorted by value
        let result = Ok(vec![batch]);
        assert!(
            compare_ordering(actual_plan.clone(), expected_plan.clone(), &result)
                .await
                .is_err()
        );
    }
}
