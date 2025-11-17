use crate::DefaultSessionBuilder;
use crate::{DistributedExt, test_utils::localhost::start_localhost_context};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    common::{internal_datafusion_err, internal_err, runtime::JoinSet},
    error::{DataFusionError, Result},
    execution::context::SessionContext,
};
use std::fmt::Display;

/// Distributed database used for fuzzing
pub struct FuzzDB {
    pub distributed_ctx: SessionContext,
    pub oracles: Vec<Box<dyn Oracle + Send + Sync>>,
    pub worker_tasks: JoinSet<()>,
}

/// Configuration parameters for randomized session setup
#[derive(Debug, Clone)]
pub struct FuzzConfig {
    pub num_workers: usize,
    pub files_per_task: usize,
    pub cardinality_task_count_factor: f64,
}

impl Display for FuzzConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FuzzConfig {{ num_workers: {}, files_per_task: {}, cardinality_task_count_factor: {} }}",
            self.num_workers, self.files_per_task, self.cardinality_task_count_factor
        )
    }
}

pub struct Stats {
    pub oracle_validiation_failed: usize,

    pub execution_errors: usize,
}

impl FuzzDB {
    /// Create a new FuzzDB with randomized session parameters and setup function
    pub async fn new<F, Fut>(cfg: FuzzConfig, setup: F) -> Result<Self>
    where
        F: Fn(SessionContext) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        // Make distributed localhost context
        let (mut distributed_ctx, worker_tasks) =
            start_localhost_context(cfg.num_workers, DefaultSessionBuilder).await;
        distributed_ctx.set_distributed_files_per_task(cfg.files_per_task)?;
        distributed_ctx.set_distributed_cardinality_effect_task_scale_factor(
            cfg.cardinality_task_count_factor,
        )?;

        // Create single node context for oracle comparison
        let single_node_ctx = SessionContext::new();

        // Call the setup function to register tables on both contexts
        setup(distributed_ctx.clone()).await?;
        setup(single_node_ctx.clone()).await?;

        // Create oracles
        let oracles: Vec<Box<dyn Oracle + Send + Sync>> =
            vec![Box::new(SingleNodeOracle::new(single_node_ctx))];

        Ok(FuzzDB {
            distributed_ctx,
            oracles,
            worker_tasks,
        })
    }

    // run runs a query and returns an error if any oracle indicates a failure. If a a query errors
    // but all oracles indicate success (ie. if the query errored in single node datafusion, so
    // we can ignore it), then Ok(None) is returned.
    pub async fn run(&self, query: &str) -> Result<Option<Vec<RecordBatch>>> {
        let result = self.run_inner(query).await;
        for oracle in &self.oracles {
            oracle.validate(query, &result).await?;
        }

        if result.is_err() {
            return Ok(None);
        }
        result.map(Some)
    }

    /// execute distributed query
    pub async fn run_inner(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self.distributed_ctx.sql(query).await?;
        df.collect().await
    }
}

/// Trait for query result validation oracles
#[async_trait]
pub trait Oracle {
    /// Validate query results
    async fn validate(&self, query: &str, results: &Result<Vec<RecordBatch>>) -> Result<()>;
}

/// Validates that the set of record batches returned by the distributed query is equal to
/// the set of record batches returned by the single node query. If both queries error, then
/// the validation succeeds.
pub struct SingleNodeOracle {
    single_node_ctx: SessionContext,
}

impl SingleNodeOracle {
    pub fn new(single_node_ctx: SessionContext) -> Self {
        Self { single_node_ctx }
    }
}

impl SingleNodeOracle {
    pub async fn run_inner(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self.single_node_ctx.sql(query).await?;
        df.collect().await
    }
}

#[async_trait]
impl Oracle for SingleNodeOracle {
    async fn validate(
        &self,
        query: &str,
        distributed_result: &Result<Vec<RecordBatch>>,
    ) -> Result<()> {
        // Execute the same query on single node context
        let single_result = self.run_inner(query).await;

        // Query failed on single node context, so the query is not valid
        let distributed_batches = match distributed_result.as_ref() {
            Ok(batches) => batches,
            Err(e) => {
                if single_result.is_ok() {
                    return internal_err!(
                        "distributed query failed but single node succeeded: {}",
                        e
                    );
                }
                return Ok(()); // Both errored, so the query is valid
            }
        };

        let single_node_batches = match single_result.as_ref() {
            Ok(batches) => batches,
            Err(e) => {
                if single_result.is_ok() {
                    return internal_err!(
                        "distributed query succeeded but single node failed: {}",
                        e
                    );
                }
                return Ok(()); // Both errored, so the query is valid
            }
        };

        // Compare results for set equality
        records_equal_as_sets(distributed_batches, single_node_batches).map_err(|e| {
            internal_datafusion_err!("SingleNodeOracle validation failed: {}", e)
        })?;

        Ok(())
    }
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
            left_rows, right_rows
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
                error_msg.push_str(&format!("\n  {}", row));
            }
        }

        if !right_only.is_empty() {
            error_msg.push_str(&format!(
                "\n\nRows only in right ({} total):",
                right_only.len()
            ));
            for row in right_only {
                error_msg.push_str(&format!("\n  {}", row));
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

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
}
