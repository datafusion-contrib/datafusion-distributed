use crate::execution_plans::MetricsWrapperExec;
use crate::execution_plans::NetworkCoalesceExec;
use crate::execution_plans::NetworkShuffleExec;
use crate::metrics::proto::MetricsSetProto;
use crate::metrics::proto::metrics_set_proto_to_df;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::tree_node::TreeNodeRewriter;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::internal_err;
use std::sync::Arc;

/// TaskMetricsRewriter is used to enrich a task with metrics by re-writing the plan using [MetricsWrapperExec] nodes.
///
/// Ex. for a plan with the form
/// AggregateExec
///  └── ProjectionExec
///      └── NetworkShuffleExec
///
/// the task will be rewritten as
///
/// MetricsWrapperExec (wrapped: AggregateExec)
///  └── MetricsWrapperExec (wrapped: ProjectionExec)
///      └── NetworkShuffleExec
/// (Note that the NetworkShuffleExec node is not wrapped)
pub struct TaskMetricsRewriter {
    metrics: Vec<MetricsSetProto>,
    idx: usize,
}

impl TaskMetricsRewriter {
    /// Create a new TaskMetricsRewriter. The provided metrics will be used to enrich the plan.
    #[allow(dead_code)]
    pub fn new(metrics: Vec<MetricsSetProto>) -> Self {
        Self { metrics, idx: 0 }
    }

    /// enrich_task_with_metrics rewrites the plan by wrapping nodes. If the length of the provided metrics set vec does not
    /// match the number of nodes in the plan, an error will be returned.
    #[allow(dead_code)]
    pub fn enrich_task_with_metrics(
        mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let transformed = plan.rewrite(&mut self)?;
        if self.idx != self.metrics.len() {
            return internal_err!(
                "too many metrics sets provided to rewrite task: {} metrics sets provided, {} nodes in plan",
                self.metrics.len(),
                self.idx
            );
        }
        Ok(transformed.data)
    }
}

impl TreeNodeRewriter for TaskMetricsRewriter {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        if plan.as_any().is::<NetworkShuffleExec>() {
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
        }
        if plan.as_any().is::<NetworkCoalesceExec>() {
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
        }
        if self.idx >= self.metrics.len() {
            return internal_err!(
                "not enough metrics provided to rewrite task: {} metrics provided",
                self.metrics.len()
            );
        }
        let proto_metrics = &self.metrics[self.idx];

        let wrapped_plan_node: Arc<dyn ExecutionPlan> = Arc::new(MetricsWrapperExec::new(
            plan.clone(),
            metrics_set_proto_to_df(proto_metrics)?,
        ));
        let result = Transformed::new(wrapped_plan_node, true, TreeNodeRecursion::Continue);
        self.idx += 1;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::metrics::proto::MetricsSetProto;
    use crate::metrics::proto::df_metrics_set_to_proto;
    use crate::metrics::task_metrics_collector::TaskMetricsCollector;
    use crate::metrics::task_metrics_rewriter::TaskMetricsRewriter;
    use crate::test_utils::metrics::make_test_metrics_set_proto_from_seed;
    use crate::test_utils::plans::count_plan_nodes;
    use crate::test_utils::session_context::register_temp_parquet_table;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionConfig;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    /// Creates a non-distributed session context and registers two tables:
    /// - table1 (id: int, name: string)
    /// - table2 (id: int, name: string, phone: string, balance: float64)
    async fn make_test_ctx() -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(2);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::from(state);

        // Create test data for table1
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batches1 = vec![
            RecordBatch::try_new(
                schema1.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap(),
        ];

        // Create test data for table2 with extended schema
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("phone", DataType::Utf8, false),
            Field::new("balance", DataType::Float64, false),
        ]));

        let batches2 = vec![
            RecordBatch::try_new(
                schema2.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![
                        "customer1",
                        "customer2",
                        "customer3",
                    ])),
                    Arc::new(StringArray::from(vec![
                        "13-123-4567",
                        "31-456-7890",
                        "23-789-0123",
                    ])),
                    Arc::new(datafusion::arrow::array::Float64Array::from(vec![
                        100.5, 250.0, 50.25,
                    ])),
                ],
            )
            .unwrap(),
        ];

        // Register the test data as parquet tables
        let _ = register_temp_parquet_table("table1", schema1, batches1, &ctx)
            .await
            .unwrap();

        let _ = register_temp_parquet_table("table2", schema2, batches2, &ctx)
            .await
            .unwrap();

        ctx
    }

    /// Asserts that we successfully re-write the metrics of a plan generated from the provided SQL query.
    /// Also asserts that the order which metrics are collected from a plan matches the order which
    /// they are re-written (ie. ensures we don't assign metrics to the wrong nodes)
    ///
    /// Only tests single node plans since the [TaskMetricsRewriter] stops on [NetworkBoundary].
    async fn run_metrics_rewriter_test(sql: &str) {
        // Generate the plan
        let ctx = make_test_ctx().await;
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        // Generate metrics for each plan node.
        let expected_metrics = (0..count_plan_nodes(&plan))
            .map(|i| make_test_metrics_set_proto_from_seed(i as u64 + 10))
            .collect::<Vec<MetricsSetProto>>();

        // Rewrite the metrics.
        let rewriter = TaskMetricsRewriter::new(expected_metrics.clone());
        let rewritten_plan = rewriter.enrich_task_with_metrics(plan.clone()).unwrap();

        // Collect metrics
        let actual_metrics = TaskMetricsCollector::new()
            .collect(rewritten_plan)
            .unwrap()
            .task_metrics;

        // Assert that all the metrics are present and in the same order.
        assert_eq!(actual_metrics.len(), expected_metrics.len());
        for (actual_metrics_set, expected_metrics_set) in actual_metrics
            .iter()
            .map(|m| df_metrics_set_to_proto(m).unwrap())
            .zip(expected_metrics)
        {
            assert_eq!(actual_metrics_set, expected_metrics_set);
        }
    }

    #[tokio::test]
    async fn test_metrics_rewriter_1() {
        run_metrics_rewriter_test(
            "SELECT sum(balance) / 7.0 as avg_yearly from table2 group by name",
        )
        .await;
    }

    #[tokio::test]
    async fn test_metrics_rewriter_2() {
        run_metrics_rewriter_test("SELECT id, COUNT(*) as count FROM table1 WHERE id > 1 GROUP BY id ORDER BY id LIMIT 10").await;
    }

    #[tokio::test]
    async fn test_metrics_rewriter_3() {
        run_metrics_rewriter_test(
            "SELECT sum(balance) / 7.0 as avg_yearly
            FROM table2
            WHERE name LIKE 'customer%'
              AND balance < (
                SELECT 0.2 * avg(balance)
                FROM table2 t2_inner
                WHERE t2_inner.id = table2.id
              )",
        )
        .await;
    }
}
