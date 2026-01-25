use crate::NetworkBroadcastExec;
use crate::execution_plans::NetworkCoalesceExec;
use crate::execution_plans::NetworkShuffleExec;
use crate::metrics::proto::MetricsSetProto;
use crate::protobuf::StageKey;
use datafusion::common::HashMap;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::tree_node::TreeNodeRewriter;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::internal_err;
use datafusion::physical_plan::metrics::MetricsSet;
use std::sync::Arc;

/// TaskMetricsCollector is used to collect metrics from a task. It implements [TreeNodeRewriter].
/// Note: TaskMetricsCollector is not a [datafusion::physical_plan::ExecutionPlanVisitor] to keep
/// parity between [TaskMetricsCollector] and [TaskMetricsRewriter].
pub struct TaskMetricsCollector {
    /// metrics contains the metrics for the current task.
    task_metrics: Vec<MetricsSet>,
    /// input_task_metrics contains metrics for tasks from child [StageExec]s if they were
    /// collected.
    input_task_metrics: HashMap<StageKey, Vec<MetricsSetProto>>,
}

/// MetricsCollectorResult is the result of collecting metrics from a task.
pub struct MetricsCollectorResult {
    // metrics is a collection of metrics for a task ordered using a pre-order traversal of the task's plan.
    pub task_metrics: Vec<MetricsSet>,
    // input_task_metrics contains metrics for child tasks if they were collected.
    pub input_task_metrics: HashMap<StageKey, Vec<MetricsSetProto>>,
}

impl TreeNodeRewriter for TaskMetricsCollector {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        // For plan nodes in this task, collect metrics.
        match plan.metrics() {
            Some(metrics) => self.task_metrics.push(metrics.clone()),
            None => {
                // TODO: Consider using a more efficent encoding scheme to avoid empty slots in the vec.
                self.task_metrics.push(MetricsSet::new())
            }
        }

        // If the plan is a network boundary, assume it has collected metrics already
        // from child tasks.
        let metrics_collection =
            if let Some(node) = plan.as_any().downcast_ref::<NetworkShuffleExec>() {
                Some(Arc::clone(&node.metrics_collection))
            } else if let Some(node) = plan.as_any().downcast_ref::<NetworkCoalesceExec>() {
                Some(Arc::clone(&node.metrics_collection))
            } else if let Some(node) = plan.as_any().downcast_ref::<NetworkBroadcastExec>() {
                Some(Arc::clone(&node.metrics_collection))
            } else {
                None
            };

        if let Some(metrics_collection) = metrics_collection {
            for mut entry in metrics_collection.iter_mut() {
                let stage_key = entry.key().clone();
                let task_metrics = std::mem::take(entry.value_mut()); // Avoid copy.
                match self.input_task_metrics.get(&stage_key) {
                    // There should never be two NetworkShuffleExec with metrics for the same stage_key.
                    // By convention, the NetworkShuffleExec which runs the last partition in a task should be
                    // sent metrics (the NetworkShuffleExec tracks it for us).
                    Some(_) => {
                        return internal_err!(
                            "duplicate task metrics for key {:?} during metrics collection",
                            stage_key
                        );
                    }
                    None => {
                        self.input_task_metrics
                            .insert(stage_key.clone(), task_metrics);
                    }
                }
            }
            // Skip the subtree of the NetworkShuffleExec.
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
        }

        Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue))
    }
}

impl TaskMetricsCollector {
    pub fn new() -> Self {
        Self {
            task_metrics: Vec::new(),
            input_task_metrics: HashMap::new(),
        }
    }

    /// collect metrics from an [ExecutionPlan] (usually a [StageExec].plan) and any child tasks.
    /// Returns
    /// - a vec representing the metrics for the current task (ordered using a pre-order traversal)
    /// - a map representing the metrics for some subset of child tasks collected from NetworkShuffleExec leaves
    pub fn collect(
        mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<MetricsCollectorResult, DataFusionError> {
        plan.rewrite(&mut self)?;
        Ok(MetricsCollectorResult {
            task_metrics: self.task_metrics,
            input_task_metrics: self.input_task_metrics,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use arrow::datatypes::UInt16Type;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use futures::StreamExt;

    use crate::execution_plans::DistributedExec;
    use crate::test_utils::in_memory_channel_resolver::{
        InMemoryChannelResolver, InMemoryWorkerResolver,
    };
    use crate::test_utils::parquet::register_parquet_tables;
    use crate::test_utils::plans::{
        count_plan_nodes_up_to_network_boundary, get_stages_and_stage_keys,
    };
    use crate::test_utils::session_context::register_temp_parquet_table;
    use crate::{DistributedExt, DistributedPhysicalOptimizerRule};
    use datafusion::execution::{SessionStateBuilder, context::SessionContext};
    use datafusion::prelude::SessionConfig;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        physical_plan::display::DisplayableExecutionPlan,
    };
    use std::sync::Arc;

    /// Creates a session context and registers two tables:
    /// - table1 (id: int, name: string)
    /// - table2 (id: int, name: string, phone: string, balance: float64)
    async fn make_test_ctx() -> SessionContext {
        // Create distributed session state with in-memory channel resolver
        let config = SessionConfig::new().with_target_partitions(2);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(10))
            .with_distributed_channel_resolver(InMemoryChannelResolver::default())
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_distributed_metrics_collection(true)
            .unwrap()
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
            Field::new(
                "company",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                false,
            ),
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
                    Arc::new(
                        vec!["company1", "company1", "company1"]
                            .into_iter()
                            .collect::<arrow::array::DictionaryArray<UInt16Type>>(),
                    ),
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

    async fn execute_plan(stage_exec: Arc<dyn ExecutionPlan>, ctx: &SessionContext) {
        let task_ctx = ctx.task_ctx();
        let stream = stage_exec.execute(0, task_ctx).unwrap();

        let schema = stream.schema();

        let mut stream = stream;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();

            assert_eq!(schema, batch.schema())
        }
    }

    /// Asserts that we can collect metrics from a distributed plan generated from the
    /// SQL query. It ensures that metrics are collected for all stages and are propagated
    /// through network boundaries.
    async fn run_metrics_collection_e2e_test(sql: &str) {
        // Plan and execute the query
        let ctx = make_test_ctx().await;
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        execute_plan(plan.clone(), &ctx).await;

        let dist_exec = plan
            .as_any()
            .downcast_ref::<DistributedExec>()
            .expect("expected DistributedExec");

        // Assert to ensure the distributed test case is sufficiently complex.
        let (stages, expected_stage_keys) = get_stages_and_stage_keys(dist_exec);
        assert!(
            expected_stage_keys.len() > 1,
            "expected more than 1 stage key in test. the plan was not distributed):\n{}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );

        // Collect metrics for all tasks from the root StageExec.
        let collector = TaskMetricsCollector::new();

        let result = collector.collect(dist_exec.plan.clone()).unwrap();

        // Ensure that there's metrics for each node for each task for each stage.
        for expected_stage_key in expected_stage_keys {
            // Get the collected metrics for this task.
            let actual_metrics = result.input_task_metrics.get(&expected_stage_key).unwrap();

            // Verify that metrics were collected for all nodes. Some nodes may legitimately have
            // empty metrics (e.g., custom execution plans without metrics), which is fine - we
            // just verify that a metrics set exists for each node. The count assertion above
            // ensures all nodes are included in the metrics collection.
            let stage = stages.get(&(expected_stage_key.stage_id as usize)).unwrap();
            let stage_plan = stage.plan.decoded().unwrap();
            assert_eq!(
                actual_metrics.len(),
                count_plan_nodes_up_to_network_boundary(stage_plan),
                "Mismatch between collected metrics and actual nodes for {expected_stage_key:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_metrics_collection_e2e_1() {
        run_metrics_collection_e2e_test("SELECT id, COUNT(*) as count FROM table1 WHERE id > 1 GROUP BY id ORDER BY id LIMIT 10").await;
    }

    // Skip this test, it's failing after upgrading to datafusion 50
    // See https://github.com/datafusion-contrib/datafusion-distributed/pull/146#issuecomment-3356621629
    #[tokio::test]
    async fn test_metrics_collection_e2e_2() {
        run_metrics_collection_e2e_test(
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

    #[tokio::test]
    async fn test_metrics_collection_e2e_3() {
        run_metrics_collection_e2e_test(
            "SELECT
                substring(phone, 1, 2) as country_code,
                count(*) as num_customers,
                sum(balance) as total_balance
            FROM table2
            WHERE substring(phone, 1, 2) IN ('13', '31', '23', '29', '30', '18')
              AND balance > (
                SELECT avg(balance)
                FROM table2
                WHERE balance > 0.00
              )
            GROUP BY substring(phone, 1, 2)
            ORDER BY country_code",
        )
        .await;
    }

    /// Skipped due to https://github.com/apache/datafusion/issues/14218
    ///
    /// When aggregating on a dictionary column (ex. `company` in this case which is Dict<UInt16, Utf8>),
    /// the aggregation seems to be outputting Utf8. Some assertion fails due to this, even in
    /// single node execution:
    /// "column types must match schema types, expected Dictionary(UInt16, Utf8) but found Utf8 at column index 0"
    #[tokio::test]
    #[ignore]
    async fn test_metrics_collection_e2e_4() {
        run_metrics_collection_e2e_test("SELECT distinct company from table2").await;
    }

    /// Test that verifies PartitionIsolatorExec nodes are preserved during metrics collection.
    /// This tests the corner case where PartitionIsolatorExec nodes (which have no metrics)
    /// must still be included in the metrics collection to maintain correct node-to-metric mapping.
    #[tokio::test]
    async fn test_metrics_collection_with_partition_isolator() {
        let ctx = make_test_ctx().await;
        ctx.sql("SET distributed.children_isolator_unions=true;")
            .await
            .unwrap();

        // Use weather dataset (already available)
        register_parquet_tables(&ctx).await.unwrap();

        // UNION query that creates PartitionIsolatorExec nodes
        let query = r#"
            SELECT "MinTemp" FROM weather WHERE "RainToday" = 'yes'
            UNION ALL
            SELECT "MaxTemp" FROM weather WHERE "RainToday" = 'no'
        "#;

        let df = ctx.sql(query).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        execute_plan(plan.clone(), &ctx).await;

        let dist_exec = plan
            .as_any()
            .downcast_ref::<DistributedExec>()
            .expect("expected DistributedExec");

        let (stages, expected_stage_keys) = get_stages_and_stage_keys(dist_exec);
        let collector = TaskMetricsCollector::new();
        let result = collector.collect(dist_exec.plan.clone()).unwrap();

        // Verify all nodes (including PartitionIsolatorExec) are preserved in metrics collection
        for expected_stage_key in expected_stage_keys {
            let actual_metrics = result.input_task_metrics.get(&expected_stage_key).unwrap();
            let stage = stages.get(&(expected_stage_key.stage_id as usize)).unwrap();
            let stage_plan = stage.plan.decoded().unwrap();

            // Verify metrics count matches - this ensures all nodes are included in metrics collection
            // regardless of whether they have metrics or not (some nodes may have empty metrics sets)
            assert_eq!(
                actual_metrics.len(),
                count_plan_nodes_up_to_network_boundary(stage_plan),
                "Metrics count must match plan nodes for stage {expected_stage_key:?}"
            );
        }
    }
}
