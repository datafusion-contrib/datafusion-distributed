use crate::distributed_planner::NetworkBoundaryExt;
use crate::execution_plans::DistributedExec;
use crate::execution_plans::MetricsWrapperExec;
use crate::metrics::MetricsCollectorResult;
use crate::metrics::TaskMetricsCollector;
use crate::metrics::proto::MetricsSetProto;
use crate::metrics::proto::metrics_set_proto_to_df;
use crate::protobuf::StageKey;
use crate::stage::Stage;
use bytes::Bytes;
use datafusion::common::HashMap;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::internal_err;
use datafusion::physical_plan::metrics::MetricsSet;
use std::sync::Arc;
use std::vec;

/// Rewrites a distributed plan with metrics. Does nothing if the root node is not a [DistributedExec].
/// Returns an error if the distributed plan was not executed.
pub fn rewrite_distributed_plan_with_metrics(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(distributed_exec) = plan.as_any().downcast_ref::<DistributedExec>() else {
        return Ok(plan);
    };

    // Collect metrics from the DistributedExec's prepared plan.
    let MetricsCollectorResult {
        task_metrics,       // Metrics for the DistributedExec plan
        input_task_metrics, // Metrics for all child stages / tasks.
    } = TaskMetricsCollector::new().collect(distributed_exec.prepared_plan()?)?;

    // Rewrite the DistributedExec's child plan with metrics.
    let dist_exec_plan_with_metrics =
        rewrite_local_plan_with_metrics(plan.children()[0].clone(), task_metrics)?;
    let plan = plan.with_new_children(vec![dist_exec_plan_with_metrics])?;

    let metrics_collection = Arc::new(input_task_metrics);

    let transformed = plan.transform_down(|plan| {
        // Transform all stages using NetworkShuffleExec and NetworkCoalesceExec as barriers.
        if let Some(network_boundary) = plan.as_network_boundary() {
            let stage = network_boundary.input_stage();
            // This transform is a bit inefficient because we traverse the plan nodes twice
            // For now, we are okay with trading off performance for simplicity.
            let plan_with_metrics = stage_metrics_rewriter(stage, metrics_collection.clone())?;
            return Ok(Transformed::yes(network_boundary.with_input_stage(
                Stage::new(
                    stage.query_id,
                    stage.num,
                    plan_with_metrics,
                    stage.tasks.len(),
                ),
            )?));
        }

        Ok(Transformed::no(plan))
    })?;
    Ok(transformed.data)
}

/// Rewrites a local plan with metrics, stopping at network boundaries.
///
/// Example:
///
/// AggregateExec [output_rows = 1, elapsed_compute = 100]
///  └── ProjectionExec [output_rows = 2, elapsed_compute = 200]
///      └── NetworkShuffleExec
///
/// The result will be:
///
/// MetricsWrapperExec (wrapped: AggregateExec) [output_rows = 1, elapsed_compute = 100]
///  └── MetricsWrapperExec (wrapped: ProjectionExec) [output_rows = 2, elapsed_compute = 200]
///      └── NetworkShuffleExec
pub fn rewrite_local_plan_with_metrics(
    plan: Arc<dyn ExecutionPlan>,
    metrics: Vec<MetricsSet>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut idx = 0;
    Ok(plan
        .transform_down(|node| {
            if node.is_network_boundary() {
                return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
            }
            if idx >= metrics.len() {
                return internal_err!("not enough metrics provided to rewrite plan");
            }
            let node_metrics = metrics[idx].clone();
            idx += 1;
            Ok(Transformed::yes(Arc::new(MetricsWrapperExec::new(
                node.clone(),
                node_metrics,
            ))))
        })?
        .data)
}

/// Enriches a stage with metrics from each task by re-writing the plan using
/// [MetricsWrapperExec] nodes.
///
/// Example:
///
/// For a stage with 2 tasks:
///
/// Task 1:
/// AggregateExec [output_rows = 1, elapsed_compute = 100]
///  └── ProjectionExec [output_rows = 2, elapsed_compute = 200]
///      └── NetworkShuffleExec
///
/// Task 2:
/// AggregateExec [output_rows = 3, elapsed_compute = 300]
///  └── ProjectionExec [output_rows = 4, elapsed_compute = 400]
///      └── NetworkShuffleExec
///
/// The result will be:
///
/// MetricsWrapperExec (wrapped: AggregateExec) [output_rows = 1, output_rows = 3, elapsed_compute = 100, elapsed_compute = 300]
///  └── MetricsWrapperExec (wrapped: ProjectionExec) [output_rows = 2, output_rows = 4, elapsed_compute = 200, elapsed_compute = 400]
///      └── NetworkShuffleExec
///
/// Note:
/// - The NetworkShuffleExec node is not wrapped
/// - Metrics may be aggregated by type (ex. output_rows) automatically by various datafusion utils.
///
/// TODO(#184): Collect metrics from network nodes
/// TODO(#185): Add labels for each task
pub fn stage_metrics_rewriter(
    stage: &Stage,
    metrics_collection: Arc<HashMap<StageKey, Vec<MetricsSetProto>>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut node_idx = 0;

    let plan = stage.plan.decoded()?;

    plan.clone().transform_down(|plan| {
        // Stop at network boundaries.
        if plan.as_network_boundary().is_some() {
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
        }

        // Collect metrics for this node. It should contain metrics from each task.
        let mut stage_metrics = MetricsSetProto::new();

        for idx in 0..stage.tasks.len() {
            let stage_key = StageKey::new(Bytes::from(stage.query_id.as_bytes().to_vec()), stage.num as u64, idx as u64);
            match metrics_collection.get(&stage_key) {
                Some(task_metrics) => {
                    if node_idx >= task_metrics.len() {
                        return internal_err!(
                            "not enough metrics provided to rewrite task: {} metrics provided",
                            task_metrics.len()
                        );
                    }
                    let node_metrics = task_metrics[node_idx].clone();
                    for metric in node_metrics.metrics.iter() {
                        stage_metrics.push(metric.clone());
                    }
                }
                None => {
                    return internal_err!(
                        "not enough metrics provided to rewrite task: missing metrics for task {} in stage {}",
                        idx,
                        stage.num
                    );
                }
            }
        }

        node_idx += 1;

        let wrapped_plan_node: Arc<dyn ExecutionPlan> = Arc::new(MetricsWrapperExec::new(
            plan.clone(),
            metrics_set_proto_to_df(&stage_metrics)?,
        ));
        Ok(Transformed::yes(wrapped_plan_node))
    }).map(|v| v.data)
}

#[cfg(test)]
mod tests {
    use crate::PartitionIsolatorExec;
    use crate::metrics::proto::{
        MetricsSetProto, df_metrics_set_to_proto, metrics_set_proto_to_df,
    };
    use crate::metrics::rewrite_distributed_plan_with_metrics;
    use crate::metrics::task_metrics_rewriter::stage_metrics_rewriter;
    use crate::protobuf::StageKey;
    use crate::test_utils::in_memory_channel_resolver::InMemoryChannelResolver;
    use crate::test_utils::metrics::make_test_metrics_set_proto_from_seed;
    use crate::test_utils::plans::count_plan_nodes;
    use crate::test_utils::session_context::register_temp_parquet_table;
    use crate::{DistributedExec, DistributedPhysicalOptimizerRule};
    use crate::{NetworkBoundaryExt, Stage};
    use bytes::Bytes;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::HashMap;
    use datafusion::execution::SessionStateBuilder;

    use datafusion::physical_plan::{ExecutionPlan, collect};
    use itertools::Itertools;
    use uuid::Uuid;

    use crate::DistributedExt;
    use crate::metrics::task_metrics_rewriter::MetricsWrapperExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::metrics::MetricsSet;
    use datafusion::prelude::SessionConfig;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    async fn make_test_ctx() -> SessionContext {
        make_test_ctx_inner(false).await
    }

    async fn make_test_distributed_ctx() -> SessionContext {
        make_test_ctx_inner(true).await
    }

    /// Creates a non-distributed session context and registers two tables:
    /// - table1 (id: int, name: string)
    /// - table2 (id: int, name: string, phone: string, balance: float64)
    async fn make_test_ctx_inner(distributed: bool) -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(4);
        let mut builder = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config);

        if distributed {
            builder = builder
                .with_distributed_channel_resolver(InMemoryChannelResolver::new(10))
                .with_distributed_metrics_collection(true)
                .unwrap()
                .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
                .with_distributed_task_estimator(2)
        }

        let state = builder.build();
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

    fn make_test_stage(plan: Arc<dyn ExecutionPlan>) -> Stage {
        Stage::new(Uuid::new_v4(), 2, plan, 4)
    }

    fn collect_metrics_from_plan(plan: &Arc<dyn ExecutionPlan>, metrics: &mut Vec<MetricsSet>) {
        metrics.extend(plan.metrics());
        for child in plan.children() {
            collect_metrics_from_plan(child, metrics);
        }
    }

    fn metrics_set_eq(a: &MetricsSet, b: &MetricsSet) -> bool {
        // Check equality by converting to proto representation.
        df_metrics_set_to_proto(a).unwrap() == df_metrics_set_to_proto(b).unwrap()
    }

    /// Asserts that we successfully re-write the metrics of a plan generated from the provided SQL query.
    /// Also asserts that the order which metrics are collected from a plan matches the order which
    /// they are re-written (ie. ensures we don't assign metrics to the wrong nodes)
    ///
    /// Only tests single node plans since the [TaskMetricsRewriter] stops on [NetworkBoundary].
    async fn run_stage_metrics_rewriter_test(sql: &str) {
        // Generate the plan
        let ctx = make_test_ctx().await;
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let stage = make_test_stage(plan.clone());

        let num_metrics_per_task_per_node = 4;

        // Generate metrics for each task and store them in the map.
        let mut metrics_collection = HashMap::new();
        for task_id in 0..stage.tasks.len() {
            let stage_key = StageKey::new(
                Bytes::from(stage.query_id.as_bytes().to_vec()),
                stage.num as u64,
                task_id as u64,
            );
            let metrics = (0..count_plan_nodes(&plan))
                .map(|node_id| {
                    make_test_metrics_set_proto_from_seed(
                        (node_id * task_id) as u64,
                        num_metrics_per_task_per_node,
                    )
                })
                .collect::<Vec<MetricsSetProto>>();

            metrics_collection.insert(stage_key, metrics);
        }
        let metrics_collection = Arc::new(metrics_collection);

        // Rewrite the plan.
        let rewritten_plan = stage_metrics_rewriter(&stage, metrics_collection.clone()).unwrap();

        // Collect metrics from the plan.
        let mut actual_metrics = vec![];
        collect_metrics_from_plan(&rewritten_plan, &mut actual_metrics);
        assert_eq!(actual_metrics.len(), count_plan_nodes(&plan));

        // Assert that metrics from all tasks are present.
        // actual_stage_node_metrics_set contains metrics for all task ex. [output_rows=1, elapsed_compute=1, output_rows=2, elapsed_compute=2...]
        for (node_id, actual_stage_node_metrics_set) in actual_metrics.iter().enumerate() {
            // actual_task_node_metrics_set contains metrics for one task ex. [output_rows=1, elapsed_compute=1]
            for (task_id, actual_task_node_metrics_set) in actual_stage_node_metrics_set
                .iter()
                .chunks(num_metrics_per_task_per_node)
                .into_iter()
                .enumerate()
            {
                let expected_task_node_metrics = metrics_collection
                    .get(&StageKey::new(
                        Bytes::from(stage.query_id.as_bytes().to_vec()),
                        stage.num as u64,
                        task_id as u64,
                    ))
                    .unwrap()[node_id]
                    .clone();

                let mut actual_metrics_set = MetricsSet::new();
                actual_task_node_metrics_set
                    .for_each(|metric| actual_metrics_set.push(metric.clone()));

                let expected_metrics_set =
                    metrics_set_proto_to_df(&expected_task_node_metrics).unwrap(); // Convert to proto to check for equality.
                assert!(metrics_set_eq(&actual_metrics_set, &expected_metrics_set));
            }
        }
    }

    #[tokio::test]
    async fn test_stage_metrics_rewriter_1() {
        run_stage_metrics_rewriter_test(
            "SELECT sum(balance) / 7.0 as avg_yearly from table2 group by name",
        )
        .await;
    }

    #[tokio::test]
    async fn test_stage_metrics_rewriter_2() {
        run_stage_metrics_rewriter_test("SELECT id, COUNT(*) as count FROM table1 WHERE id > 1 GROUP BY id ORDER BY id LIMIT 10").await;
    }

    #[tokio::test]
    async fn test_stage_metrics_rewriter_3() {
        run_stage_metrics_rewriter_test(
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
    async fn test_rewrite_unexecuted_distributed_plan_with_metrics_err() {
        let ctx = make_test_distributed_ctx().await;
        let plan = ctx
            .sql("SELECT id, COUNT(*) as count FROM table1 WHERE id > 1 GROUP BY id ORDER BY id LIMIT 10")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert!(plan.as_any().is::<DistributedExec>());
        assert!(rewrite_distributed_plan_with_metrics(plan).is_err());
    }

    // Assert every plan node has at least one metric except partition isolators, network boundary nodes, and the root DistributedExec node.
    fn assert_metrics_present_in_plan(plan: &Arc<dyn ExecutionPlan>) {
        if let Some(metrics) = plan.metrics() {
            assert!(metrics.iter().count() > 0);
        } else {
            let is_partition_isolator =
                if let Some(metrics_wrapper) = plan.as_any().downcast_ref::<MetricsWrapperExec>() {
                    metrics_wrapper
                        .as_any()
                        .downcast_ref::<PartitionIsolatorExec>()
                        .is_some()
                } else {
                    false
                };
            assert!(
                plan.is_network_boundary()
                    || is_partition_isolator
                    || plan.as_any().is::<DistributedExec>()
            );
        }
        for child in plan.children() {
            assert_metrics_present_in_plan(child);
        }
    }

    #[tokio::test]
    async fn test_executed_distributed_plan_has_metrics() {
        let ctx = make_test_distributed_ctx().await;
        let plan = ctx
            .sql("SELECT id, COUNT(*) as count FROM table1 WHERE id > 1 GROUP BY id ORDER BY id LIMIT 10")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        collect(plan.clone(), ctx.task_ctx()).await.unwrap();
        assert!(plan.as_any().is::<DistributedExec>());
        let rewritten_plan = rewrite_distributed_plan_with_metrics(plan).unwrap();
        assert_metrics_present_in_plan(&rewritten_plan);
    }

    #[test]
    // An important feature of DF execution plans which we want to preserve is the ability
    // to traverse a plan and collect metrics from specific nodes. To do this, the wrapper must
    // allow access to the inner node. This test asserts that we support this.
    fn test_wrapped_node_is_accessible() {
        let example_node = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]))));

        let wrapped = MetricsWrapperExec::new(example_node, MetricsSet::new());
        assert_eq!(wrapped.name(), "EmptyExec");
        assert!(wrapped.as_any().is::<EmptyExec>());
    }
}
