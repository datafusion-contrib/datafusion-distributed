use crate::distributed_planner::NetworkBoundaryExt;
use crate::execution_plans::DistributedExec;
use crate::execution_plans::MetricsWrapperExec;
use crate::metrics::DISTRIBUTED_DATAFUSION_TASK_ID_LABEL;
use crate::metrics::MetricsCollectorResult;
use crate::metrics::TaskMetricsCollector;
use crate::metrics::proto::{MetricsSetProto, metrics_set_proto_to_df};
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
use datafusion::physical_plan::metrics::{Label, Metric, MetricsSet};
use std::sync::Arc;
use std::vec;

/// Format to use when displaying metrics for a distributed plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistributedMetricsFormat {
    /// Metrics are aggregated across all tasks. ex. a `output_rows=X` represents the output rows for all tasks.
    Aggregated,

    /// Metric names are rewritten to include the task id. ex. `output_rows` -> `output_rows_0`, `output_rows_1` etc.
    PerTask,
}

impl DistributedMetricsFormat {
    pub(crate) fn to_rewrite_ctx(self, task_id: u64) -> RewriteCtx {
        match self {
            DistributedMetricsFormat::Aggregated => RewriteCtx::default(),
            DistributedMetricsFormat::PerTask => RewriteCtx::from_task_id(task_id),
        }
    }
}

/// Rewrites a distributed plan with metrics. Does nothing if the root node is not a [DistributedExec].
/// Returns an error if the distributed plan was not executed.
pub fn rewrite_distributed_plan_with_metrics(
    plan: Arc<dyn ExecutionPlan>,
    format: DistributedMetricsFormat,
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
    let dist_exec_plan_with_metrics = rewrite_local_plan_with_metrics(
        format.to_rewrite_ctx(0), // Task id is 0 for the DistributedExec plan
        plan.children()[0].clone(),
        task_metrics,
    )?;
    let plan = plan.with_new_children(vec![dist_exec_plan_with_metrics])?;

    let metrics_collection = Arc::new(input_task_metrics);

    let transformed = plan.transform_down(|plan| {
        // Transform all stages using NetworkShuffleExec and NetworkCoalesceExec as barriers.
        if let Some(network_boundary) = plan.as_network_boundary() {
            let stage = network_boundary.input_stage();
            // This transform is a bit inefficient because we traverse the plan nodes twice
            // For now, we are okay with trading off performance for simplicity.
            let plan_with_metrics =
                stage_metrics_rewriter(stage, metrics_collection.clone(), format)?;
            let network_boundary = network_boundary.with_input_stage(Stage::new(
                stage.query_id,
                stage.num,
                plan_with_metrics,
                stage.tasks.len(),
            ))?;
            let network_boundary =
                MetricsWrapperExec::new(network_boundary, plan.metrics().unwrap_or_default());
            return Ok(Transformed::yes(Arc::new(network_boundary)));
        }

        Ok(Transformed::no(plan))
    })?;
    Ok(transformed.data)
}

/// Extra information for rewriting local plans.
#[derive(Default)]
pub struct RewriteCtx {
    /// Used to rename metrics for the current task.
    pub task_id: Option<u64>,
}

impl RewriteCtx {
    pub(crate) fn from_task_id(task_id: u64) -> RewriteCtx {
        RewriteCtx {
            task_id: Some(task_id),
        }
    }

    /// Rewrites the [MetricsSet] depending on the context.
    pub(crate) fn maybe_rewrite_node_metics(&self, node_metrics: MetricsSet) -> MetricsSet {
        if let Some(task_id) = self.task_id {
            return annotate_metrics_set_with_task_id(node_metrics, task_id);
        }
        node_metrics
    }
}

/// Adds task id labels to all metrics in the provided [MetricsSet].
///
/// TODO: This re-allocates the vec of metrics by creating a new [MetricsSet]. It also
/// reallocates the labels vec for each metric. Can we avoid this?
/// See https://github.com/apache/datafusion/issues/19959
pub fn annotate_metrics_set_with_task_id(metrics_set: MetricsSet, task_id: u64) -> MetricsSet {
    let mut result = MetricsSet::new();

    for metric in metrics_set.iter() {
        let mut labels = metric.labels().to_vec();
        labels.push(Label::new(
            DISTRIBUTED_DATAFUSION_TASK_ID_LABEL,
            task_id.to_string(),
        ));
        result.push(Arc::new(Metric::new_with_labels(
            metric.value().clone(),
            metric.partition(),
            labels,
        )));
    }

    result
}

/// Rewrites a local plan with metrics, stopping at network boundaries.
///
/// Example:
///
/// AggregateExec [output_rows = 1, elapsed_compute = 100]
///  └── ProjectionExec [output_rows = 2, elapsed_compute = 200]
///      └── NetworkShuffleExec [bytes_transferred = 100, max_mem_used = 100]
///
/// The result will be:
///
/// MetricsWrapperExec (wrapped: AggregateExec) [output_rows = 1, elapsed_compute = 100]
///  └── MetricsWrapperExec (wrapped: ProjectionExec) [output_rows = 2, elapsed_compute = 200]
///      └── MetricsWrapperExec (wrapped: NetworkShuffleExec) [bytes_transferred = 100, max_mem_used = 100]
pub fn rewrite_local_plan_with_metrics(
    ctx: RewriteCtx,
    plan: Arc<dyn ExecutionPlan>,
    metrics: Vec<MetricsSet>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut idx = 0;
    Ok(plan
        .transform_down(|node| {
            if idx >= metrics.len() {
                return internal_err!("not enough metrics provided to rewrite plan");
            }
            let mut node_metrics = metrics[idx].clone();

            node_metrics = ctx.maybe_rewrite_node_metics(node_metrics);

            idx += 1;
            Ok(Transformed::new(
                Arc::new(MetricsWrapperExec::new(node.clone(), node_metrics)),
                true,
                if node.is_network_boundary() {
                    TreeNodeRecursion::Jump
                } else {
                    TreeNodeRecursion::Continue
                },
            ))
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
///      └── NetworkShuffleExec [bytes_transferred = 100, max_mem_used = 100]
///
/// Task 2:
/// AggregateExec [output_rows = 3, elapsed_compute = 300]
///  └── ProjectionExec [output_rows = 4, elapsed_compute = 400]
///      └── NetworkShuffleExec [bytes_transferred = 200, max_mem_used = 200]
///
/// The result will be:
///
/// MetricsWrapperExec (wrapped: AggregateExec) [output_rows = 1, output_rows = 3, elapsed_compute = 100, elapsed_compute = 300]
///  └── MetricsWrapperExec (wrapped: ProjectionExec) [output_rows = 2, output_rows = 4, elapsed_compute = 200, elapsed_compute = 400]
///      └── MetricsWrapperExec (wrapped: NetworkShuffleExec) [bytes_transferred = 100, bytes_transferred = 200, max_mem_used = 100, max_mem_used = 200]
///
/// Note: Metrics may be aggregated by name (ex. output_rows) automatically by various datafusion utils.
pub fn stage_metrics_rewriter(
    stage: &Stage,
    metrics_collection: Arc<HashMap<StageKey, Vec<MetricsSetProto>>>,
    format: DistributedMetricsFormat,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut node_idx = 0;

    let plan = stage.plan.decoded()?;

    plan.clone().transform_down(|plan| {
        // Collect metrics for this node. It should contain metrics from each task.
        let mut stage_metrics = MetricsSet::new();

        for task_id in 0..stage.tasks.len() {
            let stage_key = StageKey::new(Bytes::from(stage.query_id.as_bytes().to_vec()), stage.num as u64, task_id as u64);
            match metrics_collection.get(&stage_key) {
                Some(task_metrics) => {
                    if node_idx >= task_metrics.len() {
                        return internal_err!(
                            "not enough metrics provided to rewrite task: {} metrics provided",
                            task_metrics.len()
                        );
                    }
                    let node_metrics_protos = task_metrics[node_idx].clone();
                    let mut node_metrics = metrics_set_proto_to_df(&node_metrics_protos)?;

                    let rewrite_ctx = format.to_rewrite_ctx(task_id as u64);
                    node_metrics = rewrite_ctx.maybe_rewrite_node_metics(node_metrics);

                    for metric in node_metrics.iter().map(Arc::clone) {
                        stage_metrics.push(metric);
                    }
                }
                None => {
                    return internal_err!(
                        "not enough metrics provided to rewrite task: missing metrics for task {} in stage {}",
                        task_id,
                        stage.num
                    );
                }
            }
        }

        node_idx += 1;

        // If the plan is a network boundary, we need to recursively rewrite the child stage
        // with metrics as well. Network boundaries don't have ExecutionPlan children, so
        // transform_down won't traverse into them automatically.
        if let Some(network_boundary) = plan.as_network_boundary() {
            let child_stage = network_boundary.input_stage();
            // Recursively rewrite the child stage with metrics
            let child_plan_with_metrics =
                stage_metrics_rewriter(child_stage, metrics_collection.clone(), format)?;
            // Create a new network boundary with the rewritten child stage
            let new_network_boundary = network_boundary.with_input_stage(Stage::new(
                child_stage.query_id,
                child_stage.num,
                child_plan_with_metrics,
                child_stage.tasks.len(),
            ))?;
            // Wrap the new network boundary with metrics
            let wrapped_plan_node: Arc<dyn ExecutionPlan> =
                Arc::new(MetricsWrapperExec::new(new_network_boundary, stage_metrics));
            return Ok(Transformed::new(
                wrapped_plan_node,
                true,
                TreeNodeRecursion::Jump,
            ));
        }

        let wrapped_plan_node: Arc<dyn ExecutionPlan> = Arc::new(MetricsWrapperExec::new(
            plan.clone(),
            stage_metrics,
        ));
        Ok(Transformed::new(
            wrapped_plan_node,
            true,
            TreeNodeRecursion::Continue,
        ))
    }).map(|v| v.data)
}

#[cfg(test)]
mod tests {
    use crate::Stage;
    use crate::metrics::DISTRIBUTED_DATAFUSION_TASK_ID_LABEL;
    use crate::metrics::proto::{
        MetricsSetProto, df_metrics_set_to_proto, metrics_set_proto_to_df,
    };
    use crate::metrics::task_metrics_rewriter::{
        annotate_metrics_set_with_task_id, stage_metrics_rewriter,
    };
    use crate::metrics::{DistributedMetricsFormat, rewrite_distributed_plan_with_metrics};
    use crate::protobuf::StageKey;
    use crate::test_utils::in_memory_channel_resolver::{
        InMemoryChannelResolver, InMemoryWorkerResolver,
    };
    use crate::test_utils::metrics::make_test_metrics_set_proto_from_seed;
    use crate::test_utils::plans::count_plan_nodes_up_to_network_boundary;
    use crate::test_utils::session_context::register_temp_parquet_table;
    use crate::{DistributedExec, DistributedPhysicalOptimizerRule};
    use bytes::Bytes;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::HashMap;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::metrics::{Count, Label, Metric, MetricValue, MetricsSet};
    use test_case::test_case;

    use datafusion::physical_plan::{ExecutionPlan, collect};
    use itertools::Itertools;
    use uuid::Uuid;

    use crate::DistributedExt;
    use crate::metrics::task_metrics_rewriter::MetricsWrapperExec;
    use datafusion::physical_plan::empty::EmptyExec;
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
                .with_distributed_worker_resolver(InMemoryWorkerResolver::new(10))
                .with_distributed_channel_resolver(InMemoryChannelResolver::default())
                .with_distributed_metrics_collection(true)
                .unwrap()
                .with_distributed_bytes_processed_per_partition(1)
                .unwrap()
                .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
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
        println!("a: {a:?}");
        println!("b: {b:?}");
        // Check equality by converting to proto representation.
        df_metrics_set_to_proto(a).unwrap() == df_metrics_set_to_proto(b).unwrap()
    }

    /// Asserts that we successfully re-write the metrics of a plan generated from the provided SQL query.
    /// Also asserts that the order which metrics are collected from a plan matches the order which
    /// they are re-written (ie. ensures we don't assign metrics to the wrong nodes)
    ///
    /// Only tests single node plans since the [TaskMetricsRewriter] stops on [NetworkBoundary].
    async fn run_stage_metrics_rewriter_test(sql: &str, format: DistributedMetricsFormat) {
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
            let metrics = (0..count_plan_nodes_up_to_network_boundary(&plan))
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
        let rewritten_plan =
            stage_metrics_rewriter(&stage, metrics_collection.clone(), format).unwrap();

        // Collect metrics from the plan.
        let mut actual_metrics = vec![];
        collect_metrics_from_plan(&rewritten_plan, &mut actual_metrics);
        assert_eq!(
            actual_metrics.len(),
            count_plan_nodes_up_to_network_boundary(&plan)
        );

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

                // Convert from proto to check for equality.
                let mut expected_metrics_set =
                    metrics_set_proto_to_df(&expected_task_node_metrics).unwrap();

                if format == DistributedMetricsFormat::PerTask {
                    // Add task ids labels. We expect the actual metrics to be annotated by the
                    // rewriter when using DistributedMetricsFormat::PerTask
                    expected_metrics_set =
                        annotate_metrics_set_with_task_id(expected_metrics_set, task_id as u64);
                }
                assert!(metrics_set_eq(&actual_metrics_set, &expected_metrics_set));
            }
        }
    }

    #[test_case(DistributedMetricsFormat::Aggregated ; "aggregated_metrics")]
    #[test_case(DistributedMetricsFormat::PerTask ; "per_task_metrics")]
    #[tokio::test]
    async fn test_stage_metrics_rewriter_1(format: DistributedMetricsFormat) {
        run_stage_metrics_rewriter_test(
            "SELECT sum(balance) / 7.0 as avg_yearly from table2 group by name",
            format,
        )
        .await;
    }

    #[test_case(DistributedMetricsFormat::Aggregated ; "aggregated_metrics")]
    #[test_case(DistributedMetricsFormat::PerTask ; "per_task_metrics")]
    #[tokio::test]
    async fn test_stage_metrics_rewriter_2(format: DistributedMetricsFormat) {
        run_stage_metrics_rewriter_test("SELECT id, COUNT(*) as count FROM table1 WHERE id > 1 GROUP BY id ORDER BY id LIMIT 10", format).await;
    }

    #[test_case(DistributedMetricsFormat::Aggregated ; "aggregated_metrics")]
    #[test_case(DistributedMetricsFormat::PerTask ; "per_task_metrics")]
    #[tokio::test]
    async fn test_stage_metrics_rewriter_3(format: DistributedMetricsFormat) {
        run_stage_metrics_rewriter_test(
            "SELECT sum(balance) / 7.0 as avg_yearly
            FROM table2
            WHERE name LIKE 'customer%'
              AND balance < (
                SELECT 0.2 * avg(balance)
                FROM table2 t2_inner
                WHERE t2_inner.id = table2.id
              )",
            format,
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
        assert!(
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .is_err()
        );
    }

    // Assert every plan node has at least one metric except partition isolators, network boundary nodes, and the root DistributedExec node.
    fn assert_metrics_present_in_plan(plan: &Arc<dyn ExecutionPlan>) {
        if let Some(metrics) = plan.metrics() {
            assert!(metrics.iter().count() > 0);
        } else {
            assert!(plan.as_any().is::<DistributedExec>());
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
        let rewritten_plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .unwrap();
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

    #[test]
    fn test_annotate_metrics_set_with_task_id_output_rows() {
        // Create a MetricsSet with an OutputRows metric
        let mut metrics_set = MetricsSet::new();
        let count = Count::new();
        count.add(1234);
        let labels = vec![Label::new("operator", "scan")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::OutputRows(count),
            Some(0),
            labels,
        )));

        let task_id = 42;
        let annotated = annotate_metrics_set_with_task_id(metrics_set, task_id);

        // Verify we have one metric
        assert_eq!(annotated.iter().count(), 1);

        let metric = annotated.iter().next().unwrap();

        // Verify metric type is preserved (OutputRows)
        match metric.value() {
            MetricValue::OutputRows(count) => {
                assert_eq!(count.value(), 1234);
            }
            other => panic!("Expected OutputRows, got {:?}", other.name()),
        }

        // Verify partition is preserved
        assert_eq!(metric.partition(), Some(0));

        // Verify original labels are preserved and task_id label is added
        let labels: Vec<_> = metric.labels().iter().collect();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].name(), "operator");
        assert_eq!(labels[0].value(), "scan");
        assert_eq!(labels[1].name(), DISTRIBUTED_DATAFUSION_TASK_ID_LABEL);
        assert_eq!(labels[1].value(), "42");
    }
}
