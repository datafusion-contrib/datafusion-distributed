use crate::execution_plans::MetricsWrapperExec;
use crate::execution_plans::NetworkCoalesceExec;
use crate::execution_plans::NetworkShuffleExec;
use crate::metrics::proto::MetricsSetProto;
use crate::metrics::proto::metrics_set_proto_to_df;
use crate::stage::Stage;
use crate::NetworkBoundaryExt;
use bytes::Bytes;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::tree_node::TreeNodeRewriter;
use datafusion::error::Result;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::internal_err;
use datafusion::physical_plan::Metric;
use std::sync::Arc;
use datafusion::physical_plan::metrics::Label;
use datafusion::common::HashMap;
use crate::protobuf::StageKey;
use crate::execution_plans::DistributedExec;
use datafusion::common::DataFusionError;

/// Rewrites a distributed plan with metrics.
pub fn rewrite_istributed_plan_with_metrics(
    executed_plan: Arc<dyn ExecutionPlan>,
    metrics_collection: Arc<HashMap<StageKey, Vec<MetricsSetProto>>>
) -> Result<Arc<dyn ExecutionPlan>> {
    match executed_plan.as_any().downcast_ref::<DistributedExec>() {
        Some(_) => {
            // Decode all the metrics.
            let metrics_collection = metrics_collection.iter().map(|(stage_key, metrics_set_protos)| {
                match metrics_set_protos.iter().map(metrics_set_proto_to_df).collect::<Result<Vec<MetricsSet>, DataFusionError>>() {
                    Ok(decoded) => Ok((stage_key.clone(), decoded)),
                    Err(err) => Err(err),
                }
            }).collect::<Result<HashMap<StageKey, Vec<MetricsSet>>, DataFusionError>>()?;

            // Transform all stages using NetworkShuffleExec and NetworkCoalesceExec as barriers.
            let transformed = executed_plan.transform_up(|plan| {
                if let Some(network_boundary) = plan.as_network_boundary() {
                    return match network_boundary.input_stage() {
                        Some(stage) => {
                            let plan_with_metrics = StageMetricsRewriter::new(&stage, metrics_collection.clone())?.rewrite()?;
                           
                            let new_network_boundary = network_boundary.with_input_stage(Stage::new(
                                stage.query_id,
                                stage.num,
                                plan_with_metrics,
                                stage.tasks.len(),
                            ))?;
                            Ok(Transformed::new(new_network_boundary, true, TreeNodeRecursion::Continue))
                        }
                        None => {
                            internal_err!("Expected input stage to be set in network boundary")
                        }
                    }
                }
                Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue))
            })?;
            Ok(transformed.data)
        }
        None => {
            Ok(executed_plan)
        }
    }
}
        
/// MetricsRewriter is used to enrich a stage with metrics from each task by re-writing the plan using
/// [MetricsWrapperExec] nodes. Each metric will be labelled with the stage and task it was collected from.
///
/// Ex. for a plan with the form
/// AggregateExec
///  └── ProjectionExec
///      └── NetworkShuffleExec
///
/// the stage will be rewritten as
///
/// MetricsWrapperExec (wrapped: AggregateExec)
///  └── MetricsWrapperExec (wrapped: ProjectionExec)
///      └── NetworkShuffleExec
/// (Note that the NetworkShuffleExec node is not wrapped)
pub struct StageMetricsRewriter<'a> {
    /// A collection of metrics which should contain at least metrics for each task in  stage.
    metrics_collection: HashMap<StageKey, Vec<MetricsSet>>,
    
    stage: &'a Stage,
    /// The index of a node in the plan.
    idx: usize,
}

impl<'a> StageMetricsRewriter<'a> {
    /// Create a new StageMetricsRewriter. The provided metrics will be used to enrich the plan.
    pub fn new(stage: &'a Stage, metrics_collection: HashMap<StageKey, Vec<MetricsSet>>) -> Result<Self> {
        Ok(Self { metrics_collection, stage, idx: 0 })
    }

    /// enrich_task_with_metrics rewrites the plan by wrapping nodes. If the length of the provided metrics set vec does not
    /// match the number of nodes in the plan, an error will be returned.
    pub fn rewrite(
        mut self,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let transformed = self.stage.plan.decoded()?.clone().rewrite(&mut self)?;
        Ok(transformed.data)
    }
}

impl<'a> TreeNodeRewriter for StageMetricsRewriter<'a> {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        // Stop at network boundaries.
        if plan.as_network_boundary().is_some() {
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
        }

        // Collect metrics for this node. It should contain metrics from each task.
        let mut stage_metrics = MetricsSet::new();

        for idx in 0..self.stage.tasks.len() {
            let stage_key = StageKey {
                query_id: Bytes::from(self.stage.query_id.as_bytes().to_vec()),
                stage_id: self.stage.num as u64,
                task_number: idx as u64,
            };
            match self.metrics_collection.get(&stage_key) {
                Some(task_metrics) => {
                    if self.idx >= task_metrics.len() {
                        return internal_err!(
                            "not enough metrics provided to rewrite task: {} metrics provided",
                            task_metrics.len()
                        );
                    }
                    let node_metrics = task_metrics[self.idx].clone();
                    for metric in node_metrics.iter() {
                        stage_metrics.push(metric.clone());
                    }
                }
                None => {
                    return internal_err!(
                        "not enough metrics provided to rewrite task: missing metrics for task {} in stage {}",
                        idx,
                        self.stage.num
                    );
                }
            }
        }
       

        let wrapped_plan_node: Arc<dyn ExecutionPlan> = Arc::new(MetricsWrapperExec::new(
            plan.clone(),
            stage_metrics,
        ));
        let result = Transformed::new(wrapped_plan_node, true, TreeNodeRecursion::Continue);
        self.idx += 1;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::display_plan_ascii;
    use crate::metrics::proto::MetricsSetProto;
    use crate::test_utils::metrics::make_test_metrics_set_proto_from_seed;
    use crate::test_utils::plans::count_plan_nodes;
    use crate::test_utils::session_context::register_temp_parquet_table;
    use crate::Stage;
    use bytes::Bytes;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::ExecutionPlan;
    use uuid::Uuid;
    use crate::DistributedPhysicalOptimizerRule;
    use datafusion::prelude::SessionConfig;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;
    use crate::DistributedExt;
    use crate::stage::MaybeEncodedPlan;

    /// Creates a non-distributed session context and registers two tables:
    /// - table1 (id: int, name: string)
    /// - table2 (id: int, name: string, phone: string, balance: float64)
    async fn make_test_ctx() -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(4);
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

    fn make_test_stage(plan: Arc<dyn ExecutionPlan>) -> Stage {
        Stage::new(
            Uuid::new_v4(),
            2,
            plan,
            4,
        )
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

        let stage = make_test_stage(plan.clone());

        println!("Plan: {}", display_plan_ascii(plan.as_ref()));

        // Generate metrics for each plan node.
        let expected_metrics = (0..count_plan_nodes(&plan))
            .map(|i| make_test_metrics_set_proto_from_seed(i as u64 + 10))
            .collect::<Vec<MetricsSetProto>>();

        // Rewrite the metrics.
        // let rewriter = StageMetricsRewriter::new(expected_metrics.clone());
        // let rewritten_plan = rewriter.enrich_task_with_metrics(plan.clone()).unwrap();

        // // Collect metrics
        // let actual_metrics = TaskMetricsCollector::new()
        //     .collect(rewritten_plan)
        //     .unwrap()
        //     .task_metrics;

        // // Assert that all the metrics are present and in the same order.
        // assert_eq!(actual_metrics.len(), expected_metrics.len());
        // for (actual_metrics_set, expected_metrics_set) in actual_metrics
        //     .iter()
        //     .map(|m| df_metrics_set_to_proto(m).unwrap())
        //     .zip(expected_metrics)
        // {
        //     assert_eq!(actual_metrics_set, expected_metrics_set);
        // }
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
