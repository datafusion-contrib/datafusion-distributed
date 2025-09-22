use crate::execution_plans::{NetworkCoalesceExec, NetworkShuffleExec, StageExec};
use crate::metrics::proto::{MetricsSetProto, metrics_set_proto_to_df};
use crate::protobuf::StageKey;
use datafusion::common::internal_err;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// TaskMetricsCollector is used to collect metrics from a task. It implements [TreeNodeRewriter].
/// Note: TaskMetricsCollector is not a [datafusion::physical_plan::ExecutionPlanVisitor] to keep
/// parity between [TaskMetricsCollector] and [TaskMetricsRewriter].
pub struct TaskMetricsCollector {
    /// metrics contains the metrics for the current task.
    task_metrics: Vec<MetricsSet>,
    /// child_task_metrics contains metrics for tasks from child [StageExec]s if they were
    /// collected.
    child_task_metrics: HashMap<StageKey, Vec<MetricsSetProto>>,
}

/// MetricsCollectorResult is the result of collecting metrics from a task.
#[allow(dead_code)]
pub struct MetricsCollectorResult {
    // metrics is a collection of metrics for a task ordered using a pre-order traversal of the task's plan.
    task_metrics: Vec<MetricsSet>,
    // child_task_metrics contains metrics for child tasks if they were collected.
    child_task_metrics: HashMap<StageKey, Vec<MetricsSetProto>>,
}

impl TreeNodeRewriter for TaskMetricsCollector {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        // If the plan is an NetworkShuffleExec, assume it has collected metrics already
        // from child tasks.
        let metrics_collection =
            if let Some(node) = plan.as_any().downcast_ref::<NetworkShuffleExec>() {
                let NetworkShuffleExec::Ready(ready) = node else {
                    return internal_err!(
                        "unexpected NetworkShuffleExec::Pending during metrics collection"
                    );
                };
                Some(Arc::clone(&ready.metrics_collection))
            } else if let Some(node) = plan.as_any().downcast_ref::<NetworkCoalesceExec>() {
                let NetworkCoalesceExec::Ready(ready) = node else {
                    return internal_err!(
                        "unexpected NetworkCoalesceExec::Pending during metrics collection"
                    );
                };
                Some(Arc::clone(&ready.metrics_collection))
            } else {
                None
            };

        if let Some(metrics_collection) = metrics_collection {
            for mut entry in metrics_collection.iter_mut() {
                let stage_key = entry.key().clone();
                let task_metrics = std::mem::take(entry.value_mut()); // Avoid copy.
                match self.child_task_metrics.get(&stage_key) {
                    // There should never be two NetworkShuffleExec with metrics for the same stage_key.
                    // By convention, the NetworkShuffleExec which runs the last partition in a task should be
                    // sent metrics (the NetworkShuffleExec tracks it for us).
                    Some(_) => {
                        return internal_err!(
                            "duplicate task metrics for key {} during metrics collection",
                            stage_key
                        );
                    }
                    None => {
                        self.child_task_metrics
                            .insert(stage_key.clone(), task_metrics);
                    }
                }
            }
            // Skip the subtree of the NetworkShuffleExec.
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
        }

        // For plan nodes in this task, collect metrics.
        match plan.metrics() {
            Some(metrics) => self.task_metrics.push(metrics.clone()),
            None => {
                // TODO: Consider using a more efficent encoding scheme to avoid empty slots in the vec.
                self.task_metrics.push(MetricsSet::new())
            }
        }
        Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue))
    }
}

impl TaskMetricsCollector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            task_metrics: Vec::new(),
            child_task_metrics: HashMap::new(),
        }
    }

    /// collect metrics from a StageExec plan and any child tasks.
    /// Returns
    /// - a vec representing the metrics for the current task (ordered using a pre-order traversal)
    /// - a map representing the metrics for some subset of child tasks collected from NetworkShuffleExec leaves
    #[allow(dead_code)]
    pub fn collect(mut self, stage: &StageExec) -> Result<MetricsCollectorResult, DataFusionError> {
        stage.plan.clone().rewrite(&mut self)?;
        Ok(MetricsCollectorResult {
            task_metrics: self.task_metrics,
            child_task_metrics: self.child_task_metrics,
        })
    }
}

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

/// A transparent wrapper that delegates all execution to its child but returns custom metrics. This node is invisible during display.
/// The structure of a plan tree is closely tied to the [TaskMetricsRewriter].
struct MetricsWrapperExec {
    inner: Arc<dyn ExecutionPlan>,
    /// metrics for this plan node.
    metrics: MetricsSet,
    /// children is initially None. When used by the [TaskMetricsRewriter], the children will be updated
    /// to point at other wrapped nodes.
    children: Option<Vec<Arc<dyn ExecutionPlan>>>,
}

impl MetricsWrapperExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, metrics: MetricsSet) -> Self {
        Self {
            inner,
            metrics,
            children: None,
        }
    }
}

/// MetricsWrapperExec is invisible during display.
impl DisplayAs for MetricsWrapperExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

/// MetricsWrapperExec is visible when debugging.
impl Debug for MetricsWrapperExec {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MetricsWrapperExec ({:?})", self.inner)
    }
}

impl ExecutionPlan for MetricsWrapperExec {
    fn name(&self) -> &str {
        "MetricsWrapperExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        unimplemented!("MetricsWrapperExec does not implement properties")
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.children {
            Some(children) => children.iter().collect(),
            None => self.inner.children(),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MetricsWrapperExec {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
            children: Some(children),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _contex: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("MetricsWrapperExec does not implement execute")
    }

    // metrics returns the wrapped metrics.
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;

    use crate::DistributedExt;
    use crate::DistributedPhysicalOptimizerRule;
    use crate::test_utils::in_memory_channel_resolver::InMemoryChannelResolver;
    use crate::test_utils::metrics::make_test_metrics_set_proto_from_seed;
    use crate::test_utils::session_context::register_temp_parquet_table;
    use datafusion::execution::{SessionStateBuilder, context::SessionContext};
    use datafusion::physical_plan::metrics::MetricValue;
    use datafusion::prelude::SessionConfig;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        physical_plan::display::DisplayableExecutionPlan,
    };
    use std::sync::Arc;

    /// Creates a stage with the following structure:
    ///
    ///  SortPreservingMergeExec
    ///    SortExec
    ///      ProjectionExec
    ///        AggregateExec
    ///          CoalesceBatchesExec
    ///            NetworkShuffleExec
    ///
    ///  ... (for the purposes of these tests, we don't care about child stages).
    async fn make_test_stage_exec_with_5_nodes() -> (StageExec, SessionContext) {
        // Create distributed session state with in-memory channel resolver
        let config = SessionConfig::new().with_target_partitions(2);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_distributed_channel_resolver(InMemoryChannelResolver::new())
            .with_physical_optimizer_rule(Arc::new(
                DistributedPhysicalOptimizerRule::default()
                    .with_network_coalesce_tasks(2)
                    .with_network_shuffle_tasks(2),
            ))
            .build();

        let ctx = SessionContext::from(state);

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![4, 5, 6])),
                    Arc::new(StringArray::from(vec!["d", "e", "f"])),
                ],
            )
            .unwrap(),
        ];

        // Register the test data as a parquet table
        let _ = register_temp_parquet_table("test_table", schema.clone(), batches, &ctx)
            .await
            .unwrap();

        let df = ctx
            .sql("SELECT id, COUNT(*) as count FROM test_table WHERE id > 1 GROUP BY id ORDER BY id LIMIT 10")
            .await
            .unwrap();
        let physical_distributed = df.create_physical_plan().await.unwrap();

        let stage_exec = match physical_distributed.as_any().downcast_ref::<StageExec>() {
            Some(stage_exec) => stage_exec.clone(),
            None => panic!(
                "Expected StageExec from distributed optimization, got: {}",
                physical_distributed.name()
            ),
        };

        (stage_exec, ctx)
    }

    #[tokio::test]
    #[ignore]
    async fn test_metrics_rewriter() {
        let (test_stage, _ctx) = make_test_stage_exec_with_5_nodes().await;
        let test_metrics_sets = (0..5) // 5 nodes excluding NetworkShuffleExec
            .map(|i| make_test_metrics_set_proto_from_seed(i + 10))
            .collect::<Vec<MetricsSetProto>>();

        let rewriter = TaskMetricsRewriter::new(test_metrics_sets.clone());
        let plan_with_metrics = rewriter
            .enrich_task_with_metrics(test_stage.plan.clone())
            .unwrap();

        let plan_str =
            DisplayableExecutionPlan::with_full_metrics(plan_with_metrics.as_ref()).indent(true);
        // Expected distributed plan output with metrics
        let expected = [
            r"SortPreservingMergeExec: [id@0 ASC NULLS LAST], fetch=10, metrics=[output_rows=10, elapsed_compute=10ns, start_timestamp=2025-09-18 13:00:10 UTC, end_timestamp=2025-09-18 13:00:11 UTC]",
            r"  SortExec: TopK(fetch=10), expr=[id@0 ASC NULLS LAST], preserve_partitioning=[true], metrics=[output_rows=11, elapsed_compute=11ns, start_timestamp=2025-09-18 13:00:11 UTC, end_timestamp=2025-09-18 13:00:12 UTC]",
            r"    ProjectionExec: expr=[id@0 as id, count(Int64(1))@1 as count], metrics=[output_rows=12, elapsed_compute=12ns, start_timestamp=2025-09-18 13:00:12 UTC, end_timestamp=2025-09-18 13:00:13 UTC]",
            r"      AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[count(Int64(1))], metrics=[output_rows=13, elapsed_compute=13ns, start_timestamp=2025-09-18 13:00:13 UTC, end_timestamp=2025-09-18 13:00:14 UTC]",
            r"        CoalesceBatchesExec: target_batch_size=8192, metrics=[output_rows=14, elapsed_compute=14ns, start_timestamp=2025-09-18 13:00:14 UTC, end_timestamp=2025-09-18 13:00:15 UTC]",
            r"          NetworkShuffleExec, metrics=[]",
            "" // trailing newline
        ].join("\n");
        assert_eq!(expected, plan_str.to_string());
    }

    #[tokio::test]
    #[ignore]
    async fn test_metrics_rewriter_correct_number_of_metrics() {
        let test_metrics_set = make_test_metrics_set_proto_from_seed(10);
        let (executable_plan, _ctx) = make_test_stage_exec_with_5_nodes().await;
        let task_plan = executable_plan
            .as_any()
            .downcast_ref::<StageExec>()
            .unwrap()
            .plan
            .clone();

        // Too few metrics sets.
        let rewriter = TaskMetricsRewriter::new(vec![test_metrics_set.clone()]);
        let result = rewriter.enrich_task_with_metrics(task_plan.clone());
        assert!(result.is_err());

        // Too many metrics sets.
        let rewriter = TaskMetricsRewriter::new(vec![
            test_metrics_set.clone(),
            test_metrics_set.clone(),
            test_metrics_set.clone(),
            test_metrics_set.clone(),
        ]);
        let result = rewriter.enrich_task_with_metrics(task_plan.clone());
        assert!(result.is_err());
    }

    #[tokio::test]
    #[ignore]
    async fn test_metrics_collection() {
        let (stage_exec, ctx) = make_test_stage_exec_with_5_nodes().await;

        // Execute the plan to completion.
        let task_ctx = ctx.task_ctx();
        let stream = stage_exec.execute(0, task_ctx).unwrap();

        use futures::StreamExt;
        let mut stream = stream;
        while let Some(_batch) = stream.next().await {}

        let collector = TaskMetricsCollector::new();
        let result = collector.collect(&stage_exec).unwrap();

        // With the distributed optimizer, we get a much more complex plan structure
        // The exact number of metrics sets depends on the plan optimization, so be flexible
        assert_eq!(result.task_metrics.len(), 5);

        let expected_metrics_count = [4, 10, 8, 16, 8];
        for (node_idx, metrics_set) in result.task_metrics.iter().enumerate() {
            let metrics_count = metrics_set.iter().count();
            assert_eq!(metrics_count, expected_metrics_count[node_idx]);

            // Each node should have basic metrics: ElapsedCompute, OutputRows, StartTimestamp, EndTimestamp.
            let mut has_start_timestamp = false;
            let mut has_end_timestamp = false;
            let mut has_elapsed_compute = false;
            let mut has_output_rows = false;

            for metric in metrics_set.iter() {
                let metric_name = metric.value().name();
                let metric_value = metric.value();

                match metric_value {
                    MetricValue::StartTimestamp(_) if metric_name == "start_timestamp" => {
                        has_start_timestamp = true;
                    }
                    MetricValue::EndTimestamp(_) if metric_name == "end_timestamp" => {
                        has_end_timestamp = true;
                    }
                    MetricValue::ElapsedCompute(_) if metric_name == "elapsed_compute" => {
                        has_elapsed_compute = true;
                    }
                    MetricValue::OutputRows(_) if metric_name == "output_rows" => {
                        has_output_rows = true;
                    }
                    _ => {
                        // Other metrics are fine, we just validate the core ones
                    }
                }
            }

            // Each node should have the four basic metrics
            assert!(has_start_timestamp);
            assert!(has_end_timestamp);
            assert!(has_elapsed_compute);
            assert!(has_output_rows);
        }

        // TODO: once we propagate metrics from child stages, we can assert this.
        assert_eq!(0, result.child_task_metrics.len());
    }
}
