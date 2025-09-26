use crate::execution_plans::{stage, NetworkCoalesceExec, NetworkShuffleExec, StageExec};
use crate::metrics::proto::{MetricsSetProto, metrics_set_proto_to_df};
use arrow::ipc::writer::DictionaryTracker;
use arrow_flight::encode::{FlightDataEncoderBuilder};
use arrow_flight::FlightData;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use futures::{stream, Stream, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::writer::IpcDataGenerator;
use datafusion::common::HashSet;

use crate::metrics::proto::df_metrics_set_to_proto;
use crate::protobuf::StageKey;
use crate::protobuf::{AppMetadata, FlightAppMetadata, MetricsCollection, TaskMetrics};
use arrow_flight::{error::FlightError};
use datafusion::common::internal_err;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
use prost::Message;
use std::any::Any;
use std::fmt::{Debug, Formatter};

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
    pub(super) task_metrics: Vec<MetricsSet>,
    // child_task_metrics contains metrics for child tasks if they were collected.
    pub(super) child_task_metrics: HashMap<StageKey, Vec<MetricsSetProto>>,
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

// Collects metrics from the provided stage and encodes it into a stream of flight data using
// the schema of the stage. 
pub fn collect_and_create_metrics_flight_data(
    stage_key: StageKey,
    stage: Arc<StageExec>,
) -> Result<impl Stream<Item = Result<FlightData, FlightError>> + Send + 'static, FlightError> {
    // Get the metrics for the task executed on this worker. Separately, collect metrics for child tasks.
    let mut result = TaskMetricsCollector::new()
        .collect(stage.as_ref())
        .map_err(|err| FlightError::ProtocolError(err.to_string()))?;

    // Add the metrics for this task into the collection of task metrics.
    // Skip any metrics that can't be converted to proto (unsupported types)
    let proto_task_metrics = result
        .task_metrics
        .iter()
        .map(|metrics| {
            df_metrics_set_to_proto(metrics)
                .map_err(|err| FlightError::ProtocolError(err.to_string()))
        })
        .collect::<Result<Vec<MetricsSetProto>, FlightError>>()?;
    result
        .child_task_metrics
        .insert(stage_key.clone(), proto_task_metrics.clone());
    let keys = result.child_task_metrics.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
    println!("collected keys in stage {} : {:#?}", stage_key, keys);

    // Serialize the metrics for all tasks.
    let mut task_metrics_set = vec![];
    for (stage_key, metrics) in result.child_task_metrics.into_iter() {
        task_metrics_set.push(TaskMetrics {
            stage_key: Some(stage_key),
            metrics,
        });
    }

    let flight_app_metadata = FlightAppMetadata {
        content: Some(AppMetadata::MetricsCollection(MetricsCollection {
            tasks: task_metrics_set,
        })),
    };
   
    let metrics_flight_data = empty_flight_data_with_app_metadata(flight_app_metadata, stage.plan.schema())?;
    Ok(Box::pin(stream::once(async move { Ok(metrics_flight_data) })))
}

/// Creates a FlightData with the given app_metadata and empty RecordBatch using the provided schema.
/// We don't use [arrow_flight::encode::FlightDataEncoder] (and by extension, the [arrow_flight::encode::FlightDataEncoderBuilder])
/// since they skip messages with empty RecordBatch data. 
pub fn empty_flight_data_with_app_metadata(metadata: FlightAppMetadata, schema: SchemaRef) -> Result<FlightData, FlightError> {
    let mut buf = vec![];
    metadata
        .encode(&mut buf)
        .map_err(|err| FlightError::ProtocolError(err.to_string()))?;

    let empty_batch = RecordBatch::new_empty(schema);
    let options = IpcWriteOptions::default();
    let data_gen = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(true);
    let (_, encoded_data) = data_gen.encoded_batch(&empty_batch, &mut dictionary_tracker, &options)
        .map_err(|e| FlightError::ProtocolError(format!("Failed to create empty batch FlightData: {e}")))?;
    Ok(FlightData::from(encoded_data).with_app_metadata(buf))
}


#[cfg(test)]
mod tests {

    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use uuid::Uuid;

    use crate::metrics::proto::metric_proto_to_df;
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

    /// count_plan_nodes counts the number of execution plan nodes in a stage's plan using BFS traversal.
    /// This does NOT traverse child stages, only the execution plan tree within this stage.
    /// Excludes NetworkCoalesceExec and NetworkShuffleExec nodes from the count.
    fn count_plan_nodes(stage: &StageExec) -> usize {
        let mut count = 0;
        let mut queue = vec![stage.plan.clone()];
        
        while let Some(plan) = queue.pop() {
            // Skip NetworkCoalesceExec and NetworkShuffleExec
            if !plan.as_any().is::<NetworkCoalesceExec>() && !plan.as_any().is::<NetworkShuffleExec>() {
                count += 1;
            }
            
            // Add children to the queue for BFS traversal
            for child in plan.children() {
                queue.push(child.clone());
            }
        }
        
        count
    }

    /// get_stage_keys returns a list of all stage keys in the stage and its child stages.
    /// Keys are returned in sorted order.
    fn get_stage_keys(stage: &StageExec) -> Vec<String> {
        let query_id = stage.query_id;
        let mut i = 0;
        let mut stages = vec![stage];
        let mut stage_keys = HashSet::new();
        
        while i < stages.len() {
            let stage = stages[i];
            i += 1;

            // Add each task.
            for j in 0..stage.tasks.len() {
                let stage_key = StageKey {
                    query_id: query_id.to_string(),
                    stage_id: stage.num as u64,
                    task_number: j as u64,
                };
                stage_keys.insert(stage_key.to_string());
            }

           // Add any child stages 
            stages.extend(stage.child_stages_iter().map(|s| s));
        }
        let mut stage_keys: Vec<String> = stage_keys.into_iter().collect();
        stage_keys.sort();
        stage_keys
    }

    /// Creates a distributed session context with in-memory distributed engine
    /// and registers two tables:
    /// - table1 (id: int, name: string)
    /// - table2 (id: int, name: string, phone: string, balance: float64)
    async fn make_test_ctx() -> SessionContext {
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
                    Arc::new(StringArray::from(vec!["customer1", "customer2", "customer3"])),
                    Arc::new(StringArray::from(vec!["13-123-4567", "31-456-7890", "23-789-0123"])),
                    Arc::new(datafusion::arrow::array::Float64Array::from(vec![100.5, 250.0, 50.25])),
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

    /// runs a sql query and returns the coordinator StageExec
    async fn plan_sql(ctx: &SessionContext, sql: &str) -> StageExec {
        let df = ctx
            .sql(sql)
            .await
            .unwrap();
        let physical_distributed = df.create_physical_plan().await.unwrap();

        let stage_exec = match physical_distributed.as_any().downcast_ref::<StageExec>() {
            Some(stage_exec) => stage_exec.clone(),
            None => panic!(
                "expected StageExec from distributed optimization, got: {}",
                physical_distributed.name()
            ),
        };
        stage_exec
    }

    async fn execute_plan(stage_exec: &StageExec, ctx: &SessionContext) {
        let task_ctx = ctx.task_ctx();
        let stream = stage_exec.execute(0, task_ctx).unwrap();

        let mut stream = stream;
        while let Some(batch) = stream.next().await {
            batch.unwrap(); 
        }
    }

    // Runs a sql query and asserts that metrics are collected for all tasks
    async fn run_metrics_collection_e2e_test(sql: &str) {
        let ctx = make_test_ctx().await;
        let stage_exec = plan_sql(&ctx, sql).await;
        execute_plan(&stage_exec, &ctx).await;

        let expected = get_stage_keys(&stage_exec);
        assert!(expected.len() > 1, "expected more than 1 stage key:\n{}", DisplayableExecutionPlan::new(&stage_exec).indent(true));

        let collector = TaskMetricsCollector::new();
        let result = collector.collect(&stage_exec).unwrap();
        let mut found_keys = result.child_task_metrics.keys().map(|k| k.to_string()).collect::<Vec<_>>();
        found_keys.push(StageKey{
            query_id: stage_exec.query_id.to_string(),
            stage_id: stage_exec.num as u64,
            task_number: 0,
        }.to_string());
        found_keys.sort();

        assert_eq!(found_keys, expected);
    }

    
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
    async fn test_metrics_rewrite_and_collect() {
        let ctx = make_test_ctx().await;
        let test_stage = plan_sql(&ctx,"SELECT sum(balance) / 7.0 as avg_yearly
            FROM table2
            WHERE name LIKE 'customer%'
              AND balance < (
                SELECT 0.2 * avg(balance)
                FROM table2 t2_inner
                WHERE t2_inner.id = table2.id
              )").await;
        let num_nodes_in_stage = count_plan_nodes(&test_stage);
        let test_metrics_sets = (0..num_nodes_in_stage)
            .map(|i| make_test_metrics_set_proto_from_seed(i as u64 + 10))
            .collect::<Vec<MetricsSetProto>>();

        let rewriter = TaskMetricsRewriter::new(test_metrics_sets.clone());
        let transformed = rewriter.enrich_task_with_metrics(test_stage.plan.clone()).unwrap();

        let collector = TaskMetricsCollector::new();
        let result = collector.collect(&StageExec::new(
            Uuid::new_v4(),
            0,
            transformed,
            vec![],
            1,
        )).unwrap();
        assert_eq!(result.task_metrics.len(), num_nodes_in_stage);
        for metrics in result.task_metrics.iter() {
            let proto_metric = df_metrics_set_to_proto(metrics).unwrap();
            println!("proto_metric: {:#?}", proto_metric);
            println!("test_metrics_sets: {:#?}", test_metrics_sets);
            assert!(test_metrics_sets.contains(&proto_metric));
        }
    }

    // #[tokio::test]
    // #[ignore]
    // async fn test_metrics_rewriter_correct_number_of_metrics() {
    //     let test_metrics_set = make_test_metrics_set_proto_from_seed(10);
    //     let (executable_plan, _ctx) = make_test_stage_exec_with_5_nodes().await;
    //     let task_plan = executable_plan
    //         .as_any()
    //         .downcast_ref::<StageExec>()
    //         .unwrap()
    //         .plan
    //         .clone();

    //     // Too few metrics sets.
    //     let rewriter = TaskMetricsRewriter::new(vec![test_metrics_set.clone()]);
    //     let result = rewriter.enrich_task_with_metrics(task_plan.clone());
    //     assert!(result.is_err());

    //     // Too many metrics sets.
    //     let rewriter = TaskMetricsRewriter::new(vec![
    //         test_metrics_set.clone(),
    //         test_metrics_set.clone(),
    //         test_metrics_set.clone(),
    //         test_metrics_set.clone(),
    //     ]);
    //     let result = rewriter.enrich_task_with_metrics(task_plan.clone());
    //     assert!(result.is_err());
    // }

    // #[tokio::test]
    // #[ignore]
    // async fn test_metrics_collection() {
    //     let (stage_exec, ctx) = make_test_stage_exec_with_5_nodes().await;

    //     // Execute the plan to completion.
    //     let task_ctx = ctx.task_ctx();
    //     let stream = stage_exec.execute(0, task_ctx).unwrap();

    //     let mut stream = stream;
    //     while let Some(_batch) = stream.next().await {}

    //     let collector = TaskMetricsCollector::new();
    //     let result = collector.collect(&stage_exec).unwrap();

    //     // With the distributed optimizer, we get a much more complex plan structure
    //     // The exact number of metrics sets depends on the plan optimization, so be flexible
    //     assert_eq!(result.task_metrics.len(), 5);

    //     let expected_metrics_count = [4, 10, 8, 16, 8];
    //     for (node_idx, metrics_set) in result.task_metrics.iter().enumerate() {
    //         let metrics_count = metrics_set.iter().count();
    //         assert_eq!(metrics_count, expected_metrics_count[node_idx]);

    //         // Each node should have basic metrics: ElapsedCompute, OutputRows, StartTimestamp, EndTimestamp.
    //         let mut has_start_timestamp = false;
    //         let mut has_end_timestamp = false;
    //         let mut has_elapsed_compute = false;
    //         let mut has_output_rows = false;

    //         for metric in metrics_set.iter() {
    //             let metric_name = metric.value().name();
    //             let metric_value = metric.value();

    //             match metric_value {
    //                 MetricValue::StartTimestamp(_) if metric_name == "start_timestamp" => {
    //                     has_start_timestamp = true;
    //                 }
    //                 MetricValue::EndTimestamp(_) if metric_name == "end_timestamp" => {
    //                     has_end_timestamp = true;
    //                 }
    //                 MetricValue::ElapsedCompute(_) if metric_name == "elapsed_compute" => {
    //                     has_elapsed_compute = true;
    //                 }
    //                 MetricValue::OutputRows(_) if metric_name == "output_rows" => {
    //                     has_output_rows = true;
    //                 }
    //                 _ => {
    //                     // Other metrics are fine, we just validate the core ones
    //                 }
    //             }
    //         }

    //         // Each node should have the four basic metrics
    //         assert!(has_start_timestamp);
    //         assert!(has_end_timestamp);
    //         assert!(has_elapsed_compute);
    //         assert!(has_output_rows);
    //     }

    //     // TODO: once we propagate metrics from child stages, we can assert this.
    //     assert_eq!(0, result.child_task_metrics.len());
    // }



    #[tokio::test]
    async fn metrics_collection_1() {
        run_metrics_collection_e2e_test("SELECT id, COUNT(*) as count FROM table1 WHERE id > 1 GROUP BY id ORDER BY id LIMIT 10").await;
    }

    #[tokio::test]
    async fn metrics_collection_2() {
        run_metrics_collection_e2e_test(
            "SELECT sum(balance) / 7.0 as avg_yearly
            FROM table2
            WHERE name LIKE 'customer%'
              AND balance < (
                SELECT 0.2 * avg(balance)
                FROM table2 t2_inner
                WHERE t2_inner.id = table2.id
              )"
        ).await;
    }


    #[tokio::test]
    async fn metrics_collection_3() {
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
            ORDER BY country_code"
        ).await;
    }
}
