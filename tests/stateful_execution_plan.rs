#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        execute_stream,
    };
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DistributedExt, WorkerQueryContext, assert_snapshot, display_plan_ascii,
    };
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    use datafusion_proto::protobuf::proto_error;
    use futures::TryStreamExt;
    use prost::Message;
    use std::any::Any;
    use std::fmt::Formatter;
    use std::sync::{Arc, RwLock};
    use tokio::task::JoinHandle;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;

    // This test proves that execution nodes do not get early dropped in the Worker when all the
    // partitions start being consumed.
    //
    // It uses a StatefulPassThroughExec custom node whose execution depends on it not being dropped.
    // The node spawns a background task that collects data from its child DataSourceExec.
    // If the Worker drops the node before the stream ends, this test will fail.
    #[tokio::test]
    async fn stateful_execution_plan() -> Result<(), Box<dyn std::error::Error>> {
        async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
            Ok(ctx
                .builder
                .with_distributed_user_codec(PassThroughExecCodec)
                .build())
        }

        let (ctx_distributed, _guard, _) = start_localhost_context(3, build_state).await;
        register_parquet_tables(&ctx_distributed).await?;

        let query = r#"SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 20.0 ORDER BY "MinTemp" DESC"#;

        let df_distributed = ctx_distributed.sql(query).await?;
        let plan = df_distributed.create_physical_plan().await?;

        let transformed = plan.transform_up(|plan| {
            if plan.children().is_empty() {
                return Ok(Transformed::yes(Arc::new(StatefulPassThroughExec::new(
                    plan,
                ))));
            }
            Ok(Transformed::no(plan))
        })?;
        let plan = transformed.data;

        let plan_str = display_plan_ascii(plan.as_ref(), false);

        assert_snapshot!(plan_str,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [MinTemp@0 DESC]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=3, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0] t1:[p1] t2:[p2] 
          │ SortExec: expr=[MinTemp@0 DESC], preserve_partitioning=[true]
          │   FilterExec: MinTemp@0 > 20
          │     PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │       StatefulPassThroughExec
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 20, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 20, required_guarantees=[]
          └──────────────────────────────────────────────────
        ",
        );

        let batches_distributed = pretty_format_batches(
            &execute_stream(plan, ctx_distributed.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        // Verify that the stateful execution completes successfully
        assert!(!batches_distributed.to_string().is_empty());

        Ok(())
    }

    /// A stateful execution plan that wraps a child and spawns a background task
    /// to manage the stream collection. This tests that the node doesn't get
    /// dropped prematurely during distributed execution.
    #[derive(Debug)]
    pub struct StatefulPassThroughExec {
        plan_properties: PlanProperties,
        child: Arc<dyn ExecutionPlan>,
        task: RwLock<Option<JoinHandle<()>>>,
    }

    impl StatefulPassThroughExec {
        fn new(child: Arc<dyn ExecutionPlan>) -> Self {
            let plan_properties = PlanProperties::new(
                EquivalenceProperties::new(child.schema()),
                child.output_partitioning().clone(),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                plan_properties,
                child,
                task: RwLock::new(None),
            }
        }
    }

    impl DisplayAs for StatefulPassThroughExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "StatefulPassThroughExec")
        }
    }

    impl ExecutionPlan for StatefulPassThroughExec {
        fn name(&self) -> &str {
            "StatefulPassThroughExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.plan_properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.child]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(StatefulPassThroughExec::new(children[0].clone())))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            // Spawn a background task to demonstrate stateful behavior
            let mut stream = self.child.execute(partition, context)?;

            let handle = tokio::spawn(async move {
                // Simulate some background work
                while let Some(batch) = stream.next().await {
                    if tx.send(batch).await.is_err() {
                        return;
                    }
                }
            });
            self.task.write().unwrap().replace(handle);

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                ReceiverStream::new(rx),
            )))
        }
    }

    #[derive(Debug)]
    struct PassThroughExecCodec;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct PassThroughExecProto {
        // Empty - we'll handle the child through normal codec mechanisms
    }

    impl PhysicalExtensionCodec for PassThroughExecCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            let _node =
                PassThroughExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?;

            if inputs.len() != 1 {
                return Err(proto_error(format!(
                    "StatefulPassThroughExec expects exactly one child, got {}",
                    inputs.len()
                )));
            }

            Ok(Arc::new(StatefulPassThroughExec::new(inputs[0].clone())))
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
        ) -> datafusion::common::Result<()> {
            let Some(_plan) = node.as_any().downcast_ref::<StatefulPassThroughExec>() else {
                return Err(proto_error(format!(
                    "Expected plan to be of type StatefulPassThroughExec, but was {}",
                    node.name()
                )));
            };
            PassThroughExecProto {}
                .encode(buf)
                .map_err(|err| proto_error(format!("{err}")))
        }
    }
}
