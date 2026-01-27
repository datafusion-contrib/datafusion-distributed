#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        execute_stream,
    };
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DistributedExt, WorkerQueryContext, assert_snapshot};
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    use datafusion_proto::protobuf::proto_error;
    use futures::TryStreamExt;
    use prost::Message;
    use std::any::Any;
    use std::fmt::Formatter;
    use std::sync::Arc;

    #[tokio::test]
    async fn custom_extension_codec() -> Result<(), Box<dyn std::error::Error>> {
        async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
            Ok(ctx
                .builder
                .with_distributed_user_codec(CustomPassThroughExecCodec)
                .build())
        }

        let (ctx, _guard, _) = start_localhost_context(3, build_state).await;

        let query = r#"SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 20.0 ORDER BY "MinTemp" DESC"#;

        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let plan = df.create_physical_plan().await?;

        // Wrap leaf nodes with CustomPassThroughExec to test custom codec
        let transformed = plan.transform_up(|plan| {
            if plan.children().is_empty() {
                return Ok(Transformed::yes(Arc::new(CustomPassThroughExec::new(plan))));
            }
            Ok(Transformed::no(plan))
        })?;
        let plan = transformed.data;

        let batches = pretty_format_batches(
            &execute_stream(plan, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        // Verify that the custom execution plan completes successfully
        assert!(!batches.to_string().is_empty());
        assert_snapshot!(batches, @r"
        +---------+-----------+
        | MinTemp | RainToday |
        +---------+-----------+
        | 20.9    | No        |
        +---------+-----------+
        ");

        Ok(())
    }

    /// A custom execution plan that wraps a child and passes through execution.
    /// This tests that custom user codecs work correctly in distributed execution.
    #[derive(Debug)]
    pub struct CustomPassThroughExec {
        plan_properties: PlanProperties,
        child: Arc<dyn ExecutionPlan>,
    }

    impl CustomPassThroughExec {
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
            }
        }
    }

    impl DisplayAs for CustomPassThroughExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "CustomPassThroughExec")
        }
    }

    impl ExecutionPlan for CustomPassThroughExec {
        fn name(&self) -> &str {
            "CustomPassThroughExec"
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
            Ok(Arc::new(CustomPassThroughExec::new(children[0].clone())))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            // Simply pass through to the child
            self.child.execute(partition, context)
        }
    }

    #[derive(Debug)]
    struct CustomPassThroughExecCodec;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct CustomPassThroughExecProto {
        // Empty - we'll handle the child through normal codec mechanisms
    }

    impl PhysicalExtensionCodec for CustomPassThroughExecCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            let _node = CustomPassThroughExecProto::decode(buf)
                .map_err(|err| proto_error(format!("{err}")))?;

            if inputs.len() != 1 {
                return Err(proto_error(format!(
                    "CustomPassThroughExec expects exactly one child, got {}",
                    inputs.len()
                )));
            }

            Ok(Arc::new(CustomPassThroughExec::new(inputs[0].clone())))
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
        ) -> datafusion::common::Result<()> {
            let Some(_plan) = node.as_any().downcast_ref::<CustomPassThroughExec>() else {
                return Err(proto_error(format!(
                    "Expected plan to be of type CustomPassThroughExec, but was {}",
                    node.name()
                )));
            };
            CustomPassThroughExecProto {}
                .encode(buf)
                .map_err(|err| proto_error(format!("{err}")))
        }
    }
}
