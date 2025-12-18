#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::error::DataFusionError;
    use datafusion::execution::{
        SendableRecordBatchStream, SessionState, SessionStateBuilder, TaskContext,
    };
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        execute_stream,
    };
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DistributedExt, DistributedSessionBuilderContext};
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    use datafusion_proto::protobuf::proto_error;
    use futures::{TryStreamExt, stream};
    use prost::Message;
    use std::any::Any;
    use std::error::Error;
    use std::fmt::Formatter;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_error_propagation() -> Result<(), Box<dyn Error>> {
        async fn build_state(
            ctx: DistributedSessionBuilderContext,
        ) -> Result<SessionState, DataFusionError> {
            Ok(SessionStateBuilder::new()
                .with_runtime_env(ctx.runtime_env)
                .with_default_features()
                .with_distributed_user_codec(ErrorThrowingExecCodec)
                .build())
        }

        let (ctx, _guard) = start_localhost_context(3, build_state).await;

        let query = r#"SELECT "MinTemp" FROM weather WHERE "MinTemp" > 20.0"#;

        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let plan = df.create_physical_plan().await?;

        // Wrap leaf nodes with ErrorThrowingExec to test error propagation
        let transformed = plan.transform_up(|plan| {
            if plan.children().is_empty() {
                return Ok(Transformed::yes(Arc::new(ErrorThrowingExec::new(
                    plan,
                    "something failed",
                ))));
            }
            Ok(Transformed::no(plan))
        })?;
        let plan = transformed.data;

        let stream = execute_stream(plan, ctx.task_ctx())?;

        let Err(err) = stream.try_collect::<Vec<_>>().await else {
            panic!("Should have failed")
        };
        assert_eq!(
            DataFusionError::Execution("something failed".to_string()).to_string(),
            err.to_string()
        );

        Ok(())
    }

    /// A custom execution plan that wraps a child but always throws an error.
    /// This tests that errors are properly propagated in distributed execution.
    #[derive(Debug)]
    pub struct ErrorThrowingExec {
        msg: String,
        plan_properties: PlanProperties,
        child: Arc<dyn ExecutionPlan>,
    }

    impl ErrorThrowingExec {
        fn new(child: Arc<dyn ExecutionPlan>, msg: &str) -> Self {
            let plan_properties = PlanProperties::new(
                EquivalenceProperties::new(child.schema()),
                child.output_partitioning().clone(),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                msg: msg.to_string(),
                plan_properties,
                child,
            }
        }
    }

    impl DisplayAs for ErrorThrowingExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "ErrorThrowingExec")
        }
    }

    impl ExecutionPlan for ErrorThrowingExec {
        fn name(&self) -> &str {
            "ErrorThrowingExec"
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
            Ok(Arc::new(ErrorThrowingExec::new(
                children[0].clone(),
                &self.msg,
            )))
        }

        fn execute(
            &self,
            _: usize,
            _: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            // Return a stream that immediately fails with the configured error message
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream::iter(vec![Err(DataFusionError::Execution(self.msg.clone()))]),
            )))
        }
    }

    #[derive(Debug)]
    struct ErrorThrowingExecCodec;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct ErrorThrowingExecProto {
        #[prost(string, tag = "1")]
        msg: String,
    }

    impl PhysicalExtensionCodec for ErrorThrowingExecCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            let node =
                ErrorThrowingExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?;

            if inputs.len() != 1 {
                return Err(proto_error(format!(
                    "ErrorThrowingExec expects exactly one child, got {}",
                    inputs.len()
                )));
            }

            Ok(Arc::new(ErrorThrowingExec::new(
                inputs[0].clone(),
                &node.msg,
            )))
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
        ) -> datafusion::common::Result<()> {
            let Some(plan) = node.as_any().downcast_ref::<ErrorThrowingExec>() else {
                return Err(proto_error(format!(
                    "Expected plan to be of type ErrorThrowingExec, but was {}",
                    node.name()
                )));
            };
            ErrorThrowingExecProto {
                msg: plan.msg.clone(),
            }
            .encode(buf)
            .map_err(|err| proto_error(format!("{err}")))
        }
    }
}
