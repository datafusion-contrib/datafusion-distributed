#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::DataFusionError;
    use datafusion::execution::{
        FunctionRegistry, SendableRecordBatchStream, SessionState, SessionStateBuilder, TaskContext,
    };
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_stream,
    };
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::{
        DistributedExt, DistributedPhysicalOptimizerRule, DistributedSessionBuilderContext,
        NetworkShuffleExec,
    };
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
                .with_distributed_user_codec(ErrorExecCodec)
                .build())
        }

        let (ctx, _guard) = start_localhost_context(3, build_state).await;

        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(ErrorExec::new("something failed"));

        for size in [1, 2, 3] {
            plan = Arc::new(NetworkShuffleExec::try_new(
                plan,
                Partitioning::RoundRobinBatch(size),
                size,
            )?);
        }
        let plan = DistributedPhysicalOptimizerRule::distribute_plan(plan)?;
        let stream = execute_stream(Arc::new(plan), ctx.task_ctx())?;

        let Err(err) = stream.try_collect::<Vec<_>>().await else {
            panic!("Should have failed")
        };
        assert_eq!(
            DataFusionError::Execution("something failed".to_string()).to_string(),
            err.to_string()
        );

        Ok(())
    }

    #[derive(Debug)]
    pub struct ErrorExec {
        msg: String,
        plan_properties: PlanProperties,
    }

    impl ErrorExec {
        fn new(msg: &str) -> Self {
            let schema = Schema::new(vec![Field::new("numbers", DataType::Int64, false)]);
            Self {
                msg: msg.to_string(),
                plan_properties: PlanProperties::new(
                    EquivalenceProperties::new(Arc::new(schema)),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                ),
            }
        }
    }

    impl DisplayAs for ErrorExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "ErrorExec")
        }
    }

    impl ExecutionPlan for ErrorExec {
        fn name(&self) -> &str {
            "ErrorExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.plan_properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _: usize,
            _: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream::iter(vec![Err(DataFusionError::Execution(self.msg.clone()))]),
            )))
        }
    }

    #[derive(Debug)]
    struct ErrorExecCodec;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct ErrorExecProto {
        #[prost(string, tag = "1")]
        msg: String,
    }

    impl PhysicalExtensionCodec for ErrorExecCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            _: &[Arc<dyn ExecutionPlan>],
            _registry: &dyn FunctionRegistry,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            let node = ErrorExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?;
            Ok(Arc::new(ErrorExec::new(&node.msg)))
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
        ) -> datafusion::common::Result<()> {
            let Some(plan) = node.as_any().downcast_ref::<ErrorExec>() else {
                return Err(proto_error(format!(
                    "Expected plan to be of type ErrorExec, but was {}",
                    node.name()
                )));
            };
            ErrorExecProto {
                msg: plan.msg.clone(),
            }
            .encode(buf)
            .map_err(|err| proto_error(format!("{err}")))
        }
    }
}
