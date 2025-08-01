#[allow(dead_code)]
mod common;

#[cfg(test)]
mod tests {
    use crate::common::localhost::start_localhost_context;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::DataFusionError;
    use datafusion::execution::{
        FunctionRegistry, SendableRecordBatchStream, SessionStateBuilder, TaskContext,
    };
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        execute_stream, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    };
    use datafusion_distributed::{ArrowFlightReadExec, SessionBuilder};
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    use datafusion_proto::protobuf::proto_error;
    use futures::{stream, TryStreamExt};
    use prost::Message;
    use std::any::Any;
    use std::error::Error;
    use std::fmt::Formatter;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_error_propagation() -> Result<(), Box<dyn Error>> {
        #[derive(Clone)]
        struct CustomSessionBuilder;
        impl SessionBuilder for CustomSessionBuilder {
            fn on_new_session(&self, mut builder: SessionStateBuilder) -> SessionStateBuilder {
                let codec: Arc<dyn PhysicalExtensionCodec> = Arc::new(ErrorExecCodec);
                let config = builder.config().get_or_insert_default();
                config.set_extension(Arc::new(codec));
                builder
            }
        }
        let (ctx, _guard) =
            start_localhost_context([50050, 50051, 50053], CustomSessionBuilder).await;

        let codec: Arc<dyn PhysicalExtensionCodec> = Arc::new(ErrorExecCodec);
        ctx.state_ref()
            .write()
            .config_mut()
            .set_extension(Arc::new(codec));

        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(ErrorExec::new("something failed"));

        for size in [1, 2, 3] {
            plan = Arc::new(ArrowFlightReadExec::new(
                plan,
                Partitioning::RoundRobinBatch(size),
            ));
        }

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
