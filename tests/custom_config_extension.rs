#[cfg(all(feature = "integration", test))]
mod tests {
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{extensions_options, internal_err};
    use datafusion::config::ConfigExtension;
    use datafusion::error::DataFusionError;
    use datafusion::execution::{
        FunctionRegistry, SendableRecordBatchStream, SessionState, TaskContext,
    };
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        execute_stream, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    };
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::{add_user_codec, ConfigExtensionExt};
    use datafusion_distributed::{
        ArrowFlightReadExec, DistributedPhysicalOptimizerRule, SessionBuilder,
    };
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    use futures::TryStreamExt;
    use prost::Message;
    use std::any::Any;
    use std::fmt::Formatter;
    use std::sync::Arc;

    #[tokio::test]
    async fn custom_config_extension() -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Clone)]
        struct CustomSessionBuilder;

        #[async_trait]
        impl SessionBuilder for CustomSessionBuilder {
            async fn session_state(
                &self,
                mut state: SessionState,
            ) -> Result<SessionState, DataFusionError> {
                state.retrieve_distributed_option_extension::<CustomExtension>()?;
                add_user_codec(state.config_mut(), CustomConfigExtensionRequiredExecCodec);
                Ok(state)
            }
        }

        let (mut ctx, _guard) = start_localhost_context(3, CustomSessionBuilder).await;
        add_user_codec(&mut ctx, CustomConfigExtensionRequiredExecCodec);
        ctx.add_distributed_option_extension(CustomExtension {
            foo: "foo".to_string(),
            bar: 1,
            baz: true,
        })?;

        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(CustomConfigExtensionRequiredExec::new());

        for size in [1, 2, 3] {
            plan = Arc::new(ArrowFlightReadExec::new_pending(
                plan,
                Partitioning::RoundRobinBatch(size),
            ));
        }

        let plan = DistributedPhysicalOptimizerRule::default().distribute_plan(plan)?;
        let stream = execute_stream(Arc::new(plan), ctx.task_ctx())?;
        // It should not fail.
        stream.try_collect::<Vec<_>>().await?;

        Ok(())
    }

    extensions_options! {
        pub struct CustomExtension {
            pub foo: String, default = "".to_string()
            pub bar: usize, default = 0
            pub baz: bool, default = false
        }
    }

    impl ConfigExtension for CustomExtension {
        const PREFIX: &'static str = "custom";
    }

    #[derive(Debug)]
    pub struct CustomConfigExtensionRequiredExec {
        plan_properties: PlanProperties,
    }

    impl CustomConfigExtensionRequiredExec {
        fn new() -> Self {
            let schema = Schema::new(vec![Field::new("numbers", DataType::Int64, false)]);
            Self {
                plan_properties: PlanProperties::new(
                    EquivalenceProperties::new(Arc::new(schema)),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                ),
            }
        }
    }

    impl DisplayAs for CustomConfigExtensionRequiredExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "CustomConfigExtensionRequiredExec")
        }
    }

    impl ExecutionPlan for CustomConfigExtensionRequiredExec {
        fn name(&self) -> &str {
            "CustomConfigExtensionRequiredExec"
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
            ctx: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            if ctx
                .session_config()
                .options()
                .extensions
                .get::<CustomExtension>()
                .is_none()
            {
                return internal_err!("CustomExtension not found in context");
            }
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                futures::stream::empty(),
            )))
        }
    }

    #[derive(Debug)]
    struct CustomConfigExtensionRequiredExecCodec;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct CustomConfigExtensionRequiredExecProto {}

    impl PhysicalExtensionCodec for CustomConfigExtensionRequiredExecCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _: &[Arc<dyn ExecutionPlan>],
            _registry: &dyn FunctionRegistry,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(CustomConfigExtensionRequiredExec::new()))
        }

        fn try_encode(
            &self,
            _node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
        ) -> datafusion::common::Result<()> {
            CustomConfigExtensionRequiredExecProto::default()
                .encode(buf)
                .unwrap();
            Ok(())
        }
    }
}
