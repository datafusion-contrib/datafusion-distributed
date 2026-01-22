#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::common::{extensions_options, internal_datafusion_err, internal_err};
    use datafusion::config::ConfigExtension;
    use datafusion::error::DataFusionError;
    use datafusion::execution::{
        SendableRecordBatchStream, SessionState, SessionStateBuilder, TaskContext,
    };
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        execute_stream,
    };
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DistributedExt, WorkerQueryContext};
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    use futures::TryStreamExt;
    use prost::Message;
    use std::any::Any;
    use std::fmt::Formatter;
    use std::sync::Arc;

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_distributed_option_extension_from_headers::<CustomExtension>(&ctx.headers)?
            .with_distributed_user_codec(CustomConfigExtensionRequiredExecCodec)
            .build())
    }

    #[tokio::test]
    async fn custom_config_extension() -> Result<(), Box<dyn std::error::Error>> {
        let (mut ctx, _guard) = start_localhost_context(3, build_state).await;
        ctx = SessionStateBuilder::from(ctx.state())
            .with_distributed_option_extension(CustomExtension {
                foo: "foo".to_string(),
                bar: 1,
                baz: true,
                throw_err: false,
            })
            .build()
            .into();

        let query = r#"SELECT "MinTemp" FROM weather WHERE "MinTemp" > 20.0"#;

        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let plan = df.create_physical_plan().await?;

        // Wrap leaf nodes with CustomConfigExtensionRequiredExec to test config extension propagation
        let transformed = plan.transform_up(|plan| {
            if plan.children().is_empty() {
                return Ok(Transformed::yes(Arc::new(
                    CustomConfigExtensionRequiredExec::new(plan),
                )));
            }
            Ok(Transformed::no(plan))
        })?;
        let plan = transformed.data;

        let stream = execute_stream(plan, ctx.task_ctx())?;
        // It should not fail.
        stream.try_collect::<Vec<_>>().await?;

        Ok(())
    }

    #[tokio::test]
    async fn custom_config_extension_runtime_change() -> Result<(), Box<dyn std::error::Error>> {
        let (mut ctx, _guard) = start_localhost_context(3, build_state).await;
        ctx = SessionStateBuilder::from(ctx.state())
            .with_distributed_option_extension(CustomExtension {
                throw_err: true,
                ..Default::default()
            })
            .build()
            .into();

        let query = r#"SELECT "MinTemp" FROM weather WHERE "MinTemp" > 20.0"#;

        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let plan = df.create_physical_plan().await?;

        // Wrap leaf nodes with CustomConfigExtensionRequiredExec to test config extension propagation
        let transformed = plan.transform_up(|plan| {
            if plan.children().is_empty() {
                return Ok(Transformed::yes(Arc::new(
                    CustomConfigExtensionRequiredExec::new(plan),
                )));
            }
            Ok(Transformed::no(plan))
        })?;
        let plan = transformed.data;

        // If the value is modified after setting it as a distributed option extension, it should
        // propagate the correct headers.
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .extensions
            .get_mut::<CustomExtension>()
            .unwrap()
            .throw_err = false;
        let stream = execute_stream(plan, ctx.task_ctx())?;
        // It should not fail.
        stream.try_collect::<Vec<_>>().await?;

        Ok(())
    }

    extensions_options! {
        pub struct CustomExtension {
            pub foo: String, default = "".to_string()
            pub bar: usize, default = 0
            pub baz: bool, default = false
            pub throw_err: bool, default = true
        }
    }

    impl ConfigExtension for CustomExtension {
        const PREFIX: &'static str = "custom";
    }

    #[derive(Debug)]
    pub struct CustomConfigExtensionRequiredExec {
        plan_properties: PlanProperties,
        child: Arc<dyn ExecutionPlan>,
    }

    impl CustomConfigExtensionRequiredExec {
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
            vec![&self.child]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(CustomConfigExtensionRequiredExec::new(
                children[0].clone(),
            )))
        }

        fn execute(
            &self,
            partition: usize,
            ctx: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            let Some(ext) = ctx
                .session_config()
                .options()
                .extensions
                .get::<CustomExtension>()
            else {
                return internal_err!("CustomExtension not found in context");
            };
            if ext.throw_err {
                return internal_err!("CustomExtension requested an error to be thrown");
            }
            // Pass through to child
            self.child.execute(partition, ctx)
        }
    }

    #[derive(Debug)]
    struct CustomConfigExtensionRequiredExecCodec;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct CustomConfigExtensionRequiredExecProto {}

    impl PhysicalExtensionCodec for CustomConfigExtensionRequiredExecCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            let _node = CustomConfigExtensionRequiredExecProto::decode(buf)
                .map_err(|err| internal_datafusion_err!("{err}"))?;

            if inputs.len() != 1 {
                return internal_err!(
                    "CustomConfigExtensionRequiredExec expects exactly one child, got {}",
                    inputs.len()
                );
            }

            Ok(Arc::new(CustomConfigExtensionRequiredExec::new(
                inputs[0].clone(),
            )))
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
