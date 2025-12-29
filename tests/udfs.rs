#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::datatypes::{Field, Schema};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::error::DataFusionError;
    use datafusion::execution::SessionState;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::physical_expr::expressions::lit;
    use datafusion::physical_expr::{Partitioning, ScalarFunctionExpr};
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, execute_stream};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::{
        DistributedExt, DistributedPhysicalOptimizerRule, DistributedSessionBuilderContext,
        assert_snapshot, display_plan_ascii,
    };
    use futures::TryStreamExt;
    use std::any::Any;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_udf_in_partitioning_field() -> Result<(), Box<dyn Error>> {
        async fn build_state(
            ctx: DistributedSessionBuilderContext,
        ) -> Result<SessionState, DataFusionError> {
            Ok(ctx
                .builder
                .with_scalar_functions(vec![udf()])
                .with_distributed_task_estimator(2)
                .build())
        }

        let (ctx, _guard) = start_localhost_context(3, build_state).await;

        let wrap = |input: Arc<dyn ExecutionPlan>| -> Arc<dyn ExecutionPlan> {
            Arc::new(
                RepartitionExec::try_new(
                    input,
                    Partitioning::Hash(
                        vec![Arc::new(ScalarFunctionExpr::new(
                            "test_udf",
                            udf(),
                            vec![lit(1)],
                            Arc::new(Field::new("return", DataType::Int32, false)),
                            Default::default(),
                        ))],
                        1,
                    ),
                )
                .unwrap(),
            )
        };

        let node = wrap(wrap(Arc::new(EmptyExec::new(Arc::new(Schema::empty())))));

        let physical_distributed =
            DistributedPhysicalOptimizerRule.optimize(node, ctx.copied_config().options())?;

        let physical_distributed_str = display_plan_ascii(physical_distributed.as_ref(), false);

        assert_snapshot!(physical_distributed_str,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ RepartitionExec: partitioning=Hash([test_udf(1)], 1), input_partitions=1
        │   [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p0..p1] 
          │ RepartitionExec: partitioning=Hash([test_udf(1)], 2), input_partitions=1
          │   EmptyExec
          └──────────────────────────────────────────────────
        ",
        );

        let batches = pretty_format_batches(
            &execute_stream(physical_distributed, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        assert_snapshot!(batches, @r"
        ++
        ++
        ");
        Ok(())
    }

    fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::new_from_impl(Udf::new()))
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct Udf(Signature);

    impl Udf {
        fn new() -> Self {
            Self(Signature::any(1, Volatility::Immutable))
        }
    }

    impl ScalarUDFImpl for Udf {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "test_udf"
        }

        fn signature(&self) -> &Signature {
            &self.0
        }

        fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(arg_types[0].clone())
        }

        fn invoke_with_args(
            &self,
            mut args: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            Ok(args.args.remove(0))
        }
    }
}
