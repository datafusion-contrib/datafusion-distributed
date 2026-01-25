#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DistributedExt, WorkerQueryContext, assert_snapshot, display_plan_ascii,
    };
    use futures::TryStreamExt;
    use std::any::Any;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_udf_in_partitioning_field() -> Result<(), Box<dyn Error>> {
        async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
            Ok(ctx.builder.with_scalar_functions(vec![udf()]).build())
        }

        let (mut ctx, _guard, _) = start_localhost_context(3, build_state).await;
        ctx = SessionStateBuilder::from(ctx.state())
            .with_distributed_bytes_processed_per_partition(100)?
            .with_scalar_functions(vec![udf()])
            .build()
            .into();

        let query = r#"SELECT "MinTemp" FROM weather WHERE test_udf("MinTemp") > 20.0"#;
        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let plan = df.create_physical_plan().await?;

        let stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
        // It should not fail.
        stream.try_collect::<Vec<_>>().await?;

        let physical_distributed_str = display_plan_ascii(plan.as_ref(), false);

        assert_snapshot!(physical_distributed_str,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=3, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0] t1:[p1] t2:[p2]
          │ FilterExec: test_udf(MinTemp@0) > 20
          │   PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet, predicate=test_udf(MinTemp@0) > 20
          └──────────────────────────────────────────────────
        ",
        );

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
