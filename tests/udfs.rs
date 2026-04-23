#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{WorkerQueryContext, assert_snapshot, display_plan_ascii};
    use futures::TryStreamExt;
    use std::any::Any;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_udf_in_partitioning_field() -> Result<(), Box<dyn Error>> {
        async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
            Ok(ctx.builder.with_scalar_functions(vec![udf()]).build())
        }

        let (ctx, _guard, _) = start_localhost_context(3, build_state).await;
        let ctx = SessionStateBuilder::from(ctx.state())
            .with_scalar_functions(vec![udf()])
            .build()
            .into();

        register_parquet_tables(&ctx).await?;

        let df = ctx
            .sql(r#"SELECT test_udf("RainToday"), count(*) FROM weather GROUP BY test_udf("RainToday") ORDER BY count(*)"#)
            .await?;
        let physical_distributed = df.create_physical_plan().await?;
        let physical_distributed_str = display_plan_ascii(physical_distributed.as_ref(), false);

        assert_snapshot!(physical_distributed_str,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ ProjectionExec: expr=[test_udf(weather.RainToday)@0 as test_udf(weather.RainToday), count(*)@1 as count(*)]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=16, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p7] t1:[p0..p7] 
          │ SortExec: expr=[count(*)@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[test_udf(weather.RainToday)@0 as test_udf(weather.RainToday), count(Int64(1))@1 as count(*), count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[test_udf(weather.RainToday)@0 as test_udf(weather.RainToday)], aggr=[count(Int64(1))]
          │       LocalExchangeSplitExec: input_partitions=2, base_partitions=3, local_partitions=4, exprs=[test_udf(weather.RainToday)@0]
          │         [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] 
            │ RepartitionExec: partitioning=Hash([test_udf(weather.RainToday)@0], 3), input_partitions=1
            │   AggregateExec: mode=Partial, gby=[test_udf(RainToday@0) as test_udf(weather.RainToday)], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: tasks=3 partitions=3
            │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ",
        );

        let batches = pretty_format_batches(
            &execute_stream(physical_distributed, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        assert_snapshot!(batches, @r"
        +-----------------------------+----------+
        | test_udf(weather.RainToday) | count(*) |
        +-----------------------------+----------+
        | Yes                         | 66       |
        | No                          | 300      |
        +-----------------------------+----------+
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
