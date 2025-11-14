#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::error::DataFusionError;
    use datafusion::execution::{
        FunctionRegistry, SendableRecordBatchStream, SessionState, SessionStateBuilder, TaskContext,
    };
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, col, lit};
    use datafusion::physical_expr::{
        EquivalenceProperties, LexOrdering, Partitioning, PhysicalSortExpr,
    };
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, displayable, execute_stream,
    };
    use datafusion_distributed::NetworkShuffleExec;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::{
        DistributedExt, DistributedSessionBuilderContext, PartitionIsolatorExec, assert_snapshot,
        display_plan_ascii, distribute_plan,
    };
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    use datafusion_proto::protobuf::proto_error;
    use futures::{TryStreamExt, stream};
    use prost::Message;
    use std::any::Any;
    use std::fmt::Formatter;
    use std::sync::Arc;

    #[tokio::test]
    async fn custom_extension_codec() -> Result<(), Box<dyn std::error::Error>> {
        async fn build_state(
            ctx: DistributedSessionBuilderContext,
        ) -> Result<SessionState, DataFusionError> {
            Ok(SessionStateBuilder::new()
                .with_runtime_env(ctx.runtime_env)
                .with_default_features()
                .with_distributed_user_codec(Int64ListExecCodec)
                .build())
        }

        let (ctx, _guard) = start_localhost_context(3, build_state).await;

        let single_node_plan = build_plan(false)?;
        assert_snapshot!(displayable(single_node_plan.as_ref()).indent(true).to_string(), @r"
        SortExec: expr=[numbers@0 DESC NULLS LAST], preserve_partitioning=[false]
          FilterExec: numbers@0 > 1
            Int64ListExec: length=6
        ");

        let distributed_plan = build_plan(true)?;
        let distributed_plan = distribute_plan(distributed_plan)?.unwrap();

        assert_snapshot!(display_plan_ascii(distributed_plan.as_ref(), false), @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortExec: expr=[numbers@0 DESC NULLS LAST], preserve_partitioning=[false]
        │   RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=10
        │     [Stage 2] => NetworkShuffleExec: output_partitions=10, input_tasks=10
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p9] t1:[p0..p9] t2:[p0..p9] t3:[p0..p9] t4:[p0..p9] t5:[p0..p9] t6:[p0..p9] t7:[p0..p9] t8:[p0..p9] t9:[p0..p9] 
          │ RepartitionExec: partitioning=Hash([], 10), input_partitions=1
          │   SortExec: expr=[numbers@0 DESC NULLS LAST], preserve_partitioning=[false]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p9] 
            │ RepartitionExec: partitioning=Hash([numbers@0], 10), input_partitions=1
            │   FilterExec: numbers@0 > 1
            │     Int64ListExec: length=6
            └──────────────────────────────────────────────────
        ");

        let stream = execute_stream(single_node_plan, ctx.task_ctx())?;
        let batches_single_node = stream.try_collect::<Vec<_>>().await?;

        assert_snapshot!(pretty_format_batches(&batches_single_node).unwrap(), @r"
        +---------+
        | numbers |
        +---------+
        | 6       |
        | 5       |
        | 4       |
        | 3       |
        | 2       |
        +---------+
        ");

        let stream = execute_stream(distributed_plan, ctx.task_ctx())?;
        let batches_distributed = stream.try_collect::<Vec<_>>().await?;

        assert_snapshot!(pretty_format_batches(&batches_distributed).unwrap(), @r"
        +---------+
        | numbers |
        +---------+
        | 6       |
        | 5       |
        | 4       |
        | 3       |
        | 2       |
        +---------+
        ");
        Ok(())
    }

    fn build_plan(distributed: bool) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(Int64ListExec::new(vec![1, 2, 3, 4, 5, 6]));

        if distributed {
            plan = Arc::new(PartitionIsolatorExec::new(plan));
        }

        plan = Arc::new(FilterExec::try_new(
            Arc::new(BinaryExpr::new(
                col("numbers", &plan.schema())?,
                Operator::Gt,
                lit(1i64),
            )),
            plan,
        )?);

        if distributed {
            plan = Arc::new(NetworkShuffleExec::try_new(
                Arc::new(RepartitionExec::try_new(
                    Arc::clone(&plan),
                    Partitioning::Hash(vec![col("numbers", &plan.schema())?], 1),
                )?),
                10,
            )?);
        }

        plan = Arc::new(SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new(
                col("numbers", &plan.schema())?,
                SortOptions::new(true, false),
            )])
            .unwrap(),
            plan,
        ));

        if distributed {
            plan = Arc::new(NetworkShuffleExec::try_new(
                Arc::new(RepartitionExec::try_new(
                    plan,
                    Partitioning::Hash(vec![], 10),
                )?),
                10,
            )?);

            plan = Arc::new(RepartitionExec::try_new(
                plan,
                Partitioning::RoundRobinBatch(1),
            )?);

            plan = Arc::new(SortExec::new(
                LexOrdering::new(vec![PhysicalSortExpr::new(
                    col("numbers", &plan.schema())?,
                    SortOptions::new(true, false),
                )])
                .unwrap(),
                plan,
            ));
        }

        Ok(plan)
    }

    #[derive(Debug)]
    pub struct Int64ListExec {
        plan_properties: PlanProperties,
        numbers: Vec<i64>,
    }

    impl Int64ListExec {
        fn new(numbers: Vec<i64>) -> Self {
            let schema = Schema::new(vec![Field::new("numbers", DataType::Int64, false)]);
            Self {
                numbers,
                plan_properties: PlanProperties::new(
                    EquivalenceProperties::new(Arc::new(schema)),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                ),
            }
        }
    }

    impl DisplayAs for Int64ListExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "Int64ListExec: length={:?}", self.numbers.len())
        }
    }

    impl ExecutionPlan for Int64ListExec {
        fn name(&self) -> &str {
            "Int64ListExec"
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
            let array = Int64Array::from(self.numbers.clone());
            let batch = RecordBatch::try_new(self.schema(), vec![Arc::new(array)])?;

            let stream = stream::iter(vec![Ok(batch)]);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream,
            )))
        }
    }

    #[derive(Debug)]
    struct Int64ListExecCodec;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct Int64ListExecProto {
        #[prost(message, repeated, tag = "1")]
        numbers: Vec<i64>,
    }

    impl PhysicalExtensionCodec for Int64ListExecCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            _: &[Arc<dyn ExecutionPlan>],
            _registry: &dyn FunctionRegistry,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            let node =
                Int64ListExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?;
            Ok(Arc::new(Int64ListExec::new(node.numbers.clone())))
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
        ) -> datafusion::common::Result<()> {
            let Some(plan) = node.as_any().downcast_ref::<Int64ListExec>() else {
                return Err(proto_error(format!(
                    "Expected plan to be of type Int64ListExec, but was {}",
                    node.name()
                )));
            };
            Int64ListExecProto {
                numbers: plan.numbers.clone(),
            }
            .encode(buf)
            .map_err(|err| proto_error(format!("{err}")))
        }
    }
}
