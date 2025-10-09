#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::runtime::SpawnedTask;
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
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_stream,
    };
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::{
        DistributedExt, DistributedSessionBuilderContext, PartitionIsolatorExec, assert_snapshot,
        display_plan_ascii,
    };
    use datafusion_distributed::{DistributedPhysicalOptimizerRule, NetworkShuffleExec};
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    use datafusion_proto::protobuf::proto_error;
    use futures::TryStreamExt;
    use prost::Message;
    use std::any::Any;
    use std::fmt::Formatter;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;

    // This test proves that execution nodes do not get early dropped in the ArrowFlightEndpoint
    // when all the partitions start being consumed.
    //
    // It uses a StatefulInt64ListExec custom node whose execution depends on it not being dropped.
    // If for some reason ArrowFlightEndpoint drops the node before the stream ends, this test
    // will fail.
    #[tokio::test]
    async fn stateful_execution_plan() -> Result<(), Box<dyn std::error::Error>> {
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

        let distributed_plan = build_plan()?;
        let distributed_plan = DistributedPhysicalOptimizerRule::distribute_plan(distributed_plan)?;

        assert_snapshot!(display_plan_ascii(distributed_plan.as_ref(), false), @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortExec: expr=[numbers@0 DESC NULLS LAST], preserve_partitioning=[false]
        │   RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=10
        │     [Stage 2] => NetworkShuffleExec: output_partitions=10, input_tasks=10
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] t1:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] t2:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] t3:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] t4:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] t5:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] t6:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] t7:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] t8:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] t9:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] 
          │ RepartitionExec: partitioning=Hash([], 10), input_partitions=1
          │   SortExec: expr=[numbers@0 DESC NULLS LAST], preserve_partitioning=[false]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=1, input_tasks=1
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,p8,p9] 
            │ RepartitionExec: partitioning=Hash([numbers@0], 10), input_partitions=1
            │   FilterExec: numbers@0 > 1
            │     StatefulInt64ListExec: length=6
            └──────────────────────────────────────────────────
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

    fn build_plan() -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut plan: Arc<dyn ExecutionPlan> =
            Arc::new(StatefulInt64ListExec::new(vec![1, 2, 3, 4, 5, 6]));

        plan = Arc::new(PartitionIsolatorExec::new(plan));

        plan = Arc::new(FilterExec::try_new(
            Arc::new(BinaryExpr::new(
                col("numbers", &plan.schema())?,
                Operator::Gt,
                lit(1i64),
            )),
            plan,
        )?);

        plan = Arc::new(NetworkShuffleExec::try_new(
            Arc::new(RepartitionExec::try_new(
                Arc::clone(&plan),
                Partitioning::Hash(vec![col("numbers", &plan.schema())?], 1),
            )?),
            10,
        )?);

        plan = Arc::new(SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new(
                col("numbers", &plan.schema())?,
                SortOptions::new(true, false),
            )])
            .unwrap(),
            plan,
        ));

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

        Ok(plan)
    }

    #[derive(Debug)]
    pub struct StatefulInt64ListExec {
        plan_properties: PlanProperties,
        numbers: Vec<i64>,
        task: RwLock<Option<SpawnedTask<()>>>,
        tx: RwLock<Option<mpsc::Sender<i64>>>,
        rx: RwLock<Option<mpsc::Receiver<i64>>>,
    }

    impl StatefulInt64ListExec {
        fn new(numbers: Vec<i64>) -> Self {
            let schema = Schema::new(vec![Field::new("numbers", DataType::Int64, false)]);
            let (tx, rx) = mpsc::channel(10);
            Self {
                numbers,
                plan_properties: PlanProperties::new(
                    EquivalenceProperties::new(Arc::new(schema)),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                ),
                task: RwLock::new(None),
                tx: RwLock::new(Some(tx)),
                rx: RwLock::new(Some(rx)),
            }
        }
    }

    impl DisplayAs for StatefulInt64ListExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "StatefulInt64ListExec: length={:?}", self.numbers.len())
        }
    }

    impl ExecutionPlan for StatefulInt64ListExec {
        fn name(&self) -> &str {
            "StatefulInt64ListExec"
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
            if let Some(tx) = self.tx.write().unwrap().take() {
                let numbers = self.numbers.clone();
                self.task
                    .write()
                    .unwrap()
                    .replace(SpawnedTask::spawn(async move {
                        for n in numbers {
                            tx.send(n).await.unwrap();
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }));
            }

            let rx = self.rx.write().unwrap().take().unwrap();
            let schema = self.schema();

            let stream = ReceiverStream::new(rx).map(move |v| {
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![v]))])
                    .map_err(DataFusionError::from)
            });

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema().clone(),
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
            Ok(Arc::new(StatefulInt64ListExec::new(node.numbers.clone())))
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
        ) -> datafusion::common::Result<()> {
            let Some(plan) = node.as_any().downcast_ref::<StatefulInt64ListExec>() else {
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
