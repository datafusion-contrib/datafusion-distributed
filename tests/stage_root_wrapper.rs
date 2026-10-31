#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::common::{Result, internal_err};
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{Partitioning, PhysicalExpr};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, collect,
    };
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use datafusion_distributed::test_utils::routing::ColocateAllTasksHandler;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::{
        DistributedExec, DistributedExt, DistributedMetricsFormat, NetworkBoundary,
        NetworkCoalesceExec, NetworkShuffleExec, Stage, StageRootWrapper, WorkerQueryContext,
        rewrite_distributed_plan_with_metrics,
    };
    use datafusion_proto::physical_plan::{
        PhysicalExtensionCodec, PhysicalProtoConverterExtension,
    };
    use futures::StreamExt;
    use std::fmt::Formatter;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;
    use test_case::test_case;
    use url::Url;

    #[test_case(false, false; "static_shuffle")]
    #[test_case(true, false; "dynamic_shuffle")]
    #[test_case(false, true; "static_broadcast")]
    #[test_case(true, true; "dynamic_broadcast")]
    #[tokio::test]
    async fn wrapper_covers_final_producer_and_preserves_metrics(
        dynamic: bool,
        broadcast: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let wrapper = TrackingWrapper {
            producer: if broadcast {
                "BroadcastExec"
            } else {
                "RepartitionExec"
            },
            producer_executions: Arc::new(AtomicUsize::new(0)),
        };
        let mut ctx = tracking_context(&wrapper, false).await;
        ctx.set_distributed_dynamic_task_count(dynamic)?;
        ctx.set_distributed_dynamic_bytes_per_partition(1)?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 1, 2, 3]))],
        )?;
        let input = register_temp_parquet_table("stage_input", schema, vec![batch], &ctx).await?;
        let (sql, expected) = if broadcast {
            (
                "SELECT SUM(a.id * b.id) AS total FROM stage_input a CROSS JOIN stage_input b",
                "+-------+\n| total |\n+-------+\n| 49    |\n+-------+",
            )
        } else {
            (
                "SELECT id, COUNT(*) AS n FROM stage_input GROUP BY id ORDER BY id",
                "+----+---+\n| id | n |\n+----+---+\n| 1  | 2 |\n| 2  | 1 |\n| 3  | 1 |\n+----+---+",
            )
        };
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        let batches = tokio::time::timeout(
            Duration::from_secs(10),
            collect(plan.clone(), ctx.task_ctx()),
        )
        .await??;
        assert_eq!(pretty_format_batches(&batches)?.to_string(), expected);
        assert!(wrapper.producer_executions.load(Ordering::Relaxed) > 0);

        let reported = tokio::time::timeout(
            Duration::from_secs(10),
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::PerTask),
        )
        .await??;
        let mut reporting_boundaries = 0;
        reported.apply(|node| {
            let rows = node.metrics().map_or(0, |metrics| {
                metrics
                    .iter()
                    .filter(|metric| metric.value().name().starts_with("tracked_rows"))
                    .map(|metric| metric.value().as_usize())
                    .sum::<usize>()
            });
            if rows > 0 {
                assert_eq!(
                    node.name(),
                    "TrackingExec",
                    "wrapper metrics moved onto another operator"
                );
                reporting_boundaries += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(reporting_boundaries > 0);
        std::fs::remove_file(input)?;
        Ok(())
    }

    #[test_case(false; "grpc")]
    #[test_case(true; "in_process")]
    #[tokio::test]
    async fn wrapper_metrics_are_task_local(local: bool) -> Result<(), Box<dyn std::error::Error>> {
        let wrapper = TrackingWrapper {
            producer: "RepartitionExec",
            producer_executions: Arc::new(AtomicUsize::new(0)),
        };
        let mut ctx = tracking_context(&wrapper, local).await;
        ctx.set_distributed_route_task_handler(ColocateAllTasksHandler::default());
        ctx.set_distributed_desired_task_count_handler(3usize);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 1, 2, 3]))],
        )?;
        let input = register_temp_parquet_table("stage_input", schema, vec![batch], &ctx).await?;
        let plan = ctx
            .sql("SELECT id FROM stage_input UNION ALL SELECT id FROM stage_input")
            .await?
            .create_physical_plan()
            .await?;
        let batches = tokio::time::timeout(
            Duration::from_secs(10),
            collect(plan.clone(), ctx.task_ctx()),
        )
        .await??;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 8);
        let reported = tokio::time::timeout(
            Duration::from_secs(10),
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::PerTask),
        )
        .await??;
        let mut tracked_rows = 0;
        reported.apply(|node| {
            if node.name() == "TrackingExec" {
                tracked_rows += node
                    .metrics()
                    .unwrap()
                    .iter()
                    .filter(|metric| metric.value().name() == "tracked_rows")
                    .map(|metric| metric.value().as_usize())
                    .sum::<usize>();
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(tracked_rows, 8, "each task must report only its own rows");
        std::fs::remove_file(input)?;
        Ok(())
    }

    #[test_case(false, false; "static_no_producer")]
    #[test_case(false, true; "static_shuffle_producer")]
    #[test_case(true, false; "dynamic_no_producer")]
    #[tokio::test]
    async fn initial_producer_assembly_preserves_existing_wrappers(
        dynamic: bool,
        producer: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let wrapper = TrackingWrapper {
            producer: "RepartitionExec",
            producer_executions: Arc::new(AtomicUsize::new(0)),
        };
        let mut ctx = tracking_context(&wrapper, false).await;
        ctx.set_distributed_dynamic_task_count(dynamic)?;
        ctx.set_distributed_desired_task_count_handler(2usize);
        let source = SessionContext::new()
            .sql("SELECT 1 AS id")
            .await?
            .create_physical_plan()
            .await?;
        let partitioning = if dynamic {
            Partitioning::RoundRobinBatch(2)
        } else {
            Partitioning::Hash(vec![Arc::new(Column::new("id", 0))], 2)
        };
        let original = Arc::new(RepartitionExec::try_new(source, partitioning.clone())?);
        let existing = wrapper.wrap(original)?;
        let boundary: Arc<dyn ExecutionPlan> = if dynamic {
            existing
        } else if producer {
            let dummy = Arc::new(RepartitionExec::try_new(existing.clone(), partitioning)?);
            let boundary = NetworkShuffleExec::try_new(dummy, 2)?;
            let Stage::Local(mut stage) = boundary.input_stage().clone() else {
                return internal_err!("Expected a local stage").map_err(Into::into);
            };
            stage.plan = existing;
            boundary.with_input_stage(Stage::Local(stage))?
        } else {
            Arc::new(NetworkCoalesceExec::try_new(existing, 2, 1)?)
        };
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            DistributedExec::new(Arc::new(CoalescePartitionsExec::new(boundary)))
                .with_metrics_collection(true),
        );
        let batches = tokio::time::timeout(
            Duration::from_secs(10),
            collect(plan.clone(), ctx.task_ctx()),
        )
        .await??;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let reported = tokio::time::timeout(
            Duration::from_secs(10),
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::PerTask),
        )
        .await??;
        let mut wrappers = 0;
        let mut wrapped_producers = 0;
        reported.apply(|node| {
            if node.name() == "TrackingExec" {
                wrappers += 1;
                let children = node.children();
                assert_eq!(children[0].name(), "RepartitionExec");
                wrapped_producers +=
                    usize::from(children[0].children()[0].name() == "TrackingExec");
                let rows = node
                    .metrics()
                    .unwrap()
                    .iter()
                    .filter(|metric| metric.value().name() == "tracked_rows")
                    .map(|metric| metric.value().as_usize())
                    .sum::<usize>();
                assert_eq!(rows, 2, "wrapper metrics must stay on their original node");
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(wrappers, if producer { 2 } else { 1 });
        assert_eq!(wrapped_producers, usize::from(producer));
        Ok(())
    }

    async fn tracking_context(wrapper: &TrackingWrapper, local: bool) -> SessionContext {
        let worker_wrapper = wrapper.clone();
        let builder = move |ctx: WorkerQueryContext| {
            let wrapper = worker_wrapper.clone();
            async move {
                Ok(ctx
                    .builder
                    .with_distributed_user_codec(wrapper.clone())
                    .with_distributed_stage_root_wrapper(wrapper)
                    .build())
            }
        };
        let worker = Arc::new(OnceLock::new());
        let captured = Arc::clone(&worker);
        let mut ctx = start_configured_in_memory_context(3, builder, move |worker| {
            let _ = captured.set(worker.clone());
            worker
        })
        .await;
        if local {
            ctx.set_distributed_local_worker_context(
                worker
                    .get()
                    .unwrap()
                    .to_local_worker_context(Url::parse("http://url-0").unwrap()),
            );
        }
        ctx.set_distributed_user_codec(wrapper.clone());
        ctx.set_distributed_stage_root_wrapper(wrapper.clone());
        ctx
    }

    #[derive(Clone, Debug)]
    struct TrackingWrapper {
        producer: &'static str,
        producer_executions: Arc<AtomicUsize>,
    }

    impl StageRootWrapper for TrackingWrapper {
        fn matches(&self, plan: &dyn ExecutionPlan) -> bool {
            plan.is::<TrackingExec>()
        }

        fn wrap(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(TrackingExec {
                input,
                wrapper: self.clone(),
                metrics: ExecutionPlanMetricsSet::new(),
            }))
        }
    }

    #[derive(Debug)]
    struct TrackingExec {
        input: Arc<dyn ExecutionPlan>,
        wrapper: TrackingWrapper,
        metrics: ExecutionPlanMetricsSet,
    }

    impl DisplayAs for TrackingExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "TrackingExec")
        }
    }

    impl ExecutionPlan for TrackingExec {
        fn name(&self) -> &str {
            "TrackingExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }

        fn apply_expressions(
            &self,
            _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if children.len() != 1 {
                return internal_err!("TrackingExec requires one child");
            }
            Ok(Arc::new(Self {
                input: Arc::clone(&children[0]),
                wrapper: self.wrapper.clone(),
                metrics: self.metrics.clone(),
            }))
        }

        fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
            self.wrapper.wrap(Arc::clone(&self.input))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            if self.input.name() == self.wrapper.producer {
                self.wrapper
                    .producer_executions
                    .fetch_add(1, Ordering::Relaxed);
            }
            let rows = MetricBuilder::new(&self.metrics).counter("tracked_rows", partition);
            let stream = self.input.execute(partition, context)?.map(move |batch| {
                if let Ok(batch) = &batch {
                    rows.add(batch.num_rows());
                }
                batch
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.input.schema(),
                stream,
            )))
        }

        fn metrics(&self) -> Option<MetricsSet> {
            Some(self.metrics.clone_inner())
        }
    }

    impl PhysicalExtensionCodec for TrackingWrapper {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            _: &TaskContext,
            _: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if buf != [1] || inputs.len() != 1 {
                return internal_err!("Invalid TrackingExec encoding");
            }
            self.wrap(Arc::clone(&inputs[0]))
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
            _: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            if !self.matches(node.as_ref()) {
                return internal_err!("Expected TrackingExec");
            }
            buf.push(1);
            Ok(())
        }
    }
}
