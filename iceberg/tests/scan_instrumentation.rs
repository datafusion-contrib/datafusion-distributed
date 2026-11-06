mod tests {
    use std::error::Error;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::common::Result;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::source::{DataSource, DataSourceExec};
    use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder, TaskContext};
    use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
    use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, collect};
    use datafusion_distributed::{
        DistributedExt, DistributedMetricsFormat, ScanInstrumentation,
        ScanInstrumentationExtension, ScanOpen, rewrite_distributed_plan_with_metrics,
    };
    use datafusion_distributed_iceberg::{EagerIcebergDataSource, IcebergExt};
    use datafusion_distributed_iceberg_test_tools::{IcebergTestHarness, create_taxi_table};
    use futures::{Stream, TryStreamExt};

    #[tokio::test]
    async fn local_scans_observe_only_rows_after_the_source_limit() -> Result<()> {
        for eager in [false, true] {
            let observer = Arc::new(RecordingInstrumentation::default());
            let hook = Arc::clone(&observer);
            let harness = IcebergTestHarness::builder()
                .with_session_extension(move |state| install(state, hook.clone()))
                .with_config_option("iceberg.eager_file_planning", eager)
                .with_table(create_taxi_table().await?)
                .build()
                .await?;
            let plan = harness.scan().await?;
            let scan = plan
                .downcast_ref::<DataSourceExec>()
                .unwrap()
                .with_fetch(Some(3))
                .expect("scan supports limits");
            // Drain one limited partition directly: downstream LIMIT may stop polling
            // early and would obscure whether source-level truncation was observed.
            let batches: Vec<_> = scan
                .execute(0, harness.state().task_ctx())?
                .try_collect()
                .await?;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
            assert_eq!(observer.rows.load(Ordering::Relaxed), 3);
            assert_eq!(observer.finished.load(Ordering::Relaxed), 1);
            assert_eq!(observer.cancelled.load(Ordering::Relaxed), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn worker_metrics_and_labels_reach_the_coordinator() -> Result<()> {
        for eager in [false, true] {
            let observer = Arc::new(RecordingInstrumentation::default());
            let hook = Arc::clone(&observer);
            let harness = IcebergTestHarness::builder()
                .with_session_extension(move |state| install(state, hook.clone()))
                .with_config_option("iceberg.eager_file_planning", eager)
                .with_workers(2)
                .configure_session(|state| {
                    state.with_distributed_file_scan_config_bytes_per_partition(1)
                })?
                .with_table(create_taxi_table().await?)
                .build()
                .await?;
            let plan = harness
                .physical_plan("SELECT MAX(vendor_id) FROM taxi")
                .await?;
            collect(Arc::clone(&plan), harness.state().task_ctx()).await?;
            let plan =
                rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                    .await?;
            let mut rows = 0;
            plan.apply(|node| {
                if let Some(metrics) = node.metrics() {
                    for metric in metrics
                        .iter()
                        .filter(|m| m.value().name() == "observed_rows")
                    {
                        assert!(
                            metric
                                .labels()
                                .iter()
                                .any(|l| l.name() == "source" && l.value() == "iceberg")
                        );
                        rows += metric.value().as_usize();
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert_eq!(rows, 175_000);
            assert_eq!(observer.rows.load(Ordering::Relaxed), rows);
            assert!(observer.finished.load(Ordering::Relaxed) > 0);
            assert_eq!(observer.errors.load(Ordering::Relaxed), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn opening_errors_and_unpolled_cancellation_are_observable() -> Result<()> {
        let observer = Arc::new(RecordingInstrumentation::default());
        let hook = Arc::clone(&observer);
        let harness = IcebergTestHarness::builder()
            .with_session_extension(move |state| install(state, hook.clone()))
            .with_config_option("iceberg.eager_file_planning", true)
            .with_table(create_taxi_table().await?)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let source = plan.downcast_ref::<DataSourceExec>().unwrap().data_source();
        let error = source
            .open(usize::MAX, harness.state().task_ctx())
            .err()
            .expect("invalid partition");
        assert!(error.to_string().contains("partition"));
        assert_eq!(observer.errors.load(Ordering::Relaxed), 1);
        drop(source.open(0, harness.state().task_ctx())?);
        assert_eq!(observer.cancelled.load(Ordering::Relaxed), 1);
        assert_eq!(observer.finished.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn read_errors_are_observed_and_propagated() -> Result<(), Box<dyn Error>> {
        let observer = Arc::new(RecordingInstrumentation::default());
        let hook = Arc::clone(&observer);
        let harness = IcebergTestHarness::builder()
            .with_session_extension(move |state| install(state, hook.clone()))
            .with_config_option("iceberg.eager_file_planning", true)
            .with_table(create_taxi_table().await?)
            .build()
            .await?;
        let plan = harness.scan().await?;
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<EagerIcebergDataSource>()
            .unwrap();
        for file in source.planned_files() {
            source.file_io().delete(file.data_file_path()).await?;
        }
        source
            .open(0, harness.state().task_ctx())?
            .try_collect::<Vec<_>>()
            .await
            .expect_err("removed data files");
        assert_eq!(observer.errors.load(Ordering::Relaxed), 1);
        assert_eq!(observer.finished.load(Ordering::Relaxed), 0);
        assert_eq!(observer.cancelled.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[derive(Debug, Default, Clone)]
    struct RecordingInstrumentation {
        rows: Arc<AtomicUsize>,
        finished: Arc<AtomicUsize>,
        errors: Arc<AtomicUsize>,
        cancelled: Arc<AtomicUsize>,
    }

    impl ScanInstrumentation for RecordingInstrumentation {
        fn instrument(
            &self,
            source: &str,
            partition: usize,
            _: &TaskContext,
            metrics: &ExecutionPlanMetricsSet,
            open: ScanOpen<'_>,
        ) -> Result<SendableRecordBatchStream> {
            let input = open().inspect_err(|_| {
                self.errors.fetch_add(1, Ordering::Relaxed);
            })?;
            let rows = MetricBuilder::new(metrics)
                .with_new_label("source", source.to_owned())
                .counter("observed_rows", partition);
            Ok(Box::pin(ObservedStream {
                input,
                rows,
                observer: self.clone(),
                terminal: false,
            }))
        }
    }

    struct ObservedStream {
        input: SendableRecordBatchStream,
        rows: Count,
        observer: RecordingInstrumentation,
        terminal: bool,
    }

    impl Stream for ObservedStream {
        type Item = Result<RecordBatch>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let result = self.input.as_mut().poll_next(cx);
            match &result {
                Poll::Ready(Some(Ok(batch))) => {
                    self.rows.add(batch.num_rows());
                    self.observer
                        .rows
                        .fetch_add(batch.num_rows(), Ordering::Relaxed);
                }
                Poll::Ready(Some(Err(_))) => {
                    if !self.terminal {
                        self.observer.errors.fetch_add(1, Ordering::Relaxed);
                    }
                    self.terminal = true;
                }
                Poll::Ready(None) => {
                    if !self.terminal {
                        self.observer.finished.fetch_add(1, Ordering::Relaxed);
                    }
                    self.terminal = true;
                }
                Poll::Pending => {}
            }
            result
        }
    }
    impl RecordBatchStream for ObservedStream {
        fn schema(&self) -> SchemaRef {
            self.input.schema()
        }
    }
    impl Drop for ObservedStream {
        fn drop(&mut self) {
            if !self.terminal {
                self.observer.cancelled.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    fn install(
        mut state: SessionStateBuilder,
        hook: Arc<RecordingInstrumentation>,
    ) -> SessionStateBuilder {
        state
            .config()
            .get_or_insert_default()
            .set_extension(Arc::new(ScanInstrumentationExtension(hook)));
        state.with_iceberg_integration(Default::default())
    }
}
