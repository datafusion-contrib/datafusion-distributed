#[cfg(all(feature = "integration", test))]
mod tests {
    use std::hint::black_box;
    use std::slice::from_ref;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::DataType;
    use async_trait::async_trait;
    use datafusion::common::runtime::SpawnedTask;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::common::{Result, assert_contains, exec_err};
    use datafusion::execution::SessionState;
    use datafusion::logical_expr::{ScalarUDF, Volatility, create_udf};
    use datafusion::physical_plan::{ExecutionPlan, execute_stream};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::test_utils::routing::{
        URLEmitterExtensionCodec, URLEmitterFunction, UrlEmitterRouteTaskHandler,
        url_emitter_desired_task_count, url_emitter_scale_up_leaf_node,
    };
    use datafusion_distributed::{
        DistributedExec, DistributedExt, MappedWorkerSessionBuilderExt, NetworkBoundaryExt,
        NetworkCoalesceExec, RouteTaskEvent, RouteTaskEventResponse, RouteTaskHandler, Stage,
        WorkerQueryContext, WorkerSessionBuilder, WorkerToCoordinatorMsg,
        rewrite_distributed_plan_with_dynamic_filters,
    };
    use futures::{StreamExt, TryStreamExt, stream};
    use tokio::time::{sleep, timeout};
    use tokio_util::sync::CancellationToken;

    const WAIT: Duration = Duration::from_secs(5);
    const SCAN: &str = "SELECT task_index FROM url_emitter(2, 2, 'scan')";

    #[tokio::test]
    async fn static_scan_returns_output_before_sibling_setup() -> Result<()> {
        let gate = SetupGate::default();
        let ctx = context(build_state)
            .await
            .with_distributed_dynamic_task_count(false)?
            .with_distributed_route_task_handler(gate.clone());
        let mut output = ctx.sql(SCAN).await?.execute_stream().await?;

        let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
        assert_eq!(task_indices(from_ref(&first)), vec![0]);
        assert!(!gate.release.is_cancelled());
        timeout(WAIT, gate.entered.cancelled()).await.unwrap();
        gate.release.cancel();
        let mut batches = vec![first];
        batches.extend(
            timeout(WAIT, output.try_collect::<Vec<_>>())
                .await
                .unwrap()?,
        );
        assert_eq!(task_indices(&batches), vec![0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn adaptive_scan_returns_output_before_sibling_setup() -> Result<()> {
        let gate = SetupGate::default();
        let ctx = context(build_state)
            .await
            .with_distributed_dynamic_task_count(true)?
            .with_distributed_route_task_handler(gate.clone());
        let mut output = ctx.sql(SCAN).await?.execute_stream().await?;

        let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
        assert_eq!(task_indices(from_ref(&first)), vec![0]);
        assert!(!gate.release.is_cancelled());
        timeout(WAIT, gate.entered.cancelled()).await.unwrap();
        gate.release.cancel();
        let mut batches = vec![first];
        batches.extend(
            timeout(WAIT, output.try_collect::<Vec<_>>())
                .await
                .unwrap()?,
        );
        assert_eq!(task_indices(&batches), vec![0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn adaptive_shuffle_samples_before_sibling_setup() -> Result<()> {
        let gate = SetupGate::default();
        let ctx = context(build_state)
            .await
            .with_distributed_dynamic_task_count(true)?
            .with_distributed_route_task_handler(gate.clone());
        let query = ctx
            .sql("SELECT task_index FROM url_emitter(2, 2, 'scan') GROUP BY task_index")
            .await?;
        let execution = SpawnedTask::spawn(async move { query.collect().await });

        timeout(WAIT, gate.entered.cancelled()).await.unwrap();
        // A positive LoadInfo report proves the remote sampler has produced rows.
        timeout(WAIT, gate.sampled.cancelled()).await.unwrap();
        assert!(!gate.release.is_cancelled());
        gate.release.cancel();
        let batches = timeout(WAIT, execution.join()).await.unwrap().unwrap()?;
        assert_eq!(task_indices(&batches), vec![0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn late_setup_failure_reaches_output_and_finishes_metrics() -> Result<()> {
        let gate = SetupGate {
            fail: true,
            ..Default::default()
        };
        let ctx = context(build_state)
            .await
            .with_distributed_dynamic_task_count(false)?
            .with_distributed_metrics_collection(true)?
            .with_distributed_route_task_handler(gate.clone());
        let plan = ctx.sql(SCAN).await?.create_physical_plan().await?;
        let mut output = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;

        let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
        assert_eq!(task_indices(&[first]), vec![0]);
        gate.release.cancel();
        let error = timeout(WAIT, output.try_collect::<Vec<_>>())
            .await
            .unwrap()
            .unwrap_err();
        assert_contains!(error.to_string(), "injected setup failure");
        assert_terminal_metrics(&plan).await;
        Ok(())
    }

    #[tokio::test]
    async fn worker_response_error_after_output_finishes_reports() -> Result<()> {
        let response_error = ResponseErrorGate::default();
        let ctx = context(build_state)
            .await
            .with_distributed_dynamic_task_count(false)?
            .with_distributed_metrics_collection(true)?
            .with_distributed_dynamic_filter_collection(true)?
            .with_distributed_route_task_handler(response_error.clone());
        let plan = ctx.sql(SCAN).await?.create_physical_plan().await?;
        let mut output = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;

        let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
        assert!(matches!(task_indices(&[first]).as_slice(), [0] | [1]));
        response_error.release.cancel();
        let error = timeout(WAIT, output.try_collect::<Vec<_>>())
            .await
            .unwrap()
            .unwrap_err();
        assert_contains!(error.to_string(), "injected worker response error");

        let distributed = plan.downcast_ref::<DistributedExec>().unwrap();
        let reports = timeout(WAIT, distributed.wait_for_metrics())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reports.len(), 2);
        assert!(
            reports
                .values()
                .all(|report| report.pre_order_plan_metrics.is_empty())
        );
        let task_ctx = ctx.task_ctx();
        timeout(
            WAIT,
            rewrite_distributed_plan_with_dynamic_filters(plan, &task_ctx),
        )
        .await
        .unwrap()?;
        Ok(())
    }

    #[tokio::test]
    async fn dropping_output_cancels_pending_setup_and_finishes_metrics() -> Result<()> {
        let gate = SetupGate::default();
        let ctx = context(build_state)
            .await
            .with_distributed_dynamic_task_count(true)?
            .with_distributed_metrics_collection(true)?
            .with_distributed_route_task_handler(gate.clone());
        let plan = ctx.sql(SCAN).await?.create_physical_plan().await?;
        let mut output = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;

        let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
        assert_eq!(task_indices(&[first]), vec![0]);
        timeout(WAIT, gate.entered.cancelled()).await.unwrap();
        drop(output);
        timeout(WAIT, gate.cancelled.cancelled()).await.unwrap();
        assert!(!gate.release.is_cancelled());
        assert_terminal_metrics(&plan).await;
        Ok(())
    }

    #[tokio::test]
    async fn limit_completion_cancels_pending_setup_and_finishes_metrics() -> Result<()> {
        let gate = SetupGate {
            wait_for_sibling: true,
            ..Default::default()
        };
        let ctx = context(build_state)
            .await
            .with_distributed_dynamic_task_count(false)?
            .with_distributed_metrics_collection(true)?
            .with_distributed_route_task_handler(gate.clone());
        let plan = ctx
            .sql(&format!("{SCAN} LIMIT 1"))
            .await?
            .create_physical_plan()
            .await?;
        let output = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
        let batches = timeout(WAIT, output.try_collect::<Vec<_>>())
            .await
            .unwrap()?;

        assert_eq!(task_indices(&batches), vec![0]);
        timeout(WAIT, gate.cancelled.cancelled()).await.unwrap();
        assert!(!gate.release.is_cancelled());
        assert_terminal_metrics(&plan).await;
        Ok(())
    }

    #[tokio::test]
    async fn adaptive_nested_coalesce_has_concrete_worker_destinations() -> Result<()> {
        let gate = SetupGate::default();
        gate.release.cancel();
        let ctx = context(build_state)
            .await
            .with_distributed_dynamic_task_count(true)?
            .with_distributed_route_task_handler(gate.clone());
        let query = ctx
            .sql(
                "SELECT task_index FROM url_emitter(2, 2, 'scan') WHERE task_index = \
             (SELECT MAX(task_index) FROM url_emitter(2, 2, 'inner'))",
            )
            .await?;

        let batches = timeout(WAIT, query.collect()).await.unwrap()?;
        assert_eq!(task_indices(&batches), vec![1]);
        assert!(gate.nested_coalesces.load(Ordering::Relaxed) > 0);
        Ok(())
    }

    // Iteration zero is warmup. Compare the same test on both revisions.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "controlled startup benchmark; run with --release --ignored --nocapture"]
    async fn startup_overlap_benchmark() -> Result<()> {
        for adaptive in [false, true] {
            for delayed_task in [None, Some(0), Some(1)] {
                for iteration in 0..6 {
                    let gate = SetupGate {
                        delayed_task,
                        ..Default::default()
                    };
                    let activity = Arc::new(AtomicUsize::new(0));
                    let observation = Some((Arc::clone(&activity), gate.release.clone()));
                    let udf = Arc::new(cpu_work(observation));
                    let worker = build_state.map(move |builder| {
                        Ok(builder.with_scalar_functions(vec![udf.clone()]).build())
                    });
                    let ctx = context(worker)
                        .await
                        .with_distributed_dynamic_task_count(adaptive)?
                        .with_distributed_route_task_handler(gate.clone());
                    ctx.register_udf(cpu_work(None));
                    let plan = ctx
                        .sql("SELECT cpu_work(task_index) FROM url_emitter(2, 2, 'scan')")
                        .await?
                        .create_physical_plan()
                        .await?;
                    if delayed_task.is_none() {
                        gate.release.cancel();
                    }
                    let release = gate.clone();
                    let timer = SpawnedTask::spawn(async move {
                        if delayed_task.is_some() {
                            release.entered.cancelled().await;
                            sleep(Duration::from_millis(250)).await;
                            release.release.cancel();
                        }
                    });
                    let start = Instant::now();
                    let mut output = execute_stream(plan, ctx.task_ctx())?;
                    let first = timeout(WAIT, output.try_next()).await.unwrap()?.unwrap();
                    let first_ms = start.elapsed().as_secs_f64() * 1_000.0;
                    let mut batches = vec![first];
                    batches.extend(
                        timeout(WAIT, output.try_collect::<Vec<_>>())
                            .await
                            .unwrap()?,
                    );
                    let total_ms = start.elapsed().as_secs_f64() * 1_000.0;
                    timer.join().await.unwrap();
                    assert_eq!(task_indices(&batches), vec![0, 1]);
                    let observed = activity.load(Ordering::Relaxed);
                    assert!(observed > 0, "heavy task must execute remotely");
                    println!(
                        "adaptive={adaptive},delayed_task={delayed_task:?},iteration={iteration},first_ms={first_ms:.3},total_ms={total_ms:.3},heavy_started_before_release={}",
                        observed == 2
                    );
                }
            }
        }
        Ok(())
    }

    fn cpu_work(observation: Option<(Arc<AtomicUsize>, CancellationToken)>) -> ScalarUDF {
        create_udf(
            "cpu_work",
            vec![DataType::Int64],
            DataType::Int64,
            Volatility::Volatile,
            Arc::new(move |args| {
                let Some((activity, release)) = &observation else {
                    return exec_err!("CPU work must execute on a remote worker");
                };
                let input = args[0].clone().into_array(1)?;
                let input = input.as_any().downcast_ref::<Int64Array>().unwrap();
                for task in input.values() {
                    if *task == 0 {
                        activity.store(1 + usize::from(!release.is_cancelled()), Ordering::Relaxed);
                        let mut value = 1u64;
                        for _ in 0..100_000_000 {
                            value = black_box(value.wrapping_mul(17).wrapping_add(1));
                        }
                    }
                }
                Ok(args[0].clone())
            }),
        )
    }

    async fn context(builder: impl WorkerSessionBuilder + Send + Sync + 'static) -> SessionContext {
        let mut ctx = start_in_memory_context(2, builder).await;
        ctx.set_distributed_desired_task_count_handler(url_emitter_desired_task_count);
        ctx.set_distributed_scale_up_leaf_node_handler(url_emitter_scale_up_leaf_node);
        ctx.set_distributed_user_codec(URLEmitterExtensionCodec);
        ctx.register_udtf("url_emitter", Arc::new(URLEmitterFunction));
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 2;
        ctx
    }

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState> {
        Ok(ctx
            .builder
            .with_distributed_user_codec(URLEmitterExtensionCodec)
            .build())
    }

    fn task_indices(batches: &[RecordBatch]) -> Vec<i64> {
        let mut values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        values
    }

    async fn assert_terminal_metrics(plan: &Arc<dyn ExecutionPlan>) {
        let distributed = plan.downcast_ref::<DistributedExec>().unwrap();
        let reports = timeout(WAIT, distributed.wait_for_metrics())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reports.len(), 2);
    }

    #[derive(Clone, Default)]
    struct ResponseErrorGate {
        release: CancellationToken,
    }

    #[async_trait]
    impl RouteTaskHandler for ResponseErrorGate {
        async fn handle(
            &self,
            event: RouteTaskEvent<'_>,
        ) -> Option<Result<RouteTaskEventResponse>> {
            Some(
                UrlEmitterRouteTaskHandler
                    .handle(event)
                    .await?
                    .map(|mut response| {
                        let release = self.release.clone();
                        let error = stream::once(async move {
                            release.cancelled().await;
                            exec_err!("injected worker response error")
                        });
                        response.worker_to_coordinator_stream =
                            error.chain(response.worker_to_coordinator_stream).boxed();
                        response
                    }),
            )
        }
    }

    #[derive(Clone, Default)]
    struct SetupGate {
        entered: CancellationToken,
        release: CancellationToken,
        cancelled: CancellationToken,
        sampled: CancellationToken,
        nested_coalesces: Arc<AtomicUsize>,
        fail: bool,
        delayed_task: Option<usize>,
        wait_for_sibling: bool,
    }

    #[async_trait]
    impl RouteTaskHandler for SetupGate {
        async fn handle(
            &self,
            event: RouteTaskEvent<'_>,
        ) -> Option<Result<RouteTaskEventResponse>> {
            Some(self.route(event).await)
        }
    }

    impl SetupGate {
        async fn route(&self, event: RouteTaskEvent<'_>) -> Result<RouteTaskEventResponse> {
            event.task_specialized_plan.apply(|node| {
                if let Some(boundary) = node.as_network_boundary() {
                    assert!(matches!(boundary.input_stage(), Stage::Remote(_)));
                }
                if node.is::<NetworkCoalesceExec>() {
                    self.nested_coalesces.fetch_add(1, Ordering::Relaxed);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            if event.task_key.task_number == 0 && self.wait_for_sibling {
                self.entered.cancelled().await;
            }
            if event.task_key.task_number == self.delayed_task.unwrap_or(1) {
                let cancelled = self.cancelled.clone().drop_guard();
                self.entered.cancel();
                self.release.cancelled().await;
                cancelled.disarm();
                if self.fail {
                    return exec_err!("injected setup failure");
                }
            }
            let mut response = UrlEmitterRouteTaskHandler.handle(event).await.unwrap()?;
            let sampled = self.sampled.clone();
            response.worker_to_coordinator_stream = response.worker_to_coordinator_stream
                .inspect_ok(move |message| {
                    if matches!(message, WorkerToCoordinatorMsg::LoadInfo(info) if info.rows_ready > 0) {
                        sampled.cancel();
                    }
                }).boxed();
            Ok(response)
        }
    }
}
