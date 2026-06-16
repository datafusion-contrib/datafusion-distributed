#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::{HashSet, Result, extensions_options, internal_err};
    use datafusion::config::ConfigExtension;
    use datafusion::error::DataFusionError;
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::{DistributedExt, Worker, WorkerQueryContext};
    use std::sync::Arc;
    use std::sync::Mutex;

    const HOOK_LABEL: &str = "worker-session-value";

    extensions_options! {
        pub struct PlanHookOptions {
            pub label: String, default = "".to_string()
            pub fail_in_hook: bool, default = false
        }
    }

    impl ConfigExtension for PlanHookOptions {
        const PREFIX: &'static str = "plan_hook";
    }

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_distributed_option_extension_from_headers::<PlanHookOptions>(&ctx.headers)?
            .build())
    }

    #[tokio::test]
    async fn plan_hooks_receive_session_config_and_run_in_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let hook_calls = Arc::new(Mutex::new(HookCalls::default()));

        let mut ctx = start_configured_in_memory_context(3, build_state, {
            let hook_calls = Arc::clone(&hook_calls);
            move |mut worker| {
                add_first_hook(&mut worker, Arc::clone(&hook_calls));
                add_second_hook(&mut worker, Arc::clone(&hook_calls));
                worker
            }
        })
        .await;

        ctx.set_distributed_option_extension(PlanHookOptions {
            label: HOOK_LABEL.to_string(),
            fail_in_hook: false,
        });

        assert_eq!(
            collect_hook_query(&ctx).await?,
            "+----+\n\
             | id |\n\
             +----+\n\
             | 2  |\n\
             | 3  |\n\
             +----+"
        );

        let hook_calls = hook_calls.lock().unwrap();
        assert!(hook_calls.first > 0);
        assert_eq!(hook_calls.first, hook_calls.second);
        assert!(hook_calls.pending_plan_ids.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn plan_hook_errors_propagate_to_query() -> Result<(), Box<dyn std::error::Error>> {
        let mut ctx = start_configured_in_memory_context(3, build_state, move |mut worker| {
            worker.add_on_plan_hook(move |plan, session_config| {
                let options = plan_hook_options(session_config)?;
                if options.fail_in_hook {
                    return internal_err!("plan hook failed for {}", options.label);
                }

                Ok(plan)
            });

            worker
        })
        .await;

        ctx.set_distributed_option_extension(PlanHookOptions {
            label: HOOK_LABEL.to_string(),
            fail_in_hook: true,
        });

        let err = collect_hook_query(&ctx)
            .await
            .expect_err("plan hook error should propagate to the query");

        let err = err.to_string();
        assert!(
            err.contains("plan hook failed for worker-session-value"),
            "{err}"
        );

        Ok(())
    }

    #[derive(Default)]
    struct HookCalls {
        pending_plan_ids: HashSet<usize>,
        first: usize,
        second: usize,
    }

    fn add_first_hook(worker: &mut Worker, calls: Arc<Mutex<HookCalls>>) {
        worker.add_on_plan_hook(move |plan, session_config| {
            let options = plan_hook_options(session_config)?;
            if options.label != HOOK_LABEL {
                return internal_err!("unexpected plan hook label {}", options.label);
            }

            let mut calls = calls.lock().unwrap();
            calls.pending_plan_ids.insert(plan_identity(&plan));
            calls.first += 1;
            Ok(plan)
        });
    }

    fn add_second_hook(worker: &mut Worker, calls: Arc<Mutex<HookCalls>>) {
        worker.add_on_plan_hook(move |plan, session_config| {
            let options = plan_hook_options(session_config)?;
            if options.label != HOOK_LABEL {
                return internal_err!("unexpected plan hook label {}", options.label);
            }

            let mut calls = calls.lock().unwrap();
            if !calls.pending_plan_ids.remove(&plan_identity(&plan)) {
                return internal_err!("second hook ran before first hook");
            }

            calls.second += 1;
            Ok(plan)
        });
    }

    fn plan_identity(plan: &Arc<dyn ExecutionPlan>) -> usize {
        Arc::as_ptr(plan) as *const () as usize
    }

    async fn collect_hook_query(ctx: &SessionContext) -> Result<String> {
        let _temp_file = register_hook_input_table(ctx).await?;
        let batches = ctx
            .sql("SELECT id FROM hook_input WHERE id > 1 ORDER BY id")
            .await?
            .collect()
            .await?;
        Ok(pretty_format_batches(&batches)?.to_string())
    }

    async fn register_hook_input_table(ctx: &SessionContext) -> Result<std::path::PathBuf> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        register_temp_parquet_table("hook_input", schema, vec![batch], ctx).await
    }

    fn plan_hook_options(session_config: &SessionConfig) -> Result<&PlanHookOptions> {
        let Some(options) = session_config.options().extensions.get::<PlanHookOptions>() else {
            return internal_err!("PlanHookOptions not found in worker SessionConfig");
        };
        Ok(options)
    }
}
