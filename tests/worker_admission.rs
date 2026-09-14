#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use datafusion::common::{Result, exec_datafusion_err};
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::{
        DefaultSessionBuilder, RetryTarget, WorkerAdmissionController, WorkerAdmissionPermit,
        WorkerAdmissionRejection, WorkerAdmissionRequest,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn worker_directed_same_worker_retry_completes_query() -> Result<()> {
        let admission_attempts = Arc::new(AtomicUsize::new(0));
        let controller = RejectFirstAdmission {
            attempts: Arc::clone(&admission_attempts),
        };
        let ctx = start_configured_in_memory_context(2, DefaultSessionBuilder, move |worker| {
            worker.with_admission_controller(controller.clone())
        })
        .await;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let _file =
            register_temp_parquet_table("admission_values", schema, vec![batch], &ctx).await?;

        let batches = ctx
            .sql("SELECT value FROM admission_values WHERE value > 1 ORDER BY value")
            .await?
            .collect()
            .await?;

        assert!(!batches.is_empty());
        assert!(admission_attempts.load(Ordering::SeqCst) >= 2);
        Ok(())
    }

    #[derive(Clone)]
    struct RejectFirstAdmission {
        attempts: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl WorkerAdmissionController for RejectFirstAdmission {
        async fn admit(
            &self,
            _request: &WorkerAdmissionRequest,
        ) -> std::result::Result<WorkerAdmissionPermit, WorkerAdmissionRejection> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(WorkerAdmissionRejection::retry(
                    exec_datafusion_err!("temporarily unavailable"),
                    RetryTarget::SameWorker,
                ));
            }
            Ok(WorkerAdmissionPermit::default())
        }
    }
}
