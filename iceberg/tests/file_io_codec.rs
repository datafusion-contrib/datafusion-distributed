mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::common::{Result, exec_err};
    use datafusion::execution::{SessionStateBuilder, TaskContext};
    use datafusion_distributed::DistributedExt;
    use datafusion_distributed_iceberg::{
        IcebergExt, IcebergFileIOCodec, IcebergIntegrationOptions,
    };
    use datafusion_distributed_iceberg_test_tools::{IcebergTestHarness, create_taxi_table};
    use iceberg::io::{FileIO, FileIOBuilder, LocalFsStorageFactory};

    #[tokio::test]
    async fn lazy_workers_decode_custom_file_io_with_query_context() -> Result<()> {
        let hook = Arc::new(QueryFileIOCodec::default());
        let session_hook = Arc::clone(&hook);
        let (_, rows) = IcebergTestHarness::builder()
            .with_session_extension(move |state| install_hook(state, session_hook.clone()))
            .with_table(create_taxi_table().await?)
            .with_table_option("fixture.storage", "original")
            .with_config_option("iceberg.eager_file_planning", false)
            .with_workers(2)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .query("SELECT MAX(vendor_id) AS vendor FROM taxi")
            .await?;

        insta::assert_snapshot!(rows, @r"
        +--------+
        | vendor |
        +--------+
        | 2      |
        +--------+
        ");
        assert!(hook.decodes.load(Ordering::Relaxed) > 0);
        Ok(())
    }

    #[tokio::test]
    async fn eager_workers_decode_custom_file_io_with_query_context() -> Result<()> {
        let hook = Arc::new(QueryFileIOCodec::default());
        let session_hook = Arc::clone(&hook);
        let (_, rows) = IcebergTestHarness::builder()
            .with_session_extension(move |state| install_hook(state, session_hook.clone()))
            .with_table(create_taxi_table().await?)
            .with_table_option("fixture.storage", "original")
            .with_config_option("iceberg.eager_file_planning", true)
            .with_workers(2)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .query("SELECT MAX(vendor_id) AS vendor FROM taxi")
            .await?;

        insta::assert_snapshot!(rows, @r"
        +--------+
        | vendor |
        +--------+
        | 2      |
        +--------+
        ");
        assert!(hook.decodes.load(Ordering::Relaxed) > 0);
        Ok(())
    }

    #[tokio::test]
    async fn file_io_decode_errors_are_propagated() -> Result<()> {
        let hook = Arc::new(QueryFileIOCodec {
            fail_decode: true,
            ..Default::default()
        });
        let harness = IcebergTestHarness::builder()
            .with_session_extension(move |state| install_hook(state, hook.clone()))
            .with_table(create_taxi_table().await?)
            .with_table_option("fixture.storage", "original")
            .with_config_option("iceberg.eager_file_planning", true)
            .build()
            .await?;

        let error = harness
            .roundtrip_plan(harness.scan().await?)
            .expect_err("decode should fail");
        assert!(error.to_string().contains("query storage unavailable"));
        Ok(())
    }

    #[derive(Debug, Default)]
    struct QueryFileIOCodec {
        decodes: AtomicUsize,
        fail_decode: bool,
    }

    #[derive(Debug)]
    struct QueryStorage(String);

    impl IcebergFileIOCodec for QueryFileIOCodec {
        fn encode(&self, file_io: &FileIO) -> Result<Vec<u8>> {
            assert_eq!(file_io.config().props()["fixture.storage"], "original");
            Ok(vec![0, 255, 128, 1])
        }

        fn decode(&self, bytes: &[u8], ctx: &TaskContext) -> Result<FileIO> {
            assert_eq!(bytes, &[0, 255, 128, 1]);
            let storage = ctx
                .session_config()
                .get_extension::<QueryStorage>()
                .expect("worker query context must be available");
            assert_eq!(storage.0, "worker-storage");
            self.decodes.fetch_add(1, Ordering::Relaxed);
            if self.fail_decode {
                return exec_err!("query storage unavailable");
            }
            Ok(FileIOBuilder::new(Arc::new(LocalFsStorageFactory))
                .with_props([("fixture.storage", "original")])
                .build())
        }
    }

    fn install_hook(
        mut state: SessionStateBuilder,
        hook: Arc<QueryFileIOCodec>,
    ) -> SessionStateBuilder {
        state
            .config()
            .get_or_insert_default()
            .set_extension(Arc::new(QueryStorage("worker-storage".into())));
        state.with_iceberg_integration(IcebergIntegrationOptions {
            file_io_codec: Some(hook),
            ..Default::default()
        })
    }
}
