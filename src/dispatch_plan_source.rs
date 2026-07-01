use crate::TaskKey;
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

/// Serializes the stage subplan the coordinator dispatches, instead of the coordinator encoding
/// it with its own codec.
///
/// An embedder registers one via
/// [`crate::DistributedExt::with_distributed_dispatch_plan_source`] when the coordinator's codec
/// cannot represent its plan nodes, or when its serialization needs embedder-side handling the
/// codec extension point cannot express (the shm embedder's UDF definitions, for example). The
/// coordinator hands over `specialized`, the same ready-to-run per-task plan it would encode:
/// task-specialized, with nested stages already converted to `Remote`, so a worker executes the
/// decoded bytes as-is. `task` carries the query id, so a source registered on a session that
/// runs concurrent queries can tell them apart.
///
/// Returning `None` for a task lets the coordinator fall back to encoding the plan itself, so a
/// source that only overrides some stages stays correct.
pub trait DispatchPlanSource: Send + Sync {
    fn dispatch_plan_proto(
        &self,
        task: &TaskKey,
        specialized: &Arc<dyn ExecutionPlan>,
    ) -> Option<Result<Vec<u8>>>;
}

#[derive(Clone)]
pub(crate) struct DispatchPlanSourceExtension(pub(crate) Arc<dyn DispatchPlanSource>);

pub(crate) fn set_distributed_dispatch_plan_source(
    cfg: &mut SessionConfig,
    source: impl DispatchPlanSource + 'static,
) {
    set_distributed_dispatch_plan_source_arc(cfg, Arc::new(source))
}

pub(crate) fn set_distributed_dispatch_plan_source_arc(
    cfg: &mut SessionConfig,
    source: Arc<dyn DispatchPlanSource>,
) {
    cfg.set_extension(Arc::new(DispatchPlanSourceExtension(source)));
}

/// Returns the [`DispatchPlanSource`] registered on this config, if any.
pub fn get_distributed_dispatch_plan_source(
    cfg: &SessionConfig,
) -> Option<Arc<dyn DispatchPlanSource>> {
    cfg.get_extension::<DispatchPlanSourceExtension>()
        .map(|ext| Arc::clone(&ext.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::InProcessChannelResolver;
    use crate::{
        DistributedExt, NetworkBoundaryExt, SessionStateBuilderExt, Stage, WorkerResolver,
    };
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::common::{DataFusionError, Result};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use prost::Message;
    use std::io::Write;
    use std::sync::Mutex;
    use url::Url;

    struct Workers(usize);

    impl WorkerResolver for Workers {
        fn get_urls(&self) -> Result<Vec<Url>> {
            (0..self.0)
                .map(|i| Url::parse(&format!("http://worker-{i}")))
                .collect::<Result<_, _>>()
                .map_err(|err| DataFusionError::External(Box::new(err)))
        }
    }

    type Calls = Arc<Mutex<Vec<(TaskKey, Arc<dyn ExecutionPlan>)>>>;

    /// Records what the coordinator hands over and declines, so the coordinator's own encode
    /// still runs and the query is unaffected by the recording.
    #[derive(Default)]
    struct RecordingSource {
        calls: Calls,
    }

    impl DispatchPlanSource for RecordingSource {
        fn dispatch_plan_proto(
            &self,
            task: &TaskKey,
            specialized: &Arc<dyn ExecutionPlan>,
        ) -> Option<Result<Vec<u8>>> {
            self.calls
                .lock()
                .unwrap()
                .push((*task, Arc::clone(specialized)));
            None
        }
    }

    const QUERY: &str = "SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k";

    async fn distributed_ctx(
        name: &str,
        source: impl DispatchPlanSource + 'static,
    ) -> Result<(SessionContext, std::path::PathBuf)> {
        let path = std::env::temp_dir().join(format!("dfd_{name}_{}.csv", std::process::id()));
        let mut file =
            std::fs::File::create(&path).map_err(|e| DataFusionError::External(Box::new(e)))?;
        writeln!(file, "k,v").unwrap();
        for i in 0..200 {
            writeln!(file, "{},{}", ["a", "b", "c", "d"][i % 4], i).unwrap();
        }
        drop(file);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_distributed_planner()
            .with_distributed_worker_resolver(Workers(4))
            .with_distributed_channel_resolver(InProcessChannelResolver::default())
            .with_distributed_dispatch_plan_source(source)
            .with_distributed_file_scan_config_bytes_per_partition(1)
            .unwrap()
            .build();
        let ctx = SessionContext::from(state);
        ctx.register_csv("t", path.to_str().unwrap(), CsvReadOptions::new())
            .await?;
        Ok((ctx, path))
    }

    /// The contract a serializing source relies on: it is consulted once per dispatched task,
    /// and the plan it is handed is ready to run, with nested stages already converted to
    /// `Remote`.
    #[tokio::test]
    async fn source_is_consulted_with_the_ready_to_run_plan() -> Result<()> {
        let source = RecordingSource::default();
        let calls = Arc::clone(&source.calls);
        let (ctx, path) = distributed_ctx("recording", source).await?;

        let physical = ctx.sql(QUERY).await?.create_physical_plan().await?;
        collect(physical, ctx.task_ctx()).await?;

        let calls = calls.lock().unwrap();
        assert!(
            !calls.is_empty(),
            "the coordinator never consulted the source"
        );
        let mut seen: datafusion::common::HashSet<TaskKey> = Default::default();
        for (task, specialized) in calls.iter() {
            assert!(seen.insert(*task), "consulted twice for {task:?}");
            specialized.apply(|node| {
                if let Some(nb) = node.as_ref().as_network_boundary() {
                    assert!(
                        matches!(nb.input_stage(), Stage::Remote(_)),
                        "{task:?} carries a Local nested stage; the handed-over plan must be \
                         ready to run"
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        std::fs::remove_file(&path).ok();
        Ok(())
    }

    /// Serializes with the same codec the coordinator's fallback uses, so the worker decodes
    /// source-provided bytes exactly as it decodes coordinator-encoded ones.
    struct SerializingSource;

    impl DispatchPlanSource for SerializingSource {
        fn dispatch_plan_proto(
            &self,
            _task: &TaskKey,
            specialized: &Arc<dyn ExecutionPlan>,
        ) -> Option<Result<Vec<u8>>> {
            let codec = crate::DistributedCodec::new_combined_with_user(&SessionConfig::new());
            Some(
                PhysicalPlanNode::try_from_physical_plan(Arc::clone(specialized), &codec)
                    .map(|node| node.encode_to_vec()),
            )
        }
    }

    /// The bytes the source returns are what the workers run.
    #[tokio::test]
    async fn source_provided_bytes_run_the_query() -> Result<()> {
        let (ctx, path) = distributed_ctx("serializing", SerializingSource).await?;
        let got = ctx.sql(QUERY).await?.collect().await?;
        let got = datafusion::arrow::util::pretty::pretty_format_batches(&got)?.to_string();

        let serial = SessionContext::new();
        serial
            .register_csv("t", path.to_str().unwrap(), CsvReadOptions::new())
            .await?;
        let expected = serial.sql(QUERY).await?.collect().await?;
        let expected =
            datafusion::arrow::util::pretty::pretty_format_batches(&expected)?.to_string();

        assert_eq!(got, expected, "source-encoded dispatch != serial");
        std::fs::remove_file(&path).ok();
        Ok(())
    }

    struct FailingSource;

    impl DispatchPlanSource for FailingSource {
        fn dispatch_plan_proto(
            &self,
            _task: &TaskKey,
            _specialized: &Arc<dyn ExecutionPlan>,
        ) -> Option<Result<Vec<u8>>> {
            Some(Err(DataFusionError::Internal(
                "the embedder could not serialize this stage".into(),
            )))
        }
    }

    /// A source failure fails the dispatch instead of falling back to bytes the embedder said it
    /// could not produce.
    #[tokio::test]
    async fn source_error_fails_the_query() -> Result<()> {
        let (ctx, path) = distributed_ctx("failing", FailingSource).await?;
        let result = ctx.sql(QUERY).await?.collect().await;
        let err = result
            .expect_err("a failing source must fail the query")
            .to_string();
        assert!(
            err.contains("could not serialize"),
            "unexpected error: {err}"
        );
        std::fs::remove_file(&path).ok();
        Ok(())
    }
}
