#[cfg(all(feature = "integration", test))]
mod tests {
    use std::fs::remove_file;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, DistributedLeafExec, NetworkBoundaryExt,
        RouteTaskEvent, RouteTaskEventResponse, RouteTaskHandler, assert_snapshot,
    };

    const QUERY: &str = "SELECT id, count(*) AS n FROM input GROUP BY id ORDER BY id";

    #[tokio::test]
    async fn static_routing_exposes_complete_stage_before_task_specialization() -> Result<()> {
        let observations = StageObservations::default();
        let ctx = start_configured_in_memory_context(2, DefaultSessionBuilder, |worker| worker)
            .await
            .with_distributed_partial_reduce(false)?
            .with_distributed_dynamic_task_count(false)?
            .with_distributed_route_task_handler(observations.clone());
        let path = register_input(&ctx, vec![1, 1, 1, 2, 2, 3]).await?;
        let plan = ctx.sql(QUERY).await?.create_physical_plan().await?;
        assert!(plan.exists(|node| Ok(node.is_network_boundary()))?);
        assert!(observations.0.lock().unwrap().is_empty());

        let batches = collect(plan, ctx.task_ctx()).await?;
        let results = pretty_format_batches(&batches)?;
        assert_snapshot!(results, @r"
        +----+---+
        | id | n |
        +----+---+
        | 1  | 3 |
        | 2  | 2 |
        | 3  | 1 |
        +----+---+
        ");

        let stages = observations.0.lock().unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(
            stages
                .iter()
                .filter(|stage| stage.unspecialized_leaf)
                .count(),
            1
        );
        assert!(stages.iter().all(|stage| !stage.specialized_leaf));
        assert!(stages.iter().all(|stage| !stage.sampler));
        assert!(
            stages
                .iter()
                .any(|stage| stage.unspecialized_leaf && stage.tasks == 2)
        );
        remove_file(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_routing_exposes_stages_created_during_execution() -> Result<()> {
        let observations = StageObservations::default();
        let ctx = start_configured_in_memory_context(2, DefaultSessionBuilder, |worker| worker)
            .await
            .with_distributed_partial_reduce(false)?
            .with_distributed_dynamic_task_count(true)?
            .with_distributed_dynamic_bytes_per_partition(1)?
            .with_distributed_route_task_handler(observations.clone());
        let path = register_input(&ctx, vec![1, 1, 1, 2, 2, 3]).await?;
        let plan = ctx.sql(QUERY).await?.create_physical_plan().await?;
        assert!(!plan.exists(|node| Ok(node.is_network_boundary()))?);
        assert!(observations.0.lock().unwrap().is_empty());

        let batches = collect(plan, ctx.task_ctx()).await?;
        let results = pretty_format_batches(&batches)?;
        assert_snapshot!(results, @r"
        +----+---+
        | id | n |
        +----+---+
        | 1  | 3 |
        | 2  | 2 |
        | 3  | 1 |
        +----+---+
        ");

        let stages = observations.0.lock().unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(
            stages
                .iter()
                .filter(|stage| stage.unspecialized_leaf)
                .count(),
            1
        );
        assert!(stages.iter().all(|stage| !stage.specialized_leaf));
        assert!(stages.iter().any(|stage| stage.sampler));
        assert!(
            stages
                .iter()
                .any(|stage| stage.unspecialized_leaf && stage.tasks == 2)
        );
        remove_file(path)?;
        Ok(())
    }

    async fn register_input(ctx: &SessionContext, ids: Vec<i32>) -> Result<PathBuf> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int32Array::from(ids))])?;
        register_temp_parquet_table("input", schema, vec![batch], ctx).await
    }

    #[derive(Clone, Default)]
    struct StageObservations(Arc<Mutex<Vec<StageObservation>>>);

    struct StageObservation {
        unspecialized_leaf: bool,
        specialized_leaf: bool,
        sampler: bool,
        tasks: usize,
    }

    #[async_trait]
    impl RouteTaskHandler for StageObservations {
        async fn handle(
            &self,
            event: RouteTaskEvent<'_>,
        ) -> Option<Result<RouteTaskEventResponse>> {
            if event.task_key.task_number == 0 {
                let observation: Result<_> = (|| {
                    Ok(StageObservation {
                        unspecialized_leaf: stage_contains(event.stage_plan, |node| {
                            node.is::<DistributedLeafExec>()
                        })?,
                        specialized_leaf: stage_contains(event.task_specialized_plan, |node| {
                            node.is::<DistributedLeafExec>()
                        })?,
                        sampler: stage_contains(event.stage_plan, |node| {
                            node.name() == "SamplerExec"
                        })?,
                        tasks: event.task_count,
                    })
                })();
                match observation {
                    Ok(observation) => self.0.lock().unwrap().push(observation),
                    Err(error) => return Some(Err(error)),
                }
            }
            None
        }
    }

    fn stage_contains(
        plan: &Arc<dyn ExecutionPlan>,
        predicate: impl Fn(&dyn ExecutionPlan) -> bool,
    ) -> Result<bool> {
        let mut found = false;
        plan.apply(|node| {
            if node.is_network_boundary() {
                return Ok(TreeNodeRecursion::Jump);
            }
            if predicate(node.as_ref()) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(found)
    }
}
