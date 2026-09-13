#[cfg(test)]
mod tests {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::common::{HashMap, Result};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, NetworkBoundaryExt, RouteTaskEvent,
        RouteTaskEventResponse, RouteTaskHandler, assert_snapshot,
        discover_dynamic_filter_consumers, discover_dynamic_filter_producers,
    };
    use itertools::Itertools;
    use std::collections::BTreeSet;
    use std::fmt::Write;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tonic::async_trait;

    #[tokio::test]
    async fn discovers_dynamic_filters_in_sql_plan() -> Result<()> {
        let display = display_query(
            r#"
                    SELECT COUNT(*)
                    FROM (
                        SELECT DISTINCT "RainToday" AS key
                        FROM weather
                    ) build
                    JOIN weather probe ON build.key = probe."RainToday"
                    JOIN (
                        SELECT DISTINCT "RainTomorrow" AS key
                        FROM weather
                    ) other_build ON other_build.key = probe."RainTomorrow"
                    WHERE probe."MinTemp" > 0
                "#,
        )
        .await?;
        assert_snapshot!(display, @r"
        Stage 5
          AggregateExec
            HashJoinExec producers=[1]
              NetworkShuffleExec
              AggregateExec
                NetworkShuffleExec anchors=[1]
        Stage 4
          RepartitionExec
            AggregateExec
              DataSourceExec consumers=[1]
        Stage 3
          RepartitionExec
            HashJoinExec producers=[2]
              NetworkShuffleExec
              AggregateExec
                NetworkShuffleExec anchors=[2]
        Stage 2
          RepartitionExec
            AggregateExec
              DataSourceExec consumers=[2]
        Stage 1
          RepartitionExec
            FilterExec
              DataSourceExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn passes_anchor_through_two_shuffles() -> Result<()> {
        let display = display_query(
            r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) build
                JOIN (
                    SELECT "RainTomorrow" AS key, SUM(n) AS total
                    FROM (
                        SELECT "RainTomorrow", "RainToday", COUNT(*) AS n
                        FROM weather
                        GROUP BY "RainTomorrow", "RainToday"
                    ) grouped
                    GROUP BY "RainTomorrow"
                ) probe ON build.key = probe.key
            "#,
        )
        .await?;
        assert_snapshot!(display, @r"
        Stage 4
          AggregateExec
            HashJoinExec producers=[1]
              AggregateExec
                NetworkShuffleExec
              ProjectionExec
                AggregateExec
                  NetworkShuffleExec anchors=[1]
        Stage 3
          RepartitionExec
            AggregateExec
              ProjectionExec
                AggregateExec
                  NetworkShuffleExec anchors=[1]
        Stage 2
          RepartitionExec
            AggregateExec
              DataSourceExec consumers=[1]
        Stage 1
          RepartitionExec
            AggregateExec
              DataSourceExec
        ");
        Ok(())
    }

    async fn display_query(sql: &str) -> Result<String> {
        let captured_plans = CapturePlans::default();
        let (ctx, _guard, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let ctx = ctx
            .with_distributed_broadcast_joins(false)?
            .with_distributed_route_task_handler(captured_plans.clone());
        {
            let state = ctx.state_ref();
            let mut state = state.write();
            let optimizer = &mut state.config_mut().options_mut().optimizer;
            optimizer.hash_join_single_partition_threshold = 0;
            optimizer.hash_join_single_partition_threshold_rows = 0;
        }
        register_parquet_tables(&ctx).await?;
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        collect(plan, ctx.task_ctx()).await?;
        let captured_plans = captured_plans.0.lock().await;
        display_dynamic_filter_discovery(&captured_plans)
    }

    /// Captures the first task of each stage for displaying purposes.
    #[derive(Clone, Default)]
    struct CapturePlans(Arc<Mutex<HashMap<usize, Arc<dyn ExecutionPlan>>>>);

    #[async_trait]
    impl RouteTaskHandler for CapturePlans {
        async fn handle(
            &self,
            event: RouteTaskEvent<'_>,
        ) -> Option<Result<RouteTaskEventResponse>> {
            if event.task_key.task_number == 0 {
                self.0.lock().await.insert(
                    event.task_key.stage_id,
                    Arc::clone(event.task_specialized_plan),
                );
            }
            None
        }
    }

    /// Map random dynamic filter expression ids to monotonic numbers 1, 2, 3...
    /// for stable snapshots.
    #[derive(Default)]
    struct IdNormalizer(HashMap<u64, usize>);

    impl IdNormalizer {
        fn annotation(&mut self, name: &str, ids: BTreeSet<u64>) -> Option<String> {
            (!ids.is_empty()).then(|| {
                let ids = ids
                    .into_iter()
                    .map(|id| {
                        let next = self.0.len() + 1;
                        self.0.entry(id).or_insert(next).to_string()
                    })
                    .join(", ");
                format!("{name}=[{ids}]")
            })
        }
    }

    struct DynamicFilterIds {
        consumers: BTreeSet<u64>,
        anchors: BTreeSet<u64>,
        producers: BTreeSet<u64>,
    }

    fn dynamic_filter_annotations(
        node: &dyn ExecutionPlan,
        discovered: &DynamicFilterIds,
        normalizer: &mut IdNormalizer,
    ) -> Result<String> {
        let producers = node
            .dynamic_expressions_produced()
            .iter()
            .filter_map(dynamic_filter_id)
            .filter(|id| discovered.producers.contains(id))
            .collect::<BTreeSet<_>>();
        let is_network_boundary = node.is_network_boundary();
        let mut anchors = BTreeSet::new();
        let mut consumers = BTreeSet::new();
        node.apply_expressions(&mut |root| {
            root.apply(|expression| {
                if let Some(id) = dynamic_filter_id(expression) {
                    if is_network_boundary && discovered.anchors.contains(&id) {
                        anchors.insert(id);
                    } else if discovered.consumers.contains(&id) && !producers.contains(&id) {
                        consumers.insert(id);
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;

        let annotations = [
            ("anchors", anchors),
            ("consumers", consumers),
            ("producers", producers),
        ]
        .into_iter()
        .filter_map(|(name, ids)| normalizer.annotation(name, ids))
        .join(" ");
        Ok(if annotations.is_empty() {
            String::new()
        } else {
            format!(" {annotations}")
        })
    }

    fn dynamic_filter_id(expression: &Arc<dyn PhysicalExpr>) -> Option<u64> {
        expression.downcast_ref::<DynamicFilterPhysicalExpr>()?;
        Some(
            expression
                .expression_id()
                .expect("dynamic filters always have an expression ID"),
        )
    }

    fn display_dynamic_filter_discovery(
        plans: &HashMap<usize, Arc<dyn ExecutionPlan>>,
    ) -> Result<String> {
        fn render(
            node: &dyn ExecutionPlan,
            depth: usize,
            discovered: &DynamicFilterIds,
            normalizer: &mut IdNormalizer,
            output: &mut String,
        ) -> Result<()> {
            writeln!(
                output,
                "{}{}{}",
                "  ".repeat(depth),
                node.name(),
                dynamic_filter_annotations(node, discovered, normalizer)?,
            )
            .expect("writing to String cannot fail");
            for child in node.children() {
                render(child.as_ref(), depth + 1, discovered, normalizer, output)?;
            }
            Ok(())
        }

        let mut output = String::new();
        let mut normalizer = IdNormalizer::default();
        for stage_id in plans.keys().sorted().rev() {
            writeln!(output, "Stage {stage_id}").expect("writing to String cannot fail");
            let plan = &plans[stage_id];
            let consumers = discover_dynamic_filter_consumers(plan)?;
            let discovered = DynamicFilterIds {
                consumers: consumers
                    .consumers
                    .into_iter()
                    .map(|consumer| consumer.id)
                    .collect(),
                anchors: consumers
                    .anchors
                    .into_iter()
                    .map(|anchor| anchor.id)
                    .collect(),
                producers: discover_dynamic_filter_producers(plan)?
                    .into_iter()
                    .map(|producer| producer.id)
                    .collect(),
            };
            render(plan.as_ref(), 1, &discovered, &mut normalizer, &mut output)?;
        }
        Ok(output)
    }
}
